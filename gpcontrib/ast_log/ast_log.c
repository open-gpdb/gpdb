/*------------------------------------------------------------------------------
 * ast_log.c
 *
 * Greenplum extension for logging Query Abstract Syntax Trees (AST).
 * Compatible with Greenplum 6 (PostgreSQL 9.4).
 *------------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "pgtime.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

#define LOG_FILE_NAME "pg_log/ast.log"

#define AST_LOG_CLIENT_LEVEL	NOTICE

void _PG_init(void);

/* Log classes */
#define LOG_AST_CTAS    (1 << 0)
#define LOG_AST_SEL     (1 << 1)
#define LOG_AST_MOD     (1 << 2)

#define LOG_NONE        0
#define LOG_ALL         (LOG_AST_CTAS | LOG_AST_SEL | LOG_AST_MOD)

#define CLASS_AST_CTAS  "AST_CTAS"
#define CLASS_AST_SEL   "AST_SEL"
#define CLASS_AST_MOD   "AST_MOD"

#define CLASS_NONE      "NONE"
#define CLASS_ALL       "ALL"

/* GUC variables */
static char *auditLog = NULL;
static int   auditLogBitmap = LOG_NONE;
static bool  auditLogClient = false;

static planner_hook_type next_planner_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static int64 statementTotal = 0;
static int64 substatementTotal = 0;
static int   nestingLevel = 0;

static bool  astLogInProgress = false;

static MemoryContext astLogContext = NULL;

typedef struct
{
	List *rel_oids;
	List *func_oids;
} QueryOids;

static void
write_to_log(const char *str, size_t len)
{
	size_t	off = 0;
	int		f;

	f = OpenTransientFile(LOG_FILE_NAME, O_WRONLY | O_APPEND | O_CREAT,
						  S_IRUSR | S_IWUSR);
	if (f < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
				errmsg("could not open file " LOG_FILE_NAME ": %m")));
		return;
	}

	while (flock(f, LOCK_EX) != 0 && errno == EINTR)
		;

	while (off < len)
	{
		ssize_t rc = write(f, str + off, len - off);

		if (rc < 0)
		{
			if (errno == EINTR)
				continue;

			ereport(WARNING, (errcode_for_file_access(),
					errmsg("could not write to file " LOG_FILE_NAME ": %m")));
			break;
		}
		off += (size_t) rc;
	}

	CloseTransientFile(f);
}

static void
append_valid_csv(StringInfo buffer, const char *appendStr)
{
	const char *pChar;

	if (appendStr == NULL)
		return;

	if (strstr(appendStr, ",") || strstr(appendStr, "\"") ||
		strstr(appendStr, "\n") || strstr(appendStr, "\r"))
	{
		appendStringInfoCharMacro(buffer, '"');

		for (pChar = appendStr; *pChar; pChar++)
		{
			if (*pChar == '"')
				appendStringInfoCharMacro(buffer, *pChar);

			appendStringInfoCharMacro(buffer, *pChar);
		}

		appendStringInfoCharMacro(buffer, '"');
	}
	else
		appendStringInfoString(buffer, appendStr);
}

static void
append_log_time(StringInfo buffer)
{
	struct timeval	tv;
	pg_time_t		stamp_time;
	char			buf[128];

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;

	pg_strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S        %Z",
				pg_localtime(&stamp_time, log_timezone));

	sprintf(buf + 19, ".%06d", (int) tv.tv_usec);
	buf[19 + 1 + 6] = ' ';

	appendStringInfoString(buffer, buf);
}

static bool
walker_rel_and_func(Node *node, QueryOids *oids)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
		return query_tree_walker((Query *) node, walker_rel_and_func, oids,
								 QTW_EXAMINE_RTES);

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid))
			oids->rel_oids = list_append_unique_oid(oids->rel_oids, rte->relid);

		return false;
	}

	if (IsA(node, Aggref))
		oids->func_oids = list_append_unique_oid(oids->func_oids,
												 ((Aggref *) node)->aggfnoid);

	if (IsA(node, WindowFunc))
		oids->func_oids = list_append_unique_oid(oids->func_oids,
												 ((WindowFunc *) node)->winfnoid);

	if (IsA(node, FuncExpr))
		oids->func_oids = list_append_unique_oid(oids->func_oids,
												 ((FuncExpr *) node)->funcid);

	return expression_tree_walker(node, walker_rel_and_func, oids);
}

static void
append_func_out_args(StringInfo str, HeapTuple htFunc)
{
	Form_pg_proc	func = (Form_pg_proc) GETSTRUCT(htFunc);
	HeapTuple		htType;
	HeapTuple		htRel;
	Form_pg_class	rel;
	Oid				typrelid;
	AttrNumber		n;

	if (func->prorettype == RECORDOID)
	{
		Oid	   *argtypes;
		char  **argnames;
		char   *argmodes;
		int		numargs;
		int		i;
		bool	printComma = false;

		numargs = get_func_arg_info(htFunc, &argtypes, &argnames, &argmodes);
		pfree(argtypes);

		if (argmodes != NULL && argnames != NULL)
		{
			for (i = 0; i < numargs; i++)
			{
				if (argmodes[i] != PROARGMODE_OUT &&
					argmodes[i] != PROARGMODE_TABLE)
					continue;

				if (printComma)
					appendStringInfoChar(str, ',');

				if (argnames[i] != NULL)
					appendStringInfoString(str, argnames[i]);

				printComma = true;
			}
		}

		if (argnames != NULL)
		{
			for (i = 0; i < numargs; i++)
				pfree(argnames[i]);
			pfree(argnames);
		}

		if (argmodes != NULL)
			pfree(argmodes);

		return;
	}

	htType = SearchSysCache1(TYPEOID, ObjectIdGetDatum(func->prorettype));
	if (!HeapTupleIsValid(htType))
		return;

	typrelid = ((Form_pg_type) GETSTRUCT(htType))->typrelid;
	ReleaseSysCache(htType);

	if (!OidIsValid(typrelid))
		return;

	htRel = SearchSysCache1(RELOID, ObjectIdGetDatum(typrelid));
	if (!HeapTupleIsValid(htRel))
		return;

	rel = (Form_pg_class) GETSTRUCT(htRel);

	for (n = 1; n <= rel->relnatts; n++)
	{
		char *att = get_attname(typrelid, n);

		if (n > 1)
			appendStringInfoChar(str, ',');

		if (att != NULL)
		{
			appendStringInfoString(str, att);
			pfree(att);
		}
	}

	ReleaseSysCache(htRel);
}

static void
append_rel_attrs(StringInfo str, Oid relOid, int16 natts)
{
	AttrNumber	n;

	for (n = 1; n <= natts; n++)
	{
		char *att = get_attname(relOid, n);

		if (n > 1)
			appendStringInfoChar(str, ',');

		if (att == NULL)
			continue;

		appendStringInfoString(str, att);
		pfree(att);
	}
}

static void
append_name(StringInfo str, char type, Oid oid, Oid namespaceOid, Name name)
{
	char *strNS = get_namespace_name(namespaceOid);

	appendStringInfo(str, "{%c %u ", type, oid);

	if (strNS != NULL)
	{
		appendStringInfo(str, "%s.", strNS);
		pfree(strNS);
	}

	appendStringInfo(str, "%s {", NameStr(*name));
}

static char *
query_to_text(Query *parse)
{
	ListCell	   *cell;
	StringInfoData	str;
	QueryOids		oids = {NIL, NIL};
	char		   *parseStr = nodeToString(parse);

	query_tree_walker(parse, walker_rel_and_func, &oids, QTW_EXAMINE_RTES);

	if (oids.rel_oids == NIL && oids.func_oids == NIL)
		return parseStr;

	initStringInfo(&str);
	appendStringInfoString(&str, parseStr);
	pfree(parseStr);

	foreach(cell, oids.rel_oids)
	{
		Oid			oid = lfirst_oid(cell);
		HeapTuple	htRel = SearchSysCache1(RELOID, ObjectIdGetDatum(oid));

		if (HeapTupleIsValid(htRel))
		{
			Form_pg_class reltup = (Form_pg_class) GETSTRUCT(htRel);

			append_name(&str, 'r', oid, reltup->relnamespace, &reltup->relname);
			append_rel_attrs(&str, oid, reltup->relnatts);
			appendStringInfoString(&str, "}}");

			ReleaseSysCache(htRel);
		}
	}

	foreach(cell, oids.func_oids)
	{
		Oid			oid = lfirst_oid(cell);
		HeapTuple	htFunc = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));

		if (HeapTupleIsValid(htFunc))
		{
			Form_pg_proc func = (Form_pg_proc) GETSTRUCT(htFunc);

			append_name(&str, 'f', oid, func->pronamespace, &func->proname);

			if (func->proretset)
				append_func_out_args(&str, htFunc);

			appendStringInfoString(&str, "}}");

			ReleaseSysCache(htFunc);
		}
	}

	list_free(oids.rel_oids);
	list_free(oids.func_oids);

	return str.data;
}

static void
log_audit_record(int64 statementId, int64 subStatementId,
				 const char *className, const char *objName,
				 const char *astText)
{
	StringInfoData	buf;

	initStringInfo(&buf);

	append_log_time(&buf);
	appendStringInfo(&buf, ",con%d," INT64_FORMAT "," INT64_FORMAT ",%s,",
					 gp_session_id, statementId, subStatementId, className);

	append_valid_csv(&buf, objName);
	appendStringInfoChar(&buf, ',');
	append_valid_csv(&buf, astText);
	appendStringInfoChar(&buf, '\n');

	write_to_log(buf.data, buf.len);

	if (auditLogClient)
	{
		buf.data[buf.len - 1] = '\0';
		ereport(AST_LOG_CLIENT_LEVEL,
				(errmsg("AUDIT: %s", buf.data),
				 errhidestmt(true),
				 errhidecontext(true)));
	}

	pfree(buf.data);
}

static bool
ast_log_enabled(int logClass)
{
	if (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_UTILITY)
		return false;

	if (astLogInProgress || IsAbortedTransactionBlockState())
		return false;

	return (auditLogBitmap & logClass) != 0;
}

static void
log_query_ast(Query *parse, bool rewrite,
			  int64 statementId, int64 subStatementId,
			  const char *className, const char *objName)
{
	MemoryContext	oldContext;

	if (astLogContext == NULL)
		astLogContext = AllocSetContextCreate(TopMemoryContext,
											  "ast_log context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(astLogContext);
	astLogInProgress = true;

	PG_TRY();
	{
		Query *finalQuery = parse;

		if (rewrite)
		{
			List *rewritten = QueryRewrite((Query *) copyObject(parse));

			if (list_length(rewritten) != 1)
				ereport(ERROR,
						(errmsg("ast_log: unexpected rewrite result")));

			finalQuery = (Query *) linitial(rewritten);
		}

		log_audit_record(statementId, subStatementId, className, objName,
						 query_to_text(finalQuery));
	}
	PG_CATCH();
	{
		astLogInProgress = false;
		MemoryContextSwitchTo(oldContext);
		MemoryContextReset(astLogContext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	astLogInProgress = false;
	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(astLogContext);
}

static char *
first_relation_name(Query *parse)
{
	ListCell *cell;

	foreach(cell, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);

		if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid))
			return quote_qualified_identifier(
						get_namespace_name(get_rel_namespace(rte->relid)),
						get_rel_name(rte->relid));
	}

	return NULL;
}

static char *
result_relation_name(Query *parse)
{
	RangeTblEntry *rte;

	if (parse->resultRelation <= 0 ||
		parse->resultRelation > list_length(parse->rtable))
		return NULL;

	rte = rt_fetch(parse->resultRelation, parse->rtable);

	if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
		return NULL;

	return quote_qualified_identifier(
				get_namespace_name(get_rel_namespace(rte->relid)),
				get_rel_name(rte->relid));
}

static void
process_utility_ast(Node *parsetree, int64 statementId, int64 subStatementId)
{
	CreateTableAsStmt  *ctas;
	char			   *tableName = NULL;

	if (!IsA(parsetree, CreateTableAsStmt))
		return;

	if (!ast_log_enabled(LOG_AST_CTAS))
		return;

	ctas = (CreateTableAsStmt *) parsetree;

	if (ctas->query == NULL || !IsA(ctas->query, Query))
		return;

	if (ctas->into != NULL && ctas->into->rel != NULL)
	{
		RangeVar   *rv = ctas->into->rel;
		char	   *schemaName = rv->schemaname;

		if (schemaName == NULL)
			schemaName = get_namespace_name(RangeVarGetCreationNamespace(rv));

		tableName = quote_qualified_identifier(schemaName, rv->relname);
	}

	log_query_ast((Query *) ctas->query, true, statementId, subStatementId,
				  CLASS_AST_CTAS, tableName);

	if (tableName != NULL)
		pfree(tableName);
}

static PlannedStmt *
ast_log_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt	   *result;
	const char	   *className = NULL;
	int				logClass = 0;
	int64			statementId;
	int64			subStatementId;

	if (nestingLevel == 0)
	{
		statementTotal++;
		substatementTotal = 0;
	}
	substatementTotal++;

	statementId = statementTotal;
	subStatementId = substatementTotal;

	if (parse->parentStmtType != PARENTSTMTTYPE_CTAS)
	{
		switch (parse->commandType)
		{
			case CMD_SELECT:
				logClass = LOG_AST_SEL;
				className = CLASS_AST_SEL;
				break;

			case CMD_INSERT:
			case CMD_UPDATE:
			case CMD_DELETE:
				logClass = LOG_AST_MOD;
				className = CLASS_AST_MOD;
				break;

			default:
				break;
		}
	}

	if (className != NULL && ast_log_enabled(logClass))
	{
		char *objName = (parse->commandType == CMD_SELECT)
			? first_relation_name(parse)
			: result_relation_name(parse);

		log_query_ast(parse, false, statementId, subStatementId,
					  className, objName);

		if (objName != NULL)
			pfree(objName);
	}

	nestingLevel++;
	PG_TRY();
	{
		if (next_planner_hook)
			result = (*next_planner_hook) (parse, cursorOptions, boundParams);
		else
			result = standard_planner(parse, cursorOptions, boundParams);
	}
	PG_CATCH();
	{
		nestingLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
	nestingLevel--;

	return result;
}

static void
ast_log_ProcessUtility_hook(Node *parsetree,
							const char *queryString,
							ProcessUtilityContext context,
							ParamListInfo params,
							DestReceiver *dest,
							char *completionTag)
{
	int64	statementId;
	int64	subStatementId;

	if (nestingLevel == 0)
	{
		statementTotal++;
		substatementTotal = 0;
	}
	substatementTotal++;

	statementId = statementTotal;
	subStatementId = substatementTotal;

	nestingLevel++;
	PG_TRY();
	{
		if (next_ProcessUtility_hook)
			(*next_ProcessUtility_hook) (parsetree, queryString, context,
										 params, dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString, context,
									params, dest, completionTag);
	}
	PG_CATCH();
	{
		nestingLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
	nestingLevel--;

	process_utility_ast(parsetree, statementId, subStatementId);
}

static bool
check_audit_log(char **newVal, void **extra, GucSource source)
{
	List	   *flagRawList = NIL;
	ListCell   *lt;
	char	   *rawVal;
	int		   *flags;
	int			result = LOG_NONE;

	rawVal = pstrdup(*newVal);

	if (!SplitIdentifierString(rawVal, ',', &flagRawList))
	{
		GUC_check_errdetail("List syntax is invalid.");
		list_free(flagRawList);
		pfree(rawVal);
		return false;
	}

	foreach(lt, flagRawList)
	{
		char   *token = (char *) lfirst(lt);
		bool	subtract = false;
		int		class;

		if (token[0] == '-')
		{
			token++;
			subtract = true;
		}

		if (pg_strcasecmp(token, CLASS_NONE) == 0)
			class = LOG_NONE;
		else if (pg_strcasecmp(token, CLASS_ALL) == 0)
			class = LOG_ALL;
		else if (pg_strcasecmp(token, CLASS_AST_CTAS) == 0)
			class = LOG_AST_CTAS;
		else if (pg_strcasecmp(token, CLASS_AST_SEL) == 0)
			class = LOG_AST_SEL;
		else if (pg_strcasecmp(token, CLASS_AST_MOD) == 0)
			class = LOG_AST_MOD;
		else
		{
			GUC_check_errdetail("Unrecognized class: \"%s\".", token);
			list_free(flagRawList);
			pfree(rawVal);
			return false;
		}

		if (subtract)
			result &= ~class;
		else
			result |= class;
	}

	list_free(flagRawList);
	pfree(rawVal);

	if (!(flags = (int *) malloc(sizeof(int))))
	{
		GUC_check_errdetail("Out of memory.");
		return false;
	}

	*flags = result;
	*extra = flags;

	return true;
}

static void
assign_audit_log(const char *newVal, void *extra)
{
	if (extra)
		auditLogBitmap = *(int *) extra;
}

void
_PG_init(void)
{
	static bool inited = false;

	if (inited)
		return;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("ast_log must be loaded via shared_preload_libraries")));

	DefineCustomStringVariable(
		"ast_log.log",

		"Specifies which classes of statements will be logged: ast_sel, "
		"ast_mod, ast_ctas. Multiple classes can be provided using a "
		"comma-separated list and classes can be subtracted by prefacing the "
		"class with a - sign.",

		NULL,
		&auditLog,
		"none",
		PGC_SUSET,
		GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE,
		check_audit_log,
		assign_audit_log,
		NULL);

	DefineCustomBoolVariable(
		"ast_log.log_client",

		"Specifies whether AST records should also be sent to the client as "
		"a notice. Intended for regression testing and debugging.",

		NULL,
		&auditLogClient,
		false,
		PGC_SUSET,
		GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);

	EmitWarningsOnPlaceholders("ast_log");

	next_planner_hook = planner_hook;
	planner_hook = ast_log_planner_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = ast_log_ProcessUtility_hook;

#ifndef EXEC_BACKEND
	ereport(LOG, (errmsg("ast_log extension initialized")));
#else
	ereport(DEBUG1, (errmsg("ast_log extension initialized")));
#endif

	inited = true;
}
