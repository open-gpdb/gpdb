#include "postgres.h"

#include <sys/file.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbvars.h"
#include "commands/prepare.h"
#include "funcapi.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

#define LOG_FILE_NAME "pg_log/ast.log"

void _PG_init(void);

static const char *const CLASS_CTAS   = "CTAS";
static const char *const CLASS_INSERT = "INSERT";

static bool ast_log_ctas = false;
static bool ast_log_insert = false;

static planner_hook_type next_planner_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

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
	if (appendStr == NULL)
		return;

	if (strpbrk(appendStr, ",\"\n\r") == NULL)
		appendStringInfoString(buffer, appendStr);
	else
	{
		appendStringInfoCharMacro(buffer, '"');

		for (const char *pChar = appendStr; *pChar; pChar++)
		{
			if (*pChar == '"')
				appendStringInfoCharMacro(buffer, *pChar);

			appendStringInfoCharMacro(buffer, *pChar);
		}

		appendStringInfoCharMacro(buffer, '"');
	}
}

static void
append_log_time(StringInfo buffer)
{
	struct timeval	tv;
	pg_time_t		stamp_time;

	/*
	 * 19 ("YYYY-MM-DD HH:MM:SS") + 1 (".") + 6 (microseconds) + 1 ("\0").
	 */
	char			buf[19 + 1 + 6 + 1];

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;

	pg_strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S",
				pg_localtime(&stamp_time, log_timezone));

	sprintf(buf + 19, ".%06d", (int) tv.tv_usec);

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
append_attrs(StringInfo str, Oid classOid, int16 natts)
{
	for (AttrNumber n = 1; n <= natts; n++)
	{
		char *att = get_attname(classOid, n);

		if (n > 1)
			appendStringInfoChar(str, ',');

		if (att == NULL)
			continue;

		appendStringInfoString(str, att);
		pfree(att);
	}
}

static void
append_func_out_args(StringInfo str, HeapTuple htFunc)
{
	Form_pg_proc	func = (Form_pg_proc) GETSTRUCT(htFunc);
	HeapTuple		htType;
	HeapTuple		htRel;
	Form_pg_class	rel;
	Oid				typrelid;

	if (func->prorettype == RECORDOID)
	{
		Oid	   *argtypes;
		char  **argnames;
		char   *argmodes;
		int		numargs;
		bool	printComma = false;

		numargs = get_func_arg_info(htFunc, &argtypes, &argnames, &argmodes);
		pfree(argtypes);

		if (argmodes != NULL && argnames != NULL)
		{
			for (int i = 0; i < numargs; i++)
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
			for (int i = 0; i < numargs; i++)
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

	append_attrs(str, typrelid, rel->relnatts);

	ReleaseSysCache(htRel);
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
			append_attrs(&str, oid, reltup->relnatts);
			appendStringInfoString(&str, "}}");

			ReleaseSysCache(htRel);
		}
	}
	list_free(oids.rel_oids);

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
	list_free(oids.func_oids);

	return str.data;
}

static void
log_audit_record(const char *className, const char *objName,
				 const char *astText)
{
	StringInfoData	buf;

	initStringInfo(&buf);

	append_log_time(&buf);
	appendStringInfo(&buf, ",%d,%s,",
					 gp_session_id, className);

	append_valid_csv(&buf, objName);
	appendStringInfoChar(&buf, ',');
	append_valid_csv(&buf, astText);
	appendStringInfoChar(&buf, '\n');

	write_to_log(buf.data, buf.len);

	pfree(buf.data);
}

static bool
ast_log_active(void)
{
	if (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_UTILITY)
		return false;

	if (IsAbortedTransactionBlockState())
		return false;

	return true;
}

static void
log_query_ast(Query *parse, bool rewrite,
			  const char *className, const char *objName)
{
	static MemoryContext	astLogContext = NULL;
	MemoryContext			oldContext;
	Query					*finalQuery = parse;

	if (astLogContext == NULL)
		astLogContext = AllocSetContextCreate(TopMemoryContext,
											  "ast_log context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(astLogContext);


	if (rewrite)
	{
		List *rewritten = QueryRewrite((Query *) copyObject(parse));

		if (list_length(rewritten) != 1)
			ereport(ERROR,
					(errmsg("ast_log: unexpected rewrite result")));

		finalQuery = (Query *) linitial(rewritten);
	}

	log_audit_record(className, objName, query_to_text(finalQuery));

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(astLogContext);
}

static char *
result_relation_name(Query *parse)
{
	RangeTblEntry *rte;

	rte = rt_fetch(parse->resultRelation, parse->rtable);

	if (rte->rtekind != RTE_RELATION)
		return NULL;

	return quote_qualified_identifier(
				get_namespace_name(get_rel_namespace(rte->relid)),
				get_rel_name(rte->relid));
}

/*
 * Over-logging these harmless false positives is preferable to
 * the extra complexity of ruling them out:
 * 1) INSERT INTO t SELECT 1
 * 2) INSERT ... VALUES (...) RETURNING (SELECT ...)
 */
static bool
insert_has_source(Query *parse)
{
	ListCell   *cell;
	int			rtindex = 0;

	if (parse->hasSubLinks)
		return true;

	foreach(cell, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);

		if (++rtindex == parse->resultRelation)
			continue;

		if (rte->rtekind == RTE_VALUES)
			continue;

		if (rte->rtekind == RTE_RELATION && !rte->inFromCl)
			continue;

		return true;
	}

	return false;
}

static Query *
resolve_ctas_query(Node *node)
{
	Query *query;

	if (node == NULL || !IsA(node, Query))
		return NULL;

	query = (Query *) node;

	if (query->commandType == CMD_UTILITY &&
		query->utilityStmt != NULL &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		ExecuteStmt		   *execStmt = (ExecuteStmt *) query->utilityStmt;
		PreparedStatement  *entry = FetchPreparedStatement(execStmt->name, false);

		if (entry == NULL || entry->plansource == NULL ||
			list_length(entry->plansource->query_list) != 1)
			return NULL;

		return (Query *) linitial(entry->plansource->query_list);
	}

	return query;
}

static void
process_utility_ast(Node *parsetree)
{
	CreateTableAsStmt  *ctas;
	Query			   *query;
	char			   *tableName = NULL;

	if (!IsA(parsetree, CreateTableAsStmt))
		return;

	if (!ast_log_ctas || !ast_log_active())
		return;

	ctas = (CreateTableAsStmt *) parsetree;

	query = resolve_ctas_query(ctas->query);
	if (query == NULL)
		return;

	if (ctas->into != NULL && ctas->into->rel != NULL)
	{
		RangeVar   *rv = ctas->into->rel;
		char	   *schemaName = rv->schemaname;

		if (schemaName == NULL)
			schemaName = get_namespace_name(RangeVarGetCreationNamespace(rv));

		tableName = quote_qualified_identifier(schemaName, rv->relname);
	}

	log_query_ast(query, true, CLASS_CTAS, tableName);

	if (tableName != NULL)
		pfree(tableName);
}

static PlannedStmt *
ast_log_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	if (parse->commandType == CMD_INSERT &&
		ast_log_insert && ast_log_active() &&
		insert_has_source(parse))
	{
		char *objName = result_relation_name(parse);

		log_query_ast(parse, false, CLASS_INSERT, objName);

		if (objName != NULL)
			pfree(objName);
	}

	if (next_planner_hook)
		return (*next_planner_hook) (parse, cursorOptions, boundParams);
	else
		return standard_planner(parse, cursorOptions, boundParams);
}

static void
ast_log_ProcessUtility_hook(Node *parsetree,
							const char *queryString,
							ProcessUtilityContext context,
							ParamListInfo params,
							DestReceiver *dest,
							char *completionTag)
{
	if (next_ProcessUtility_hook)
		(*next_ProcessUtility_hook) (parsetree, queryString, context,
									 params, dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);

	process_utility_ast(parsetree);
}

void
_PG_init(void)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		return;

	DefineCustomBoolVariable(
		"ast_log.ctas",
		"Logs the AST of the underlying query of CREATE TABLE AS SELECT "
		"(also covers SELECT INTO and CREATE MATERIALIZED VIEW).",
		NULL,
		&ast_log_ctas,
		false,
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL);

	DefineCustomBoolVariable(
		"ast_log.insert",
		"Logs the AST of INSERT ... SELECT statements.",
		NULL,
		&ast_log_insert,
		false,
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL);

	EmitWarningsOnPlaceholders("ast_log");

	next_planner_hook = planner_hook;
	planner_hook = ast_log_planner_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = ast_log_ProcessUtility_hook;
}
