#include "postgres.h"

#include <unistd.h>

#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "libpq/auth.h"
#include "lib/stringinfo.h"
#include "utils/rel.h"
#include "utils/syscache.h"


PG_MODULE_MAGIC;

#define LOG_FILE_NAME "pg_log/access.log"

void _PG_init(void);


static init_scan_hook_type next_init_scan_hook = NULL;
static ExecutorStart_hook_type next_ExecutorStart_hook = NULL;

static void
write_to_log(const char* str)
{
	/*
	 * Synchronization is not necessary because `man write` says:
	 * "If the file was open(2)ed with O_APPEND, the file offset is first set to
	 * the end of the file before writing. The adjustment of the file offset and
	 * the write operation are performed as an atomic step."
	 */
	int f = OpenTransientFile(LOG_FILE_NAME, O_WRONLY | O_APPEND | O_CREAT,
							  S_IRUSR | S_IWUSR);
	if (f < 0 )
	{
		ereport(WARNING, (errcode_for_file_access(),
				errmsg("could not open file " LOG_FILE_NAME ": %m")));
		return;
	}
	write(f, str, strlen(str));
	CloseTransientFile(f);
}

static void
append_log_time(StringInfo buffer)
{
	struct timeval	tv;
	pg_time_t		stamp_time;

	/*
	 * 19 ("YYYY-MM-DD HH:MM:SS")
	 * + 1 (".")
	 * + 6 (microseconds)
	 * + 1 (" ")
	 * + 5 (time zone abbreviation)
	 * + 1 ("\0").
	 */
	char			buf[19 + 1 + 6 + 1 + 5 + 1];

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;

	pg_strftime(buf, sizeof(buf),
				"%Y-%m-%d %H:%M:%S",
				pg_localtime(&stamp_time, log_timezone));

	sprintf(buf + 19, ".%06d", (int) tv.tv_usec);

	appendStringInfoString(buffer, buf);

	pg_strftime(buf, sizeof(buf),
				" %Z",
				pg_localtime(&stamp_time, log_timezone));

	appendStringInfoString(buffer, buf);
}

static void
append_relation_name(StringInfo buf, Relation currentRelation)
{
	HeapTuple	tp;

	tp = SearchSysCache1(NAMESPACEOID,
					ObjectIdGetDatum(currentRelation->rd_rel->relnamespace));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);

		appendStringInfoString(buf, NameStr(nsptup->nspname));
		appendStringInfoChar(buf, '.');

		ReleaseSysCache(tp);
	}

	appendStringInfoString(buf,
						   NameStr(currentRelation->rd_rel->relname));
}

static void
append_access_record(StringInfo buf,
					 Relation currentRelation,
					 const char *operation)
{
	append_log_time(buf);
	appendStringInfoChar(buf, ',');

	if (MyProcPort != NULL && MyProcPort->user_name != NULL)
		appendStringInfoString(buf, MyProcPort->user_name);

	appendStringInfo(buf, ",con%d,", gp_session_id);

	append_relation_name(buf, currentRelation);

	appendStringInfo(buf, ",%s\n", operation);
}

static void
access_log_write_relation(Relation currentRelation)
{
	StringInfoData	buf;

	if (currentRelation == NULL || currentRelation->rd_rel == NULL)
		return;

	if (currentRelation->rd_rel->relkind != RELKIND_RELATION)
		return;

	initStringInfo(&buf);
	append_access_record(&buf, currentRelation, "write");

	write_to_log(buf.data);

	pfree(buf.data);
}

static void
access_log_init_scan_hook(Relation currentRelation)
{
	StringInfoData	buf;

	initStringInfo(&buf);
	append_access_record(&buf, currentRelation, "read");

	write_to_log(buf.data);

	pfree(buf.data);

	if (next_init_scan_hook)
		next_init_scan_hook(currentRelation);
}

static bool
access_log_relation_seen(Relation *relations, int count, Relation relation)
{
	int i;

	for (i = 0; i < count; i++)
	{
		if (RelationGetRelid(relations[i]) == RelationGetRelid(relation))
			return true;
	}

	return false;
}

static void
access_log_dml(QueryDesc *queryDesc)
{
	EState		   *estate;
	ResultRelInfo *resultRelInfo;
	Relation	   *loggedRelations;
	int				numResultRelations;
	int				numLoggedRelations = 0;
	int				i;

	if (queryDesc == NULL)
		return;

	switch (queryDesc->operation)
	{
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
			break;

		default:
			return;
	}

	estate = queryDesc->estate;
	if (estate == NULL)
		return;

	resultRelInfo = estate->es_result_relations;
	numResultRelations = estate->es_num_result_relations;

	if (resultRelInfo == NULL || numResultRelations <= 0)
		return;

	loggedRelations =
		(Relation *) palloc(sizeof(Relation) * numResultRelations);

	for (i = 0; i < numResultRelations; i++)
	{
		Relation currentRelation = resultRelInfo[i].ri_RelationDesc;

		if (currentRelation == NULL || currentRelation->rd_rel == NULL)
			continue;

		if (currentRelation->rd_rel->relkind != RELKIND_RELATION)
			continue;

		if (access_log_relation_seen(loggedRelations,
									 numLoggedRelations,
									 currentRelation))
			continue;

		loggedRelations[numLoggedRelations++] = currentRelation;
		access_log_write_relation(currentRelation);
	}

	pfree(loggedRelations);
}

static void
access_log_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (next_ExecutorStart_hook)
		next_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	access_log_dml(queryDesc);
}

void
_PG_init(void)
{
	next_init_scan_hook = init_scan_hook;
	init_scan_hook = access_log_init_scan_hook;

	next_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = access_log_ExecutorStart;
}
