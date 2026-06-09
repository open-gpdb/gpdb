#include "postgres.h"

#include <unistd.h>

#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "executor/nodeSeqscan.h"
#include "libpq/auth.h"
#include "utils/syscache.h"


PG_MODULE_MAGIC;

#define LOG_FILE_NAME "pg_log/access.log"

void _PG_init(void);


static init_scan_hook_type next_init_scan_hook = NULL;

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
access_log_init_scan_hook(Relation currentRelation)
{
	char		buf[512];
	HeapTuple	tp;
	struct timeval	tv;
	pg_time_t	stamp_time;
	size_t		len;

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;
	pg_strftime(buf, sizeof(buf),
			"%Y-%m-%d %H:%M:%S        %Z,",
			pg_localtime(&stamp_time, log_timezone));

	/* paste milliseconds into place */
	sprintf(buf + 19, ".%06d", (int) (tv.tv_usec));
	/* restore space which has been rewritten by sprintf with \0 */
	buf[19 + 1 + 6] = ' ';

	if (MyProcPort != NULL && MyProcPort->user_name != NULL)
		strlcat(buf, MyProcPort->user_name, sizeof(buf));
	len = strlen(buf);
	snprintf(buf + len, sizeof(buf) - len, ",con%d,", gp_session_id);

	tp = SearchSysCache1(NAMESPACEOID,
					ObjectIdGetDatum(currentRelation->rd_rel->relnamespace));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
		strlcat(buf, NameStr(nsptup->nspname), sizeof(buf));
		strlcat(buf, ".", sizeof(buf));
		ReleaseSysCache(tp);
	}

	strlcat(buf, NameStr(currentRelation->rd_rel->relname), sizeof(buf));
	strlcat(buf, "\n", sizeof(buf));

	write_to_log(buf);

	if (next_init_scan_hook)
		next_init_scan_hook(currentRelation);
}

void
_PG_init(void)
{
	/* Be sure we do initialization only once */
	static bool inited = false;

	if (inited)
		return;

	next_init_scan_hook = init_scan_hook;
	init_scan_hook = access_log_init_scan_hook;

	inited = true;
}
