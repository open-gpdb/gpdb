#include "postgres.h"

#include <unistd.h>

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "libpq/libpq-be.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

#define LOG_FILE_NAME "pg_log/view.log"

void _PG_init(void);

static post_parse_analyze_hook_type next_post_parse_analyze_hook = NULL;

static void log_views_from_rtable(Query *query);

static void
write_to_log(const char* str)
{
    int f = OpenTransientFile(LOG_FILE_NAME, O_WRONLY | O_APPEND | O_CREAT,
                              S_IRUSR | S_IWUSR);
    if (f < 0)
    {
        ereport(WARNING, (errcode_for_file_access(),
                errmsg("could not open file " LOG_FILE_NAME ": %m")));
        return;
    }
    write(f, str, strlen(str));
    CloseTransientFile(f);
}

static bool
find_sublink_walker(Node *node)
{
    if (node == NULL)
        return false;

    if (IsA(node, SubLink))
    {
        SubLink *sublink = (SubLink *) node;
        log_views_from_rtable((Query *) sublink->subselect);
        return false;
    }

    return expression_tree_walker(node, find_sublink_walker, NULL);
}

static void
log_views_from_rtable(Query *query)
{
    ListCell   *lc;

    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

        if (rte->rtekind == RTE_SUBQUERY)
        {
            log_views_from_rtable(rte->subquery);
            continue;
        }

        if (rte->rtekind != RTE_RELATION)
            continue;

        HeapTuple       tp;
        Form_pg_class   classtuple;

        tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));
        classtuple = (Form_pg_class) GETSTRUCT(tp);

        if (classtuple->relkind == RELKIND_VIEW)
        {
            char        buf[256];
            struct timeval  tv;
            pg_time_t   stamp_time;
            size_t      len;
            HeapTuple   nspTuple;
            char       *nspName;

            gettimeofday(&tv, NULL);
            stamp_time = (pg_time_t) tv.tv_sec;
            pg_strftime(buf, sizeof(buf),
                    "%Y-%m-%d %H:%M:%S        %Z,",
                    pg_localtime(&stamp_time, log_timezone));

            sprintf(buf + 19, ".%06d", (int) (tv.tv_usec));
            buf[19 + 1 + 6] = ' ';

            strlcat(buf, MyProcPort->user_name, sizeof(buf));

            len = strlen(buf);
            snprintf(buf + len, sizeof(buf) - len, ",con%d,", gp_session_id);

            nspTuple = SearchSysCache1(NAMESPACEOID,
                    ObjectIdGetDatum(classtuple->relnamespace));
            if (HeapTupleIsValid(nspTuple))
            {
                nspName = NameStr(((Form_pg_namespace) GETSTRUCT(nspTuple))->nspname);
                ReleaseSysCache(nspTuple);
            }

            strlcat(buf, nspName, sizeof(buf));
            strlcat(buf, ".", sizeof(buf));
            strlcat(buf, NameStr(classtuple->relname), sizeof(buf));
            strlcat(buf, "\n", sizeof(buf));

            write_to_log(buf);
        }
        ReleaseSysCache(tp);
    }
    if (query->targetList)
        find_sublink_walker((Node *) query->targetList);
    if (query->jointree)
        find_sublink_walker((Node *) query->jointree->quals);
    if (query->havingQual)
        find_sublink_walker((Node *) query->havingQual);
}

static void
view_log_post_parse_analyze_hook(ParseState *pstate, Query *query)
{
    log_views_from_rtable(query);

    if (next_post_parse_analyze_hook)
        (*next_post_parse_analyze_hook)(pstate, query);
}

void
_PG_init(void)
{
    if (Gp_role == GP_ROLE_EXECUTE)
        return;

    next_post_parse_analyze_hook = post_parse_analyze_hook;
    post_parse_analyze_hook = view_log_post_parse_analyze_hook;
}
