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
    int         f;
    size_t      len = strlen(str);
    const char *ptr = str;

    f = OpenTransientFile(LOG_FILE_NAME, O_WRONLY | O_APPEND | O_CREAT,
                          S_IRUSR | S_IWUSR);
    if (f < 0)
    {
        ereport(WARNING, (errcode_for_file_access(),
                errmsg("could not open file %s: %m", LOG_FILE_NAME)));
        return;
    }

    while (len > 0)
    {
        ssize_t written = write(f, ptr, len);

        if (written <= 0)
        {
            if (errno == EINTR)
                continue;
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not write to file %s: %m",
                           LOG_FILE_NAME)));
            CloseTransientFile(f);
            return;
        }
        ptr    += written;
        len    -= written;
    }

    CloseTransientFile(f);
}

static bool
find_sublink_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Query))
    {
        log_views_from_rtable((Query *) node);
        return false;
    }

    return expression_tree_walker(node, find_sublink_walker, context);
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

        if (rte->rtekind == RTE_RELATION)
        {
            HeapTuple       tp;
            Form_pg_class   classtuple;

            tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));
            if (!HeapTupleIsValid(tp))
                continue;

            classtuple = (Form_pg_class) GETSTRUCT(tp);

            if (classtuple->relkind == RELKIND_VIEW)
            {
                /*
                 * Log line format:
                 *
                 *   YYYY-MM-DD HH:MM:SS.ffffff TZ \t user \t con<session_id> \t schema.viewname \n
                 *
                 * The TAB separator avoids conflicts with view names that contain commas.
                 */
                char        buf[256];
                struct timeval  tv;
                pg_time_t   stamp_time;
                size_t      len;
                HeapTuple   nspTuple;
                char        nspNameBuf[NAMEDATALEN];
                const char *nspName;

                gettimeofday(&tv, NULL);
                stamp_time = (pg_time_t) tv.tv_sec;
                pg_strftime(buf, sizeof(buf),
                        "%Y-%m-%d %H:%M:%S        %Z\t",
                        pg_localtime(&stamp_time, log_timezone));

                sprintf(buf + 19, ".%06d", (int) (tv.tv_usec));
                buf[19 + 1 + 6] = ' ';

                strlcat(buf, MyProcPort->user_name, sizeof(buf));

                len = strlen(buf);
                snprintf(buf + len, sizeof(buf) - len, "\tcon%d\t", gp_session_id);

                nspTuple = SearchSysCache1(NAMESPACEOID,
                        ObjectIdGetDatum(classtuple->relnamespace));
                if (HeapTupleIsValid(nspTuple))
                {
                    strlcpy(nspNameBuf,
                            NameStr(((Form_pg_namespace) GETSTRUCT(nspTuple))->nspname),
                            sizeof(nspNameBuf));
                    ReleaseSysCache(nspTuple);
                    nspName = nspNameBuf;
                }
                else
                    nspName = "unknown";

                strlcat(buf, nspName, sizeof(buf));
                strlcat(buf, ".", sizeof(buf));
                strlcat(buf, NameStr(classtuple->relname), sizeof(buf));
                strlcat(buf, "\n", sizeof(buf));

                write_to_log(buf);
            }
            ReleaseSysCache(tp);
            continue;
        }

        if (rte->rtekind == RTE_FUNCTION || rte->rtekind == RTE_TABLEFUNCTION)
            find_sublink_walker((Node *) rte->funcexpr, NULL);
        else if (rte->rtekind == RTE_VALUES)
            find_sublink_walker((Node *) rte->values_lists, NULL);
    }

    find_sublink_walker((Node *) query->targetList, NULL);
    if (query->jointree)
        find_sublink_walker((Node *) query->jointree, NULL);
    find_sublink_walker((Node *) query->havingQual, NULL);
    find_sublink_walker((Node *) query->limitCount, NULL);
    find_sublink_walker((Node *) query->limitOffset, NULL);

    foreach(lc, query->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        if (cte->ctequery && IsA(cte->ctequery, Query))
            log_views_from_rtable((Query *) cte->ctequery);
    }
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
