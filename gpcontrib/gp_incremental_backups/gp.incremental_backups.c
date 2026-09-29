#include "postgres.h"

/* c.h / GpIdentity */
#include "c.h"

/* access */
#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "access/xact.h"
#include "access/xlog.h"

/* catalog */
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"

/* cdb */
#include "cdb/cdbvars.h"

/* commands / executor / tcop */
#include "commands/extension.h"
#include "executor/spi.h"
#include "tcop/utility.h"

/* common / nodes */
#include "common/relpath.h"
#include "nodes/primnodes.h"

/* storage */
#include "storage/lmgr.h"

/* utils */
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM < 10000
#include "utils/tqual.h"
#endif

#include "fmgr.h"
#include "funcapi.h"
#include "pgstat.h"

/* yezzey local headers */
#include "yezzey.h"

#include "binary_upgrade.h"
#include "offload.h"
#include "offload_policy.h"
#include "offload_tablespace_map.h"
#include "partition.h"
#include "relfilelocator.h"
#include "storage.h"
#include "util.h"
#include "virtual_index.h"
#include "virtual_tablespace.h"
#include "xvacuum.h"

static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

static void
gp_incremental_backups__ProcessUtility_hook(PlannedStmt *pstmt, const char *queryString,
                           bool readOnlyTree, ProcessUtilityContext context,
                           ParamListInfo params, QueryEnvironment *queryEnv,
                           DestReceiver *dest, QueryCompletion *qc)
{
    // get parsetree
    Node *parsetree;
    if (pstmt->utilityStmt) {
        parsetree = pstmt->utilityStmt;
    } else {
        /*  when?  */
        return prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context,
                                        params, queryEnv, dest, qc);
    }

    // process truncate and vacuum
    Oid filenode;
    switch (nodeTag(parsetree)) {
    case T_VacuumStmt:
    {
        VacuumStmt *stmt = (VacuumStmt *)parsetree;
        if (!stmt->relation) {
            filenode = 0;
        break;
        }
        //TODO lock type
        Relation rel = relation_openrv(stmt->relation, AccessShareLock);
        filenode = rel->rd_node.relNode;
        relation_close(rel, AccessShareLock);
    }
    break;
    case T_TruncateStmt:
    {
        TruncateStmt *stmt = (TruncateStmt *)parsetree;
        if (!stmt->relation) {
            // possible?
            filenode = 0;
        break;
        }
        //TODO lock type
        Relation rel = relation_openrv(stmt->relation, AccessShareLock);
        filenode = rel->rd_node.relNode;
        relation_close(rel, AccessShareLock);
        break;
    }
        break;
    default:
        break;
    }

        prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params,
                            queryEnv, dest, qc);

        // TODO write data to vacuum table
          RangeVar *possible_vacuums = makeRangeVar("gp_incremental_backups", "possible_vacuum_list");
        Relation pv = relation_openrv(possible_vacuums, RowExclusiveLock);
        
        relation_close(pv, RowExclusiveLock);
    }
}


void _PG_init(void) {
    prev_ProcessUtility_hook =
    ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
    ProcessUtility_hook = gp_incremental_backups__ProcessUtility_hook;
}
