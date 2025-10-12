#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "optimizer/plancat.h"

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(create_hypodist);
PG_FUNCTION_INFO_V1(drop_hypodist);

typedef struct HypoDist
{
    Oid table;
    AttrNumber attnum;
} HypoDist;

void _PG_init(void);
void _PG_fini(void);
static HypoDist dist;
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;

static void hypodist_get_relation_info_hook(PlannerInfo *root,
                                            Oid relationObjectId,
                                            bool inhparent,
                                            RelOptInfo *rel);

Datum
create_hypodist(PG_FUNCTION_ARGS)
{
    if (InvalidOid != dist.table) {
        ereport(ERROR, (errmsg("Only one hypothetical distkey is supported at the moment")));
    }
    Name            table_name, column_name;
    StringInfoData  sql;
    int             proc;
    SPITupleTable   *tuptable;
	HeapTuple	    spi_tuple;
	TupleDesc	    spi_tupdesc;
    char            *table_oid, *att_num;

    table_name = PG_GETARG_NAME(0);
    column_name = PG_GETARG_NAME(1);
    initStringInfo(&sql);
    appendStringInfo(&sql, 
        "select '%s'::regclass::oid, attnum from pg_attribute " 
        "where  attrelid = '%s'::regclass::oid and attname = '%s'",
        table_name->data, table_name->data, column_name->data
    );

    SPI_connect();
    SPI_execute(sql.data, true, 0);
    proc = SPI_processed;
    if (proc == 0) 
    {
        ereport(NOTICE, (errmsg("No match found for %s.%s", table_name->data, column_name->data)));
    } else if (proc == 2)
    {
        ereport(NOTICE, (errmsg("The name is ambiguous %s.%s", table_name->data, column_name->data)));
    } else
    {
        tuptable = SPI_tuptable;
	    spi_tupdesc = tuptable->tupdesc;
        spi_tuple = tuptable->vals[0];
        
        table_oid = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
        att_num = SPI_getvalue(spi_tuple, spi_tupdesc, 2);
        dist.table = atoi(table_oid);
        dist.attnum = atoi(att_num);
        ereport(NOTICE, (errmsg("Table %d, Column %d", dist.table, dist.attnum)));
    }
    SPI_finish();

    PG_RETURN_VOID();
}

Datum
drop_hypodist(PG_FUNCTION_ARGS)
{
    dist.table = InvalidOid;
    dist.attnum = 0;
    PG_RETURN_VOID();
}

void 
hypodist_get_relation_info_hook(PlannerInfo *root,
                                Oid relationObjectId,
                                bool inhparent,
                                RelOptInfo *rel)
{
    if (relationObjectId != dist.table /*|| also should ignore any query but explain, TODO*/)
        return;
    Assert(rel->cdbpolicy->nattrs > 0);
    rel->cdbpolicy->nattrs = 1;
    rel->cdbpolicy->attrs[0] = dist.attnum;
}

void
_PG_init(void)
{
    dist.table = InvalidOid;
    dist.attnum = 0;
    prev_get_relation_info_hook = get_relation_info_hook;
    get_relation_info_hook = hypodist_get_relation_info_hook;
}

void
_PG_fini(void)
{
    get_relation_info_hook = prev_get_relation_info_hook;
}