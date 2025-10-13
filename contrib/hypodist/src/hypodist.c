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
static HypoDist dist[3]; // it's a hack, but no more than 3 right now
static int hypodist_cnt = 0;
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;

static void hypodist_get_relation_info_hook(PlannerInfo *root,
                                            Oid relationObjectId,
                                            bool inhparent,
                                            RelOptInfo *rel);

Datum
create_hypodist(PG_FUNCTION_ARGS)
{
    if (hypodist_cnt == 3) 
    {
        ereport(ERROR, (errmsg("Only three hypothetical distkeys are supported at the moment")));
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
        dist[hypodist_cnt].table = atoi(table_oid);
        dist[hypodist_cnt].attnum = atoi(att_num);
        ereport(NOTICE, (errmsg("Table %d, Column %d", dist[hypodist_cnt].table, dist[hypodist_cnt].attnum)));
        hypodist_cnt++;
    }
    SPI_finish();

    PG_RETURN_VOID();
}

Datum
drop_hypodist(PG_FUNCTION_ARGS)
{
    hypodist_cnt = 0;
    PG_RETURN_VOID();
}

void 
hypodist_get_relation_info_hook(PlannerInfo *root,
                                Oid relationObjectId,
                                bool inhparent,
                                RelOptInfo *rel)
{
    for (int i = 0; i < hypodist_cnt; ++i)
    {
        if (relationObjectId == dist[i].table)
        {
            Assert(rel->cdbpolicy->nattrs > 0);
            rel->cdbpolicy->nattrs = 1;
            rel->cdbpolicy->attrs[0] = dist[i].attnum;
            break;
        }
    }    
}

void
_PG_init(void)
{
    prev_get_relation_info_hook = get_relation_info_hook;
    get_relation_info_hook = hypodist_get_relation_info_hook;
}

void
_PG_fini(void)
{
    get_relation_info_hook = prev_get_relation_info_hook;
}