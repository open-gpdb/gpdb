
#include "postgres.h"

#include "utils/builtins.h"

PG_MODULE_MAGIC;
void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_event_trigger_ddl_commands);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_oid);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_reason);

Datum
pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_ddl_commands_internal(fcinfo);
}

Datum
pg_event_trigger_table_rewrite_oid(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_table_rewrite_oid_internal(fcinfo);
}

Datum
pg_event_trigger_table_rewrite_reason(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_table_rewrite_reason_internal(fcinfo);
}