
#include "postgres.h"

#include "utils/builtins.h"

PG_MODULE_MAGIC;
void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_event_trigger_ddl_commands);

Datum
pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS)
{
    (void)pg_event_trigger_ddl_commands_internal(fcinfo);
    PG_RETURN_NULL();
}
