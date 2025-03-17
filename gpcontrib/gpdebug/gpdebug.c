

/*------------------------------------------------------------------------------
 * gpdebug.c
 *
 * Copyright (c) 2014-2021, PostgreSQL Global Development Group
 *------------------------------------------------------------------------------
 */
#include "postgres.h"


#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"

PG_MODULE_MAGIC;

void _PG_init(void);

PG_FUNCTION_INFO_V1(cleanupAllGangs);
Datum
cleanupAllGangs(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "cleanupAllGangs can only be executed on master");
	DisconnectAndDestroyAllGangs(false);
	PG_RETURN_BOOL(true);
}