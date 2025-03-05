
#include "postgres.h"


#include "bloom.h"


Datum
blgettuple(PG_FUNCTION_ARGS)
{
    elog(ERROR, "unsupported");

	PG_RETURN_BOOL(false);
}


/*
 * bmmarkpos() -- save the current scan position.
 */
Datum
blmarkpos(PG_FUNCTION_ARGS)
{

    elog(ERROR, "unsupported");

	PG_RETURN_VOID();
}

/*
 * bmrestrpos() -- restore a scan to the last saved position.
 */
Datum
blrestrpos(PG_FUNCTION_ARGS)
{
	
    elog(ERROR, "unsupported");

	PG_RETURN_VOID();
}

