/*-------------------------------------------------------------------------
 *
 * cdbtempresult.c
 *	  POC: session-level registry for "catalogless" temp tables
 *	  created via CREATE TEMP TABLE ... WITH (catalogless) AS SELECT.
 *
 *	  See src/include/cdb/cdbtempresult.h for an overview.
 *
 * Portions Copyright (c) 2026, Open GPDB POC
 *
 * IDENTIFICATION
 *	  src/backend/cdb/cdbtempresult.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbtempresult.h"

/*
 * GUC: defined here, registered in guc_gp.c.  Superuser-only kill-switch
 * for creation (default on); the per-object trigger is the CTAS
 * WITH (catalogless) option.  Lookups do not depend on it, so turning it
 * off never changes what an existing name resolves to.
 */
bool		gp_enable_catalogless_temp = true;
