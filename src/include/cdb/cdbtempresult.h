/*-------------------------------------------------------------------------
 *
 * cdbtempresult.h
 *	  POC: session-level registry for "catalogless" temporary tables
 *	  created via CREATE TEMP TABLE ... WITH (catalogless) AS SELECT.
 *
 *	  Instead of creating catalog rows and a relfilenode, the result of
 *	  the CTAS query is stored in a segment-local NTupleStore with a
 *	  deterministic file name ("CTMPRES_<gp_session_id>_<virtual_id>"),
 *	  and the table's metadata (tuple descriptor, distribution policy,
 *	  row count) is kept in a per-session in-memory hash table, both on
 *	  the QD and on the QE writers.
 *
 *	  Identity of a table is its virtual id (vid), assigned on the QD and
 *	  carried in the IntoClause, the range table entry and the scan plan
 *	  node; the name is only used to resolve unqualified references.
 *
 *	  Scope of the POC is a single top-level transaction: everything is
 *	  dropped at commit/prepare/abort.
 *
 * Portions Copyright (c) 2026, Open GPDB POC
 *
 * src/include/cdb/cdbtempresult.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBTEMPRESULT_H
#define CDBTEMPRESULT_H

#include "access/tupdesc.h"
#include "catalog/gp_policy.h"
#include "nodes/primnodes.h"

/* Upper bound on the number of live catalogless temp tables per session */
#define TEMPRESULT_MAX_ENTRIES	1024

/*
 * One entry per catalogless temp table, keyed by (unqualified) table name.
 * All pointer members live in TopMemoryContext.
 */
typedef struct TempResultEntry
{
	char		name[NAMEDATALEN];	/* hash key: table name */
	int32		vid;			/* per-session virtual id (assigned on QD,
								 * dispatched to QEs in the IntoClause) */
	TupleDesc	tupdesc;		/* result row descriptor */
	GpPolicy   *policy;			/* distribution policy */
	int64		rowcount;		/* QD: global row count; QE: local count */
} TempResultEntry;

/* GUC */
extern bool gp_enable_catalogless_temp;

/* registry */
extern bool TempResultHasEntries(void);
extern TempResultEntry *TempResultLookup(const char *name);
extern TempResultEntry *TempResultLookupId(const char *name, int32 vid);
extern TempResultEntry *TempResultResolve(const RangeVar *rv);
extern IntoClause *TempResultPrepareInto(IntoClause *into);
extern TempResultEntry *TempResultRegister(const char *name, int32 vid,
										   TupleDesc tupdesc, GpPolicy *policy);
extern void TempResultRemove(const char *name);

#endif   /* CDBTEMPRESULT_H */
