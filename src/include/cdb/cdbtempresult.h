/*-------------------------------------------------------------------------
 *
 * cdbtempresult.h
 *	  POC: session-level registry for "catalogless" temporary tables
 *	  created via CREATE TEMP TABLE ... AS SELECT when
 *	  gp_enable_catalogless_temp is on.
 *
 *	  Instead of creating catalog rows and a relfilenode, the result of
 *	  the CTAS query is stored in a segment-local NTupleStore with a
 *	  deterministic file name ("CTMPRES_<gp_session_id>_<virtual_id>"),
 *	  and the table's metadata (tuple descriptor, distribution policy,
 *	  row count) is kept in a per-session in-memory hash table, both on
 *	  the QD and on the QEs.
 *
 *	  Scope of the POC is a single top-level transaction: everything is
 *	  dropped at commit/abort.
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

struct NTupleStore;				/* avoid including tuplestorenew.h here */
struct NTupleStoreAccessor;

/*
 * One entry per catalogless temp table, keyed by (unqualified) table name.
 * All pointer members live in TopMemoryContext.
 */
typedef struct TempResultEntry
{
	char		name[NAMEDATALEN];	/* hash key: table name */
	int			vid;			/* per-session virtual id (assigned on QD,
								 * dispatched to QEs in the IntoClause) */
	TupleDesc	tupdesc;		/* result row descriptor */
	GpPolicy   *policy;			/* distribution policy */
	int64		rowcount;		/* QD: global row count; QE: local count */

	/*
	 * QE-writer side only: the tuplestore this segment wrote, kept open
	 * until end of transaction so that later readers in the same session
	 * find the file.
	 */
	struct NTupleStore *store;
	struct NTupleStoreAccessor *writeacc;
} TempResultEntry;

/* GUC */
extern bool gp_enable_catalogless_temp;

extern TempResultEntry *TempResultLookup(const char *name);
extern TempResultEntry *TempResultRegister(const char *name, int vid,
										   TupleDesc tupdesc, GpPolicy *policy);
extern void TempResultRemove(const char *name);
extern int	TempResultAssignId(void);
extern char *TempResultStoreName(int vid);

/* helpers used by the executor read path */
extern struct NTupleStore *TempResultOpenReader(int vid);
extern void TempResultTrackReader(struct NTupleStore *store);

#endif   /* CDBTEMPRESULT_H */
