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

/* GUC */
extern bool gp_enable_catalogless_temp;

#endif   /* CDBTEMPRESULT_H */
