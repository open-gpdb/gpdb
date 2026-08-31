/*-------------------------------------------------------------------------
 *
 * nodeTempResultScan.c
 *	  POC: support routines for scanning a "catalogless" temp table,
 *	  i.e. the segment-local NTupleStore written by a previous
 *	  CREATE TEMP TABLE ... AS SELECT with gp_enable_catalogless_temp.
 *
 *	  The tuplestore is opened lazily on the first fetch: at ExecInit time
 *	  the store file may not exist on this process (e.g. QD initializing
 *	  an alien node, or EXPLAIN).  The scan tuple descriptor is rebuilt
 *	  from the type information carried in the plan node, so no catalog
 *	  or registry access is strictly required for that.
 *
 * Portions Copyright (c) 2026, Open GPDB POC
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeTempResultScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "cdb/cdbtempresult.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeTempResultScan.h"
#include "nodes/value.h"
#include "utils/tuplestorenew.h"

static TupleTableSlot *TempResultNext(TempResultScanState *node);
static bool TempResultRecheck(TempResultScanState *node, TupleTableSlot *slot);
static void TempResultScanOpenStore(TempResultScanState *node);

/*
 * TempResultRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
TempResultRecheck(TempResultScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/*
 * Open the underlying tuplestore for reading (lazily, on first fetch).
 */
static void
TempResultScanOpenStore(TempResultScanState *node)
{
	TempResultScan *plan = (TempResultScan *) node->ss.ps.plan;

	Assert(!node->ts_opened);

	node->ts_store = TempResultOpenReader(plan->tempresid);
	node->ts_acc = ntuplestore_create_accessor(node->ts_store,
											   false /* reader */ );
	ntuplestore_acc_seek_bof(node->ts_acc);
	node->ts_opened = true;
}

/* ----------------------------------------------------------------
 *		TempResultNext
 *
 *		Fetch the next tuple from the local tuplestore.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
TempResultNext(TempResultScanState *node)
{
	TupleTableSlot *slot;
	EState	   *estate = node->ss.ps.state;
	bool		forward = ScanDirectionIsForward(estate->es_direction);
	bool		gotOK;

	if (!node->ts_opened)
		TempResultScanOpenStore(node);

	slot = node->ss.ss_ScanTupleSlot;

	ntuplestore_acc_advance(node->ts_acc, forward ? 1 : -1);
	gotOK = ntuplestore_acc_current_tupleslot(node->ts_acc, slot);

	if (!gotOK)
	{
		ExecClearTuple(slot);
		return NULL;
	}

	return slot;
}

/* ----------------------------------------------------------------
 *		ExecTempResultScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecTempResultScan(TempResultScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) TempResultNext,
					(ExecScanRecheckMtd) TempResultRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitTempResultScan
 * ----------------------------------------------------------------
 */
TempResultScanState *
ExecInitTempResultScan(TempResultScan *node, EState *estate, int eflags)
{
	TempResultScanState *scanstate;
	TupleDesc	tupdesc;
	int			natts;
	int			attno;
	ListCell   *lct;
	ListCell   *lcm;
	ListCell   *lcc;
	ListCell   *lcn;

	/* TempResultScan should not have any children */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	scanstate = makeNode(TempResultScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ts_store = NULL;
	scanstate->ts_acc = NULL;
	scanstate->ts_opened = false;

	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/* tuple table initialization */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/* initialize child expressions */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	/*
	 * Build the scan tuple descriptor from the column type info carried in
	 * the plan node.  This must match the layout of the tuples stored by
	 * the CTAS writer.
	 */
	natts = list_length(node->coltypes);
	tupdesc = CreateTemplateTupleDesc(natts, false);
	attno = 0;
	lcn = list_head(node->colnames);
	forthree(lct, node->coltypes,
			 lcm, node->coltypmods,
			 lcc, node->colcollations)
	{
		Oid			coltype = lfirst_oid(lct);
		int32		coltypmod = lfirst_int(lcm);
		Oid			colcoll = lfirst_oid(lcc);
		char	   *colname;

		if (lcn)
		{
			colname = strVal(lfirst(lcn));
			lcn = lnext(lcn);
		}
		else
			colname = "?column?";

		attno++;
		TupleDescInitEntry(tupdesc, (AttrNumber) attno, colname,
						   coltype, coltypmod, 0);
		TupleDescInitEntryCollation(tupdesc, (AttrNumber) attno, colcoll);
	}

	ExecAssignScanType(&scanstate->ss, tupdesc);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndTempResultScan
 * ----------------------------------------------------------------
 */
void
ExecEndTempResultScan(TempResultScanState *node)
{
	ExecFreeExprContext(&node->ss.ps);
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (node->ts_acc)
	{
		ntuplestore_destroy_accessor(node->ts_acc);
		node->ts_acc = NULL;
	}

	/*
	 * NB: the reader NTupleStore itself is deliberately NOT destroyed here.
	 * It is tracked by cdbtempresult.c and destroyed at the end of the
	 * transaction, so that destroying it does not remove the shared files
	 * that other readers (or a later statement) may still need.
	 */
	node->ts_store = NULL;
	node->ts_opened = false;
}

/* ----------------------------------------------------------------
 *		ExecReScanTempResultScan
 * ----------------------------------------------------------------
 */
void
ExecReScanTempResultScan(TempResultScanState *node)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (node->ts_opened)
		ntuplestore_acc_seek_bof(node->ts_acc);

	ExecScanReScan(&node->ss);
}
