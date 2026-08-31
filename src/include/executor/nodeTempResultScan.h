/*-------------------------------------------------------------------------
 *
 * nodeTempResultScan.h
 *	  POC: scan of a catalogless temp table (session tuplestore).
 *
 * Portions Copyright (c) 2026, Open GPDB POC
 *
 * src/include/executor/nodeTempResultScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODETEMPRESULTSCAN_H
#define NODETEMPRESULTSCAN_H

#include "nodes/execnodes.h"

extern TempResultScanState *ExecInitTempResultScan(TempResultScan *node,
						EState *estate, int eflags);
extern TupleTableSlot *ExecTempResultScan(TempResultScanState *node);
extern void ExecEndTempResultScan(TempResultScanState *node);
extern void ExecReScanTempResultScan(TempResultScanState *node);

#endif   /* NODETEMPRESULTSCAN_H */
