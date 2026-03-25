/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "nodes/execnodes.h"

extern SeqScanState *ExecInitSeqScan(SeqScan *node, EState *estate, int eflags);
extern SeqScanState *ExecInitSeqScanForPartition(SeqScan *node, EState *estate, int eflags,
							Relation currentRelation);
extern TupleTableSlot *ExecSeqScan(SeqScanState *node);
extern void ExecEndSeqScan(SeqScanState *node);
extern void ExecReScanSeqScan(SeqScanState *node);

typedef void (*init_scan_hook_type)(Relation currentRelation);
extern PGDLLIMPORT init_scan_hook_type init_scan_hook;

#endif   /* NODESEQSCAN_H */
