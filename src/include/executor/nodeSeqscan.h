#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern SeqScanState *ExecInitSeqScan(SeqScan *node, EState *estate, int eflags,
									 PlanState *parent);
extern bool pushTupleToSeqScan(SeqScanState *node);
extern void ExecEndSeqScan(SeqScanState *node);

#endif   /* NODESEQSCAN */
