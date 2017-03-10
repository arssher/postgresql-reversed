/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "access/parallel.h"
#include "access/relscan.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "utils/memutils.h"
#include "miscadmin.h"

extern SeqScanState *ExecInitSeqScan(SeqScan *node, EState *estate, int eflags,
									 PlanState *parent);
extern bool pushTupleToSeqScan(SeqScanState *node);
extern void ExecEndSeqScan(SeqScanState *node);
extern void ExecReScanSeqScan(SeqScanState *node);

/* parallel scan support */
extern void ExecSeqScanEstimate(SeqScanState *node, ParallelContext *pcxt);
extern void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt);
extern void ExecSeqScanInitializeWorker(SeqScanState *node, shm_toc *toc);

/* inline functions decls and implementations */
#pragma GCC diagnostic warning "-Winline"
static inline void SeqPushNull(PlanState *node, SeqScanState *pusher);
static inline TupleTableSlot *SeqStoreTuple(SeqScanState *node,
											HeapTuple tuple);
static inline bool SeqPushHeapTuple(HeapTuple tuple, PlanState *node,
									SeqScanState *pusher);

/* push NULL to the parent, signaling that we are done */
static inline void
SeqPushNull(PlanState *node, SeqScanState *pusher)
{
	ProjectionInfo *projInfo;
	TupleTableSlot *slot;

	projInfo = pusher->ss.ps.ps_ProjInfo;
	slot = pusher->ss.ss_ScanTupleSlot;

	ExecClearTuple(slot);

	if (projInfo)
		pushTuple(ExecClearTuple(projInfo->pi_slot), node,
				  (PlanState *) pusher);
	else
		pushTuple(slot, node,
				  (PlanState *) pusher);
}

/*
 * HeapTuple --> node->ss_ScanTupleSlot, part of original SeqNext after
 * heap_getnext
 */
static inline TupleTableSlot *
SeqStoreTuple(SeqScanState *node, HeapTuple tuple)
{
	HeapScanDesc scandesc;
	TupleTableSlot *slot;

	/*
	 * get information from the scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	slot = node->ss.ss_ScanTupleSlot;

	Assert(tuple);

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot.  Note: we pass 'false' because tuples returned by
	 * heap_getnext() are pointers onto disk pages and were not created with
	 * palloc() and so should not be pfree()'d.  Note also that ExecStoreTuple
	 * will increment the refcount of the buffer; the refcount will not be
	 * dropped until the tuple table slot is cleared.
	 */
	ExecStoreTuple(tuple,	/* tuple to store */
				   slot,	/* slot to store in */
				   scandesc->rs_cbuf,		/* buffer associated with this
											 * tuple */
				   false);	/* don't pfree this pointer */
	return slot;
}

/* Push ready HeapTuple from SeqScanState
 *
 * check qual for the tuple and push it. Tuple must be not NULL.
 * Returns true, if parent accepts more tuples, false otherwise
 */
static inline bool SeqPushHeapTuple(HeapTuple tuple, PlanState *node,
							 SeqScanState *pusher)
{
	ExprContext *econtext;
	List	   *qual;
	ProjectionInfo *projInfo;
	TupleTableSlot *slot;

	if (tuple->t_data == NULL)
	{
		SeqPushNull(node, pusher);
		return false;
	}

	/*
	 * Fetch data from node
	 */
	qual = pusher->ss.ps.qual;
	projInfo = pusher->ss.ps.ps_ProjInfo;
	econtext = pusher->ss.ps.ps_ExprContext;

	CHECK_FOR_INTERRUPTS();

	slot = SeqStoreTuple(pusher, tuple);

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo)
	{
		return pushTuple(slot, node, (PlanState *) pusher);
	}

	ResetExprContext(econtext);
	/*
	 * place the current tuple into the expr context
	 */
	econtext->ecxt_scantuple = slot;

	/*
	 * check that the current tuple satisfies the qual-clause
	 *
	 * check for non-nil qual here to avoid a function call to ExecQual()
	 * when the qual is nil ... saves only a few cycles, but they add up
	 * ...
	 */
	if (!qual || ExecQual(qual, econtext, false))
	{
		/*
		 * Found a satisfactory scan tuple.
		 */
		if (projInfo)
		{
			/*
			 * Form a projection tuple, store it in the result tuple slot
			 * and push it --- unless we find we can project no tuples
			 * from this scan tuple, in which case continue scan.
			 */
			slot = ExecProject(projInfo);
		}
		/*
		 * Here, we aren't projecting, so just push scan tuple.
		 */
		return pushTuple(slot, node, (PlanState *) pusher);
	}
	else
		InstrCountFiltered1(pusher, 1);

	return true;
}


#endif   /* NODESEQSCAN_H */
