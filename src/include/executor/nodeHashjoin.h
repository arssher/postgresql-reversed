/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.h
 *	  prototypes for nodeHashjoin.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASHJOIN_H
#define NODEHASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "access/htup_details.h"
#include "utils/memutils.h"

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

extern HashJoinState *ExecInitHashJoin(HashJoin *node, EState *estate,
									   int eflags, PlanState *parent);
extern bool pushTupleToHashJoinFromInner(TupleTableSlot *slot,
								  HashJoinState *node);
extern bool pushTupleToHashJoinFromOuter(TupleTableSlot *slot,
										 HashJoinState *node);
extern void ExecEndHashJoin(HashJoinState *node);
extern void ExecReScanHashJoin(HashJoinState *node);

extern void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr);

/* inline funcs decls and implementations */
#pragma GCC diagnostic warning "-Winline"
static inline bool CheckOtherQualAndPush(HashJoinState *node);
static inline bool PushUnmatched(HashJoinState *node);
static inline bool CheckJoinQualAndPush(HashJoinState *node);

/*
 * Everything is ready for checking otherqual and projecting; do that,
 * and push the result.
 *
 * Returns true if parent accepts more tuples, false otherwise
 */
static inline bool CheckOtherQualAndPush(HashJoinState *node)
{
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	List *otherqual = node->js.ps.qual;
	TupleTableSlot *slot;

	if (otherqual == NIL ||
		ExecQual(otherqual, econtext, false))
	{
		slot = ExecProject(node->js.ps.ps_ProjInfo);
		return pushTuple(slot, node->js.ps.parent, (PlanState *) node);
	}
	else
		InstrCountFiltered2(node, 1);
	return true;
}

/*
 * Push inner tuple with no match, ExecScanHashTableForUnmatchedAndPush
 * prepared state needed for ExecQual.
 *
 * Returns true if parent accepts more tuples, false otherwise.
 */
static inline bool PushUnmatched(HashJoinState *node)
{
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage.
	 */
	ResetExprContext(econtext);

	/*
	 * Generate a fake join tuple with nulls for the outer tuple,
	 * and return it if it passes the non-join quals.
	 */
	econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;
	return CheckOtherQualAndPush(node);
}

/*
 * We have found inner tuple with hashed quals matched to the current outer
 * tuple. Now check non-hashed quals, other quals, then project and push
 * the result.
 *
 * State for ExecQual was already set by ExecScanHashBucketAndPush and before.
 * Returns true if parent accepts more tuples, false otherwise.
 */
static inline bool CheckJoinQualAndPush(HashJoinState *node)
{
	List	   *joinqual = node->js.joinqual;
	ExprContext *econtext = node->js.ps.ps_ExprContext;

	/*
	 * Only the joinquals determine tuple match status, but all
	 * quals must pass to actually return the tuple.
	 */
	if (joinqual == NIL || ExecQual(joinqual, econtext, false))
	{
		node->hj_MatchedOuter = true;
		HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));
		return CheckOtherQualAndPush(node);
	}
	else
		InstrCountFiltered1(node, 1);

	return true;
}

#endif	 /* NODEHASHJOIN_H */
