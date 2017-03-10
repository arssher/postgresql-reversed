/*-------------------------------------------------------------------------
 *
 * execProcnode.c
 *	 contains dispatch functions which call the appropriate "initialize",
 *	 "push a tuple", and "cleanup" routines for the given node type.
 *	 If the node has children, then it will presumably call ExecInitNode
 *	 and ExecEndNode on its subnodes and ExecPushTuple to push processed tuple
 *	 to its parent.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execProcnode.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecInitNode	-		initialize a plan node and its subplans
 *		ExecLeaf		-		start execution of the leaf
 *		ExecPushTuple	-		push tuple to the parent node
 *		ExecPushNull	-		let parent know that we are done
 *		ExecEndNode		-		shut down a plan node and its subplans
 *
 *	 NOTES
 *		This used to be three files. It is now all combined into
 *		one file so that it is easier to keep ExecInitNode, ExecLeaf,
 *		ExecPushTuple, ExecPushNull and ExecEndNode in sync when new nodes
 *		are added.
 *
 *	 EXAMPLE Suppose we want the age of the manager of the shoe department and
 *		the number of employees in that department.  So we have the query:
 *
 *				select DEPT.no_emps, EMP.age
 *				from DEPT, EMP
 *				where EMP.name = DEPT.mgr and
 *					  DEPT.name = "shoe"
 *
 *		Suppose the planner gives us the following plan:
 *
 *						Nest Loop (DEPT.mgr = EMP.name)
 *						/		\
 *					   /		 \
 *				   Seq Scan		Seq Scan
 *					DEPT		  EMP
 *				(name = "shoe")
 *
 *		ExecutorStart() is called first.
 *		It calls InitPlan() which calls ExecInitNode() on
 *		the root of the plan -- the nest loop node.
 *
 *	  * ExecInitNode() notices that it is looking at a nest loop and
 *		as the code below demonstrates, it calls ExecInitNestLoop().
 *		Eventually this calls ExecInitNode() on the right and left subplans
 *		and so forth until the entire plan is initialized.  The result
 *		of ExecInitNode() is a plan state tree built with the same structure
 *		as the underlying plan tree.
 *
 *	  * Then when ExecutorRun() is called, it calls ExecLeaf on each leaf of
 *		the plan state with inner leafs first, outer second. So, in this case
 *		it will call it on DEPT SeqScan, and then on EMP SeqScan. ExecLeaf
 *		chooses the corresponding implementation -- here it is ExecSeqScan,
 *		sequential scan. ExecSeqScan retrieves tuples and for each of them it
 *		calls ExecPushTuple to pass tuple to nodeSeqScan's parent, Nest Loop
 *		in this case. ExecPushTuple resolves the call, i.e. it finds something
 *		like ExecPushTupleToNestLoopFromOuter and calls it. Then the process
 *		repeats, so ExecPushTuple is called recursively. We have two corner
 *		cases:
 *
 *		1) When node have nothing more to push, e.g. nodeSeqScan have
 *		   scanned all the tuples. Then it calls ExecPushNull once to let its
 *		   parent know that nodeSeqScan have finished its work.
 *		2) When node has no parent (top-level node). In this case
 *		   ExecPushTuple calls SendReadyTuple which sends the tuple to its
 *		   final destination.
 *
 *		So, in our example DEPT ExecSeqScan will eventually call ExecPushNull,
 *		so Nest Loop node will learn that its inner side is done. Then EMP
 *		SeqScan will start pushing, and inside EMP Seqscan's ExecPushTuple
 *		Nested Loop will match the tuples and push them to the final
 *		destination. Eventually EMP Seqscan will call ExecPushNull, inside it
 *		Nested Loop will call ExecPushNull too and the nest loop join ends.
 *
 *		ExecPushTuple returns bool value which tells whether parent still
 *		accepts the tuples. It allows to stop execution in the middle; e.g. if
 *		we have node Limit above SeqScan node, the latter needs to scan only
 *		LIMIT tuples. We don't push anything after receiving false from
 *		ExecPushTuple; obviously, even if we have pushed all the tuples and
 *		the last ExecPushTuple call returned false, we don't call
 *		ExecPushNull.
 *
 *		Lastly, ExecutorEnd() calls ExecEndNode() which
 *		calls ExecEndNestLoop() which in turn calls ExecEndNode() on
 *		its subplans which result in ExecEndSeqScan().
 *
 *		This should show how the executor works by having ExecInitNode(),
 *		ExecLeaf, ExecPushTuple, ExecPushNull and ExecEndNode() dispatch their
 *		work to the appropriate node support routines which may in turn call
 *		these routines themselves on their subplans.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeAppend.h"
#include "executor/nodeBitmapAnd.h"
#include "executor/nodeBitmapHeapscan.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeBitmapOr.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeCustom.h"
#include "executor/nodeForeignscan.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeGather.h"
#include "executor/nodeGatherMerge.h"
#include "executor/nodeGroup.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeLimit.h"
#include "executor/nodeLockRows.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeMergeAppend.h"
#include "executor/nodeMergejoin.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeNestloop.h"
#include "executor/nodeProjectSet.h"
#include "executor/nodeRecursiveunion.h"
#include "executor/nodeResult.h"
#include "executor/nodeSamplescan.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeSetOp.h"
#include "executor/nodeSort.h"
#include "executor/nodeSubplan.h"
#include "executor/nodeSubqueryscan.h"
#include "executor/nodeTableFuncscan.h"
#include "executor/nodeTidscan.h"
#include "executor/nodeUnique.h"
#include "executor/nodeValuesscan.h"
#include "executor/nodeWindowAgg.h"
#include "executor/nodeWorktablescan.h"
#include "nodes/nodeFuncs.h"
#include "miscadmin.h"


/* ------------------------------------------------------------------------
 *		ExecInitNode
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *		  'parent' is parent of the node
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags, PlanState *parent)
{
	PlanState  *result;

	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		/*
		 * scan nodes
		 */
		case T_SeqScan:
			result = (PlanState *) ExecInitSeqScan((SeqScan *) node,
												   estate, eflags, parent);
			break;

		/*
		 * join nodes
		 */
		case T_HashJoin:
			result = (PlanState *) ExecInitHashJoin((HashJoin *) node,
													estate, eflags, parent);
			break;

		/*
		 * materialization nodes
		 */
		case T_Hash:
			result = (PlanState *) ExecInitHash((Hash *) node,
												estate, eflags, parent);
			break;

		case T_Limit:
			result = (PlanState *) ExecInitLimit((Limit *) node,
												 estate, eflags, parent);
			break;

		default:
			elog(ERROR, "unrecognized/unsupported node type: %d",
				 (int) nodeTag(node));
			return NULL;		/* keep compiler quiet */
	}

	/* Set up instrumentation for this node if requested */
	if (estate->es_instrument)
		result->instrument = InstrAlloc(1, estate->es_instrument);

	return result;
}


/*
 * Unsupported, left to avoid deleting 19k lines of existing code
 */
TupleTableSlot *
ExecProcNode(PlanState *node)
{
	elog(ERROR, "ExecProcNode is not supported");
	return NULL;
}

/*
 * Tell the 'node' leaf to start the execution
 */
void
ExecLeaf(PlanState *node)
{
	CHECK_FOR_INTERRUPTS();

	switch (nodeTag(node))
	{
		case T_SeqScanState:
			ExecSeqScan((SeqScanState *) node);
			break;

		default:
			elog(ERROR, "bottom node type not supported: %d",
				 (int) nodeTag(node));
	}
}

/*
 * Instead of ExecProcNode, here we will have function ExecPushTuple pushing
 * one tuple.
 * 'slot' is tuple to push, it must be not null; when node finished its work
 * it must call ExecPushNull instead.
 * 'pusher' is sender of a tuple, it's parent is the receiver. We take it as a
 * param instead of its parent directly because we need it to distinguish
 * inner and outer pushes.
 *
 * Returns true if node is still accepting tuples, false if not.
 *
 * If tuple was pushed into a node which returned 'false' before, the
 * behaviour is undefined, i.e. it is not allowed; we will try to catch such
 * situations with asserts.
 */
bool
ExecPushTuple(TupleTableSlot *slot, PlanState *pusher)
{
	PlanState *receiver = pusher->parent;
	bool push_from_outer;

	Assert(!TupIsNull(slot));

	CHECK_FOR_INTERRUPTS();

	/* If the receiver is NULL, then pusher is top-level node, so we need
	 * to send the tuple to the dest
	 */
	if (receiver == NULL)
	{
		return SendReadyTuple(slot, pusher);
	}

	if (nodeTag(receiver) == T_LimitState)
		return ExecPushTupleToLimit(slot, (LimitState *) receiver);
	else if (nodeTag(receiver) == T_HashState)
		return ExecPushTupleToHash(slot, (HashState *) receiver);

	/* does push come from the outer side? */
	push_from_outer = outerPlanState(receiver) == pusher;

	if (nodeTag(receiver) == T_HashJoinState && push_from_outer)
		return ExecPushTupleToHashJoinFromOuter(slot,
											   (HashJoinState *) receiver);

	elog(ERROR, "node type not supported: %d", (int) nodeTag(receiver));
}

/*
 * Signal parent that we are done. Like in ExecPushTuple, sender is param
 * here because we need to distinguish inner and outer pushes.
 *
 * 'slot' must be null tuple. It exists to be able to transfer correct
 * tupleDesc.
 */
void
ExecPushNull(TupleTableSlot *slot, PlanState *pusher)
{
	PlanState *receiver = pusher->parent;
	bool push_from_outer;

	Assert(TupIsNull(slot));

	CHECK_FOR_INTERRUPTS();

	/*
	 * If the receiver is NULL, then pusher is top-level node; end of
	 * the execution
	 */
	if (receiver == NULL)
	{
		SendReadyTuple(slot, pusher);
		return;
	}

	if (nodeTag(receiver) == T_LimitState)
		return ExecPushNullToLimit(slot, (LimitState *) receiver);
	else if (nodeTag(receiver) == T_HashState)
		return ExecPushNullToHash(slot, (HashState *) receiver);

	/* does push come from the outer side? */
	push_from_outer = outerPlanState(receiver) == pusher;

	if (nodeTag(receiver) == T_HashJoinState && push_from_outer)
		return ExecPushNullToHashJoinFromOuter(slot,
											   (HashJoinState *) receiver);

	else if (nodeTag(receiver) == T_HashJoinState && !push_from_outer)
		return ExecPushNullToHashJoinFromInner(slot,
											   (HashJoinState *) receiver);

	elog(ERROR, "node type not supported: %d", (int) nodeTag(receiver));
}

/* ----------------------------------------------------------------
 * Unsupported too; we don't need it in push model
 * ----------------------------------------------------------------
 */
Node *
MultiExecProcNode(PlanState *node)
{
	elog(ERROR, "MultiExecProcNode is not supported");
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecEndNode
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.  This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
void
ExecEndNode(PlanState *node)
{
	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return;

	if (node->chgParam != NULL)
	{
		bms_free(node->chgParam);
		node->chgParam = NULL;
	}

	switch (nodeTag(node))
	{
		/*
		 * scan nodes
		 */
		case T_SeqScanState:
			ExecEndSeqScan((SeqScanState *) node);
			break;

		/*
		 * join nodes
		 */
		case T_HashJoinState:
			ExecEndHashJoin((HashJoinState *) node);
			break;

		/*
		 * materialization nodes
		 */
		case T_HashState:
			ExecEndHash((HashState *) node);
			break;

		case T_LimitState:
			ExecEndLimit((LimitState *) node);
			break;

		default:
			elog(ERROR, "unrecognized/unsupported node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * ExecShutdownNode
 *
 * Give execution nodes a chance to stop asynchronous resource consumption
 * and release any resources still held.  Currently, this is only used for
 * parallel query, but we might want to extend it to other cases also (e.g.
 * FDW).  We might also want to call it sooner, as soon as it's evident that
 * no more rows will be needed (e.g. when a Limit is filled) rather than only
 * at the end of ExecutorRun.
 */
bool
ExecShutdownNode(PlanState *node)
{
	if (node == NULL)
		return false;

	planstate_tree_walker(node, ExecShutdownNode, NULL);

	switch (nodeTag(node))
	{
		case T_GatherState:
			ExecShutdownGather((GatherState *) node);
			break;
		case T_ForeignScanState:
			ExecShutdownForeignScan((ForeignScanState *) node);
			break;
		case T_CustomScanState:
			ExecShutdownCustomScan((CustomScanState *) node);
			break;
		case T_GatherMergeState:
			ExecShutdownGatherMerge((GatherMergeState *) node);
			break;
		default:
			break;
	}

	return false;
}
