/*-------------------------------------------------------------------------
 *
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeLimit.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeLimit.h"
#include "nodes/nodeFuncs.h"

static void recompute_limits(LimitState *node);
static void pass_down_bound(LimitState *node, PlanState *child_node);

bool
pushTupleToLimit(TupleTableSlot *slot, LimitState *node)
{
	bool parent_accepts_tuples;
	bool limit_accepts_tuples;
	/* last tuple in the window just pushed */
	bool last_tuple_pushed;

	/*
	 * Backward direction is not supported at the moment
	 */
	Assert(ScanDirectionIsForward(node->ps.state->es_direction));
	/* guard against calling pushTupleToLimit after it returned false */
	Assert(node->lstate != LIMIT_DONE);

	if (TupIsNull(slot))
	{
		/* NULL came from below, so this is the end of input anyway */
		node->lstate = LIMIT_DONE;
		pushTuple(slot, node->ps.parent, (PlanState *) node);
		return false;
	}

	if (node->lstate == LIMIT_INITIAL)
	{
		/*
		 * First call for this node, so compute limit/offset. (We can't do
		 * this any earlier, because parameters from upper nodes will not
		 * be set during ExecInitLimit.) This also sets position = 0.
		 */
		recompute_limits(node);

		/*
		 * Check for empty window; if so, treat like empty subplan.
		 */
		if (!node->noCount && node->count <= 0)
		{
			node->lstate = LIMIT_DONE;
			pushTuple(NULL, node->ps.parent, (PlanState *) node);
			return false;
		}

		node->lstate = LIMIT_ACTIVE;
	}

	if (++node->position <= node->offset)
	{
		/* we are not inside the window yet, wait for the next tuple */
		return true;
	}
	/* Now we are sure that we are inside the window and this tuple has to be
	   pushed */
	parent_accepts_tuples = pushTuple(slot, node->ps.parent,
									  (PlanState *) node);
	/* Probably OFFSET is exhausted */
	last_tuple_pushed = !node->noCount &&
		node->position - node->offset >= node->count;
	limit_accepts_tuples = parent_accepts_tuples && !last_tuple_pushed;
	if (!limit_accepts_tuples)
		node->lstate = LIMIT_DONE;
	return limit_accepts_tuples;
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum		val;
	bool		isNull;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull,
										NULL);
		/* Interpret NULL offset as no offset */
		if (isNull)
			node->offset = 0;
		else
		{
			node->offset = DatumGetInt64(val);
			if (node->offset < 0)
				ereport(ERROR,
				 (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
				  errmsg("OFFSET must not be negative")));
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull,
										NULL);
		/* Interpret NULL count as no count (LIMIT ALL) */
		if (isNull)
		{
			node->count = 0;
			node->noCount = true;
		}
		else
		{
			node->count = DatumGetInt64(val);
			if (node->count < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
						 errmsg("LIMIT must not be negative")));
			node->noCount = false;
		}
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;

	/* Notify child node about limit, if useful */
	pass_down_bound(node, outerPlanState(node));
}

/*
 * If we have a COUNT, and our input is a Sort node, notify it that it can
 * use bounded sort.  Also, if our input is a MergeAppend, we can apply the
 * same bound to any Sorts that are direct children of the MergeAppend,
 * since the MergeAppend surely need read no more than that many tuples from
 * any one input.  We also have to be prepared to look through a Result,
 * since the planner might stick one atop MergeAppend for projection purposes.
 *
 * This is a bit of a kluge, but we don't have any more-abstract way of
 * communicating between the two nodes; and it doesn't seem worth trying
 * to invent one without some more examples of special communication needs.
 *
 * Note: it is the responsibility of nodeSort.c to react properly to
 * changes of these parameters.  If we ever do redesign this, it'd be a
 * good idea to integrate this signaling with the parameter-change mechanism.
 */
static void
pass_down_bound(LimitState *node, PlanState *child_node)
{
	if (IsA(child_node, SortState))
	{
		SortState  *sortState = (SortState *) child_node;
		int64		tuples_needed = node->count + node->offset;

		/* negative test checks for overflow in sum */
		if (node->noCount || tuples_needed < 0)
		{
			/* make sure flag gets reset if needed upon rescan */
			sortState->bounded = false;
		}
		else
		{
			sortState->bounded = true;
			sortState->bound = tuples_needed;
		}
	}
	else if (IsA(child_node, MergeAppendState))
	{
		MergeAppendState *maState = (MergeAppendState *) child_node;
		int			i;

		for (i = 0; i < maState->ms_nplans; i++)
			pass_down_bound(node, maState->mergeplans[i]);
	}
	else if (IsA(child_node, ResultState))
	{
		/*
		 * An extra consideration here is that if the Result is projecting a
		 * targetlist that contains any SRFs, we can't assume that every input
		 * tuple generates an output tuple, so a Sort underneath might need to
		 * return more than N tuples to satisfy LIMIT N. So we cannot use
		 * bounded sort.
		 *
		 * If Result supported qual checking, we'd have to punt on seeing a
		 * qual, too.  Note that having a resconstantqual is not a
		 * showstopper: if that fails we're not getting any rows at all.
		 */
		if (outerPlanState(child_node) &&
			!expression_returns_set((Node *) child_node->plan->targetlist))
			pass_down_bound(node, outerPlanState(child_node));
	}
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags, PlanState *parent)
{
	LimitState *limitstate;
	Plan	   *outerPlan;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	limitstate = makeNode(LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;
	limitstate->ps.parent = parent;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);

	/*
	 * Tuple table initialization (XXX not actually used...)
	 */
	ExecInitResultTupleSlot(estate, &limitstate->ps);

	/*
	 * then initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags,
											  (PlanState *) limitstate);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&limitstate->ps);
	limitstate->ps.ps_ProjInfo = NULL;

	return limitstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndLimit(LimitState *node)
{
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
}


void
ExecReScanLimit(LimitState *node)
{
	/*
	 * Recompute limit/offset in case parameters changed, and reset the state
	 * machine.  We must do this before rescanning our child node, in case
	 * it's a Sort that we are passing the parameters down to.
	 */
	recompute_limits(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}
