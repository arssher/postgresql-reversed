/*-------------------------------------------------------------------------
 *
 * execAmi.c
 *	  miscellaneous executor access method routines
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/executor/execAmi.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "executor/execdebug.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeLimit.h"
#include "executor/nodeSeqscan.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "nodes/extensible.h"


static bool TargetListSupportsBackwardScan(List *targetlist);
static bool IndexSupportsBackwardScan(Oid indexid);

/* not supported yet */
void
ExecReScan(PlanState *node)
{
	elog(ERROR, "ExecRescan was called");
}

/*
 * ExecSupportsMarkRestore - does a Path support mark/restore?
 *
 * This is used during planning and so must accept a Path, not a Plan.
 * We keep it here to be adjacent to the routines above, which also must
 * know which plan types support mark/restore.
 */
bool
ExecSupportsMarkRestore(Path *pathnode)
{
	/*
	 * For consistency with the routines above, we do not examine the nodeTag
	 * but rather the pathtype, which is the Plan node type the Path would
	 * produce.
	 */
	switch (pathnode->pathtype)
	{
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_Material:
		case T_Sort:
			return true;

		case T_CustomScan:
			Assert(IsA(pathnode, CustomPath));
			if (((CustomPath *) pathnode)->flags & CUSTOMPATH_SUPPORT_MARK_RESTORE)
				return true;
			return false;

		case T_Result:

			/*
			 * Result supports mark/restore iff it has a child plan that does.
			 *
			 * We have to be careful here because there is more than one Path
			 * type that can produce a Result plan node.
			 */
			if (IsA(pathnode, ProjectionPath))
				return ExecSupportsMarkRestore(((ProjectionPath *) pathnode)->subpath);
			else if (IsA(pathnode, MinMaxAggPath))
				return false;	/* childless Result */
			else
			{
				Assert(IsA(pathnode, ResultPath));
				return false;	/* childless Result */
			}

		default:
			break;
	}

	return false;
}

/*
 * ExecSupportsBackwardScan - does a plan type support backwards scanning?
 *
 * Ideally, all plan types would support backwards scan, but that seems
 * unlikely to happen soon.  In some cases, a plan node passes the backwards
 * scan down to its children, and so supports backwards scan only if its
 * children do.  Therefore, this routine must be passed a complete plan tree.
 */
bool
ExecSupportsBackwardScan(Plan *node)
{
	if (node == NULL)
		return false;

	/*
	 * Parallel-aware nodes return a subset of the tuples in each worker, and
	 * in general we can't expect to have enough bookkeeping state to know
	 * which ones we returned in this worker as opposed to some other worker.
	 */
	if (node->parallel_aware)
		return false;

	switch (nodeTag(node))
	{
		case T_Result:
			if (outerPlan(node) != NULL)
				return ExecSupportsBackwardScan(outerPlan(node)) &&
					TargetListSupportsBackwardScan(node->targetlist);
			else
				return false;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) node)->appendplans)
				{
					if (!ExecSupportsBackwardScan((Plan *) lfirst(l)))
						return false;
				}
				/* need not check tlist because Append doesn't evaluate it */
				return true;
			}

		case T_SeqScan:
		case T_TidScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
			return TargetListSupportsBackwardScan(node->targetlist);

		case T_SampleScan:
			/* Simplify life for tablesample methods by disallowing this */
			return false;

		case T_Gather:
			return false;

		case T_IndexScan:
			return IndexSupportsBackwardScan(((IndexScan *) node)->indexid) &&
				TargetListSupportsBackwardScan(node->targetlist);

		case T_IndexOnlyScan:
			return IndexSupportsBackwardScan(((IndexOnlyScan *) node)->indexid) &&
				TargetListSupportsBackwardScan(node->targetlist);

		case T_SubqueryScan:
			return ExecSupportsBackwardScan(((SubqueryScan *) node)->subplan) &&
				TargetListSupportsBackwardScan(node->targetlist);

		case T_CustomScan:
			{
				uint32		flags = ((CustomScan *) node)->flags;

				if ((flags & CUSTOMPATH_SUPPORT_BACKWARD_SCAN) &&
					TargetListSupportsBackwardScan(node->targetlist))
					return true;
			}
			return false;

		case T_Material:
		case T_Sort:
			/* these don't evaluate tlist */
			return true;

		case T_LockRows:
		case T_Limit:
			/* these don't evaluate tlist */
			return ExecSupportsBackwardScan(outerPlan(node));

		default:
			return false;
	}
}

/*
 * If the tlist contains set-returning functions, we can't support backward
 * scan, because the TupFromTlist code is direction-ignorant.
 */
static bool
TargetListSupportsBackwardScan(List *targetlist)
{
	if (expression_returns_set((Node *) targetlist))
		return false;
	return true;
}

/*
 * An IndexScan or IndexOnlyScan node supports backward scan only if the
 * index's AM does.
 */
static bool
IndexSupportsBackwardScan(Oid indexid)
{
	bool		result;
	HeapTuple	ht_idxrel;
	Form_pg_class idxrelrec;
	IndexAmRoutine *amroutine;

	/* Fetch the pg_class tuple of the index relation */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation %u", indexid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/* Fetch the index AM's API struct */
	amroutine = GetIndexAmRoutineByAmId(idxrelrec->relam, false);

	result = amroutine->amcanbackward;

	pfree(amroutine);
	ReleaseSysCache(ht_idxrel);

	return result;
}

/*
 * ExecMaterializesOutput - does a plan type materialize its output?
 *
 * Returns true if the plan node type is one that automatically materializes
 * its output (typically by keeping it in a tuplestore).  For such plans,
 * a rescan without any parameter change will have zero startup cost and
 * very low per-tuple cost.
 */
bool
ExecMaterializesOutput(NodeTag plantype)
{
	switch (plantype)
	{
		case T_Material:
		case T_FunctionScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_Sort:
			return true;

		default:
			break;
	}

	return false;
}
