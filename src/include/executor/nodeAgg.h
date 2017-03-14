/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *	  prototypes for nodeAgg.c
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAGG_H
#define NODEAGG_H

#include "nodes/execnodes.h"
#include "executor.h"
#include "utils/memutils.h"
#include "utils/expandeddatum.h"
#include "utils/datum.h"


/*
 * AggStatePerTransData - per aggregate state value information
 *
 * Working state for updating the aggregate's state value, by calling the
 * transition function with an input row. This struct does not store the
 * information needed to produce the final aggregate result from the transition
 * state, that's stored in AggStatePerAggData instead. This separation allows
 * multiple aggregate results to be produced from a single state value.
 */
typedef struct AggStatePerTransData
{
	/*
	 * These values are set up during ExecInitAgg() and do not change
	 * thereafter:
	 */

	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple Aggref's sharing the same state value, as long as
	 * the inputs and transition function are identical. This points to the
	 * first one of them.
	 */
	Aggref	   *aggref;

	/*
	 * Nominal number of arguments for aggregate function.  For plain aggs,
	 * this excludes any ORDER BY expressions.  For ordered-set aggs, this
	 * counts both the direct and aggregated (ORDER BY) arguments.
	 */
	int			numArguments;

	/*
	 * Number of aggregated input columns.  This includes ORDER BY expressions
	 * in both the plain-agg and ordered-set cases.  Ordered-set direct args
	 * are not counted, though.
	 */
	int			numInputs;

	/*
	 * Number of aggregated input columns to pass to the transfn.  This
	 * includes the ORDER BY columns for ordered-set aggs, but not for plain
	 * aggs.  (This doesn't count the transition state value!)
	 */
	int			numTransInputs;

	/* Oid of the state transition or combine function */
	Oid			transfn_oid;

	/* Oid of the serialization function or InvalidOid */
	Oid			serialfn_oid;

	/* Oid of the deserialization function or InvalidOid */
	Oid			deserialfn_oid;

	/* Oid of state value's datatype */
	Oid			aggtranstype;

	/* ExprStates of the FILTER and argument expressions. */
	ExprState  *aggfilter;		/* state of FILTER expression, if any */
	List	   *args;			/* states of aggregated-argument expressions */
	List	   *aggdirectargs;	/* states of direct-argument expressions */

	/*
	 * fmgr lookup data for transition function or combine function.  Note in
	 * particular that the fn_strict flag is kept here.
	 */
	FmgrInfo	transfn;

	/* fmgr lookup data for serialization function */
	FmgrInfo	serialfn;

	/* fmgr lookup data for deserialization function */
	FmgrInfo	deserialfn;

	/* Input collation derived for aggregate */
	Oid			aggCollation;

	/* number of sorting columns */
	int			numSortCols;

	/* number of sorting columns to consider in DISTINCT comparisons */
	/* (this is either zero or the same as numSortCols) */
	int			numDistinctCols;

	/* deconstructed sorting information (arrays of length numSortCols) */
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirst;

	/*
	 * fmgr lookup data for input columns' equality operators --- only
	 * set/used when aggregate has DISTINCT flag.  Note that these are in
	 * order of sort column index, not parameter index.
	 */
	FmgrInfo   *equalfns;		/* array of length numDistinctCols */

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * We need the len and byval info for the agg's input and transition data
	 * types in order to know how to copy/delete values.
	 *
	 * Note that the info for the input type is used only when handling
	 * DISTINCT aggs with just one argument, so there is only one input type.
	 */
	int16		inputtypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				transtypeByVal;

	/*
	 * Stuff for evaluation of inputs.  We used to just use ExecEvalExpr, but
	 * with the addition of ORDER BY we now need at least a slot for passing
	 * data to the sort object, which requires a tupledesc, so we might as
	 * well go whole hog and use ExecProject too.
	 */
	TupleDesc	evaldesc;		/* descriptor of input tuples */
	ProjectionInfo *evalproj;	/* projection machinery */

	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row.
	 */
	TupleTableSlot *evalslot;	/* current input tuple */
	TupleTableSlot *uniqslot;	/* used for multi-column DISTINCT */

	/*
	 * These values are working state that is initialized at the start of an
	 * input tuple group and updated for each input tuple.
	 *
	 * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
	 * values straight to the transition function.  If it's DISTINCT or
	 * requires ORDER BY, we pass the input values into a Tuplesort object;
	 * then at completion of the input tuple group, we scan the sorted values,
	 * eliminate duplicates if needed, and run the transition function on the
	 * rest.
	 *
	 * We need a separate tuplesort for each grouping set.
	 */

	Tuplesortstate **sortstates;	/* sort objects, if DISTINCT or ORDER BY */

	/*
	 * This field is a pre-initialized FunctionCallInfo struct used for
	 * calling this aggregate's transfn.  We save a few cycles per row by not
	 * re-initializing the unchanging fields; which isn't much, but it seems
	 * worth the extra space consumption.
	 */
	FunctionCallInfoData transfn_fcinfo;

	/* Likewise for serialization and deserialization functions */
	FunctionCallInfoData serialfn_fcinfo;

	FunctionCallInfoData deserialfn_fcinfo;
}	AggStatePerTransData;

/*
 * AggStatePerAggData - per-aggregate information
 *
 * This contains the information needed to call the final function, to produce
 * a final aggregate result from the state value. If there are multiple
 * identical Aggrefs in the query, they can all share the same per-agg data.
 *
 * These values are set up during ExecInitAgg() and do not change thereafter.
 */
typedef struct AggStatePerAggData
{
	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple identical Aggref's sharing the same per-agg. This
	 * points to the first one of them.
	 */
	Aggref	   *aggref;

	/* index to the state value which this agg should use */
	int			transno;

	/* Optional Oid of final function (may be InvalidOid) */
	Oid			finalfn_oid;

	/*
	 * fmgr lookup data for final function --- only valid when finalfn_oid oid
	 * is not InvalidOid.
	 */
	FmgrInfo	finalfn;

	/*
	 * Number of arguments to pass to the finalfn.  This is always at least 1
	 * (the transition state value) plus any ordered-set direct args. If the
	 * finalfn wants extra args then we pass nulls corresponding to the
	 * aggregated input columns.
	 */
	int			numFinalArgs;

	/*
	 * We need the len and byval info for the agg's result data type in order
	 * to know how to copy/delete values.
	 */
	int16		resulttypeLen;
	bool		resulttypeByVal;

}	AggStatePerAggData;


/*
 * AggStatePerGroupData - per-aggregate-per-group working state
 *
 * These values are working state that is initialized at the start of
 * an input tuple group and updated for each input tuple.
 *
 * In AGG_PLAIN and AGG_SORTED modes, we have a single array of these
 * structs (pointed to by aggstate->pergroup); we re-use the array for
 * each input group, if it's AGG_SORTED mode.  In AGG_HASHED mode, the
 * hash table contains an array of these structs for each tuple group.
 *
 * Logically, the sortstate field belongs in this struct, but we do not
 * keep it here for space reasons: we don't support DISTINCT aggregates
 * in AGG_HASHED mode, so there's no reason to use up a pointer field
 * in every entry of the hashtable.
 */
typedef struct AggStatePerGroupData
{
	Datum		transValue;		/* current transition value */
	bool		transValueIsNull;

	bool		noTransValue;	/* true if transValue not set yet */

	/*
	 * Note: noTransValue initially has the same value as transValueIsNull,
	 * and if true both are cleared to false at the same time.  They are not
	 * the same though: if transfn later returns a NULL, we want to keep that
	 * NULL and not auto-replace it with a later input value. Only the first
	 * non-NULL input will be auto-substituted.
	 */
} AggStatePerGroupData;

/*
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.  We compute the hash key from
 * the GROUP BY columns.
 */
typedef struct AggHashEntryData *AggHashEntry;

typedef struct AggHashEntryData
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	/* per-aggregate transition status array */
	AggStatePerGroupData pergroup[FLEXIBLE_ARRAY_MEMBER];
}	AggHashEntryData;


extern AggState *ExecInitAgg(Agg *node, EState *estate, int eflags,
							 PlanState *parent);
extern bool pushTupleToAgg(TupleTableSlot *slot, AggState *node);
extern void ExecEndAgg(AggState *node);
extern void ExecReScanAgg(AggState *node);

extern Size hash_agg_entry_size(int numAggs);

extern Datum aggregate_dummy(PG_FUNCTION_ARGS);

/* inline functions decls and implementations */
#pragma GCC diagnostic warning "-Winline"
static inline void finalize_aggregate(AggState *aggstate,
									  AggStatePerAgg peragg,
									  AggStatePerGroup pergroupstate,
									  Datum *resultVal, bool *resultIsNull);
static inline void finalize_aggregates(AggState *aggstate,
									   AggStatePerAgg peraggs,
									   AggStatePerGroup pergroup,
									   int currentSet);
static inline bool project_aggregates_and_push(AggState *aggstate);
static inline bool AggPushHashEntry(AggState *aggstate, AggHashEntry entry);

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 *
 * The finalfn uses the state as set in the transno. This also might be
 * being used by another aggregate function, so it's important that we do
 * nothing destructive here.
 */
static inline void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peragg,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	FunctionCallInfoData fcinfo;
	bool		anynull = false;
	MemoryContext oldContext;
	int			i;
	ListCell   *lc;
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Evaluate any direct arguments.  We do this even if there's no finalfn
	 * (which is unlikely anyway), so that side-effects happen as expected.
	 * The direct arguments go into arg positions 1 and up, leaving position 0
	 * for the transition state value.
	 */
	i = 1;
	foreach(lc, pertrans->aggdirectargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		fcinfo.arg[i] = ExecEvalExpr(expr,
									 aggstate->ss.ps.ps_ExprContext,
									 &fcinfo.argnull[i],
									 NULL);
		anynull |= fcinfo.argnull[i];
		i++;
	}

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peragg->finalfn_oid))
	{
		int			numFinalArgs = peragg->numFinalArgs;

		/* set up aggstate->curpertrans for AggGetAggref() */
		aggstate->curpertrans = pertrans;

		InitFunctionCallInfoData(fcinfo, &peragg->finalfn,
								 numFinalArgs,
								 pertrans->aggCollation,
								 (void *) aggstate, NULL);

		/* Fill in the transition state value */
		fcinfo.arg[0] = MakeExpandedObjectReadOnly(pergroupstate->transValue,
											 pergroupstate->transValueIsNull,
												   pertrans->transtypeLen);
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		anynull |= pergroupstate->transValueIsNull;

		/* Fill any remaining argument positions with nulls */
		for (; i < numFinalArgs; i++)
		{
			fcinfo.arg[i] = (Datum) 0;
			fcinfo.argnull[i] = true;
			anynull = true;
		}

		if (fcinfo.flinfo->fn_strict && anynull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
		aggstate->curpertrans = NULL;
	}
	else
	{
		/* Don't need MakeExpandedObjectReadOnly; datumCopy will copy it */
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peragg->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peragg->resulttypeByVal,
							   peragg->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Compute the final value of all aggregates for one group.
 *
 * This function handles only one grouping set at a time.
 *
 * Results are stored in the output econtext aggvalues/aggnulls.
 */
static inline void
finalize_aggregates(AggState *aggstate,
					AggStatePerAgg peraggs,
					AggStatePerGroup pergroup,
					int currentSet)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Datum	   *aggvalues = econtext->ecxt_aggvalues;
	bool	   *aggnulls = econtext->ecxt_aggnulls;
	int			aggno;

	Assert(currentSet == 0 ||
		   ((Agg *) aggstate->ss.ps.plan)->aggstrategy != AGG_HASHED);

	aggstate->current_set = currentSet;

	/*
	 * Run the final functions.
	 */
	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peragg = &peraggs[aggno];
		int			transno = peragg->transno;
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno + (currentSet * (aggstate->numtrans))];

		finalize_aggregate(aggstate, peragg, pergroupstate,
						   &aggvalues[aggno], &aggnulls[aggno]);
	}
}

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates), and push all tuples. Returns true if all tuples are
 * were pushed, false if the parent doesn't want to accept tuples anymore.
 */
static inline bool
project_aggregates_and_push(AggState *aggstate)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	PlanState *parent = aggstate->ss.ps.parent;
	bool parent_accepts_tuples = true;

	/*
	 * Check the qual (HAVING clause); if the group does not match, ignore it.
	 */
	if (ExecQual(aggstate->ss.ps.qual, econtext, false))
	{
		/*
		 * Form and return or store a projection tuple using the aggregate
		 * results and the representative input tuple.
		 */
		ExprDoneCond isDone;
		TupleTableSlot *result;

		result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			parent_accepts_tuples = pushTuple(result, parent,
											  (PlanState *) aggstate);
			while (parent_accepts_tuples && isDone == ExprMultipleResult)
			{
				result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);
				parent_accepts_tuples = pushTuple(result, parent,
												  (PlanState *) aggstate);
			}
		}
	}
	else
		InstrCountFiltered1(aggstate, 1);
	return parent_accepts_tuples;
}

/*
 * Finalize one AggHashEntry, project the result and push it. Returns true if
 * all tuples are were pushed, false if the parent doesn't want to accept
 * tuples anymore.
 */
static inline bool
AggPushHashEntry(AggState *aggstate, AggHashEntry entry)
{
	ExprContext *econtext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleTableSlot *firstSlot;

	/*
	 * get state info from node
	 */
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->ss.ps.ps_ExprContext;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * Clear the per-output-tuple context for each group
	 *
	 * We intentionally don't use ReScanExprContext here; if any aggs have
	 * registered shutdown callbacks, they mustn't be called yet, since we
	 * might not be done with that agg.
	 */
	ResetExprContext(econtext);

	/*
	 * Store the copied first input tuple in the tuple table slot reserved
	 * for it, so that it can be used in ExecProject.
	 */
	ExecStoreMinimalTuple(entry->shared.firstTuple,
						  firstSlot,
						  false);

	pergroup = entry->pergroup;

	finalize_aggregates(aggstate, peragg, pergroup, 0);

	/*
	 * Use the representative input tuple for any references to
	 * non-aggregated input columns in the qual and tlist.
	 */
	econtext->ecxt_outertuple = firstSlot;

	return project_aggregates_and_push(aggstate);
}

#endif   /* NODEAGG_H */
