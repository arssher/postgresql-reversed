/*-------------------------------------------------------------------------
 *
 * execExpr.c
 *	  Expression evaluation infrastructure.
 *
 *	Expression evaluation works by first converting the expression tree (which
 *	went through planning first) into an ExprState using ExecInitExpr() et
 *	al. This converts the tree into a opcode based program (ExprEvalStep
 *	representing individual instructions), allocated as a flat array of steps.
 *
 *	This flat representation has the big advantage that it can be implemented
 *	non-recursively, within a single function.  This allows to interpret
 *	larger expressions from largely within a single function, keeping state
 *	between them.  In contrast to that, tree-walk based approaches
 *	often run into performance issues due to function call / stack
 *	manipulation overhead.
 *
 *	The ExprEvalStep representation is designed to be usable for interpreting
 *	the expression, as well as compiling into native code. As much complexity
 *	as possible should be handled by ExecInitExpr() (and helpers), instead of
 *	execution time where both interpreted and compiled versions would need to
 *	deal with the complexity. Besides duplicating effort between execution
 *	approaches, runtime initialization checks also have a small but noticeable
 *	cost every time the expression is evaluated.
 *
 *	The next step is preparing the ExprState for execution, using
 *	ExecInstantiateExpr(). This is internally done by ExecInitExpr() and other
 *	functions that prepare for expression evaluation.  ExecInstantiateExpr()
 *	initializes the expression for the relevant method chosen to evaluate the
 *	expression.
 *
 *	Note that a lot of the more complex expression evaluation steps, which are
 *	less performance critical than some of the simpler and more common ones,
 *	are implemented as separate functions outside the fast-path of interpreted
 *	expression, like e.g. ExecEvalRow(), so that the implementation can be
 *	shared between interpreted and compiled expression evaluation.  That means
 *	that ExecInstantiateExpr() always has to initialize the expression for
 *	evaluation by execInterpExpr.c.  It also means that these helper functions
 *	are not "allowed" to perform dispatch themselves, as the method of
 *	dispatch will vary based on the caller.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execExpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execExpr.h"
#include "executor/execdebug.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


/*
 * Support for building execution state.
 */
static void ExecInstantiateExpr(ExprState *state);
static void ExecInitExprRec(Expr *node, PlanState *parent, ExprState *state,
				Datum *resv, bool *resnull);
static void ExprEvalPushStep(ExprState *es, ExprEvalStep *s);
static void ExecInitFunc(ExprEvalStep *scratch, Expr *node, List *args,
			 Oid funcid, Oid inputcollid, PlanState *parent,
			 ExprState *state, Datum *resv, bool *resnull);
static void ExecInitWholeRowVar(ExprEvalStep *scratch, Var *variable,
					PlanState *parent, ExprState *state,
					Datum *resv, bool *resnull);
static void ExecInitArrayRef(ExprEvalStep *scratch, ArrayRef *aref,
				 PlanState *parent, ExprState *state,
				 Datum *resv, bool *resnull);
static void ExecInitExprSlots(ExprState *state, Node *node);

/* support functions */
static bool isAssignmentIndirectionExpr(Expr *expr);


/*
 * ExecInitExpr: prepare an expression tree for execution
 *
 * This function builds and returns an ExprState implementing the given
 * Expr node tree.  The return ExprState can then be handed to ExecEvalExpr
 * for execution.  Because the Expr tree itself is read-only as far as
 * ExecInitExpr and ExecEvalExpr are concerned, several different executions
 * of the same plan tree can occur concurrently.
 *
 * This must be called in a memory context that will last as long as repeated
 * executions of the expression are needed.  Typically the context will be
 * the same as the per-query context of the associated ExprContext.
 *
 * Any Aggref, WindowFunc, or SubPlan nodes found in the tree are added to the
 * lists of such nodes held by the parent PlanState.
 *
 * Note: there is no ExecEndExpr function; we assume that any resource
 * cleanup needed will be handled by just releasing the memory context
 * in which the state tree is built.  Functions that require additional
 * cleanup work can register a shutdown callback in the ExprContext.
 *
 *	'node' is the root of the expression tree to examine
 *	'parent' is the PlanState node that owns the expression.
 *
 * 'parent' may be NULL if we are preparing an expression that is not
 * associated with a plan tree.  (If so, it can't have aggs or subplans.)
 * This case should usually come through ExecPrepareExpr, not directly here.
 */
ExprState *
ExecInitExpr(Expr *node, PlanState *parent)
{
	ExprState  *state = makeNode(ExprState);
	ExprEvalStep scratch;

	if (node == NULL)
		return NULL;

	state->expr = node;
	ExecInitExprSlots(state, (Node *) node);

	ExecInitExprRec(node, parent, state, &state->resvalue, &state->resnull);

	scratch.resvalue = &state->resvalue;
	scratch.resnull = &state->resnull;
	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecInstantiateExpr(state);

	return state;
}

/*
 * ExecInitQual: prepare a qual for execution
 *
 * Prepares for the evaluation of a conjunctive boolean expression (qual
 * list) that returns true iff none of the subexpressions are false.  (We
 * also return true if the list is empty.)
 *
 * If some of the subexpressions yield NULL, then the result of the
 * conjunction is false.  This makes this routine primarily useful for
 * evaluating WHERE clauses, since SQL specifies that tuples with null WHERE
 * results do not get selected.
 */
ExprState *
ExecInitQual(List *qual, PlanState *parent)
{
	ExprState  *state = makeNode(ExprState);
	ExprEvalStep scratch;
	ListCell   *lc;
	List	   *adjust_bailout = NIL;

	/* short-circuit (here and in ExecQual) for empty restriction list */
	if (qual == NULL)
		return NULL;

	Assert(IsA(qual, List));

	/*
	 * ExecQual() needs to return false for expression returning NULL. That
	 * allows to short-circuit the evaluation the first time a NULL is
	 * encountered.  As qual evaluation is a hot-path this warrants using a
	 * special opcode for qual evaluation that's simpler than BOOL_AND (which
	 * has more complex NULL handling).
	 */
	state->expr = (Expr *) qual;
	ExecInitExprSlots(state, (Node *) qual);

	scratch.opcode = EEOP_QUAL;
	scratch.resvalue = &state->resvalue;
	scratch.resnull = &state->resnull;

	foreach(lc, qual)
	{
		Expr	   *node = (Expr *) lfirst(lc);

		/* first evaluate expression */
		ExecInitExprRec(node, parent, state, &state->resvalue, &state->resnull);

		/* then check whether it's false or NULL */
		scratch.d.qualexpr.jumpdone = -1;
		ExprEvalPushStep(state, &scratch);
		adjust_bailout = lappend_int(adjust_bailout,
									 state->steps_len - 1);
	}

	/* adjust early bail out jump target */
	foreach(lc, adjust_bailout)
	{
		ExprEvalStep *as = &state->steps[lfirst_int(lc)];

		Assert(as->d.qualexpr.jumpdone == -1);
		as->d.qualexpr.jumpdone = state->steps_len;
	}

	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecInstantiateExpr(state);

	return state;
}

/*
 * ExecInitCheck: prepare a check constraint for execution
 *
 * Prepares for the evaluation of a conjunctive boolean expression (qual
 * list) that returns true iff none of the subexpressions are false.  (We
 * also return true if the list is empty.)
 *
 * If some of the subexpressions yield NULL, then the result of the
 * conjunction is true, since SQL specifies that NULL constraint conditions
 * are not failures.
 */
ExprState *
ExecInitCheck(List *qual, PlanState *parent)
{
	Expr	   *expr;

	if (qual == NULL)
		return NULL;

	Assert(IsA(qual, List));

	if (list_length(qual) == 1)
		expr = linitial(qual);
	else
	{
		/*
		 * Just whip-up a boolean AND expression, that behaves just as needed.
		 * It'd be valid to implement short-circuiting behaviour on NULLs, but
		 * that doesn't seem warranted.
		 */
		expr = makeBoolExpr(AND_EXPR, qual, -1);
	}

	return ExecInitExpr(expr, parent);
}

/* ----------------
 *		ExecBuildProjectionInfo
 *
 * Build a ProjectionInfo node for evaluating the given tlist in the given
 * econtext, and storing the result into the tuple slot.  (Caller must have
 * ensured that tuple slot has a descriptor matching the tlist!)  Note that
 * the given tlist should be a list of ExprState nodes, not Expr nodes.
 *
 * inputDesc can be NULL, but if it is not, we check to see whether simple
 * Vars in the tlist match the descriptor.  It is important to provide
 * inputDesc for relation-scan plan nodes, as a cross check that the relation
 * hasn't been changed since the plan was made.  At higher levels of a plan,
 * there is no need to recheck.
 *
 * This is implemented by internally building an ExprState that performs the
 * projection. That way faster implementations of expression evaluation, e.g
 * compiled to native code, can evaluate the whole projection in one go.
 * ----------------
 */
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						PlanState *parent,
						TupleDesc inputDesc)
{
	ProjectionInfo *projInfo = makeNode(ProjectionInfo);
	ExprEvalStep scratch;
	ListCell   *lc;
	ExprState  *state;

	projInfo->pi_exprContext = econtext;
	projInfo->pi_state.tag.type = T_ExprState;
	state = &projInfo->pi_state;
	state->expr = (Expr *) targetList;
	state->resultslot = slot;
	ExecInitExprSlots(state, (Node *) targetList);

	foreach(lc, targetList)
	{
		TargetEntry *tle;
		Var		   *variable = NULL;
		AttrNumber	attnum;
		bool		isSimpleVar = false;

		Assert(IsA(lfirst(lc), TargetEntry));

		tle = (TargetEntry *) lfirst(lc);

		if (tle->expr != NULL &&
			IsA(tle->expr, Var) &&
			((Var *) tle->expr)->varattno > 0)
		{
			variable = (Var *) tle->expr;
			attnum = variable->varattno;

			if (!inputDesc)
				isSimpleVar = true;		/* can't check type, assume OK */
			else if (variable->varattno <= inputDesc->natts)
			{
				Form_pg_attribute attr;

				attr = inputDesc->attrs[variable->varattno - 1];
				if (!attr->attisdropped && variable->vartype == attr->atttypid)
					isSimpleVar = true;
			}
		}

		if (isSimpleVar)
		{
			switch (variable->varno)
			{
				case INNER_VAR:	/* get the tuple from the inner node */
					scratch.opcode = EEOP_ASSIGN_INNER_VAR;
					break;

				case OUTER_VAR:	/* get the tuple from the outer node */
					scratch.opcode = EEOP_ASSIGN_OUTER_VAR;
					break;

					/* INDEX_VAR is handled by default case */
				default:		/* get the tuple from the relation being
								 * scanned */
					scratch.opcode = EEOP_ASSIGN_SCAN_VAR;
					break;
			}

			scratch.d.assign_var.attnum = attnum - 1;
			scratch.d.assign_var.resultnum = tle->resno - 1;
			ExprEvalPushStep(state, &scratch);
		}
		else
		{
			/*
			 * We can't directly point into the result slot for the contained
			 * expression, as the result slot (and the exprstate for that
			 * matter) can change below us. So we instead evaluate into a
			 * temporary value and then move.
			 */
			ExecInitExprRec(tle->expr, parent, state, &state->resvalue, &state->resnull);
			if (get_typlen(exprType((Node *) tle->expr)) == -1)
				scratch.opcode = EEOP_ASSIGN_TMP_UNEXPAND;
			else
				scratch.opcode = EEOP_ASSIGN_TMP;
			scratch.d.assign_tmp.resultnum = tle->resno - 1;
			ExprEvalPushStep(state, &scratch);
		}
	}

	scratch.resvalue = &state->resvalue;
	scratch.resnull = &state->resnull;
	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecInstantiateExpr(state);

	return projInfo;
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List *
ExecInitExprList(List *nodes, PlanState *parent)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, nodes)
	{
		Expr	   *e = lfirst(lc);

		result = lappend(result, ExecInitExpr(e, parent));
	}

	return result;
}

/*
 * ExecPrepareExpr --- initialize for expression execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitExpr in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we run the
 * passed expression tree through expression_planner() to prepare it for
 * execution.  (In ordinary Plan trees the regular planning process will have
 * made the appropriate transformations on expressions, but for standalone
 * expressions this won't have happened.)
 */
ExprState *
ExecPrepareExpr(Expr *node, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	node = expression_planner(node);

	result = ExecInitExpr(node, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * ExecPrepareQual --- initialize for qual execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitExpr in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we run the
 * passed expression tree through expression_planner() to prepare it for
 * execution.  (In ordinary Plan trees the regular planning process will have
 * made the appropriate transformations on expressions, but for standalone
 * expressions this won't have happened.)
 */
ExprState *
ExecPrepareQual(List *qual, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	qual = (List *) expression_planner((Expr *) qual);

	result = ExecInitQual(qual, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * ExecPrepareCheck -- initialize qual for execution outside a normal Plan
 * tree context.
 *
 * See ExecPrepareExpr() and ExecInitQual() for details.
 */
ExprState *
ExecPrepareCheck(List *qual, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	qual = (List *) expression_planner((Expr *) qual);

	result = ExecInitCheck(qual, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * Call ExecPrepareExpr() on each member of nodes, and return list of
 * ExprStates.
 *
 * See ExecPrepareExpr() for details.
 */
List *
ExecPrepareExprList(List *nodes, EState *estate)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, nodes)
	{
		Expr	   *e = lfirst(lc);

		result = lappend(result, ExecPrepareExpr(e, estate));
	}

	return result;
}

/*
 * ExecCheck - evaluate a check constraint prepared with ExecInitCheck
 * (possibly via ExecPrepareCheck).
 */
bool
ExecCheck(ExprState *state, ExprContext *econtext)
{
	bool		isnull;
	Datum		ret;

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	if (isnull)
		return true;
	return DatumGetBool(ret);
}

/*
 * Prepare an expression for execution.  This has to be called for every
 * ExprState before it can be executed.
 *
 * NB: While this currently only calls ExecInstantiateInterpretedExpr(), this
 * will likely get extended to further expression evaluation methods.
 * Therefore this should be used instead of ExecInstantiateInterpretedExpr().
 */
static void
ExecInstantiateExpr(ExprState *state)
{
	ExecInstantiateInterpretedExpr(state);
}

/*
 * Append evaluation of node to ExprState, possibly recursing into
 * sub-expressions of node.
 */
static void
ExecInitExprRec(Expr *node, PlanState *parent, ExprState *state, Datum *resv, bool *resnull)
{
	ExprEvalStep scratch;

	/*
	 * Guard against stack overflow due to overly complex expressions. Because
	 * expression evaluation is not recursive, but expression planning is,
	 * we'll hit stack limits due to recursion either here, or when recursing
	 * into a separate expression evaluation inside a function call.  This
	 * lets us avoid repeatedly doing the quite expensive stack depth checks
	 * during expression evaluation.
	 */
	check_stack_depth();

	Assert(resv != NULL && resnull != NULL);
	scratch.resvalue = resv;
	scratch.resnull = resnull;

	/* cases should be ordered as they are in enum NodeTag */
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *variable = (Var *) node;

				/* varattno == InvalidAttrNumber means it's a whole-row Var */
				if (variable->varattno == InvalidAttrNumber)
				{
					ExecInitWholeRowVar(&scratch, variable, parent, state, resv, resnull);

					scratch.opcode = EEOP_WHOLEROW;
				}
				else if (variable->varattno <= 0)
				{
					scratch.d.var.attnum = variable->varattno;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_SYSVAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_SYSVAR;
							break;
						default:
							scratch.opcode = EEOP_SCAN_SYSVAR;
							break;
					}
				}
				else
				{
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_VAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_VAR;
							break;
						default:
							scratch.opcode = EEOP_SCAN_VAR;
							break;
					}
					scratch.d.var.attnum = variable->varattno - 1;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Const:
			{
				Const	   *con = (Const *) node;

				scratch.opcode = EEOP_CONST;
				scratch.d.constval.value = con->constvalue;
				scratch.d.constval.isnull = con->constisnull;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Param:
			{
				Param	   *param = (Param *) node;

				switch (param->paramkind)
				{
					case PARAM_EXEC:
						{
							scratch.opcode = EEOP_PARAM_EXEC;
							scratch.d.param.paramid = param->paramid;
							scratch.d.param.paramtype = InvalidOid;
							break;
						}
					case PARAM_EXTERN:
						{
							scratch.opcode = EEOP_PARAM_EXTERN;
							scratch.d.param.paramid = param->paramid;
							scratch.d.param.paramtype = param->paramtype;
							break;
						}
					default:
						elog(ERROR, "unrecognized paramkind: %d",
							 (int) ((Param *) node)->paramkind);
						break;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				AggrefExprState *astate = makeNode(AggrefExprState);

				scratch.opcode = EEOP_AGGREF;
				scratch.d.aggref.astate = astate;
				astate->aggref = aggref;
				if (parent && IsA(parent, AggState))
				{
					AggState   *aggstate = (AggState *) parent;

					aggstate->aggs = lcons(astate, aggstate->aggs);
					aggstate->numaggs++;
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "Aggref found in non-Agg plan node");
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_GroupingFunc:
			{
				GroupingFunc *grp_node = (GroupingFunc *) node;
				Agg		   *agg = NULL;

				if (!parent || !IsA(parent, AggState) ||!IsA(parent->plan, Agg))
					elog(ERROR, "parent of GROUPING is not Agg node");

				scratch.opcode = EEOP_GROUPING_FUNC;
				scratch.d.grouping_func.parent = (AggState *) parent;

				agg = (Agg *) (parent->plan);

				if (agg->groupingSets)
					scratch.d.grouping_func.clauses = grp_node->cols;
				else
					scratch.d.grouping_func.clauses = NIL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				WindowFuncExprState *wfstate = makeNode(WindowFuncExprState);

				wfstate->wfunc = wfunc;

				if (parent && IsA(parent, WindowAggState))
				{
					WindowAggState *winstate = (WindowAggState *) parent;
					int			nfuncs;

					winstate->funcs = lcons(wfstate, winstate->funcs);
					nfuncs = ++winstate->numfuncs;
					if (wfunc->winagg)
						winstate->numaggs++;

					/* for now intialize agg using old style expressions */
					wfstate->args = NIL;
					wfstate->args = ExecInitExprList(wfunc->args, parent);
					wfstate->aggfilter = ExecInitExpr(wfunc->aggfilter,
													  parent);

					/*
					 * Complain if the windowfunc's arguments contain any
					 * windowfuncs; nested window functions are semantically
					 * nonsensical.  (This should have been caught earlier,
					 * but we defend against it here anyway.)
					 */
					if (nfuncs != winstate->numfuncs)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
						  errmsg("window function calls cannot be nested")));
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "WindowFunc found in non-WindowAgg plan node");
				}

				scratch.opcode = EEOP_WINDOW_FUNC;
				scratch.d.window_func.wfstate = wfstate;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				ExecInitArrayRef(&scratch, aref, parent, state, resv, resnull);
				break;
			}

		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;

				ExecInitFunc(&scratch, node, func->args, func->funcid, func->inputcollid,
							 parent, state, resv, resnull);
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;

				ExecInitFunc(&scratch, node, op->args, op->opfuncid, op->inputcollid,
							 parent, state, resv, resnull);
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_DistinctExpr:
			{
				DistinctExpr *op = (DistinctExpr *) node;

				ExecInitFunc(&scratch, node, op->args, op->opfuncid, op->inputcollid,
							 parent, state, resv, resnull);

				/*
				 * Can't use normal function call, override opcode for
				 * DISTINCT
				 */

				/*
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation
				 */
				scratch.opcode = EEOP_DISTINCT;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_NullIfExpr:
			{
				NullIfExpr *op = (NullIfExpr *) node;

				ExecInitFunc(&scratch, node, op->args, op->opfuncid, op->inputcollid,
							 parent, state, resv, resnull);

				/* Can't use normal function call, override opcode for NULL() */

				/*
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation
				 */
				scratch.opcode = EEOP_NULLIF;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) node;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;

				finfo = palloc0(sizeof(FmgrInfo));
				fcinfo = palloc0(sizeof(FunctionCallInfoData));
				fmgr_info(opexpr->opfuncid, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 opexpr->inputcollid, NULL, NULL);

				scratch.opcode = EEOP_SCALARARRAYOP;
				scratch.d.scalararrayop.opexpr = opexpr;
				scratch.d.scalararrayop.finfo = finfo;
				scratch.d.scalararrayop.fcinfo_data = fcinfo;
				scratch.d.scalararrayop.fn_addr = fcinfo->flinfo->fn_addr;

				Assert(fcinfo->nargs == 2);
				Assert(list_length(opexpr->args) == 2);

				/* evaluate scalar directly into function argument */
				ExecInitExprRec((Expr *) linitial(opexpr->args), parent, state,
								&fcinfo->arg[0], &fcinfo->argnull[0]);

				/*
				 * Evaluate array argument into our return value, overwrite
				 * with comparison results afterwards.
				 */
				ExecInitExprRec((Expr *) lsecond(opexpr->args), parent, state,
								resv, resnull);

				scratch.d.scalararrayop.element_type = InvalidOid;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				ListCell   *lc;
				List	   *adjust_bailout = NIL;
				int			nargs = list_length(boolexpr->args);
				int			off = 0;

				/* allocate scratch memory used by all steps */
				scratch.d.boolexpr.value = palloc0(sizeof(Datum));
				scratch.d.boolexpr.isnull = palloc0(sizeof(bool));
				scratch.d.boolexpr.anynull = palloc0(sizeof(bool));

				/*
				 * For each argument evaluate the argument itself, then
				 * perform the bool operation's appropriate handling.
				 */
				foreach(lc, boolexpr->args)
				{
					Expr	   *arg = (Expr *) lfirst(lc);

					switch (boolexpr->boolop)
					{
						case AND_EXPR:
							Assert(list_length(boolexpr->args) >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_AND_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_AND_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_AND_STEP;
							break;
						case OR_EXPR:
							Assert(list_length(boolexpr->args) >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_OR_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_OR_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_OR_STEP;
							break;
						case NOT_EXPR:
							Assert(list_length(boolexpr->args) == 1);

							scratch.opcode = EEOP_BOOL_NOT_STEP;
							break;
						default:
							elog(ERROR, "unrecognized boolop: %d",
								 (int) boolexpr->boolop);
							break;
					}

					ExecInitExprRec(arg, parent, state,
									scratch.d.boolexpr.value,
									scratch.d.boolexpr.isnull);
					scratch.d.boolexpr.jumpdone = -1;
					ExprEvalPushStep(state, &scratch);
					adjust_bailout = lappend_int(adjust_bailout,
												 state->steps_len - 1);
					off++;
				}

				/* adjust early bail out jump target */
				foreach(lc, adjust_bailout)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.boolexpr.jumpdone == -1);
					as->d.boolexpr.jumpdone = state->steps_len;
				}

				break;
			}

		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;
				SubPlanState *sstate;

				if (!parent)
					elog(ERROR, "SubPlan found with no parent plan");

				sstate = ExecInitSubPlan(subplan, parent);

				/* Add SubPlanState nodes to parent->subPlan */
				parent->subPlan = lappend(parent->subPlan, sstate);

				scratch.opcode = EEOP_SUBPLAN;
				scratch.d.subplan.sstate = sstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_AlternativeSubPlan:
			{
				AlternativeSubPlan *asplan = (AlternativeSubPlan *) node;
				AlternativeSubPlanState *asstate;

				if (!parent)
					elog(ERROR, "AlternativeSubPlan found with no parent plan");

				asstate = ExecInitAlternativeSubPlan(asplan, parent);

				scratch.opcode = EEOP_ALTERNATIVE_SUBPLAN;
				scratch.d.alternative_subplan.asstate = asstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;


				/* evaluate argument */
				ExecInitExprRec(fselect->arg, parent, state, resv, resnull);

				scratch.opcode = EEOP_FIELDSELECT;
				scratch.d.fieldselect.fieldnum = fselect->fieldnum;
				scratch.d.fieldselect.resulttype = fselect->resulttype;
				scratch.d.fieldselect.argdesc = NULL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				ListCell   *l1,
						   *l2;
				Datum	   *values;
				bool	   *nulls;
				TupleDesc  *descp;

				/* FIXME: properly size workspace */
				values = (Datum *) palloc(sizeof(Datum) * MaxTupleAttributeNumber);
				nulls = (bool *) palloc(sizeof(bool) * MaxTupleAttributeNumber);
				descp = (TupleDesc *) palloc(sizeof(TupleDesc));
				*descp = NULL;

				/* prepare argument evaluation */
				ExecInitExprRec(fstore->arg, parent, state, resv, resnull);

				/* first deform the input tuple */
				scratch.opcode = EEOP_FIELDSTORE_DEFORM;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;
				ExprEvalPushStep(state, &scratch);

				/* evaluate new values, one step for each arg */
				forboth(l1, fstore->newvals, l2, fstore->fieldnums)
				{
					Expr	   *e = (Expr *) lfirst(l1);
					AttrNumber	fieldnum = lfirst_int(l2);
					Datum	   *save_innermost_caseval = NULL;
					bool	   *save_innermost_casenull = NULL;

					/*
					 * Use the CaseTestExpr mechanism to pass down the old
					 * value of the field being replaced; this is needed in
					 * case the newval is itself a FieldStore or ArrayRef that
					 * has to obtain and modify the old value.  It's safe to
					 * reuse the CASE mechanism because there cannot be a CASE
					 * between here and where the value would be needed, and a
					 * field assignment can't be within a CASE either.  (So
					 * saving and restoring the caseValue is just paranoia,
					 * but let's do it anyway.)
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = &values[fieldnum - 1];
					state->innermost_casenull = &nulls[fieldnum - 1];

					ExecInitExprRec(e, parent, state,
									&values[fieldnum - 1],
									&nulls[fieldnum - 1]);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;
				}

				/* then form result tuple */
				scratch.opcode = EEOP_FIELDSTORE_FORM;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RelabelType:
			{
				/* at runtime relabel doesn't need to do anything */
				RelabelType *relabel = (RelabelType *) node;

				ExecInitExprRec(relabel->arg, parent, state, resv, resnull);
				break;
			}

		case T_CoerceViaIO:
			{
				CoerceViaIO *iocoerce = (CoerceViaIO *) node;
				Oid			iofunc;
				bool		typisvarlena;

				/* evaluate argument */
				ExecInitExprRec(iocoerce->arg, parent, state, resv, resnull);

				/*
				 * Compute coercion by preparing both output / input calls, to
				 * be evaluated inside a single evaluation step for speed -
				 * this can be a very common operation.
				 */
				scratch.opcode = EEOP_IOCOERCE;

				/* lookup the input type's output function */
				scratch.d.iocoerce.finfo_out = palloc0(sizeof(*scratch.d.iocoerce.finfo_out));
				scratch.d.iocoerce.fcinfo_data_out = palloc0(sizeof(*scratch.d.iocoerce.fcinfo_data_out));

				getTypeOutputInfo(exprType((Node *) iocoerce->arg),
								  &iofunc, &typisvarlena);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_out);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_out);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_out,
										 scratch.d.iocoerce.finfo_out,
										 1, InvalidOid, NULL, NULL);

				/* lookup the result type's input function */
				scratch.d.iocoerce.finfo_in = palloc0(sizeof(*scratch.d.iocoerce.finfo_in));
				scratch.d.iocoerce.fcinfo_data_in = palloc0(sizeof(*scratch.d.iocoerce.fcinfo_data_in));

				getTypeInputInfo(iocoerce->resulttype, &iofunc,
								 &scratch.d.iocoerce.intypioparam);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_in);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_in);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_in,
										 scratch.d.iocoerce.finfo_in,
										 3, InvalidOid, NULL, NULL);

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acoerce = (ArrayCoerceExpr *) node;

				/* evaluate argument */
				ExecInitExprRec(acoerce->arg, parent, state, resv, resnull);

				scratch.opcode = EEOP_ARRAYCOERCE;
				scratch.d.arraycoerce.coerceexpr = acoerce;
				scratch.d.arraycoerce.resultelemtype =
					get_element_type(acoerce->resulttype);
				if (scratch.d.arraycoerce.resultelemtype == InvalidOid)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("target type is not an array")));
				/* Arrays over domains aren't supported yet */
				Assert(getBaseType(scratch.d.arraycoerce.resultelemtype) ==
					   scratch.d.arraycoerce.resultelemtype);
				scratch.d.arraycoerce.elemfunc =
					(FmgrInfo *) palloc(sizeof(FmgrInfo));
				scratch.d.arraycoerce.elemfunc->fn_oid =
					InvalidOid; /* not initialized */
				scratch.d.arraycoerce.amstate =
					(ArrayMapState *) palloc0(sizeof(ArrayMapState));

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;

				/* evaluate argument */
				ExecInitExprRec(convert->arg, parent, state, resv, resnull);

				/* and push conversion step */
				scratch.opcode = EEOP_CONVERT_ROWTYPE;
				scratch.d.convert_rowtype.convert = convert;
				scratch.d.convert_rowtype.indesc = NULL;
				scratch.d.convert_rowtype.outdesc = NULL;
				scratch.d.convert_rowtype.map = NULL;
				scratch.d.convert_rowtype.initialized = false;

				ExprEvalPushStep(state, &scratch);
				break;
			}

			/* note that CaseWhen expressions are handled within this block */
		case T_CaseExpr:
			{
				CaseExpr   *caseExpr = (CaseExpr *) node;
				ListCell   *clause;
				List	   *adjust_bailout = NIL;
				ListCell   *lc;
				Datum	   *casevalue = palloc0(sizeof(Datum));
				bool	   *caseisnull = palloc0(sizeof(bool));
				Datum	   *save_innermost_caseval = NULL;
				bool	   *save_innermost_casenull = NULL;
				Datum	   *caseval = NULL;
				bool	   *casenull = NULL;

				/* arg == NULL -> CASE WHEN foo */
				/* arg != NULL -> CASE foo WHEN blarg */
				if (caseExpr->arg != NULL)
				{
					caseval = palloc0(sizeof(Datum));
					casenull = palloc0(sizeof(bool));

					ExecInitExprRec(caseExpr->arg, parent, state,
									caseval, casenull);
				}

				/*
				 * If there's a test expression, we have to evaluate it and
				 * save the value where the CaseTestExpr placeholders can find
				 * it.  We must save and restore prior setting of caseValue
				 * fields, in case this node is itself within a larger CASE.
				 *
				 * If there's no test expression, we don't actually need to
				 * save and restore these fields; but it's less code to just
				 * do so unconditionally.
				 */


				/*
				 * Prepare to evaluate each of the WHEN clauses in turn, as
				 * soon as one is true we return the corresponding result; and
				 * If none are true then we return the value of the default
				 * clause, or NULL if there is none.
				 */
				foreach(clause, caseExpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(clause);
					int			whenstep;

					/* evaluate condition */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = caseval;
					state->innermost_casenull = casenull;

					ExecInitExprRec(when->expr, parent, state,
									casevalue, caseisnull);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;

					scratch.opcode = EEOP_CASE_WHEN_STEP;
					scratch.d.casewhen.value = casevalue;
					scratch.d.casewhen.isnull = caseisnull;
					scratch.d.casewhen.jumpfalse = -1;	/* computed later */
					ExprEvalPushStep(state, &scratch);
					whenstep = state->steps_len - 1;

					/* evaluate result */
					ExecInitExprRec(when->result, parent, state, resv, resnull);

					scratch.opcode = EEOP_CASE_THEN_STEP;
					scratch.d.casewhen.value = casevalue;
					scratch.d.casewhen.isnull = caseisnull;
					scratch.d.casethen.jumpdone = -1;	/* computed later */
					ExprEvalPushStep(state, &scratch);

					/*
					 * Don't know "address" of jump target yet, compute once
					 * the whole case expression is built.
					 */
					adjust_bailout = lappend_int(adjust_bailout,
												 state->steps_len - 1);

					/* adjust jump target for WHEN step, for the !match case */
					state->steps[whenstep].d.casewhen.jumpfalse = state->steps_len;
				}

				if (caseExpr->defresult)
				{
					/* evaluate result, directly into result datum */
					ExecInitExprRec(caseExpr->defresult, parent, state,
									resv, resnull);
				}
				else
				{
					/* statically return NULL */
					scratch.opcode = EEOP_CONST;
					scratch.d.constval.isnull = true;
					scratch.d.constval.value = 0;
					ExprEvalPushStep(state, &scratch);
				}

				/* adjust early bail out jump target */
				foreach(lc, adjust_bailout)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.casethen.jumpdone == -1);
					as->d.casethen.jumpdone = state->steps_len;
				}

				break;
			}

		case T_CaseTestExpr:
			{
				CaseTestExpr *casetestexpr = (CaseTestExpr *) node;

				scratch.d.casetest.value = state->innermost_caseval;
				scratch.d.casetest.isnull = state->innermost_casenull;

				/* only check for extended datums if possible */
				if (get_typlen(casetestexpr->typeId) == -1)
					scratch.opcode = EEOP_CASE_TESTVAL_UNEXPAND;
				else
					scratch.opcode = EEOP_CASE_TESTVAL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				int			nelems = list_length(arrayexpr->elements);
				ListCell   *lc;
				int			elemoff;

				/*
				 * Evaluate by computing each element, and then forming the
				 * array.
				 */
				scratch.opcode = EEOP_ARRAYEXPR;
				scratch.d.arrayexpr.arrayexpr = arrayexpr;
				scratch.d.arrayexpr.nelems = nelems;
				scratch.d.arrayexpr.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.arrayexpr.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);

				/* do one-time catalog lookup for type info */
				get_typlenbyvalalign(arrayexpr->element_typeid,
									 &scratch.d.arrayexpr.elemlength,
									 &scratch.d.arrayexpr.elembyval,
									 &scratch.d.arrayexpr.elemalign);

				/* prepare to evaluate all arguments */
				elemoff = 0;
				foreach(lc, arrayexpr->elements)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, parent, state,
									&scratch.d.arrayexpr.elemvalues[elemoff],
									&scratch.d.arrayexpr.elemnulls[elemoff]);
					elemoff++;
				}

				/* and then to collect collect all into an array */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				int			nelems = list_length(rowexpr->args);
				TupleDesc	tupdesc;
				Form_pg_attribute *attrs;
				int			i;
				ListCell   *l;

				/*
				 * Evaluate by first building datums for each field, and then
				 * a final step forming the composite datum.
				 */
				scratch.opcode = EEOP_ROW;

				/* space for the individual field datums */
				scratch.d.row.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.row.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);

				/* Build tupdesc to describe result tuples */
				if (rowexpr->row_typeid == RECORDOID)
				{
					/* generic record, use types of given expressions */
					tupdesc = ExecTypeFromExprList(rowexpr->args);
				}
				else
				{
					/* it's been cast to a named type, use that */
					tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
				}

				scratch.d.row.tupdesc = tupdesc;

				/* In either case, adopt RowExpr's column aliases */
				ExecTypeSetColNames(tupdesc, rowexpr->colnames);
				/* Bless the tupdesc in case it's now of type RECORD */
				BlessTupleDesc(tupdesc);

				/* Set up evaluation, skipping any deleted columns */
				attrs = tupdesc->attrs;
				i = 0;
				foreach(l, rowexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(l);

					if (!attrs[i]->attisdropped)
					{
						/*
						 * Guard against ALTER COLUMN TYPE on rowtype since
						 * the RowExpr was created.  XXX should we check
						 * typmod too?	Not sure we can be sure it'll be the
						 * same.
						 */
						if (exprType((Node *) e) != attrs[i]->atttypid)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("ROW() column has type %s instead of type %s",
										format_type_be(exprType((Node *) e)),
									   format_type_be(attrs[i]->atttypid))));
					}
					else
					{
						/*
						 * Ignore original expression and insert a NULL. We
						 * don't really care what type of NULL it is, so
						 * always make an int4 NULL.
						 */
						e = (Expr *) makeNullConst(INT4OID, -1, InvalidOid);
					}

					ExecInitExprRec(e, parent, state,
									&scratch.d.row.elemvalues[i],
									&scratch.d.row.elemnulls[i]);
					i++;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				int			nopers = list_length(rcexpr->opnos);
				List	   *adjust_bailout = NIL;
				ListCell   *l_left_expr,
						   *l_right_expr,
						   *l_opno,
						   *l_opfamily,
						   *l_inputcollid;
				ListCell   *lc;
				int			off;

				/*
				 * Iterate over each field, prepare comparisons. To handle
				 * NULL results, prepare jumps to after the expression. If
				 * expression yields a != 0 result, jump to the final step.
				 */
				Assert(list_length(rcexpr->largs) == nopers);
				Assert(list_length(rcexpr->rargs) == nopers);

				off = 0;
				for (off = 0,
					 l_left_expr = list_head(rcexpr->largs),
					 l_right_expr = list_head(rcexpr->rargs),
					 l_opno = list_head(rcexpr->opnos),
					 l_opfamily = list_head(rcexpr->opfamilies),
					 l_inputcollid = list_head(rcexpr->inputcollids);
					 off < nopers;
					 off++,
					 l_left_expr = lnext(l_left_expr),
					 l_right_expr = lnext(l_right_expr),
					 l_opno = lnext(l_opno),
					 l_opfamily = lnext(l_opfamily),
					 l_inputcollid = lnext(l_inputcollid))
				{
					Expr	   *left_expr = (Expr *) lfirst(l_left_expr);
					Expr	   *right_expr = (Expr *) lfirst(l_right_expr);
					Oid			opno = lfirst_oid(l_opno);
					Oid			opfamily = lfirst_oid(l_opfamily);
					Oid			inputcollid = lfirst_oid(l_inputcollid);
					int			strategy;
					Oid			lefttype;
					Oid			righttype;
					Oid			proc;
					FmgrInfo   *finfo;
					FunctionCallInfo fcinfo;

					get_op_opfamily_properties(opno, opfamily, false,
											   &strategy,
											   &lefttype,
											   &righttype);
					proc = get_opfamily_proc(opfamily,
											 lefttype,
											 righttype,
											 BTORDER_PROC);

					/* Set up the primary fmgr lookup information */
					finfo = palloc0(sizeof(FmgrInfo));
					fcinfo = palloc0(sizeof(FunctionCallInfoData));
					fmgr_info(proc, finfo);
					fmgr_info_set_expr((Node *) node, finfo);
					InitFunctionCallInfoData(*fcinfo, finfo, 2, inputcollid, NULL, NULL);

					/*
					 * If we enforced permissions checks on index support
					 * functions, we'd need to make a check here.  But the
					 * index support machinery doesn't do that, and thus
					 * neither does this code.
					 */

					/* evaluate left and right expression directly into fcinfo */
					ExecInitExprRec(left_expr, parent, state,
									&fcinfo->arg[0], &fcinfo->argnull[0]);
					ExecInitExprRec(right_expr, parent, state,
									&fcinfo->arg[1], &fcinfo->argnull[1]);

					scratch.opcode = EEOP_ROWCOMPARE_STEP;

					/* jump targets computed later */
					scratch.d.rowcompare_step.jumpnull = -1;
					scratch.d.rowcompare_step.jumpdone = -1;

					scratch.d.rowcompare_step.finfo = finfo;
					scratch.d.rowcompare_step.fcinfo_data = fcinfo;
					scratch.d.rowcompare_step.fn_addr = fcinfo->flinfo->fn_addr;

					ExprEvalPushStep(state, &scratch);
					adjust_bailout = lappend_int(adjust_bailout,
												 state->steps_len - 1);
				}

				/* and then compare the last result */
				scratch.opcode = EEOP_ROWCOMPARE_FINAL;
				scratch.d.rowcompare_final.rctype = rcexpr->rctype;
				ExprEvalPushStep(state, &scratch);

				/* adjust early bail out jump targets */
				foreach(lc, adjust_bailout)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.rowcompare_step.jumpdone == -1);
					Assert(as->d.rowcompare_step.jumpnull == -1);

					/* jump to comparison evaluation */
					as->d.rowcompare_step.jumpdone = state->steps_len - 1;
					/* jump to the following expression */
					as->d.rowcompare_step.jumpnull = state->steps_len;
				}

				break;
			}

		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				List	   *adjust_bailout = NIL;
				ListCell   *lc;

				Assert(list_length(coalesce->args) > 0);

				/*
				 * Prepare evaluation of all coalesced arguments, after each
				 * push a step that short-circuits if not null.
				 */
				foreach(lc, coalesce->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					/* evaluate result, directly into result datum */
					ExecInitExprRec(e, parent, state, resv, resnull);

					/* then push step checking for NULLs */
					scratch.opcode = EEOP_COALESCE;
					scratch.d.coalesce.jumpdone = -1;	/* adjust later */
					ExprEvalPushStep(state, &scratch);

					adjust_bailout = lappend_int(adjust_bailout,
												 state->steps_len - 1);
				}

				/*
				 * No need to add a constant NULL return - we only can get to
				 * the end of the expression if a NULL already is being
				 * returned.
				 */

				/* adjust early bail out jump target */
				foreach(lc, adjust_bailout)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.coalesce.jumpdone == -1);
					as->d.coalesce.jumpdone = state->steps_len;
				}

				break;
			}

		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
				int			nelems = list_length(minmaxexpr->args);
				TypeCacheEntry *typentry;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;
				ListCell   *lc;
				int			off;

				/* Look up the btree comparison function for the datatype */
				typentry = lookup_type_cache(minmaxexpr->minmaxtype,
											 TYPECACHE_CMP_PROC);
				if (!OidIsValid(typentry->cmp_proc))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("could not identify a comparison function for type %s",
									format_type_be(minmaxexpr->minmaxtype))));

				/*
				 * If we enforced permissions checks on index support
				 * functions, we'd need to make a check here.  But the index
				 * support machinery doesn't do that, and thus neither does
				 * this code.
				 */
				finfo = palloc0(sizeof(FmgrInfo));
				fcinfo = palloc0(sizeof(FunctionCallInfoData));
				fmgr_info(typentry->cmp_proc, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 minmaxexpr->inputcollid, NULL, NULL);

				scratch.opcode = EEOP_MINMAX;
				/* allocate space to store arguments */
				scratch.d.minmax.values =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.minmax.nulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.minmax.nelems = nelems;
				scratch.d.minmax.op = minmaxexpr->op;

				scratch.d.minmax.finfo = finfo;
				scratch.d.minmax.fcinfo_data = fcinfo;

				/* evaluate expressions into minmax->values/nulls */
				off = 0;
				foreach(lc, minmaxexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, parent, state,
									&scratch.d.minmax.values[off],
									&scratch.d.minmax.nulls[off]);
					off++;
				}

				/* and push the final comparison */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_SQLValueFunction:
			{
				SQLValueFunction *svf = (SQLValueFunction *) node;

				scratch.opcode = EEOP_SQLVALUEFUNCTION;
				scratch.d.sqlvaluefunction.svf = svf;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;
				ListCell   *arg;
				int			nnamed = list_length(xexpr->named_args);
				int			nargs = list_length(xexpr->args);
				int			off;

				scratch.opcode = EEOP_XMLEXPR;
				scratch.d.xmlexpr.xexpr = xexpr;

				/* allocate space for storing all the arguments */
				if (nnamed)
				{
					scratch.d.xmlexpr.named_argvalue =
						(Datum *) palloc(sizeof(Datum) * nnamed);
					scratch.d.xmlexpr.named_argnull =
						(bool *) palloc(sizeof(bool) * nnamed);
				}
				else
				{
					scratch.d.xmlexpr.named_argvalue = NULL;
					scratch.d.xmlexpr.named_argnull = NULL;
				}

				if (nargs)
				{
					scratch.d.xmlexpr.argvalue =
						(Datum *) palloc(sizeof(Datum) * nargs);
					scratch.d.xmlexpr.argnull =
						(bool *) palloc(sizeof(bool) * nargs);
				}
				else
				{
					scratch.d.xmlexpr.argvalue = NULL;
					scratch.d.xmlexpr.argnull = NULL;
				}

				/* prepare argument execution */
				off = 0;
				foreach(arg, xexpr->named_args)
				{
					Expr	   *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, parent, state,
									&scratch.d.xmlexpr.named_argvalue[off],
									&scratch.d.xmlexpr.named_argnull[off]);
					off++;
				}

				off = 0;
				foreach(arg, xexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, parent, state,
									&scratch.d.xmlexpr.argvalue[off],
									&scratch.d.xmlexpr.argnull[off]);
					off++;
				}

				/* and evaluate the actual XML expression */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;

				if (ntest->nulltesttype == IS_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNULL;
				}
				else if (ntest->nulltesttype == IS_NOT_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNOTNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
				}
				else
				{
					elog(ERROR, "unrecognized nulltesttype: %d",
						 (int) ntest->nulltesttype);
				}

				/* first evaluate argument */
				ExecInitExprRec(ntest->arg, parent, state,
								resv, resnull);

				/* then push the test of that argument */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;

				/*
				 * Evaluate argument, directly into result datum. That's a bit
				 * debatable, because the types will be different, but it's
				 * efficient...  The evaluation step will then store an actual
				 * boolean.
				 */
				ExecInitExprRec(btest->arg, parent, state, resv, resnull);

				switch (btest->booltesttype)
				{
					case IS_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_TRUE;
						break;
					case IS_NOT_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_TRUE;
						break;
					case IS_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_FALSE;
						break;
					case IS_NOT_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_FALSE;
						break;
					case IS_UNKNOWN:
						scratch.opcode = EEOP_BOOLTEST_IS_UNKNOWN;
						break;
					case IS_NOT_UNKNOWN:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_UNKNOWN;
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) btest->booltesttype);
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;
				Datum	   *save_innermost_domainval = NULL;
				bool	   *save_innermost_domainnull = NULL;
				DomainConstraintRef *constraint_ref =
				palloc(sizeof(DomainConstraintRef));
				ListCell   *l;

				scratch.d.domaincheck.resulttype = ctest->resulttype;
				scratch.d.domaincheck.checkvalue = (Datum *) palloc(sizeof(Datum));
				scratch.d.domaincheck.checknull = (bool *) palloc(sizeof(bool));

				/* evaluate argument */
				ExecInitExprRec(ctest->arg, parent, state, resv, resnull);

				/*
				 * XXX: In contrast to the old implementation we're evaluating
				 * the set of to-be-checked constraints at query start - that
				 * seems perfectly sensible to me.  But perhaps there's a
				 * reason the previous implementation did what it did? ISTM
				 * that was just a side-effect of using the typecache (which
				 * is longer lived than a single query).
				 */

				/* Make sure we have up-to-date constraints */
				InitDomainConstraintRef(ctest->resulttype,
										constraint_ref,
										CurrentMemoryContext);
				UpdateDomainConstraintRef(constraint_ref);

				/*
				 * Set up value to be returned by CoerceToDomainValue nodes.
				 * We must save and restore innermost_domainval/null fields,
				 * in case this node is itself within a check expression for
				 * another domain.
				 */
				save_innermost_domainval = state->innermost_domainval;
				save_innermost_domainnull = state->innermost_domainnull;
				state->innermost_domainval = resv;
				state->innermost_domainnull = resnull;

				foreach(l, constraint_ref->constraints)
				{
					DomainConstraintState *con = (DomainConstraintState *) lfirst(l);

					scratch.d.domaincheck.constraintname = con->name;

					switch (con->constrainttype)
					{
						case DOM_CONSTRAINT_NOTNULL:
							scratch.opcode = EEOP_DOMAIN_NOTNULL;
							ExprEvalPushStep(state, &scratch);
							break;
						case DOM_CONSTRAINT_CHECK:
							/* evaluate check expression value */
							ExecInitExprRec(con->check_expr, parent, state,
											scratch.d.domaincheck.checkvalue,
											scratch.d.domaincheck.checknull);

							/* and then check result value */
							scratch.opcode = EEOP_DOMAIN_CHECK;
							ExprEvalPushStep(state, &scratch);
							break;
						default:
							elog(ERROR, "unrecognized constraint type: %d",
								 (int) con->constrainttype);
							break;
					}
				}

				state->innermost_domainval = save_innermost_domainval;
				state->innermost_domainnull = save_innermost_domainnull;

				break;
			}

		case T_CoerceToDomainValue:
			{
				CoerceToDomainValue *domainval = (CoerceToDomainValue *) node;

				/* share datastructure with case testval */
				scratch.d.casetest.value = state->innermost_domainval;
				scratch.d.casetest.isnull = state->innermost_domainnull;

				if (get_typlen(domainval->typeId) == -1)
					scratch.opcode = EEOP_DOMAIN_TESTVAL_UNEXPAND;
				else
					scratch.opcode = EEOP_DOMAIN_TESTVAL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_CurrentOfExpr:
			{
				scratch.opcode = EEOP_CURRENTOFEXPR;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Add another expression evaluation step to ExprState->steps.
 *
 * Note that this potentially re-allocates ->steps, therefore no pointer into
 * the array directly may be used while the expression is still being built.
 */
static void
ExprEvalPushStep(ExprState *es, ExprEvalStep *s)
{
	if (es->steps_alloc == 0)
	{
		es->steps_alloc = 16;
		es->steps = palloc(sizeof(ExprEvalStep) * es->steps_alloc);
	}
	else if (es->steps_alloc == es->steps_len)
	{
		es->steps_alloc *= 2;
		es->steps = repalloc(es->steps,
							 sizeof(ExprEvalStep) * es->steps_alloc);
	}

	memcpy(&es->steps[es->steps_len++], s, sizeof(ExprEvalStep));
}

static void
ExecInitFunc(ExprEvalStep *scratch, Expr *node, List *args, Oid funcid,
			 Oid inputcollid, PlanState *parent, ExprState *state,
			 Datum *resv, bool *resnull)
{
	ListCell   *lc;
	AclResult	aclresult;
	int			nargs = list_length(args);
	FunctionCallInfo fcinfo;
	int			argno;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(funcid));
	InvokeFunctionExecuteHook(funcid);

	/*
	 * Safety check on nargs.  Under normal circumstances this should never
	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
	 * declared in pg_proc?
	 */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
			 errmsg_plural("cannot pass more than %d argument to a function",
						   "cannot pass more than %d arguments to a function",
						   FUNC_MAX_ARGS,
						   FUNC_MAX_ARGS)));

	/* Set up the primary fmgr lookup information */
	scratch->d.func.finfo = palloc0(sizeof(*scratch->d.func.finfo));
	scratch->d.func.fcinfo_data = palloc0(sizeof(*scratch->d.func.fcinfo_data));

	fcinfo = scratch->d.func.fcinfo_data;
	fmgr_info(funcid, scratch->d.func.finfo);
	fmgr_info_set_expr((Node *) node, scratch->d.func.finfo);
	InitFunctionCallInfoData(*fcinfo, scratch->d.func.finfo,
							 nargs, inputcollid, NULL, NULL);
	scratch->d.func.fn_addr = scratch->d.func.fcinfo_data->flinfo->fn_addr;
	if (scratch->d.func.finfo->fn_retset)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	}

	argno = 0;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			/*
			 * Don't evaluate const arguments every round; especially
			 * interesting for constants in comparisons.
			 */
			Const	   *con = (Const *) arg;

			fcinfo->arg[argno] = con->constvalue;
			fcinfo->argnull[argno] = con->constisnull;
		}
		else
		{
			ExecInitExprRec(arg, parent, state, &fcinfo->arg[argno], &fcinfo->argnull[argno]);
		}
		argno++;
	}

	scratch->d.func.nargs = nargs;

	if (pgstat_track_functions <= scratch->d.func.finfo->fn_stats)
	{
		if (scratch->d.func.finfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT;
		else
			scratch->opcode = EEOP_FUNCEXPR;
	}
	else
	{
		if (scratch->d.func.finfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT_FUSAGE;
		else
			scratch->opcode = EEOP_FUNCEXPR_FUSAGE;
	}
}

static void
ExecInitExprSlots(ExprState *state, Node *node)
{
	ExprEvalStep scratch;
	int			last_outer = -1;
	int			last_inner = -1;
	int			last_scan = -1;

	/*
	 * Figure out which attributes we're going to need.
	 */
	ExecGetLastAttnums((Node *) node,
					   &last_outer,
					   &last_inner,
					   &last_scan);
	if (last_inner > 0)
	{
		scratch.opcode = EEOP_INNER_FETCHSOME;
		scratch.d.fetch.last_var = last_inner;
		ExprEvalPushStep(state, &scratch);
	}
	if (last_outer > 0)
	{
		scratch.opcode = EEOP_OUTER_FETCHSOME;
		scratch.d.fetch.last_var = last_outer;
		ExprEvalPushStep(state, &scratch);
	}
	if (last_scan > 0)
	{
		scratch.opcode = EEOP_SCAN_FETCHSOME;
		scratch.d.fetch.last_var = last_scan;
		ExprEvalPushStep(state, &scratch);
	}
}

static void
ExecInitWholeRowVar(ExprEvalStep *scratch, Var *variable, PlanState *parent, ExprState *state, Datum *resv, bool *resnull)
{
	scratch->d.wholerow.tupdesc = NULL;
	scratch->d.wholerow.junkFilter = NULL;
	scratch->d.wholerow.var = variable;
	scratch->d.wholerow.first = true;

	/*
	 * If the input tuple came from a subquery, it might contain "resjunk"
	 * columns (such as GROUP BY or ORDER BY columns), which we don't want to
	 * keep in the whole-row result.  We can get rid of such columns by
	 * passing the tuple through a JunkFilter --- but to make one, we have to
	 * lay our hands on the subquery's targetlist.  Fortunately, there are not
	 * very many cases where this can happen, and we can identify all of them
	 * by examining our parent PlanState.  We assume this is not an issue in
	 * standalone expressions that don't have parent plans.  (Whole-row Vars
	 * can occur in such expressions, but they will always be referencing
	 * table rows.)
	 */

	if (parent)
	{
		PlanState  *subplan = NULL;

		switch (nodeTag(parent))
		{
			case T_SubqueryScanState:
				subplan = ((SubqueryScanState *) parent)->subplan;
				break;
			case T_CteScanState:
				subplan = ((CteScanState *) parent)->cteplanstate;
				break;
			default:
				break;
		}

		if (subplan)
		{
			bool		junk_filter_needed = false;
			ListCell   *tlist;

			/* Detect whether subplan tlist actually has any junk columns */
			foreach(tlist, subplan->plan->targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tlist);

				if (tle->resjunk)
				{
					junk_filter_needed = true;
					break;
				}
			}

			if (junk_filter_needed)
			{
				/* If so, build the junkfilter in the query memory context */
				scratch->d.wholerow.junkFilter =
					ExecInitJunkFilter(subplan->plan->targetlist,
									   ExecGetResultType(subplan)->tdhasoid,
									   ExecInitExtraTupleSlot(parent->state));
			}
		}
	}
}

static void
ExecInitArrayRef(ExprEvalStep *scratch, ArrayRef *aref, PlanState *parent,
				 ExprState *state, Datum *resv, bool *resnull)
{
	List	   *adjust_bailout = NIL;
	ListCell   *lc;
	bool		isAssignment = (aref->refassgnexpr != NULL);
	ArrayRefState *arefstate = palloc(sizeof(ArrayRefState));
	int			i;

	if (list_length(aref->refupperindexpr) >= MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						list_length(aref->refupperindexpr), MAXDIM)));

	if (list_length(aref->reflowerindexpr) >= MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						list_length(aref->refupperindexpr), MAXDIM)));

	if (list_length(aref->reflowerindexpr) > 0 &&
	list_length(aref->refupperindexpr) != list_length(aref->reflowerindexpr))
		elog(ERROR, "upper and lower index lists are not same length");

	arefstate->isassignment = isAssignment;
	arefstate->refattrlength = get_typlen(aref->refarraytype);
	arefstate->refelemtype = aref->refelemtype;
	get_typlenbyvalalign(aref->refelemtype,
						 &arefstate->refelemlength,
						 &arefstate->refelembyval,
						 &arefstate->refelemalign);

	/* evaluate input */
	ExecInitExprRec(aref->refexpr, parent, state,
					resv, resnull);

	/*
	 * If refexpr yields NULL, and it's a fetch, then result is NULL. In the
	 * assignment case, we'll create an empty input.
	 */
	if (!isAssignment)
	{
		scratch->opcode = EEOP_ARRAYREF_CHECKINPUT;
		scratch->d.arrayref.state = arefstate;
		scratch->d.arrayref.jumpdone = -1;		/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_bailout = lappend_int(adjust_bailout,
									 state->steps_len - 1);
	}

	/* evaluate upper subscripts */
	i = 0;
	foreach(lc, aref->refupperindexpr)
	{
		Expr	   *e = (Expr *) lfirst(lc);

		if (!e)
		{
			arefstate->upperprovided[i] = false;
			i++;
			continue;
		}

		arefstate->upperprovided[i] = true;

		ExecInitExprRec(e, parent, state,
						&arefstate->upper[i], &arefstate->uppernull[i]);

		scratch->opcode = EEOP_ARRAYREF_CHECKSUBSCRIPT;
		scratch->d.arrayref_checksubscript.state = arefstate;
		scratch->d.arrayref_checksubscript.off = i;
		scratch->d.arrayref_checksubscript.isupper = true;
		scratch->d.arrayref_checksubscript.jumpdone = -1;		/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_bailout = lappend_int(adjust_bailout,
									 state->steps_len - 1);
		i++;
	}
	arefstate->numupper = i;

	/* evaluate lower subscripts */
	i = 0;
	foreach(lc, aref->reflowerindexpr)
	{
		Expr	   *e = (Expr *) lfirst(lc);

		if (!e)
		{
			arefstate->lowerprovided[i] = false;
			i++;
			continue;
		}

		arefstate->lowerprovided[i] = true;

		ExecInitExprRec(e, parent, state,
						&arefstate->lower[i], &arefstate->lowernull[i]);

		scratch->opcode = EEOP_ARRAYREF_CHECKSUBSCRIPT;
		scratch->d.arrayref_checksubscript.state = arefstate;
		scratch->d.arrayref_checksubscript.off = i;
		scratch->d.arrayref_checksubscript.isupper = false;
		scratch->d.arrayref_checksubscript.jumpdone = -1;		/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_bailout = lappend_int(adjust_bailout,
									 state->steps_len - 1);
		i++;
	}
	arefstate->numlower = i;

	if (isAssignment)
	{
		Datum	   *save_innermost_caseval = NULL;
		bool	   *save_innermost_casenull = NULL;

		/*
		 * We might have a nested-assignment situation, in which the
		 * refassgnexpr is itself a FieldStore or ArrayRef that needs to
		 * obtain and modify the previous value of the array element or slice
		 * being replaced.  If so, we have to extract that value from the
		 * array and pass it down via the econtext's caseValue. It's safe to
		 * reuse the CASE mechanism because there cannot be a CASE between
		 * here and where the value would be needed, and an array assignment
		 * can't be within a CASE either.  (So saving and restoring the
		 * caseValue is just paranoia, but let's do it anyway.)
		 *
		 * Since fetching the old element might be a nontrivial expense, do it
		 * only if the argument appears to actually need it.
		 */
		if (isAssignmentIndirectionExpr(aref->refassgnexpr))
		{
			scratch->opcode = EEOP_ARRAYREF_OLD;
			scratch->d.arrayref.state = arefstate;
			ExprEvalPushStep(state, scratch);
		}

		save_innermost_caseval = state->innermost_caseval;
		save_innermost_casenull = state->innermost_casenull;
		state->innermost_caseval = &arefstate->prevvalue;
		state->innermost_casenull = &arefstate->prevnull;

		/* evaluate replacement value */
		ExecInitExprRec(aref->refassgnexpr, parent, state,
						&arefstate->replacevalue, &arefstate->replacenull);

		state->innermost_caseval = save_innermost_caseval;
		state->innermost_casenull = save_innermost_casenull;

		scratch->opcode = EEOP_ARRAYREF_ASSIGN;
		scratch->d.arrayref.state = arefstate;
		ExprEvalPushStep(state, scratch);
	}
	else
	{
		scratch->opcode = EEOP_ARRAYREF_FETCH;
		scratch->d.arrayref.state = arefstate;
		ExprEvalPushStep(state, scratch);
	}

	/* adjust early bail out jump target */
	foreach(lc, adjust_bailout)
	{
		ExprEvalStep *as = &state->steps[lfirst_int(lc)];

		if (as->opcode == EEOP_ARRAYREF_CHECKSUBSCRIPT)
		{
			Assert(as->d.arrayref_checksubscript.jumpdone == -1);
			as->d.arrayref_checksubscript.jumpdone = state->steps_len;
		}
		else
		{
			Assert(as->d.arrayref.jumpdone == -1);
			as->d.arrayref.jumpdone = state->steps_len;
		}
	}
}

/*
 * Helper for preparing ArrayRef expressions for evaluation: is expr a nested
 * FieldStore or ArrayRef that might need the old element value passed down?
 *
 * (We could use this in FieldStore too, but in that case passing the old
 * value is so cheap there's no need.)
 */
static bool
isAssignmentIndirectionExpr(Expr *expr)
{
	if (expr == NULL)
		return false;			/* just paranoia */
	if (IsA(expr, FieldStore))
	{
		FieldStore *fstore = (FieldStore *) expr;

		if (fstore->arg && IsA(fstore->arg, CaseTestExpr))
			return true;
	}
	else if (IsA(expr, ArrayRef))
	{
		ArrayRef   *arrayRef = (ArrayRef *) expr;

		if (arrayRef->refexpr && IsA(arrayRef->refexpr, CaseTestExpr))
			return true;
	}
	return false;
}
