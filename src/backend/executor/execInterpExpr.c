/*-------------------------------------------------------------------------
 *
 * execInterpExpr.c
 *	  Opcode based expression evaluation.
 *
 * This file provides a "switch threaded" (all compilers) and "direct
 * threaded" (gcc, clang and compatible) implementation of expression
 * evaluation.  The former is amongst the fastest known methods of
 * interpreting programs without resorting to assembly level work, or
 * just-in-time compilation, but requires support for computed gotos.  The
 * latter is amongst the fastest approaches doable in standard C.
 *
 * Both work by using ExprEvalStep->opcode to dispatch into code blocks
 * implementing the specific method.  Switch-based uses a plain switch()
 * statement to perform the dispatch. This has the advantages of being plain C
 * and allowing to warn if implementation of a specific opcode has been
 * forgotten.  The disadvantage is that dispatches will, as commonly
 * implemented by compilers, happen from a single location, causing bad branch
 * prediction.  Direct dispatch uses the label-as-values gcc extension - also
 * adopted by some other compilers - to replace ExprEvalStep->opcode with the
 * address of the block implementing the instruction. This allows for better
 * branch prediction (the jumps are happening from different locations) and
 * fewer jumps (no jumps to common dispatch location needed).
 *
 * If computed gotos are available, ExecInstantiateInterpretedExpr will
 * replace ->opcode with the address of relevant code-block and
 * ExprState->flags will contain EEO_FLAG_DIRECT_THREADED to "remember" that
 * fact.
 *
 * For very simple instructions the overhead of the full interpreter
 * "startup", as minimal as it is, is noticeable.  Therefore
 * ExecInstantiateInterpretedExpr() will choose to implement simple scalar Var
 * and Const expressions using special cased routines (ExecJust*).
 * Benchmarking shows anything more complex than those will benefit from the
 * "full interpreter".
 *
 * Complex or uncommon instructions are not implemented in ExecInterpExpr. For
 * one, there'd not be a noticeable performance benefit, but more importantly
 * those complex routines are intended to be shared between different
 * expression evaluation approaches.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execInterprExpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeSubplan.h"
#include "executor/execExpr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/xml.h"


/*
 * Use computed goto based opcode dispatch when computed gotos are
 * available. But allow to disable locally in this file for development.
 */
#ifdef HAVE__COMPUTED_GOTO
#define EEO_USE_COMPUTED_GOTO
#endif   /* HAVE__COMPUTED_GOTO */

/*
 * Macros for opcode dispatch.
 */
#if defined(EEO_USE_COMPUTED_GOTO)
static void **dispatch_table = NULL;
#define EEO_SWITCH()
#define EEO_DISPATCH() goto *((void *) op->opcode)
#define EEO_CASE(name) CASE_##name:
#else
#define EEO_SWITCH() switch ((ExprEvalOp) op->opcode)
#define EEO_DISPATCH() goto starteval
#define EEO_CASE(name) case name:
#endif   /* EEO_USE_COMPUTED_GOTO */

#define EEO_JUMP(stepno) \
	do \
	{ \
		op = &state->steps[stepno]; \
		EEO_DISPATCH(); \
	} while (0)

#define EEO_NEXT() \
	do \
	{ \
		op++; \
		EEO_DISPATCH(); \
	} \
	while (0)


static Datum ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull);
static void ExecInitInterpreter(void);

/* support functions */
static void ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op, bool checkisnull);
static TupleDesc get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext);
static void ShutdownTupleDescRef(Datum arg);

/* fast-path evaluation functions */
static Datum ExecJustOuterVar(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustInnerVar(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustScanVar(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustConst(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustAssignOuterVar(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustAssignInnerVar(ExprState *state, ExprContext *econtext, bool *isnull);
static Datum ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull);


/*
 * Prepare ExprState for interpreted execution.
 */
void
ExecInstantiateInterpretedExpr(ExprState *state)
{
	ExecInitInterpreter();

	Assert(state->steps_len >= 1);
	Assert(state->steps[state->steps_len - 1].opcode == EEOP_DONE);

	/*
	 * Don't perform redundant initialization. This will be unreachable in
	 * most cases, but might be hit if there's additional expression
	 * evaluation methods that rely interpeted execution to work.
	 */
	if (state->flags & EEO_FLAG_INTERPRETER_INITIALIZED)
		return;

	Assert((state->flags & EEO_FLAG_DIRECT_THREADED) == 0);

	/*
	 * There shouldn't be any errors before the expression is fully
	 * initialized, and even if so, it'd would lead to the expression being
	 * abandoned.
	 */
	state->flags |= EEO_FLAG_INTERPRETER_INITIALIZED;

	/*
	 * Fast-path for very simple expressions. "starting up" the full
	 * interpreter is a measurable overhead for these.  Plain Vars and Const
	 * seem to be the only ones where the intrinsic cost is small enough that
	 * the overhead of ExecInterpExpr matters.  For more complex expressions
	 * it's cheaper to use ExecInterpExpr anyways.
	 */
	if (state->steps_len == 3)
	{
		ExprEvalOp	step0 = state->steps[0].opcode;
		ExprEvalOp	step1 = state->steps[1].opcode;

		if (step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_INNER_VAR)
		{
			state->evalfunc = ExecJustInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_OUTER_VAR)
		{
			state->evalfunc = ExecJustOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_SCAN_VAR)
		{
			state->evalfunc = ExecJustScanVar;
			return;
		}
		else if (step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_ASSIGN_INNER_VAR)
		{
			state->evalfunc = ExecJustAssignInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_ASSIGN_OUTER_VAR)
		{
			state->evalfunc = ExecJustAssignOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_ASSIGN_SCAN_VAR)
		{
			state->evalfunc = ExecJustAssignScanVar;
			return;
		}
	}
	else if (state->steps_len == 2 &&
			 state->steps[0].opcode == EEOP_CONST)
	{
		state->evalfunc = ExecJustConst;
		return;
	}

#if defined(EEO_USE_COMPUTED_GOTO)

	/*
	 * In the jump-threaded implementation, replace opcode with the address to
	 * jump to. To reverse, use ExecEvalStepOp().
	 */
	{
		int			off;

		for (off = 0; off < state->steps_len; off++)
		{
			ExprEvalStep *op = &state->steps[off];

			op->opcode = (size_t) dispatch_table[op->opcode];
		}

		state->flags |= EEO_FLAG_DIRECT_THREADED;
	}
#endif   /* defined(EEO_USE_COMPUTED_GOTO) */

	state->evalfunc = ExecInterpExpr;
}


static Datum
ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op;
	const TupleTableSlot *resultslot;
	TupleTableSlot *innerslot;
	TupleTableSlot *outerslot;
	TupleTableSlot *scanslot;

	/*
	 * This array has to be in the same order as ExprEvalOp.
	 */
#if defined(EEO_USE_COMPUTED_GOTO)
	static const void **dispatch_table[] = {
		&&CASE_EEOP_DONE,
		&&CASE_EEOP_INNER_FETCHSOME,
		&&CASE_EEOP_OUTER_FETCHSOME,
		&&CASE_EEOP_SCAN_FETCHSOME,
		&&CASE_EEOP_INNER_VAR,
		&&CASE_EEOP_OUTER_VAR,
		&&CASE_EEOP_SCAN_VAR,
		&&CASE_EEOP_ASSIGN_INNER_VAR,
		&&CASE_EEOP_ASSIGN_OUTER_VAR,
		&&CASE_EEOP_ASSIGN_SCAN_VAR,
		&&CASE_EEOP_ASSIGN_TMP,
		&&CASE_EEOP_ASSIGN_TMP_UNEXPAND,
		&&CASE_EEOP_INNER_SYSVAR,
		&&CASE_EEOP_OUTER_SYSVAR,
		&&CASE_EEOP_SCAN_SYSVAR,
		&&CASE_EEOP_CONST,
		&&CASE_EEOP_FUNCEXPR,
		&&CASE_EEOP_FUNCEXPR_STRICT,
		&&CASE_EEOP_FUNCEXPR_FUSAGE,
		&&CASE_EEOP_FUNCEXPR_STRICT_FUSAGE,
		&&CASE_EEOP_BOOL_AND_STEP_FIRST,
		&&CASE_EEOP_BOOL_AND_STEP,
		&&CASE_EEOP_BOOL_AND_STEP_LAST,
		&&CASE_EEOP_BOOL_OR_STEP_FIRST,
		&&CASE_EEOP_BOOL_OR_STEP,
		&&CASE_EEOP_BOOL_OR_STEP_LAST,
		&&CASE_EEOP_BOOL_NOT_STEP,
		&&CASE_EEOP_QUAL,
		&&CASE_EEOP_NULLTEST_ISNULL,
		&&CASE_EEOP_NULLTEST_ISNOTNULL,
		&&CASE_EEOP_NULLTEST_ROWISNULL,
		&&CASE_EEOP_NULLTEST_ROWISNOTNULL,
		&&CASE_EEOP_PARAM_EXEC,
		&&CASE_EEOP_PARAM_EXTERN,
		&&CASE_EEOP_CASE_WHEN_STEP,
		&&CASE_EEOP_CASE_THEN_STEP,
		&&CASE_EEOP_CASE_TESTVAL,
		&&CASE_EEOP_CASE_TESTVAL_UNEXPAND,
		&&CASE_EEOP_COALESCE,
		&&CASE_EEOP_BOOLTEST_IS_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_FALSE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_FALSE,
		&&CASE_EEOP_BOOLTEST_IS_UNKNOWN,
		&&CASE_EEOP_BOOLTEST_IS_NOT_UNKNOWN,
		&&CASE_EEOP_WHOLEROW,
		&&CASE_EEOP_IOCOERCE,
		&&CASE_EEOP_DISTINCT,
		&&CASE_EEOP_NULLIF,
		&&CASE_EEOP_SQLVALUEFUNCTION,
		&&CASE_EEOP_CURRENTOFEXPR,
		&&CASE_EEOP_ARRAYEXPR,
		&&CASE_EEOP_ARRAYCOERCE,
		&&CASE_EEOP_ROW,
		&&CASE_EEOP_ROWCOMPARE_STEP,
		&&CASE_EEOP_ROWCOMPARE_FINAL,
		&&CASE_EEOP_MINMAX,
		&&CASE_EEOP_FIELDSELECT,
		&&CASE_EEOP_FIELDSTORE_DEFORM,
		&&CASE_EEOP_FIELDSTORE_FORM,
		&&CASE_EEOP_ARRAYREF_CHECKINPUT,
		&&CASE_EEOP_ARRAYREF_CHECKSUBSCRIPT,
		&&CASE_EEOP_ARRAYREF_OLD,
		&&CASE_EEOP_ARRAYREF_ASSIGN,
		&&CASE_EEOP_ARRAYREF_FETCH,
		&&CASE_EEOP_CONVERT_ROWTYPE,
		&&CASE_EEOP_SCALARARRAYOP,
		&&CASE_EEOP_DOMAIN_TESTVAL,
		&&CASE_EEOP_DOMAIN_TESTVAL_UNEXPAND,
		&&CASE_EEOP_DOMAIN_NOTNULL,
		&&CASE_EEOP_DOMAIN_CHECK,
		&&CASE_EEOP_XMLEXPR,
		&&CASE_EEOP_AGGREF,
		&&CASE_EEOP_GROUPING_FUNC,
		&&CASE_EEOP_WINDOW_FUNC,
		&&CASE_EEOP_SUBPLAN,
		&&CASE_EEOP_ALTERNATIVE_SUBPLAN,
		&&CASE_EEOP_LAST
	};

	StaticAssertStmt(EEOP_LAST + 1 == lengthof(dispatch_table),
					 "dispatch_table out of whack with ExprEvalOp");
#endif

#ifdef EEO_USE_COMPUTED_GOTO
	if (unlikely(state == NULL))
	{
		return PointerGetDatum(dispatch_table);
	}
#endif

	/* setup state */
	op = state->steps;
	resultslot = state->resultslot;
	innerslot = econtext->ecxt_innertuple;
	outerslot = econtext->ecxt_outertuple;
	scanslot = econtext->ecxt_scantuple;

#if defined(EEO_USE_COMPUTED_GOTO)
	EEO_DISPATCH();
#else
starteval:
#endif
	EEO_SWITCH()
	{
		EEO_CASE(EEOP_DONE)
		{
			goto out;
		}

		EEO_CASE(EEOP_INNER_FETCHSOME)
		{
			/* XXX: worthwhile to check tts_nvalid inline first? */
			slot_getsomeattrs(innerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_FETCHSOME)
		{
			slot_getsomeattrs(outerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_FETCHSOME)
		{
			slot_getsomeattrs(scanslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/*
			 * Can't assert tts_nvalid, as wholerow var evaluation or such
			 * could have materialized the slot - but the contents are still
			 * valid :/
			 */
			Assert(op->d.var.attnum >= 0);
			*op->resnull = innerslot->tts_isnull[attnum];
			*op->resvalue = innerslot->tts_values[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEO_INNER_VAR comments */
			Assert(op->d.var.attnum >= 0);
			*op->resnull = outerslot->tts_isnull[attnum];
			*op->resvalue = outerslot->tts_values[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEOP_INNER_VAR comments */
			Assert(op->d.var.attnum >= 0);
			*op->resnull = scanslot->tts_isnull[attnum];
			*op->resvalue = scanslot->tts_values[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_INNER_VAR)
		{
			size_t		resultnum = op->d.assign_var.resultnum;
			size_t		attnum = op->d.assign_var.attnum;

			resultslot->tts_values[resultnum] = innerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_OUTER_VAR)
		{
			size_t		resultnum = op->d.assign_var.resultnum;
			size_t		attnum = op->d.assign_var.attnum;

			resultslot->tts_values[resultnum] = outerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_SCAN_VAR)
		{
			size_t		resultnum = op->d.assign_var.resultnum;
			size_t		attnum = op->d.assign_var.attnum;

			resultslot->tts_values[resultnum] = scanslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP)
		{
			size_t		resultnum = op->d.assign_tmp.resultnum;

			resultslot->tts_values[resultnum] = state->resvalue;
			resultslot->tts_isnull[resultnum] = state->resnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP_UNEXPAND)
		{
			size_t		resultnum = op->d.assign_tmp.resultnum;

			resultslot->tts_isnull[resultnum] = state->resnull;

			if (!resultslot->tts_isnull[resultnum])
				resultslot->tts_values[resultnum] =
					MakeExpandedObjectReadOnlyInternal(state->resvalue);
			else
				resultslot->tts_values[resultnum] = state->resvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;

			Assert(op->d.var.attnum < 0);
			*op->resvalue = heap_getsysattr(innerslot->tts_tuple, attnum,
											innerslot->tts_tupleDescriptor,
											op->resnull);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;

			Assert(op->d.var.attnum < 0);
			*op->resvalue = heap_getsysattr(scanslot->tts_tuple, attnum,
											scanslot->tts_tupleDescriptor,
											op->resnull);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_SYSVAR)
		{
			int			attnum = op->d.var.attnum;

			Assert(op->d.var.attnum < 0);
			*op->resvalue = heap_getsysattr(scanslot->tts_tuple, attnum,
											scanslot->tts_tupleDescriptor,
											op->resnull);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONST)
		{
			*op->resnull = op->d.constval.isnull;
			*op->resvalue = op->d.constval.value;

			EEO_NEXT();
		}

		/*
		 * Function-call implementations. Arguments have previously been
		 * evaluated directly into fcinfo->args.
		 *
		 * As both STRICT checks and function-usage are noticeable performance
		 * wise, and function calls are a very hot-path (they also back
		 * operators!), separate all of them into separate opcodes.
		 */
		EEO_CASE(EEOP_FUNCEXPR)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			fcinfo->isnull = false;
			*op->resvalue = (op->d.func.fn_addr) (fcinfo);
			*op->resnull = fcinfo->isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_STRICT)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
			int			argno;
			bool	   *argnull = fcinfo->argnull;

			/* strict function, check for NULL args */
			for (argno = 0; argno < op->d.func.nargs; argno++)
			{
				if (argnull[argno])
				{
					*op->resnull = true;
					goto strictfail;
				}
			}
			fcinfo->isnull = false;
			*op->resvalue = (op->d.func.fn_addr) (fcinfo);
			*op->resnull = fcinfo->isnull;

	strictfail:
			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_FUSAGE)
		{
			PgStat_FunctionCallUsage fcusage;
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			pgstat_init_function_usage(fcinfo, &fcusage);

			fcinfo->isnull = false;
			*op->resvalue = (op->d.func.fn_addr) (fcinfo);
			*op->resnull = fcinfo->isnull;

			pgstat_end_function_usage(&fcusage, true);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_STRICT_FUSAGE)
		{
			PgStat_FunctionCallUsage fcusage;
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
			int			argno;
			bool	   *argnull = fcinfo->argnull;

			/* strict function, check for NULL args */
			for (argno = 0; argno < op->d.func.nargs; argno++)
			{
				if (argnull[argno])
				{
					*op->resnull = true;
					goto strictfail_fusage;
				}
			}

			pgstat_init_function_usage(fcinfo, &fcusage);

			fcinfo->isnull = false;
			*op->resvalue = (op->d.func.fn_addr) (fcinfo);
			*op->resnull = fcinfo->isnull;
	strictfail_fusage:
			pgstat_end_function_usage(&fcusage, true);

			EEO_NEXT();
		}

		/*
		 * If any of the clauses is FALSE, the AND result is FALSE regardless
		 * of the states of the rest of the clauses, so we can stop evaluating
		 * and return FALSE immediately.  If none are FALSE and one or more is
		 * NULL, we return NULL; otherwise we return TRUE.  This makes sense
		 * when you interpret NULL as "don't know", using the same sort of
		 * reasoning as for OR, above.
		 */
		EEO_CASE(EEOP_BOOL_AND_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * Fallthrough (can't be last - ANDs have two arguments at least).
			 */
		}

		EEO_CASE(EEOP_BOOL_AND_STEP)
		{
			if (*op->d.boolexpr.isnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (!DatumGetBool(*op->d.boolexpr.value))
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_AND_STEP_LAST)
		{
			if (*op->d.boolexpr.isnull)
			{
				*op->resnull = true;
				*op->resvalue = 0;
			}
			else if (!DatumGetBool(*op->d.boolexpr.value))
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);

				/*
				 * No point jumping early to jumpdone - would be same target
				 * (as this is the last argument to the AND expression),
				 * except more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resnull = true;
				*op->resvalue = 0;
			}
			else
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(true);
			}

			EEO_NEXT();
		}

		/*
		 * If any of the clauses is TRUE, the OR result is TRUE regardless of
		 * the states of the rest of the clauses, so we can stop evaluating
		 * and return TRUE immediately.  If none are TRUE and one or more is
		 * NULL, we return NULL; otherwise we return FALSE.  This makes sense
		 * when you interpret NULL as "don't know": if we have a TRUE then the
		 * OR is TRUE even if we aren't sure about some of the other inputs.
		 * If all the known inputs are FALSE, but we have one or more "don't
		 * knows", then we have to report that we "don't know" what the OR's
		 * result should be --- perhaps one of the "don't knows" would have
		 * been TRUE if we'd known its value.  Only when all the inputs are
		 * known to be FALSE can we state confidently that the OR's result is
		 * FALSE.
		 */
		EEO_CASE(EEOP_BOOL_OR_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * Fallthrough (can't be last - ORs have two arguments at least).
			 */
		}

		EEO_CASE(EEOP_BOOL_OR_STEP)
		{
			if (*op->d.boolexpr.isnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (DatumGetBool(*op->d.boolexpr.value))
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(true);

				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_OR_STEP_LAST)
		{
			if (*op->d.boolexpr.isnull)
			{
				*op->resnull = true;
				*op->resvalue = 0;
			}
			else if (DatumGetBool(*op->d.boolexpr.value))
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(true);

				/*
				 * No point jumping to jumpdone - would be same target (as
				 * this is the last argument to the AND expression), except
				 * more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resnull = true;
				*op->resvalue = 0;
			}
			else
			{
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_NOT_STEP)
		{
			/*
			 * If the expression evaluates to null, then just return null
			 * back.
			 */
			*op->resnull = *op->d.boolexpr.isnull;

			/*
			 * evaluation of 'not' is simple.. expr is false, then return
			 * 'true' and vice versa.
			 */
			*op->resvalue = BoolGetDatum(!DatumGetBool(*op->d.boolexpr.value));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_QUAL)
		{
			/* special case for ExecQual() */

			if (*op->resnull ||
				!DatumGetBool(*op->resvalue))
			{
				/* bail out early */
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
				EEO_JUMP(op->d.qualexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNULL)
		{
			*op->resvalue = BoolGetDatum(*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNOTNULL)
		{
			*op->resvalue = BoolGetDatum(!*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNull(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNOTNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNotNull(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXEC)
		{
			/* out of line implementation: too large */
			ExecEvalParamExec(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXTERN)
		{
			/* out of line implementation: too large */
			ExecEvalParamExtern(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_CASE_WHEN_STEP)
		{
			if (!*op->d.casewhen.isnull
				&& DatumGetBool(*op->d.casewhen.value))
			{
				EEO_NEXT();
			}
			else
			{
				EEO_JUMP(op->d.casewhen.jumpfalse);
			}
		}

		EEO_CASE(EEOP_CASE_THEN_STEP)
		{
			/*
			 * results already placed in correct place during preceding steps
			 */
			EEO_JUMP(op->d.casethen.jumpdone);
		}

		EEO_CASE(EEOP_CASE_TESTVAL)
		{
			/*
			 * Normally upper parts in the expression tree have setup the
			 * values to be returned here, but some parts of the system
			 * currently misuse {caseValue,domainValue}_{datum,isNull} to set
			 * run-time data.  So if no values have been set-up, use
			 * ExprContext's.  This isn't pretty, but also not *that* ugly,
			 * and this is unlikely to be performance sensitive enoiugh to
			 * worry about a branch.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->caseValue_datum;
				*op->resnull = econtext->caseValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CASE_TESTVAL_UNEXPAND)
		{
			/*
			 * See EEOP_CASE_TESTVAL comment.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->caseValue_datum;
				*op->resnull = econtext->caseValue_isNull;
			}

			/*
			 * Since caseValue_datum may be read multiple times, force to R/O
			 */
			if (!*op->resnull)
			{
				*op->resvalue =
					MakeExpandedObjectReadOnlyInternal(*op->resvalue);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_TESTVAL)
		{
			/*
			 * See EEOP_CASE_TESTVAL comment.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->domainValue_datum;
				*op->resnull = econtext->domainValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_TESTVAL_UNEXPAND)
		{
			/*
			 * See EEOP_CASE_TESTVAL comment.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->domainValue_datum;
				*op->resnull = econtext->domainValue_isNull;
			}

			/*
			 * Since domainValue_datum may be read multiple times, force to
			 * R/O
			 */
			if (!*op->resnull)
			{
				*op->resvalue =
					MakeExpandedObjectReadOnlyInternal(*op->resvalue);
			}

			EEO_NEXT();
		}

		/*
		 * Evaluate a single argument to COALESCE and jump-out if a NOT NULL
		 * one has been found.
		 */
		EEO_CASE(EEOP_COALESCE)
		{
			if (!*op->resnull)
			{
				EEO_JUMP(op->d.coalesce.jumpdone);
			}

			EEO_NEXT();
		}

		/* BooleanTest implementations for all booltesttypes */
		EEO_CASE(EEOP_BOOLTEST_IS_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = false;
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = true;
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = false;
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = true;
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_UNKNOWN)
		{
			*op->resvalue = BoolGetDatum(*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_UNKNOWN)
		{
			*op->resvalue = BoolGetDatum(!*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_WHOLEROW)
		{
			/* too complex for an inline implementation */
			ExecEvalWholeRowVar(state, op, econtext);

			EEO_NEXT();
		}

		/*
		 * Evaluate a CoerceViaIO node.  This can be quite a hot path, so
		 * inline as much work as possible.
		 */
		EEO_CASE(EEOP_IOCOERCE)
		{
			char	   *str;

			/* call output function (similar to OutputFunctionCall) */
			if (*op->resnull)
			{
				/* output functions are not called on nulls */
				str = NULL;
			}
			else
			{
				FunctionCallInfo fcinfo_out;

				fcinfo_out = op->d.iocoerce.fcinfo_data_out;
				fcinfo_out->arg[0] = *op->resvalue;
				fcinfo_out->argnull[0] = false;

				str = DatumGetPointer(FunctionCallInvoke(fcinfo_out));
				Assert(!fcinfo_out->isnull);
			}

			/* call input function (similar to InputFunctionCall) */
			if (!op->d.iocoerce.finfo_in->fn_strict || str != NULL)
			{
				FunctionCallInfo fcinfo_in;

				fcinfo_in = op->d.iocoerce.fcinfo_data_in;
				fcinfo_in->arg[0] = PointerGetDatum(str);
				fcinfo_in->argnull[0] = BoolGetDatum(*op->resnull);
				fcinfo_in->arg[1] = op->d.iocoerce.intypioparam;
				fcinfo_in->argnull[1] = false;
				fcinfo_in->arg[2] = -1;
				fcinfo_in->argnull[2] = false;

				fcinfo_in->isnull = false;
				*op->resvalue = FunctionCallInvoke(fcinfo_in);

				/* Should get null result if and only if str is NULL */
				if (str == NULL)
				{
					Assert(*op->resnull);
					Assert(fcinfo_in->isnull);
				}
				else
				{
					Assert(!*op->resnull);
					Assert(!fcinfo_in->isnull);
				}

			}

			EEO_NEXT();
		}

		/*
		 * IS DISTINCT FROM must evaluate arguments (already done into
		 * fcinfo->arg/argnull) to determine whether they are NULL; if either
		 * is NULL then the result is already known. If neither is NULL, then
		 * proceed to evaluate the comparison function. Note that this is
		 * *always* derived from the equals operator, but since we need
		 * special processing of the arguments we can not simply reuse
		 * ExecEvalOper() or ExecEvalFunc().
		 */
		EEO_CASE(EEOP_DISTINCT)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* check function arguments for NULLness */
			if (fcinfo->argnull[0] && fcinfo->argnull[1])
			{
				/* Both NULL? Then is not distinct... */
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
			}
			else if (fcinfo->argnull[0] || fcinfo->argnull[1])
			{
				/* Only one is NULL? Then is distinct... */
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(true);
			}
			else
			{
				fcinfo->isnull = false;
				*op->resvalue = (op->d.func.fn_addr) (fcinfo);
				/* Must invert result of "=" */
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
				*op->resnull = fcinfo->isnull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLIF)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* if either argument is NULL they can't be equal */
			if (!fcinfo->argnull[0] && !fcinfo->argnull[1])
			{
				bool		result;

				fcinfo->isnull = false;
				result = (op->d.func.fn_addr) (fcinfo);

				/* if the arguments are equal return null */
				if (!fcinfo->isnull && DatumGetBool(result))
				{
					*op->resnull = true;
					*op->resvalue = true;

					EEO_NEXT();
				}
			}
			*op->resnull = fcinfo->argnull[0];
			*op->resvalue = fcinfo->arg[0];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SQLVALUEFUNCTION)
		{
			/*
			 * Doesn't seem worthwhile to have an inline implementation
			 * efficency-wise.
			 */
			ExecEvalSQLValueFunction(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CURRENTOFEXPR)
		{
			/* error invocation uses space, and shouldn't ever occur */
			ExecEvalCurrentOfExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYCOERCE)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayCoerce(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROW)
		{
			/* too complex for an inline implementation */
			ExecEvalRow(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_STEP)
		{
			FunctionCallInfo fcinfo = op->d.rowcompare_step.fcinfo_data;

			/* force NULL result */
			if (op->d.rowcompare_step.finfo->fn_strict &&
				(fcinfo->argnull[0] || fcinfo->argnull[1]))
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}

			fcinfo->isnull = false;
			*op->resvalue = (op->d.rowcompare_step.fn_addr) (fcinfo);

			/* force NULL result */
			if (fcinfo->isnull)
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}

			/* no need to compare remaining columns */
			if (DatumGetInt32(*op->resvalue) != 0)
			{
				EEO_JUMP(op->d.rowcompare_step.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_FINAL)
		{
			int32		cmpresult = DatumGetInt32(*op->resvalue);
			RowCompareType rctype = op->d.rowcompare_final.rctype;

			*op->resnull = false;
			switch (rctype)
			{
					/* EQ and NE cases aren't allowed here */
				case ROWCOMPARE_LT:
					*op->resvalue = BoolGetDatum(cmpresult < 0);
					break;
				case ROWCOMPARE_LE:
					*op->resvalue = BoolGetDatum(cmpresult <= 0);
					break;
				case ROWCOMPARE_GE:
					*op->resvalue = BoolGetDatum(cmpresult >= 0);
					break;
				case ROWCOMPARE_GT:
					*op->resvalue = BoolGetDatum(cmpresult > 0);
					break;
				default:
					Assert(false);
					break;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_MINMAX)
		{
			/* too complex for an inline implementation */
			ExecEvalMinMax(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSELECT)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldSelect(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_DEFORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreDeForm(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_FORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreForm(state, op, econtext);

			EEO_NEXT();
		}

		/*
		 * Verify input for an ArrayRef reference (not assignment).  If the
		 * array is null, return the entire result is NULL.
		 */
		EEO_CASE(EEOP_ARRAYREF_CHECKINPUT)
		{
			Assert(!op->d.arrayref.state->isassignment);

			/*
			 * If refexpr yields NULL, and it's a fetch, then result is NULL.
			 */
			if (*op->resnull &&
				!op->d.arrayref.state->isassignment)
			{
				EEO_JUMP(op->d.arrayref.jumpdone);
			}

			EEO_NEXT();
		}

		/*
		 * Check whether a subscript is NULL and handle that.
		 */
		EEO_CASE(EEOP_ARRAYREF_CHECKSUBSCRIPT)
		{
			/* too complex for an inline implementation */
			if (ExecEvalArrayRefCheckSubscript(state, op))
			{
				EEO_NEXT();
			}
			else
			{
				EEO_JUMP(op->d.arrayref_checksubscript.jumpdone);
			}
		}

		/*
		 * Fetch the old value in an arrayref, if referenced (via a
		 * CaseTestExpr) inside the assignment expression.
		 */
		EEO_CASE(EEOP_ARRAYREF_OLD)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefOld(state, op);

			EEO_NEXT();
		}

		/*
		 * Perform ArrayRef assignment
		 */
		EEO_CASE(EEOP_ARRAYREF_ASSIGN)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefAssign(state, op);

			EEO_NEXT();
		}

		/*
		 * Fetch subset of an array.
		 */
		EEO_CASE(EEOP_ARRAYREF_FETCH)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefFetch(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONVERT_ROWTYPE)
		{
			/* too complex for an inline implementation */
			ExecEvalConvertRowtype(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCALARARRAYOP)
		{
			/* too complex for an inline implementation */
			ExecEvalScalarArrayOp(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_NOTNULL)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintNotNull(state, op);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_CHECK)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintCheck(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_XMLEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalXmlExpr(state, op);

			EEO_NEXT();
		}

		/*
		 * Returns a Datum whose value is the value of the precomputed
		 * aggregate found in the given expression context.
		 */
		EEO_CASE(EEOP_AGGREF)
		{
			AggrefExprState *aggref = op->d.aggref.astate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resnull = econtext->ecxt_aggnulls[aggref->aggno];
			*op->resvalue = econtext->ecxt_aggvalues[aggref->aggno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_GROUPING_FUNC)
		{
			/* too complex/uncommon for an inline implementation */
			ExecEvalGroupingFunc(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_WINDOW_FUNC)
		{
			WindowFuncExprState *wfunc = op->d.window_func.wfstate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resnull = econtext->ecxt_aggnulls[wfunc->wfuncno];
			*op->resvalue = econtext->ecxt_aggvalues[wfunc->wfuncno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ALTERNATIVE_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalAlternativeSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_LAST)
		{
			/* unreachable */
			Assert(false);
			goto out;
		}
	}
out:
	*isnull = state->resnull;
	return state->resvalue;
}

/* Fast-path functions, for very simple expressions */

static Datum
ExecJustOuterVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_outertuple;

	return slot_getattr(slot, attnum, isnull);
}

static Datum
ExecJustInnerVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_innertuple;

	return slot_getattr(slot, attnum, isnull);
}

static Datum
ExecJustScanVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_scantuple;

	return slot_getattr(slot, attnum, isnull);
}

static Datum
ExecJustConst(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[0];

	*isnull = op->d.constval.isnull;
	return op->d.constval.value;
}

static Datum
ExecJustAssignOuterVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.assign_var.attnum + 1;
	size_t		resultnum = op->d.assign_var.resultnum;
	TupleTableSlot *inslot = econtext->ecxt_outertuple;
	TupleTableSlot *outslot = state->resultslot;

	outslot->tts_values[resultnum] =
		slot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
	return 0;
}

static Datum
ExecJustAssignInnerVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.assign_var.attnum + 1;
	size_t		resultnum = op->d.assign_var.resultnum;
	TupleTableSlot *inslot = econtext->ecxt_innertuple;
	TupleTableSlot *outslot = state->resultslot;

	outslot->tts_values[resultnum] =
		slot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
	return 0;
}

static Datum
ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.assign_var.attnum + 1;
	size_t		resultnum = op->d.assign_var.resultnum;
	TupleTableSlot *inslot = econtext->ecxt_scantuple;
	TupleTableSlot *outslot = state->resultslot;

	outslot->tts_values[resultnum] =
		slot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
	return 0;
}

static void
ExecInitInterpreter(void)
{
	static bool prepared = false;

	if (prepared)
		return;

#if defined(EEO_USE_COMPUTED_GOTO)
	if (dispatch_table == NULL)
		dispatch_table = (void **) DatumGetPointer(ExecInterpExpr(NULL, NULL, NULL));
#endif

	prepared = true;
}

/*
 * Function to return the opcode of an expression step.
 *
 * When direct-threading is in use, ExprState->opcode isn't easily
 * decipherable. This function returns the appropriate enum member.
 *
 * This currently is only supposed to be used in paths that aren't critical
 * performance wise.  If that changes, we could add an inverse dispatch_table
 * that's sorted on the address, so a binary search can be performed.
 */
ExprEvalOp
ExecEvalStepOp(ExprState *state, ExprEvalStep *op)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	if (state->flags & EEO_FLAG_DIRECT_THREADED)
	{
		int			i;

		for (i = 0; i < EEOP_LAST; i++)
		{
			if ((void *) op->opcode == dispatch_table[i])
			{
				return (ExprEvalOp) i;
			}
		}
		elog(ERROR, "unknown opcode");
	}
#endif
	return (ExprEvalOp) op->opcode;
}

/*
 * Computes the value for a PARAM_EXEC parameter.
 */
void
ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ParamExecData *prm;

	prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
	if (unlikely(prm->execPlan != NULL))
	{
		/*
		 * XXX: Worthwhile to extract this into a separate opcode?
		 */
		ExecSetParamPlan(prm->execPlan, econtext);
		/* ExecSetParamPlan should have processed this param... */
		Assert(prm->execPlan == NULL);
	}
	*op->resvalue = prm->value;
	*op->resnull = prm->isnull;
}

/*
 * Computes the value for a PARAM_EXTERN parameter.
 */
void
ExecEvalParamExtern(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ParamListInfo paramInfo = econtext->ecxt_param_list_info;
	int			paramId = op->d.param.paramid;

	if (likely(paramInfo &&
			   paramId > 0 && paramId <= paramInfo->numParams))
	{
		ParamExternData *prm = &paramInfo->params[paramId - 1];

		/* give hook a chance in case parameter is dynamic */
		/* XXX: Use separate opcode in future? */
		if (!OidIsValid(prm->ptype) && paramInfo->paramFetch != NULL)
			(*paramInfo->paramFetch) (paramInfo, paramId);

		if (likely(OidIsValid(prm->ptype)))
		{
			/* safety check in case hook did something unexpected */
			/* XXX: assert instead? */
			if (unlikely(prm->ptype != op->d.param.paramtype))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
								paramId,
								format_type_be(prm->ptype),
								format_type_be(op->d.param.paramtype))));
			*op->resvalue = prm->value;
			*op->resnull = prm->isnull;
			return;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("no value found for parameter %d", paramId)));
}

/*
 * Evaluate SQLValueFunction expressions.
 */
void
ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op)
{
	FunctionCallInfoData fcinfo;
	SQLValueFunction *svf = op->d.sqlvaluefunction.svf;

	*op->resnull = false;

	/*
	 * Note: current_schema() can return NULL.  current_user() etc currently
	 * cannot, but might as well code those cases the same way for safety.
	 */
	switch (svf->op)
	{
		case SVFOP_CURRENT_DATE:
			*op->resvalue = DateADTGetDatum(GetSQLCurrentDate());
			break;
		case SVFOP_CURRENT_TIME:
		case SVFOP_CURRENT_TIME_N:
			*op->resvalue = TimeTzADTPGetDatum(GetSQLCurrentTime(svf->typmod));
			break;
		case SVFOP_CURRENT_TIMESTAMP:
		case SVFOP_CURRENT_TIMESTAMP_N:
			*op->resvalue = TimestampTzGetDatum(GetSQLCurrentTimestamp(svf->typmod));
			break;
		case SVFOP_LOCALTIME:
		case SVFOP_LOCALTIME_N:
			*op->resvalue = TimeADTGetDatum(GetSQLLocalTime(svf->typmod));
			break;
		case SVFOP_LOCALTIMESTAMP:
		case SVFOP_LOCALTIMESTAMP_N:
			*op->resvalue = TimestampGetDatum(GetSQLLocalTimestamp(svf->typmod));
			break;
		case SVFOP_CURRENT_ROLE:
		case SVFOP_CURRENT_USER:
		case SVFOP_USER:
			InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
			*op->resvalue = current_user(&fcinfo);
			*op->resnull = fcinfo.isnull;
			break;
		case SVFOP_SESSION_USER:
			InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
			*op->resvalue = session_user(&fcinfo);
			*op->resnull = fcinfo.isnull;
			break;
		case SVFOP_CURRENT_CATALOG:
			InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
			*op->resvalue = current_database(&fcinfo);
			*op->resnull = fcinfo.isnull;
			break;
		case SVFOP_CURRENT_SCHEMA:
			InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
			*op->resvalue = current_schema(&fcinfo);
			*op->resnull = fcinfo.isnull;
			break;
	}
}

void
ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)
{
	/*
	 * The planner should convert CURRENT OF into a TidScan qualification, or
	 * some other special handling in a ForeignScan node.  So we have to be
	 * able to do ExecInitExpr on a CurrentOfExpr, but we shouldn't ever
	 * actually execute it.  If we get here, we suppose we must be dealing
	 * with CURRENT OF on a foreign table whose FDW doesn't handle it, and
	 * complain accordingly.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		   errmsg("WHERE CURRENT OF is not supported for this table type")));
}

/*
 * Evaluate NullTest / IS NULL for rows.
 */
void
ExecEvalRowNull(ExprState *state, ExprEvalStep *op)
{
	ExecEvalRowNullInt(state, op, true);
}

/*
 * Evaluate NullTest / IS NOT NULL for rows.
 */
void
ExecEvalRowNotNull(ExprState *state, ExprEvalStep *op)
{
	ExecEvalRowNullInt(state, op, false);
}

static void
ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op, bool checkisnull)
{
	Datum		value = *op->resvalue;
	bool		isnull = *op->resnull;
	HeapTupleHeader tuple;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	HeapTupleData tmptup;
	int			att;

	*op->resnull = false;

	/* NULL row variables are treated just as NULL scalar columns */
	if (isnull && checkisnull)
	{
		*op->resvalue = BoolGetDatum(true);
		return;
	}
	else if (isnull)
	{
		*op->resvalue = BoolGetDatum(false);
		return;
	}

	/*
	 * The SQL standard defines IS [NOT] NULL for a non-null rowtype argument
	 * as:
	 *
	 * "R IS NULL" is true if every field is the null value.
	 *
	 * "R IS NOT NULL" is true if no field is the null value.
	 *
	 * This definition is (apparently intentionally) not recursive; so our
	 * tests on the fields are primitive attisnull tests, not recursive checks
	 * to see if they are all-nulls or no-nulls rowtypes.
	 *
	 * The standard does not consider the possibility of zero-field rows, but
	 * here we consider them to vacuously satisfy both predicates.
	 */

	tuple = DatumGetHeapTupleHeader(value);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);

	/* Lookup tupdesc if first time through or if type changes */
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/*
	 * heap_attisnull needs a HeapTuple not a bare HeapTupleHeader.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	for (att = 1; att <= tupDesc->natts; att++)
	{
		/* ignore dropped columns */
		if (tupDesc->attrs[att - 1]->attisdropped)
			continue;
		if (heap_attisnull(&tmptup, att))
		{
			/* null field disproves IS NOT NULL */
			if (!checkisnull)
			{
				ReleaseTupleDesc(tupDesc);
				*op->resvalue = BoolGetDatum(false);
				return;
			}
		}
		else
		{
			/* non-null field disproves IS NULL */
			if (checkisnull)
			{
				ReleaseTupleDesc(tupDesc);
				*op->resvalue = BoolGetDatum(false);
				return;
			}
		}
	}

	ReleaseTupleDesc(tupDesc);
	*op->resvalue = BoolGetDatum(true);
}


/*
 * Evaluate ARRAY[] expressions.
 */
void
ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)
{
	ArrayType  *result;
	ArrayExpr  *arrayExpr = op->d.arrayexpr.arrayexpr;
	Oid			element_type = arrayExpr->element_typeid;
	int			ndims = 0;
	int			dims[MAXDIM];
	int			lbs[MAXDIM];
	int			nelems = op->d.arrayexpr.nelems;

	/* Set non-null as default */
	*op->resnull = false;

	if (!arrayExpr->multidims)
	{
		/* Elements are presumably of scalar type */
		Datum	   *dvalues = op->d.arrayexpr.elemvalues;
		bool	   *dnulls = op->d.arrayexpr.elemnulls;

		ndims = 1;

		/* Shouldn't happen here, but if length is 0, return empty array */
		if (nelems == 0)
		{
			*op->resvalue = PointerGetDatum(construct_empty_array(element_type));
			return;
		}

		/* setup for 1-D array of the given length */
		dims[0] = nelems;
		lbs[0] = 1;

		result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
									element_type,
									op->d.arrayexpr.elemlength,
									op->d.arrayexpr.elembyval,
									op->d.arrayexpr.elemalign);
	}
	else
	{
		/* Must be nested array expressions */
		int			nbytes = 0;
		int			nitems = 0;
		int			outer_nelems = 0;
		int			elem_ndims = 0;
		int		   *elem_dims = NULL;
		int		   *elem_lbs = NULL;
		bool		firstone = true;
		bool		havenulls = false;
		bool		haveempty = false;
		char	  **subdata;
		bits8	  **subbitmaps;
		int		   *subbytes;
		int		   *subnitems;
		int32		dataoffset;
		char	   *dat;
		int			iitem;
		int			elemoff;
		int			i;

		subdata = (char **) palloc(nelems * sizeof(char *));
		subbitmaps = (bits8 **) palloc(nelems * sizeof(bits8 *));
		subbytes = (int *) palloc(nelems * sizeof(int));
		subnitems = (int *) palloc(nelems * sizeof(int));

		/* loop through and get data area from each element */
		for (elemoff = 0; elemoff < nelems; elemoff++)
		{
			bool		eisnull;
			Datum		arraydatum;
			ArrayType  *array;
			int			this_ndims;

			arraydatum = op->d.arrayexpr.elemvalues[elemoff];
			eisnull = op->d.arrayexpr.elemnulls[elemoff];

			/* temporarily ignore null subarrays */
			if (eisnull)
			{
				haveempty = true;
				continue;
			}

			array = DatumGetArrayTypeP(arraydatum);

			/* run-time double-check on element type */
			if (element_type != ARR_ELEMTYPE(array))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("cannot merge incompatible arrays"),
						 errdetail("Array with element type %s cannot be "
						 "included in ARRAY construct with element type %s.",
								   format_type_be(ARR_ELEMTYPE(array)),
								   format_type_be(element_type))));

			this_ndims = ARR_NDIM(array);
			/* temporarily ignore zero-dimensional subarrays */
			if (this_ndims <= 0)
			{
				haveempty = true;
				continue;
			}

			if (firstone)
			{
				/* Get sub-array details from first member */
				elem_ndims = this_ndims;
				ndims = elem_ndims + 1;
				if (ndims <= 0 || ndims > MAXDIM)
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						  errmsg("number of array dimensions (%d) exceeds " \
								 "the maximum allowed (%d)", ndims, MAXDIM)));

				elem_dims = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int));
				elem_lbs = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int));

				firstone = false;
			}
			else
			{
				/* Check other sub-arrays are compatible */
				if (elem_ndims != this_ndims ||
					memcmp(elem_dims, ARR_DIMS(array),
						   elem_ndims * sizeof(int)) != 0 ||
					memcmp(elem_lbs, ARR_LBOUND(array),
						   elem_ndims * sizeof(int)) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
							 errmsg("multidimensional arrays must have array "
									"expressions with matching dimensions")));
			}

			subdata[outer_nelems] = ARR_DATA_PTR(array);
			subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
			subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
			nbytes += subbytes[outer_nelems];
			subnitems[outer_nelems] = ArrayGetNItems(this_ndims,
													 ARR_DIMS(array));
			nitems += subnitems[outer_nelems];
			havenulls |= ARR_HASNULL(array);
			outer_nelems++;
		}

		/*
		 * If all items were null or empty arrays, return an empty array;
		 * otherwise, if some were and some weren't, raise error.  (Note: we
		 * must special-case this somehow to avoid trying to generate a 1-D
		 * array formed from empty arrays.  It's not ideal...)
		 */
		if (haveempty)
		{
			if (ndims == 0)		/* didn't find any nonempty array */
			{
				*op->resvalue = PointerGetDatum(construct_empty_array(element_type));
				return;
			}
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("multidimensional arrays must have array "
							"expressions with matching dimensions")));
		}

		/* setup for multi-D array */
		dims[0] = outer_nelems;
		lbs[0] = 1;
		for (i = 1; i < ndims; i++)
		{
			dims[i] = elem_dims[i - 1];
			lbs[i] = elem_lbs[i - 1];
		}

		if (havenulls)
		{
			dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
			nbytes += dataoffset;
		}
		else
		{
			dataoffset = 0;		/* marker for no null bitmap */
			nbytes += ARR_OVERHEAD_NONULLS(ndims);
		}

		result = (ArrayType *) palloc(nbytes);
		SET_VARSIZE(result, nbytes);
		result->ndim = ndims;
		result->dataoffset = dataoffset;
		result->elemtype = element_type;
		memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
		memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

		dat = ARR_DATA_PTR(result);
		iitem = 0;
		for (i = 0; i < outer_nelems; i++)
		{
			memcpy(dat, subdata[i], subbytes[i]);
			dat += subbytes[i];
			if (havenulls)
				array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
								  subbitmaps[i], 0,
								  subnitems[i]);
			iitem += subnitems[i];
		}
	}

	*op->resvalue = PointerGetDatum(result);
}



/*
 * Evaluate an ArrayCoerceExpr expression.
 */
void
ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op)
{
	ArrayCoerceExpr *acoerce = op->d.arraycoerce.coerceexpr;
	Datum		result;
	FunctionCallInfoData locfcinfo;

	/* input is in *step->resvalue/resnull */
	result = *op->resvalue;
	if (*op->resnull)
	{
		return;					/* nothing to do */
	}

	/*
	 * If it's binary-compatible, modify the element type in the array header,
	 * but otherwise leave the array as we received it.
	 */
	if (!OidIsValid(acoerce->elemfuncid))
	{
		/* Detoast input array if necessary, and copy in any case */
		ArrayType  *array = DatumGetArrayTypePCopy(result);

		ARR_ELEMTYPE(array) = op->d.arraycoerce.resultelemtype;
		*op->resvalue = PointerGetDatum(array);
		return;
	}

	/* Initialize function cache if first time through */
	if (op->d.arraycoerce.elemfunc->fn_oid == InvalidOid)
	{
		AclResult	aclresult;

		/* Check permission to call function */
		aclresult = pg_proc_aclcheck(acoerce->elemfuncid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC,
						   get_func_name(acoerce->elemfuncid));
		InvokeFunctionExecuteHook(acoerce->elemfuncid);

		/* Set up the primary fmgr lookup information */
		fmgr_info(acoerce->elemfuncid, op->d.arraycoerce.elemfunc);
		fmgr_info_set_expr((Node *) acoerce, op->d.arraycoerce.elemfunc);
	}

	/*
	 * Use array_map to apply the function to each array element.
	 *
	 * We pass on the desttypmod and isExplicit flags whether or not the
	 * function wants them.
	 *
	 * Note: coercion functions are assumed to not use collation.
	 */
	InitFunctionCallInfoData(locfcinfo, op->d.arraycoerce.elemfunc, 3,
							 InvalidOid, NULL, NULL);
	locfcinfo.arg[0] = result;
	locfcinfo.arg[1] = Int32GetDatum(acoerce->resulttypmod);
	locfcinfo.arg[2] = BoolGetDatum(acoerce->isExplicit);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = false;
	locfcinfo.argnull[2] = false;

	*op->resvalue = array_map(&locfcinfo, op->d.arraycoerce.resultelemtype,
							  op->d.arraycoerce.amstate);
}

/*
 * Evaluate ROW() expressions.
 *
 * The individual columns have already been evaluated into
 * op->d.row.elemvalues/nulls.
 */
void
ExecEvalRow(ExprState *state, ExprEvalStep *op)
{
	HeapTuple	tuple;

	/* Set non-null as default */
	*op->resnull = false;

	/* build tuple from evaluated field values */
	tuple = heap_form_tuple(op->d.row.tupdesc,
							op->d.row.elemvalues,
							op->d.row.elemnulls);

	*op->resvalue = HeapTupleGetDatum(tuple);
}

/*
 * Evaluate GREATEST()/LEAST() type expression (note this is *not* MIN()/
 * MAX()).
 *
 * All of the to-be-compared expressions have already been evaluated into
 * ->op->d.minmax.values/nulls.
 */
void
ExecEvalMinMax(ExprState *state, ExprEvalStep *op)
{
	int			off;
	Datum	   *values = op->d.minmax.values;
	bool	   *nulls = op->d.minmax.nulls;
	FunctionCallInfo fcinfo = op->d.minmax.fcinfo_data;
	MinMaxOp	operator = op->d.minmax.op;

	/* set at initialization */
	Assert(fcinfo->argnull[0] == false);
	Assert(fcinfo->argnull[1] == false);

	*op->resnull = true;

	for (off = 0; off < op->d.minmax.nelems; off++)
	{
		/* ignore NULL inputs */
		if (nulls[off])
			continue;

		if (*op->resnull)
		{
			/* first nonnull input, adopt value */
			*op->resvalue = values[off];
			*op->resnull = false;
		}
		else
		{
			int			cmpresult;

			/* apply comparison function */
			fcinfo->isnull = false;
			fcinfo->arg[0] = *op->resvalue;
			fcinfo->arg[1] = values[off];
			Assert(!fcinfo->isnull);

			cmpresult = DatumGetInt32(FunctionCallInvoke(fcinfo));

			if (cmpresult > 0 && operator == IS_LEAST)
				*op->resvalue = values[off];
			else if (cmpresult < 0 && operator == IS_GREATEST)
				*op->resvalue = values[off];
		}
	}
}

/*
 * Evaluate a FieldSelect node.
 */
void
ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	AttrNumber	fieldnum = op->d.fieldselect.fieldnum;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	Form_pg_attribute attr;
	HeapTupleData tmptup;

	tupDatum = *op->resvalue;

	/* this test covers the isDone exception too: */
	if (*op->resnull)
		return;

	tuple = DatumGetHeapTupleHeader(tupDatum);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);

	/* Lookup tupdesc if first time through or if type changes */
	tupDesc = get_cached_rowtype(tupType, tupTypmod,
								 &op->d.fieldselect.argdesc,
								 econtext);

	/*
	 * Find field's attr record.  Note we don't support system columns here: a
	 * datum tuple doesn't have valid values for most of the interesting
	 * system columns anyway.
	 */
	if (fieldnum <= 0)			/* should never happen */
		elog(ERROR, "unsupported reference to system column %d in FieldSelect",
			 fieldnum);
	if (fieldnum > tupDesc->natts)		/* should never happen */
		elog(ERROR, "attribute number %d exceeds number of columns %d",
			 fieldnum, tupDesc->natts);
	attr = tupDesc->attrs[fieldnum - 1];

	/* Check for dropped column, and force a NULL result if so */
	if (attr->attisdropped)
	{
		*op->resnull = true;
		return;
	}

	/* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
	/* As in ExecEvalScalarVar, we should but can't check typmod */
	if (op->d.fieldselect.resulttype != attr->atttypid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("attribute %d has wrong type", fieldnum),
				 errdetail("Table has type %s, but query expects %s.",
						   format_type_be(attr->atttypid),
						   format_type_be(op->d.fieldselect.resulttype))));

	/* heap_getattr needs a HeapTuple not a bare HeapTupleHeader */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	*op->resvalue = heap_getattr(&tmptup,
								 fieldnum,
								 tupDesc,
								 op->resnull);
}

/*
 * Deform tuple before evaluating the individual new values as part of a
 * FieldStore expression.
 */
void
ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	/* Lookup tupdesc if first time through or after rescan */
	get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1,
					   op->d.fieldstore.argdesc, econtext);

	if (*op->resnull)
	{
		/* Convert null input tuple into an all-nulls row */
		memset(op->d.fieldstore.nulls, true,
			   MaxTupleAttributeNumber * sizeof(bool));
	}
	else
	{
		/*
		 * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader. We
		 * set all the fields in the struct just in case.
		 */
		Datum		tupDatum = *op->resvalue;
		HeapTupleHeader tuphdr;
		HeapTupleData tmptup;

		tuphdr = DatumGetHeapTupleHeader(tupDatum);
		tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = tuphdr;

		heap_deform_tuple(&tmptup, *op->d.fieldstore.argdesc,
						  op->d.fieldstore.values,
						  op->d.fieldstore.nulls);
	}
}

/*
 * Compute the new wholerow datum after each individual row part of a
 * FieldStore expression has been evaluated.
 */
void
ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	HeapTuple	tuple;

	/* Result is never null */
	*op->resnull = false;

	tuple = heap_form_tuple(*op->d.fieldstore.argdesc,
							op->d.fieldstore.values,
							op->d.fieldstore.nulls);

	*op->resvalue = HeapTupleGetDatum(tuple);
}

void
ExecEvalArrayRefOld(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;

	if (*op->resnull)
	{
		arefstate->prevnull = true;
		arefstate->prevvalue = (Datum) 0;
	}
	else if (arefstate->numlower == 0)
	{
		arefstate->prevvalue =
			array_get_element(*op->resvalue,
							  arefstate->numupper,
							  arefstate->upperindex,
							  arefstate->refattrlength,
							  arefstate->refelemlength,
							  arefstate->refelembyval,
							  arefstate->refelemalign,
							  &arefstate->prevnull);
	}
	else
	{
		/* this is currently unreachable */
		arefstate->prevvalue =
			array_get_slice(*op->resvalue,
							arefstate->numupper,
							arefstate->upperindex,
							arefstate->lowerindex,
							arefstate->upperprovided,
							arefstate->lowerprovided,
							arefstate->refattrlength,
							arefstate->refelemlength,
							arefstate->refelembyval,
							arefstate->refelemalign);
	}
}

void
ExecEvalArrayRefAssign(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;

	/*
	 * For an assignment to a fixed-length array type, both the original array
	 * and the value to be assigned into it must be non-NULL, else we punt and
	 * return the original array.
	 */
	if (arefstate->refattrlength > 0 &&
		(*op->resnull || arefstate->replacenull))
	{
		return;
	}

	/*
	 * For assignment to varlena arrays, we handle a NULL original array by
	 * substituting an empty (zero-dimensional) array; insertion of the new
	 * element will result in a singleton array value.  It does not matter
	 * whether the new element is NULL.
	 */
	if (*op->resnull)
	{
		*op->resvalue = PointerGetDatum(construct_empty_array(arefstate->refelemtype));
		*op->resnull = false;
	}

	if (arefstate->numlower == 0)
	{
		*op->resvalue =
			array_set_element(*op->resvalue, arefstate->numupper,
							  arefstate->upperindex,
							  arefstate->replacevalue,
							  arefstate->replacenull,
							  arefstate->refattrlength,
							  arefstate->refelemlength,
							  arefstate->refelembyval,
							  arefstate->refelemalign);
	}
	else
	{
		*op->resvalue =
			array_set_slice(*op->resvalue, arefstate->numupper,
							arefstate->upperindex,
							arefstate->lowerindex,
							arefstate->upperprovided,
							arefstate->lowerprovided,
							arefstate->replacevalue,
							arefstate->replacenull,
							arefstate->refattrlength,
							arefstate->refelemlength,
							arefstate->refelembyval,
							arefstate->refelemalign);
	}
}

void
ExecEvalArrayRefFetch(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;

	if (arefstate->numlower == 0)
	{

		*op->resvalue =
			array_get_element(*op->resvalue,
							  arefstate->numupper,
							  arefstate->upperindex,
							  arefstate->refattrlength,
							  arefstate->refelemlength,
							  arefstate->refelembyval,
							  arefstate->refelemalign,
							  op->resnull);
	}
	else
	{
		*op->resvalue =
			array_get_slice(*op->resvalue,
							arefstate->numupper,
							arefstate->upperindex,
							arefstate->lowerindex,
							arefstate->upperprovided,
							arefstate->lowerprovided,
							arefstate->refattrlength,
							arefstate->refelemlength,
							arefstate->refelembyval,
							arefstate->refelemalign);
	}
}

bool
ExecEvalArrayRefCheckSubscript(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref_checksubscript.state;
	int			off = op->d.arrayref_checksubscript.off;
	bool	   *nulls;
	Datum	   *values;
	int		   *indexes;

	if (op->d.arrayref_checksubscript.isupper)
	{
		nulls = arefstate->uppernull;
		values = arefstate->upper;
		indexes = arefstate->upperindex;
	}
	else
	{
		nulls = arefstate->lowernull;
		values = arefstate->lower;
		indexes = arefstate->lowerindex;
	}

	/* If any index expr yields NULL, result is NULL or error */
	if (nulls[off])
	{
		if (arefstate->isassignment)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				  errmsg("array subscript in assignment must not be null")));
		*op->resnull = true;
		return false;
	}

	/* convert datum to int */
	indexes[off] = values[off];

	return true;
}

/*
 * Evaluate a rowtype coercion operation.  This may require rearranging field
 * positions.
 */
void
ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ConvertRowtypeExpr *convert = op->d.convert_rowtype.convert;
	HeapTuple	result;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	HeapTupleData tmptup;
	TupleDesc	indesc,
				outdesc;

	tupDatum = *op->resvalue;

	/* this test covers the isDone exception too: */
	if (*op->resnull)
		return;

	tuple = DatumGetHeapTupleHeader(tupDatum);

	/* Lookup tupdescs if first time through or after rescan */
	if (op->d.convert_rowtype.indesc == NULL)
	{
		get_cached_rowtype(exprType((Node *) convert->arg), -1,
						   &op->d.convert_rowtype.indesc,
						   econtext);
		op->d.convert_rowtype.initialized = false;
	}
	if (op->d.convert_rowtype.outdesc == NULL)
	{
		get_cached_rowtype(convert->resulttype, -1,
						   &op->d.convert_rowtype.outdesc,
						   econtext);
		op->d.convert_rowtype.initialized = false;
	}

	indesc = op->d.convert_rowtype.indesc;
	outdesc = op->d.convert_rowtype.outdesc;

	/*
	 * We used to be able to assert that incoming tuples are marked with
	 * exactly the rowtype of cstate->indesc.  However, now that
	 * ExecEvalWholeRowVar might change the tuples' marking to plain RECORD
	 * due to inserting aliases, we can only make this weak test:
	 */
	Assert(HeapTupleHeaderGetTypeId(tuple) == indesc->tdtypeid ||
		   HeapTupleHeaderGetTypeId(tuple) == RECORDOID);

	/* if first time through, initialize conversion map */
	if (!op->d.convert_rowtype.initialized)
	{
		MemoryContext old_cxt;

		/* allocate map in long-lived memory context */
		old_cxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		/* prepare map from old to new attribute numbers */
		op->d.convert_rowtype.map =
			convert_tuples_by_name(indesc, outdesc,
								 gettext_noop("could not convert row type"));
		op->d.convert_rowtype.initialized = true;

		MemoryContextSwitchTo(old_cxt);
	}

	/*
	 * No-op if no conversion needed (not clear this can happen here).
	 */
	if (op->d.convert_rowtype.map == NULL)
		return;

	/*
	 * do_convert_tuple needs a HeapTuple not a bare HeapTupleHeader.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	result = do_convert_tuple(&tmptup, op->d.convert_rowtype.map);

	*op->resvalue = HeapTupleGetDatum(result);

}

/*
 * ExecEvalScalarArrayOp
 *
 * Evaluate "scalar op ANY/ALL (array)".  The operator always yields boolean,
 * and we combine the results across all array elements using OR and AND
 * (for ANY and ALL respectively).  Of course we short-circuit as soon as
 * the result is known.
 */
void
ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)
{
	ScalarArrayOpExpr *opexpr = op->d.scalararrayop.opexpr;
	bool		useOr = opexpr->useOr;
	FunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo_data;
	bool		strictfunc = op->d.scalararrayop.finfo->fn_strict;
	ArrayType  *arr;
	int			nitems;
	Datum		result;
	bool		resultnull;
	int			i;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char	   *s;
	bits8	   *bitmap;
	int			bitmask;

	/*
	 * If the array is NULL then we return NULL --- it's not very meaningful
	 * to do anything else, even if the operator isn't strict.
	 */
	if (*op->resnull)
	{
		return;
	}

	/* no "bad" nulls, okay to fetch and detoast the array */
	arr = DatumGetArrayTypeP(*op->resvalue);

	/*
	 * If the array is empty, we return either FALSE or TRUE per the useOr
	 * flag.  This is correct even if the scalar is NULL; since we would
	 * evaluate the operator zero times, it matters not whether it would want
	 * to return NULL.
	 */
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	if (nitems <= 0)
	{
		*op->resnull = false;
		*op->resvalue = BoolGetDatum(!useOr);
		return;
	}

	/*
	 * If the scalar is NULL, and the function is strict, return NULL; no
	 * point in iterating the loop.
	 */
	if (fcinfo->argnull[0] && strictfunc)
	{
		*op->resnull = true;
		return;
	}

	/*
	 * We arrange to look up info about the element type only once per series
	 * of calls, assuming the element type doesn't change underneath us.
	 */
	if (op->d.scalararrayop.element_type != ARR_ELEMTYPE(arr))
	{
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &op->d.scalararrayop.typlen,
							 &op->d.scalararrayop.typbyval,
							 &op->d.scalararrayop.typalign);
		op->d.scalararrayop.element_type = ARR_ELEMTYPE(arr);
	}

	typlen = op->d.scalararrayop.typlen;
	typbyval = op->d.scalararrayop.typbyval;
	typalign = op->d.scalararrayop.typalign;

	result = BoolGetDatum(!useOr);
	resultnull = false;

	/* Loop over the array elements */
	s = (char *) ARR_DATA_PTR(arr);
	bitmap = ARR_NULLBITMAP(arr);
	bitmask = 1;

	for (i = 0; i < nitems; i++)
	{
		Datum		elt;
		Datum		thisresult;

		/* Get array element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			fcinfo->arg[1] = (Datum) 0;
			fcinfo->argnull[1] = true;
		}
		else
		{
			elt = fetch_att(s, typbyval, typlen);
			s = att_addlength_pointer(s, typlen, s);
			s = (char *) att_align_nominal(s, typalign);
			fcinfo->arg[1] = elt;
			fcinfo->argnull[1] = false;
		}

		/* Call comparison function */
		if (fcinfo->argnull[1] && strictfunc)
		{
			fcinfo->isnull = true;
			thisresult = (Datum) 0;
		}
		else
		{
			fcinfo->isnull = false;
			thisresult = (op->d.scalararrayop.fn_addr) (fcinfo);
		}

		/* Combine results per OR or AND semantics */
		if (fcinfo->isnull)
			resultnull = true;
		else if (useOr)
		{
			if (DatumGetBool(thisresult))
			{
				result = BoolGetDatum(true);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}
		else
		{
			if (!DatumGetBool(thisresult))
			{
				result = BoolGetDatum(false);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	*op->resnull = resultnull;
	*op->resvalue = result;
}

/*
 * Evaluate a NOT NULL constraint.
 */
void
ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)
{
	if (*op->resnull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("domain %s does not allow null values",
						format_type_be(op->d.domaincheck.resulttype)),
				 errdatatype(op->d.domaincheck.resulttype)));
}

/*
 * Evaluate a CHECK constraint.
 */
void
ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)
{
	if (!*op->d.domaincheck.checknull &&
		!DatumGetBool(*op->d.domaincheck.checkvalue))
		ereport(ERROR,
				(errcode(ERRCODE_CHECK_VIOLATION),
			   errmsg("value for domain %s violates check constraint \"%s\"",
					  format_type_be(op->d.domaincheck.resulttype),
					  op->d.domaincheck.constraintname),
				 errdomainconstraint(op->d.domaincheck.resulttype,
									 op->d.domaincheck.constraintname)));
}

/*
 * Evaluate the various forms of XmlExpr.
 */
void
ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)
{
	XmlExpr    *xexpr = op->d.xmlexpr.xexpr;
	Datum		value;
	ListCell   *arg;
	ListCell   *narg;
	int			i;

	*op->resnull = true;		/* until we get a result */
	*op->resvalue = (Datum) 0;

	switch (xexpr->op)
	{
		case IS_XMLCONCAT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				List	   *values = NIL;

				for (i = 0; i < list_length(xexpr->args); i++)
				{
					if (!argnull[i])
						values = lappend(values, DatumGetPointer(argvalue[i]));
				}

				if (list_length(values) > 0)
				{
					*op->resnull = false;
					*op->resvalue = PointerGetDatum(xmlconcat(values));
				}
			}
			break;

		case IS_XMLFOREST:
			{
				Datum	   *argvalue = op->d.xmlexpr.named_argvalue;
				bool	   *argnull = op->d.xmlexpr.named_argnull;
				StringInfoData buf;

				initStringInfo(&buf);

				i = 0;
				forboth(arg, xexpr->named_args, narg, xexpr->arg_names)
				{
					char	   *argname = strVal(lfirst(narg));
					Expr	   *e = (Expr *) lfirst(arg);

					if (!argnull[i])
					{
						value = argvalue[i];
						appendStringInfo(&buf, "<%s>%s</%s>",
										 argname,
										 map_sql_value_to_xml_value(value, exprType((Node *) e), true),
										 argname);
						*op->resnull = false;
					}
					i++;
				}

				if (*op->resnull)
				{
					pfree(buf.data);
				}
				else
				{
					text	   *result;

					result = cstring_to_text_with_len(buf.data, buf.len);
					pfree(buf.data);

					*op->resvalue = PointerGetDatum(result);
				}
			}
			break;

		case IS_XMLELEMENT:
			*op->resnull = false;
			*op->resvalue = PointerGetDatum(
											xmlelement(xexpr,
												 op->d.xmlexpr.named_argnull,
												op->d.xmlexpr.named_argvalue,
													   op->d.xmlexpr.argnull,
													op->d.xmlexpr.argvalue));
			break;
		case IS_XMLPARSE:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				text	   *data;
				bool		preserve_whitespace;

				/* arguments are known to be text, bool */
				Assert(list_length(xexpr->args) == 2);

				if (argnull[0])
					return;
				value = argvalue[0];
				data = DatumGetTextPP(value);

				if (argnull[1]) /* probably can't happen */
					return;
				value = argvalue[1];
				preserve_whitespace = DatumGetBool(value);

				*op->resnull = false;
				*op->resvalue = PointerGetDatum(
					  xmlparse(data, xexpr->xmloption, preserve_whitespace));
			}
			break;

		case IS_XMLPI:
			{
				text	   *arg;
				bool		isnull;

				/* optional argument is known to be text */
				Assert(list_length(xexpr->args) <= 1);

				if (xexpr->args)
				{
					isnull = op->d.xmlexpr.argnull[0];
					if (isnull)
						arg = NULL;
					else
						arg = DatumGetTextPP(op->d.xmlexpr.argvalue[0]);

				}
				else
				{
					arg = NULL;
					isnull = false;
				}

				*op->resvalue = PointerGetDatum(xmlpi(xexpr->name, arg,
													  isnull, op->resnull));
			}
			break;

		case IS_XMLROOT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				xmltype    *data;
				text	   *version;
				int			standalone;

				/* arguments are known to be xml, text, int */
				Assert(list_length(xexpr->args) == 3);

				if (argnull[0])
					return;
				data = DatumGetXmlP(argvalue[0]);

				if (argnull[1])
					version = NULL;
				else
					version = DatumGetTextPP(argvalue[1]);

				Assert(!argnull[2]);	/* always present */
				standalone = DatumGetInt32(argvalue[2]);

				*op->resnull = false;
				*op->resvalue = PointerGetDatum(xmlroot(data,
														version,
														standalone));
			}
			break;

		case IS_XMLSERIALIZE:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;

				/* argument type is known to be xml */
				Assert(list_length(xexpr->args) == 1);

				if (argnull[0])
					return;

				value = argvalue[0];
				*op->resnull = false;
				*op->resvalue = PointerGetDatum(
								xmltotext_with_xmloption(DatumGetXmlP(value),
														 xexpr->xmloption));
			}
			break;

		case IS_DOCUMENT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;

				/* optional argument is known to be xml */
				Assert(list_length(xexpr->args) == 1);

				if (argnull[0])
					break;

				value = argvalue[0];
				*op->resnull = false;
				*op->resvalue =
					BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
			}
			break;
		default:
			elog(ERROR, "unrecognized XML operation");
	}
}

/*
 * ExecEvalGroupingFunc
 *
 * Computes a bitmask with a bit for each (unevaluated) argument expression
 * (rightmost arg is least significant bit).
 *
 * A bit is set if the corresponding expression is NOT part of the set of
 * grouping expressions in the current grouping set.
 */
void
ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op)
{
	int			result = 0;
	int			attnum = 0;
	Bitmapset  *grouped_cols = op->d.grouping_func.parent->grouped_cols;
	ListCell   *lc;

	*op->resnull = false;

	foreach(lc, op->d.grouping_func.clauses)
	{
		attnum = lfirst_int(lc);

		result = result << 1;

		if (!bms_is_member(attnum, grouped_cols))
			result = result | 1;
	}

	*op->resvalue = Int32GetDatum(result);
}

/*
 * Hand off evaluation of a subplan to nodeSubplan.c
 */
void
ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	SubPlanState *sstate = op->d.subplan.sstate;

	/* could potentially be nested, so make sure there's enough stack */
	check_stack_depth();

	*op->resvalue = ExecSubPlan(sstate, econtext, op->resnull);
}

/*
 * Hand off evaluation of an alternative subplan to nodeSubplan.c
 */
void
ExecEvalAlternativeSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	AlternativeSubPlanState *asstate = op->d.alternative_subplan.asstate;

	/* could potentially be nested, so make sure there's enough stack */
	check_stack_depth();

	*op->resvalue = ExecAlternativeSubPlan(asstate, econtext, op->resnull);
}

/*
 * Evaluate a wholerow Var expression.
 *
 * Returns a Datum whose value is the value of a whole-row range variable with
 * respect to given expression context.
 */
void
ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	Var		   *variable = op->d.wholerow.var;
	TupleTableSlot *slot;
	TupleDesc	output_tupdesc;
	MemoryContext oldcontext;
	bool		needslow = false;
	HeapTupleHeader dtuple;
	HeapTuple	tuple;

	/* This was checked by ExecInitExpr */
	Assert(variable->varattno == InvalidAttrNumber);

	/* Get the input slot we want */
	switch (variable->varno)
	{
		case INNER_VAR: /* get the tuple from the inner node */
			slot = econtext->ecxt_innertuple;
			break;

		case OUTER_VAR: /* get the tuple from the outer node */
			slot = econtext->ecxt_outertuple;
			break;

			/* INDEX_VAR is handled by default case */

		default:				/* get the tuple from the relation being
								 * scanned */
			slot = econtext->ecxt_scantuple;
			break;
	}


	/* Apply the junkfilter if any */
	if (op->d.wholerow.junkFilter != NULL)
		slot = ExecFilterJunk(op->d.wholerow.junkFilter, slot);

	/*
	 * First time through, perform initialization.
	 *
	 * XXX: It'd be great if this could be moved to the the expression
	 * initialization phase, but due to using slots that's currently not
	 * feasible.
	 */
	if (op->d.wholerow.first)
	{
		/*
		 * If the Var identifies a named composite type, we must check that
		 * the actual tuple type is compatible with it.
		 */
		if (variable->vartype != RECORDOID)
		{
			TupleDesc	var_tupdesc;
			TupleDesc	slot_tupdesc;
			int			i;

			/*
			 * We really only care about numbers of attributes and data types.
			 * Also, we can ignore type mismatch on columns that are dropped
			 * in the destination type, so long as (1) the physical storage
			 * matches or (2) the actual column value is NULL.  Case (1) is
			 * helpful in some cases involving out-of-date cached plans, while
			 * case (2) is expected behavior in situations such as an INSERT
			 * into a table with dropped columns (the planner typically
			 * generates an INT4 NULL regardless of the dropped column type).
			 * If we find a dropped column and cannot verify that case (1)
			 * holds, we have to use ExecEvalWholeRowSlow to check (2) for
			 * each row.
			 */
			var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

			slot_tupdesc = slot->tts_tupleDescriptor;

			if (var_tupdesc->natts != slot_tupdesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail_plural("Table row contains %d attribute, but query expects %d.",
				   "Table row contains %d attributes, but query expects %d.",
										  slot_tupdesc->natts,
										  slot_tupdesc->natts,
										  var_tupdesc->natts)));

			for (i = 0; i < var_tupdesc->natts; i++)
			{
				Form_pg_attribute vattr = var_tupdesc->attrs[i];
				Form_pg_attribute sattr = slot_tupdesc->attrs[i];

				if (vattr->atttypid == sattr->atttypid)
					continue;	/* no worries */
				if (!vattr->attisdropped)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("table row type and query-specified row type do not match"),
							 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
									   format_type_be(sattr->atttypid),
									   i + 1,
									   format_type_be(vattr->atttypid))));

				if (vattr->attlen != sattr->attlen ||
					vattr->attalign != sattr->attalign)
					needslow = true;	/* need runtime check for null */
			}

			/*
			 * Use the variable's declared rowtype as the descriptor for the
			 * output values, modulo possibly assigning new column names
			 * below. In particular, we *must* absorb any attisdropped
			 * markings.
			 */
			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			output_tupdesc = CreateTupleDescCopy(var_tupdesc);
			MemoryContextSwitchTo(oldcontext);

			ReleaseTupleDesc(var_tupdesc);
		}
		else
		{
			/*
			 * In the RECORD case, we use the input slot's rowtype as the
			 * descriptor for the output values, modulo possibly assigning new
			 * column names below.
			 */
			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			output_tupdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
			MemoryContextSwitchTo(oldcontext);
		}

		/*
		 * Construct a tuple descriptor for the composite values we'll
		 * produce, and make sure its record type is "blessed".  The main
		 * reason to do this is to be sure that operations such as
		 * row_to_json() will see the desired column names when they look up
		 * the descriptor from the type information embedded in the composite
		 * values.
		 *
		 * We already got the correct physical datatype info above, but now we
		 * should try to find the source RTE and adopt its column aliases, in
		 * case they are different from the original rowtype's names.  For
		 * example, in "SELECT foo(t) FROM tab t(x,y)", the first two columns
		 * in the composite output should be named "x" and "y" regardless of
		 * tab's column names.
		 *
		 * If we can't locate the RTE, assume the column names we've got are
		 * OK. (As of this writing, the only cases where we can't locate the
		 * RTE are in execution of trigger WHEN clauses, and then the Var will
		 * have the trigger's relation's rowtype, so its names are fine.)
		 * Also, if the creator of the RTE didn't bother to fill in an eref
		 * field, assume our column names are OK.  (This happens in COPY, and
		 * perhaps other places.)
		 */
		if (econtext->ecxt_estate &&
		variable->varno <= list_length(econtext->ecxt_estate->es_range_table))
		{
			RangeTblEntry *rte = rt_fetch(variable->varno,
									  econtext->ecxt_estate->es_range_table);

			if (rte->eref)
				ExecTypeSetColNames(output_tupdesc, rte->eref->colnames);
		}

		/* Bless the tupdesc if needed, and save it in the execution state */
		op->d.wholerow.tupdesc = BlessTupleDesc(output_tupdesc);

		op->d.wholerow.first = false;
	}

	if (needslow)
	{
		TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
		TupleDesc	var_tupdesc = op->d.wholerow.tupdesc;
		int			i;

		tuple = ExecFetchSlotTuple(slot);


		/* Check to see if any dropped attributes are non-null */
		for (i = 0; i < var_tupdesc->natts; i++)
		{
			Form_pg_attribute vattr = var_tupdesc->attrs[i];
			Form_pg_attribute sattr = tupleDesc->attrs[i];

			if (!vattr->attisdropped)
				continue;		/* already checked non-dropped cols */
			if (heap_attisnull(tuple, i + 1))
				continue;		/* null is always okay */
			if (vattr->attlen != sattr->attlen ||
				vattr->attalign != sattr->attalign)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.",
								   i + 1)));
		}
	}

	/*
	 * Copy the slot tuple and make sure any toasted fields get detoasted.
	 */
	dtuple = DatumGetHeapTupleHeader(ExecFetchSlotTupleDatum(slot));

	/*
	 * Label the datum with the composite type info we identified before.
	 */
	HeapTupleHeaderSetTypeId(dtuple, op->d.wholerow.tupdesc->tdtypeid);
	HeapTupleHeaderSetTypMod(dtuple, op->d.wholerow.tupdesc->tdtypmod);

	*op->resnull = false;
	*op->resvalue = PointerGetDatum(dtuple);
}

/*
 * Callback function to release a tupdesc refcount at expression tree shutdown
 */
static void
ShutdownTupleDescRef(Datum arg)
{
	TupleDesc  *cache_field = (TupleDesc *) DatumGetPointer(arg);

	if (*cache_field)
		ReleaseTupleDesc(*cache_field);
	*cache_field = NULL;
}

/*
 * get_cached_rowtype: utility function to lookup a rowtype tupdesc
 *
 * type_id, typmod: identity of the rowtype
 * cache_field: where to cache the TupleDesc pointer in expression state node
 *		(field must be initialized to NULL)
 * econtext: expression context we are executing in
 *
 * NOTE: because the shutdown callback will be called during plan rescan,
 * must be prepared to re-do this during any node execution; cannot call
 * just once during expression initialization
 */
static TupleDesc
get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext)
{
	TupleDesc	tupDesc = *cache_field;

	/* Do lookup if no cached value or if requested type changed */
	if (tupDesc == NULL ||
		type_id != tupDesc->tdtypeid ||
		typmod != tupDesc->tdtypmod)
	{
		tupDesc = lookup_rowtype_tupdesc(type_id, typmod);

		if (*cache_field)
		{
			/* Release old tupdesc; but callback is already registered */
			ReleaseTupleDesc(*cache_field);
		}
		else
		{
			/* Need to register shutdown callback to release tupdesc */
			RegisterExprContextCallback(econtext,
										ShutdownTupleDescRef,
										PointerGetDatum(cache_field));
		}
		*cache_field = tupDesc;
	}
	return tupDesc;
}
