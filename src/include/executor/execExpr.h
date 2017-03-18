/*-------------------------------------------------------------------------
 *
 * execExpr.h
 *	  Low level infrastructure related to expression evaluation
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execExpr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXEC_EXPR_H
#define EXEC_EXPR_H

#include "nodes/execnodes.h"

struct ArrayRefState;

/* flag signalling that interpreter has been initialized */
#define EEO_FLAG_INTERPRETER_INITIALIZED (1 << 0)
/* flag signalling that jump-threading is in use */
#define EEO_FLAG_DIRECT_THREADED (1 << 1)

typedef enum ExprEvalOp
{
	EEOP_DONE,
	EEOP_INNER_FETCHSOME,
	EEOP_OUTER_FETCHSOME,
	EEOP_SCAN_FETCHSOME,
	EEOP_INNER_VAR,
	EEOP_OUTER_VAR,
	EEOP_SCAN_VAR,
	EEOP_ASSIGN_INNER_VAR,
	EEOP_ASSIGN_OUTER_VAR,
	EEOP_ASSIGN_SCAN_VAR,
	EEOP_ASSIGN_TMP,
	EEOP_ASSIGN_TMP_UNEXPAND,
	EEOP_INNER_SYSVAR,
	EEOP_OUTER_SYSVAR,
	EEOP_SCAN_SYSVAR,
	EEOP_CONST,
	EEOP_FUNCEXPR,
	EEOP_FUNCEXPR_STRICT,
	EEOP_FUNCEXPR_FUSAGE,
	EEOP_FUNCEXPR_STRICT_FUSAGE,
	EEOP_BOOL_AND_STEP_FIRST,
	EEOP_BOOL_AND_STEP,
	EEOP_BOOL_AND_STEP_LAST,
	EEOP_BOOL_OR_STEP_FIRST,
	EEOP_BOOL_OR_STEP,
	EEOP_BOOL_OR_STEP_LAST,
	EEOP_BOOL_NOT_STEP,
	EEOP_QUAL,
	EEOP_NULLTEST_ISNULL,
	EEOP_NULLTEST_ISNOTNULL,
	EEOP_NULLTEST_ROWISNULL,
	EEOP_NULLTEST_ROWISNOTNULL,
	EEOP_PARAM_EXEC,
	EEOP_PARAM_EXTERN,
	EEOP_CASE_WHEN_STEP,
	EEOP_CASE_THEN_STEP,
	EEOP_CASE_TESTVAL,
	EEOP_CASE_TESTVAL_UNEXPAND,
	EEOP_COALESCE,
	EEOP_BOOLTEST_IS_TRUE,
	EEOP_BOOLTEST_IS_NOT_TRUE,
	EEOP_BOOLTEST_IS_FALSE,
	EEOP_BOOLTEST_IS_NOT_FALSE,
	EEOP_BOOLTEST_IS_UNKNOWN,
	EEOP_BOOLTEST_IS_NOT_UNKNOWN,
	EEOP_WHOLEROW,
	EEOP_IOCOERCE,
	EEOP_DISTINCT,
	EEOP_NULLIF,
	EEOP_SQLVALUEFUNCTION,
	EEOP_CURRENTOFEXPR,
	EEOP_ARRAYEXPR,
	EEOP_ARRAYCOERCE,
	EEOP_ROW,
	EEOP_ROWCOMPARE_STEP,
	EEOP_ROWCOMPARE_FINAL,
	EEOP_MINMAX,
	EEOP_FIELDSELECT,
	EEOP_FIELDSTORE_DEFORM,
	EEOP_FIELDSTORE_FORM,
	EEOP_ARRAYREF_CHECKINPUT,
	EEOP_ARRAYREF_CHECKSUBSCRIPT,
	EEOP_ARRAYREF_OLD,
	EEOP_ARRAYREF_ASSIGN,
	EEOP_ARRAYREF_FETCH,
	EEOP_CONVERT_ROWTYPE,
	EEOP_SCALARARRAYOP,
	EEOP_DOMAIN_TESTVAL,
	EEOP_DOMAIN_TESTVAL_UNEXPAND,
	EEOP_DOMAIN_NOTNULL,
	EEOP_DOMAIN_CHECK,
	EEOP_XMLEXPR,
	EEOP_AGGREF,
	EEOP_GROUPING_FUNC,
	EEOP_WINDOW_FUNC,
	EEOP_SUBPLAN,
	EEOP_ALTERNATIVE_SUBPLAN,
	EEOP_LAST
} ExprEvalOp;


typedef struct ExprEvalStep
{
	/*
	 * Instruction to be executed. During instruction preparation this is an
	 * ExprEvalOp, but during execution it can be swapped out to some other
	 * type, e.g. a pointer for computed goto (that's why it's a intptr_t).
	 */
	intptr_t	opcode;

	/* target for the result of the current instruction */
	bool	   *resnull;
	Datum	   *resvalue;

	/*
	 * Data for an operation. Inline stored data is faster to access, but also
	 * bloats the size of all instructions. The union should be kept below 40
	 * bytes an 64bit systems (so the entire struct is below 64bytes, a single
	 * cacheline on common systems).
	 */
	union
	{
		struct
		{
			int			attnum;
		}			var;

		struct
		{
			Var		   *var;
			bool		first;	/* first time through, initialize */
			TupleDesc	tupdesc;	/* descriptor for resulting tuples */
			JunkFilter *junkFilter;		/* JunkFilter to remove resjunk cols */
		}			wholerow;

		struct
		{
			Datum		value;
			bool		isnull;
		}			constval;

		struct
		{
			FmgrInfo   *finfo;
			FunctionCallInfo fcinfo_data;
			/* faster to access without additional indirection */
			PGFunction	fn_addr;
			int			nargs;
		}			func;

		struct
		{
			Datum	   *value;
			bool	   *isnull;
			bool	   *anynull;
			int			jumpdone;
		}			boolexpr;

		struct
		{
			int			jumpdone;
		}			qualexpr;

		struct
		{
			TupleDesc	argdesc;
		}			nulltest_row;

		struct
		{
			int			paramid;
			int			paramtype;
		}			param;

		struct
		{
			Datum	   *value;
			bool	   *isnull;
			int			jumpfalse;
		}			casewhen;

		struct
		{
			Datum	   *value;
			bool	   *isnull;
			int			jumpdone;
		}			casethen;

		struct
		{
			Datum	   *value;
			bool	   *isnull;
		}			casetest;

		struct
		{
			int			jumpdone;
		}			coalesce;

		struct
		{
			/* lookup info for source output function */
			FmgrInfo   *finfo_out;
			FunctionCallInfo fcinfo_data_out;
			/* lookup info for result input function */
			FmgrInfo   *finfo_in;
			FunctionCallInfo fcinfo_data_in;
			Oid			intypioparam;	/* argument needed for input function */
		}			iocoerce;

		struct
		{
			SQLValueFunction *svf;
		}			sqlvaluefunction;

		struct
		{
			ArrayExpr  *arrayexpr;
			Datum	   *elemvalues;
			bool	   *elemnulls;
			int			nelems;
			int16		elemlength;		/* typlen of the array element type */
			bool		elembyval;		/* is the element type pass-by-value? */
			char		elemalign;		/* typalign of the element type */
		}			arrayexpr;

		struct
		{
			ArrayCoerceExpr *coerceexpr;
			Oid			resultelemtype; /* element type of result array */
			FmgrInfo   *elemfunc;		/* lookup info for element coercion
										 * function */
			struct ArrayMapState *amstate;		/* workspace for array_map */
		}			arraycoerce;

		struct
		{
			RowExpr    *rowexpr;
			/* the arguments */
			Datum	   *elemvalues;
			bool	   *elemnulls;
			TupleDesc	tupdesc;	/* descriptor for result tuples */
		}			row;

		struct
		{
			FmgrInfo   *finfo;
			FunctionCallInfo fcinfo_data;
			PGFunction	fn_addr;
			int			jumpnull;
			int			jumpdone;
		}			rowcompare_step;

		struct
		{
			RowCompareType rctype;
		}			rowcompare_final;

		struct
		{
			/* the arguments */
			Datum	   *values;
			bool	   *nulls;
			int			nelems;

			MinMaxOp	op;
			FmgrInfo   *finfo;
			FunctionCallInfo fcinfo_data;
		}			minmax;

		struct
		{
			/* tupdesc for most recent input */
			TupleDesc	argdesc;
			AttrNumber	fieldnum;
			Oid			resulttype;
		}			fieldselect;

		struct
		{
			/* tupdesc for most recent input */
			TupleDesc  *argdesc;
			FieldStore *fstore;

			/* the arguments */
			Datum	   *values;
			bool	   *nulls;
		}			fieldstore;

		struct
		{
			/* too big to have inline */
			struct ArrayRefState *state;
			int			jumpdone;
		}			arrayref;

		struct
		{
			/* too big to have inline */
			struct ArrayRefState *state;
			int			off;
			int			jumpdone;
			bool		isupper;
		}			arrayref_checksubscript;

		struct
		{
			ConvertRowtypeExpr *convert;
			TupleDesc	indesc;
			TupleDesc	outdesc;
			TupleConversionMap *map;
			bool		initialized;
		}			convert_rowtype;

		struct
		{
			ScalarArrayOpExpr *opexpr;
			Oid			element_type;
			int16		typlen;
			bool		typbyval;
			char		typalign;
			FmgrInfo   *finfo;
			FunctionCallInfo fcinfo_data;
			PGFunction	fn_addr;
		}			scalararrayop;

		struct
		{
			char	   *constraintname;
			Datum	   *checkvalue;
			bool	   *checknull;
			Oid			resulttype;
		}			domaincheck;

		struct
		{
			XmlExpr    *xexpr;
			Datum	   *named_argvalue;
			bool	   *named_argnull;
			Datum	   *argvalue;
			bool	   *argnull;
		}			xmlexpr;

		struct
		{
			AggrefExprState *astate;
		}			aggref;

		struct
		{
			AggState   *parent;
			List	   *clauses;
		}			grouping_func;

		struct
		{
			WindowFuncExprState *wfstate;
		}			window_func;

		struct
		{
			SubPlanState *sstate;
		}			subplan;

		struct
		{
			AlternativeSubPlanState *asstate;
		}			alternative_subplan;

		struct
		{
			size_t		resultnum;
			int			attnum;
		}			assign_var;

		struct
		{
			size_t		resultnum;
		}			assign_tmp;

		struct
		{
			int			last_var;
		}			fetch;
	}			d;
} ExprEvalStep;


typedef struct ArrayRefState
{
	bool		isassignment;
	int			numupper;
	Datum		upper[MAXDIM];
	int			upperindex[MAXDIM];
	bool		uppernull[MAXDIM];
	bool		upperprovided[MAXDIM];
	int			numlower;
	Datum		lower[MAXDIM];
	int			lowerindex[MAXDIM];
	bool		lowernull[MAXDIM];
	bool		lowerprovided[MAXDIM];

	Oid			refelemtype;
	int16		refattrlength;	/* typlen of array type */
	int16		refelemlength;	/* typlen of the array element type */
	bool		refelembyval;	/* is the element type pass-by-value? */
	char		refelemalign;	/* typalign of the element type */

	Datum		replacevalue;
	bool		replacenull;

	Datum		prevvalue;
	bool		prevnull;
}	ArrayRefState;

extern void ExecInstantiateInterpretedExpr(ExprState *state);

extern ExprEvalOp ExecEvalStepOp(ExprState *state, ExprEvalStep *op);

/*
 * Non fast-path execution functions. These are externs instead of static in
 * execInterpExpr.c, because that allows them to be used by other methods of
 * expression evaluation.
 */
extern void ExecEvalParamExtern(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op);
extern void ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op);
extern void ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op);
extern void ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op);
extern void ExecEvalRow(ExprState *state, ExprEvalStep *op);
extern void ExecEvalMinMax(ExprState *state, ExprEvalStep *op);
extern void ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern bool ExecEvalArrayRefCheckSubscript(ExprState *state, ExprEvalStep *op);
extern void ExecEvalArrayRefFetch(ExprState *state, ExprEvalStep *op);
extern void ExecEvalArrayRefAssign(ExprState *state, ExprEvalStep *op);
extern void ExecEvalArrayRefOld(ExprState *state, ExprEvalStep *op);
extern void ExecEvalRowNull(ExprState *state, ExprEvalStep *op);
extern void ExecEvalRowNotNull(ExprState *state, ExprEvalStep *op);
extern void ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op);
extern void ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op);
extern void ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op);
extern void ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op);
extern void ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op);
extern void ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalAlternativeSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext);
extern void ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext);

#endif   /* EXEC_EXPR_H */
