/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"


/*
 * nodeHashJoin execution states
 */
#define HJ_BUILD_HASHTABLE				1
#define HJ_SCAN_OUTER					2
#define HJ_DONE							3
/* left ONLY to avoid deleting ReScan, it is not used */
#define HJ_NEED_NEW_OUTER			   -1

static TupleTableSlot *ExecHashJoinGetSavedTuple(BufFile *file,
												 uint32 *hashvalue,
												 TupleTableSlot *tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState *hjstate);
static TupleTableSlot *ExecHashJoinTakeOuterFromTempFile(HashJoinState *hjstate,
														 uint32 *hashvalue);
static inline bool ExecHashJoinNewOuter(HashJoinState *hjstate);
static inline bool ExecHashJoinEndOfBatch(HashJoinState *hjstate);

/*
 * This function will be called from Hash node with NULL slot, signaling
 * that the hashtable is built.
 * "Extract-one-outer-tuple-to-check-if-it-is-null-before-building-hashtable"
 * optimization is not implemented for now, the hashtable will be always built
 * first.
 */
void
ExecPushNullToHashJoinFromInner(TupleTableSlot *slot, HashJoinState *node)
{
	HashJoinTable hashtable;
	HashState *hashNode;

	hashNode = (HashState *) innerPlanState(node);

	/* we should get there only once */
	Assert(node->hj_JoinState == HJ_BUILD_HASHTABLE);
	/* we will fish out the tuples from Hash node ourselves */
	Assert(TupIsNull(slot));

	/* we always build the hashtable first */
	node->hj_FirstOuterTupleSlot = NULL;

	hashtable = hashNode->hashtable;
	node->hj_HashTable = hashtable;

	/*
	 * need to remember whether nbatch has increased since we
	 * began scanning the outer relation
	 */
	hashtable->nbatch_outstart = hashtable->nbatch;

	/*
	 * Reset OuterNotEmpty for scan.
	 */
	node->hj_OuterNotEmpty = false;

	node->hj_JoinState = HJ_SCAN_OUTER;
}

/*
 * Null push from the outer side, so this is the end of the first
 * batch. Finalize it and handle other batches, taking outer tuples from temp
 * files.
 */
void
ExecPushNullToHashJoinFromOuter(TupleTableSlot *slot, HashJoinState *node)
{
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	HashJoinTable hashtable = node->hj_HashTable;
	/* we don't need it, just to conform ExecHashGetBucketAndBatch signature */
	int batchno;
	bool parent_accepts_tuples;

	/* We must always be in this state during pushes from outer  */
	Assert(node->hj_JoinState == HJ_SCAN_OUTER);

	/* end of the first batch */
	parent_accepts_tuples = ExecHashJoinEndOfBatch(node);

	/* loop while we run out of batches or parent stops accepting tuples */
	while (node->hj_JoinState != HJ_DONE && parent_accepts_tuples)
	{
		slot = ExecHashJoinTakeOuterFromTempFile(node, &node->hj_CurHashValue);
		if (TupIsNull(slot))
			/* end of batch, no more outer tuples here */
			parent_accepts_tuples = ExecHashJoinEndOfBatch(node);
		else
		{
			econtext->ecxt_outertuple = slot;
			/*
			 * Find the corresponding bucket for this tuple in the main
			 * hash table or skew hash table.
			 */
			ExecHashGetBucketAndBatch(hashtable, node->hj_CurHashValue,
									  &node->hj_CurBucketNo, &batchno);
			node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
															 node->hj_CurHashValue);
			parent_accepts_tuples = ExecHashJoinNewOuter(node);
		}

	}
}

/*
 * Non-null push from the outer side. Finds matches and sends them upward to
 * HashJoin's parent. Returns true if the parent still waits for tuples, false
 * otherwise. When this function is called, the hashtable must already be
 * filled.
 */
bool
ExecPushTupleToHashJoinFromOuter(TupleTableSlot *slot,
								 HashJoinState *node)
{
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	HashJoinTable hashtable = node->hj_HashTable;
	int			batchno;

	/* We must always be in this state during pushes from outer	 */
	Assert(node->hj_JoinState == HJ_SCAN_OUTER);

	/*
	 * We have to compute the tuple's hash value.
	 */
	econtext->ecxt_outertuple = slot;
	if (!ExecHashGetHashValue(hashtable, econtext,
							  node->hj_OuterHashKeys,
							  true,		/* outer tuple */
							  HJ_FILL_OUTER(node),
							  &node->hj_CurHashValue))
	{
		/*
		 * That tuple couldn't match because of a NULL hashed attr, so discard
		 * it and wait for the next one.
		 */
		return true;
	}

	/*
	 * Find the corresponding bucket for this tuple in the main
	 * hash table or skew hash table.
	 */
	ExecHashGetBucketAndBatch(hashtable, node->hj_CurHashValue,
							  &node->hj_CurBucketNo, &batchno);
	node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
													 node->hj_CurHashValue);

	/*
	 * The tuple might not belong to the current batch which is 0 (it is
	 * always 0 while we receiving non-null tuples from below). "current
	 * batch" also includes the skew buckets if any).
	 */
	if (batchno != 0 && node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
	{
		/*
		 * Need to postpone this outer tuple to a later batch.
		 * Save it in the corresponding outer-batch file.
		 */
		Assert(batchno > hashtable->curbatch);
		ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(slot),
							  node->hj_CurHashValue,
							  &hashtable->outerBatchFile[batchno]);
		/* wait for the next tuple */
		return true;
	}

	/*
	 * Ok, now we have non-null outer tuple which belongs to current batch --
	 * time to search for matches
	 */
	return ExecHashJoinNewOuter(node);
}

/*
 * Called when we process non-null tuple from the outer side. Finds matches
 * for it and pushes them. Pushes dummy outer-join tuple, if no matches were
 * found.
 *
 * Outer tuple must be stored in
 * hjstate->js.ps.ps_ExprContext->ecxt_outertuple. Besides, bucket to scan
 * must be stored in node->CurBucketNo and node->hj_CurSkewBucketNo; probably
 * we could calculate them right in this function, but then we would have to
 * add here batchno != hashtable->curbatch check which is not needed when
 * batcho > 0, not nice.
 *
 * Returns true if the parent still waits for tuples, false otherwise.
 */
static inline bool ExecHashJoinNewOuter(HashJoinState *hjstate)
{
	ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
	JoinType	jointype = hjstate->js.jointype;

	/* not sure we should do it here */
	CHECK_FOR_INTERRUPTS();

	hjstate->hj_CurTuple = NULL;
	hjstate->hj_MatchedOuter = false;

	/*
	 * Push all matching tuples from selected hash bucket
	 */
	if (!ExecScanHashBucketAndPush(hjstate, econtext))
		return false;

	/* if in anti or semi join tuple matched we are done with it */
	if (hjstate->hj_MatchedOuter &&
		(jointype == JOIN_ANTI || jointype == JOIN_SEMI))
		return true;

	if (!hjstate->hj_MatchedOuter && HJ_FILL_OUTER(hjstate))
	{
		/*
		 * Generate a fake join tuple with nulls for the inner
		 * tuple, and push it if it passes the non-join quals.
		 */
		econtext->ecxt_innertuple = hjstate->hj_NullInnerTupleSlot;

		return CheckOtherQualAndPush(hjstate);
	}

	return true;
}

/*
 * Called when we finished the batch; push unmatched inner tuples, if we are
 * filling inner, and advance the batch. Returns true if the parent still
 * waits for tuples, false otherwise. Sets hjstate->hj_JoinState to HJ_DONE
 * if there are no more batches: this is the end of join. It signals parent
 * about the latter, pushing NULL -- caller mus.
 */
static inline bool ExecHashJoinEndOfBatch(HashJoinState *hjstate)
{
	ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

	if (HJ_FILL_INNER(hjstate))
	{
		/*
		 * We are doing right/full join,
		 * so any unmatched inner tuples in the hashtable have to be
		 * emitted before we continue to the next batch.
		 */

		/* set up to scan for unmatched inner tuples */
		ExecPrepHashTableForUnmatched(hjstate);
		if (!ExecScanHashTableForUnmatchedAndPush(hjstate, econtext))
			return false;
	}

	/* advance the batch TODO */
	if (!ExecHashJoinNewBatch(hjstate))
	{
		hjstate->hj_JoinState = HJ_DONE;	/* end of join */
		/* let parent know that we are done */
		ExecPushNull(NULL, (PlanState *) hjstate);
	}

	return true;
}

/*
 * Get next outer tuple from saved temp files. We are processing not the first
 * batch if we are here. On success, the tuple's hash value is stored at
 * *hashvalue, re-read from the temp file.
 * Returns NULL on the end of batch, a tuple otherwise.
 */
static TupleTableSlot *ExecHashJoinTakeOuterFromTempFile(HashJoinState *hjstate,
														 uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	BufFile    *file = hashtable->outerBatchFile[curbatch];

	/*
	 * In outer-join cases, we could get here even though the batch file
	 * is empty.
	 */
	if (file == NULL)
		return NULL;

	return ExecHashJoinGetSavedTuple(file,
									 hashvalue,
									 hjstate->hj_OuterTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags, PlanState *parent)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;
	hjstate->js.ps.parent = parent;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) hjstate);
	hjstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) hjstate);
	hjstate->js.jointype = node->join.jointype;
	hjstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) hjstate);
	hjstate->hashclauses = (List *)
		ExecInitExpr((Expr *) node->hashclauses,
					 (PlanState *) hjstate);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags,
										   (PlanState *) hjstate);
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags,
										   (PlanState *) hjstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &hjstate->js.ps);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(hjstate)));
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(outerPlanState(hjstate)));
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(outerPlanState(hjstate)));
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(hjstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(hjstate)));

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, hjstate->hashclauses)
	{
		FuncExprState *fstate = castNode(FuncExprState, lfirst(l));
		OpExpr	   *hclause = castNode(OpExpr, fstate->xprstate.expr);

		lclauses = lappend(lclauses, linitial(fstate->args));
		rclauses = lappend(rclauses, lsecond(fstate->args));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->hj_OuterHashKeys = lclauses;
	hjstate->hj_InnerHashKeys = rclauses;
	hjstate->hj_HashOperators = hoperators;
	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;

	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash table
	 */
	if (node->hj_HashTable)
	{
		ExecHashTableDestroy(node->hj_HashTable);
		node->hj_HashTable = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterTupleSlot);
	ExecClearTuple(node->hj_HashTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			nbatch;
	int			curbatch;
	BufFile    *innerFile;
	TupleTableSlot *slot;
	uint32		hashvalue;

	nbatch = hashtable->nbatch;
	curbatch = hashtable->curbatch;

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 */
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
	}
	else	/* we just finished the first batch */
	{
		/*
		 * Reset some of the skew optimization state variables, since we no
		 * longer need to consider skew tuples after the first batch. The
		 * memory context reset we are about to do will release the skew
		 * hashtable itself.
		 */
		hashtable->skewEnabled = false;
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->nSkewBuckets = 0;
		hashtable->spaceUsedSkew = 0;
	}

	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a left/full outer join, we have to process outer batches even if
	 * the inner batch is empty.  Similarly, in a right/full outer join, we
	 * have to process inner batches even if the outer batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 */
	curbatch++;
	while (curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		if (hashtable->outerBatchFile[curbatch] &&
			HJ_FILL_OUTER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			HJ_FILL_INNER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (hashtable->outerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (hashtable->innerBatchFile[curbatch])
			BufFileClose(hashtable->innerBatchFile[curbatch]);
		hashtable->innerBatchFile[curbatch] = NULL;
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
		curbatch++;
	}

	if (curbatch >= nbatch)
		return false;			/* no more batches */

	hashtable->curbatch = curbatch;

	/*
	 * Reload the hash table with the new inner batch (which could be empty)
	 */
	ExecHashTableReset(hashtable);

	innerFile = hashtable->innerBatchFile[curbatch];

	if (innerFile != NULL)
	{
		if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));

		while ((slot = ExecHashJoinGetSavedTuple(innerFile,
												 &hashvalue,
												 hjstate->hj_HashTupleSlot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}

		/*
		 * after we build the hash table, the inner batch file is no longer
		 * needed
		 */
		BufFileClose(innerFile);
		hashtable->innerBatchFile[curbatch] = NULL;
	}

	/*
	 * Rewind outer batch file (if present), so that we can start reading it.
	 */
	if (hashtable->outerBatchFile[curbatch] != NULL)
	{
		if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));
	}

	return true;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr)
{
	BufFile    *file = *fileptr;
	size_t		written;

	if (file == NULL)
	{
		/* First write to this batch file, so open it. */
		file = BufFileCreateTemp(false);
		*fileptr = file;
	}

	written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
	if (written != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));

	written = BufFileWrite(file, (void *) tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * We check for interrupts here because this is typically taken as an
	 * alternative code path to an ExecProcNode() call, which would include
	 * such a check.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}

void
ExecReScanHashJoin(HashJoinState *node)
{
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTable != NULL)
	{
		if (node->hj_HashTable->nbatch == 1 &&
			node->js.ps.righttree->chgParam == NULL)
		{
			/*
			 * Okay to reuse the hash table; needn't rescan inner, either.
			 *
			 * However, if it's a right/full join, we'd better reset the
			 * inner-tuple match flags contained in the table.
			 */
			if (HJ_FILL_INNER(node))
				ExecHashTableResetMatchFlags(node->hj_HashTable);

			/*
			 * Also, we need to reset our state about the emptiness of the
			 * outer relation, so that the new scan of the outer will update
			 * it correctly if it turns out to be empty this time. (There's no
			 * harm in clearing it now because ExecHashJoin won't need the
			 * info.  In the other cases, where the hash table doesn't exist
			 * or we are destroying it, we leave this state alone because
			 * ExecHashJoin will need it the first time through.)
			 */
			node->hj_OuterNotEmpty = false;

			/* ExecHashJoin can skip the BUILD_HASHTABLE step */
			node->hj_JoinState = HJ_NEED_NEW_OUTER;
		}
		else
		{
			/* must destroy and rebuild hash table */
			ExecHashTableDestroy(node->hj_HashTable);
			node->hj_HashTable = NULL;
			node->hj_JoinState = HJ_BUILD_HASHTABLE;

			/*
			 * if chgParam of subnode is not null then plan will be re-scanned
			 * by first ExecProcNode.
			 */
			if (node->js.ps.righttree->chgParam == NULL)
				ExecReScan(node->js.ps.righttree);
		}
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;

	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->js.ps.lefttree->chgParam == NULL)
		ExecReScan(node->js.ps.lefttree);
}
