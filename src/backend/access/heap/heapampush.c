#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"

#include "access/heapam.h"
#include "executor/executor.h"

/* local decls */
static inline bool seqPushHeapTuple(HeapTuple tuple, PlanState *node,
									SeqScanState *pusher);
static inline TupleTableSlot *SeqStoreTuple(SeqScanState *node, HeapTuple tuple);
static inline void seqPushNull(PlanState *node, SeqScanState *pusher);

/* local decls for copied funcs copied from postgres */
static BlockNumber heap_parallelscan_nextpage(HeapScanDesc scan);


/* push NULL to the parent, sinaling that we are done */
static inline void
seqPushNull(PlanState *node, SeqScanState *pusher)
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

/* HeapTuple --> node->ss_ScanTupleSlot, part of original SeqNext after
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
static inline bool seqPushHeapTuple(HeapTuple tuple, PlanState *node,
									SeqScanState *pusher)
{
	ExprContext *econtext;
	List	   *qual;
	ProjectionInfo *projInfo;
	TupleTableSlot *slot;
	ExprDoneCond isDone;
	TupleTableSlot *resultSlot;
	bool parent_accepts_tuples = true;

	if (tuple->t_data == NULL)
	{
		seqPushNull(node, pusher);
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
		ResetExprContext(econtext);
		return pushTuple(slot, node, (PlanState *) pusher);
	}

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
			resultSlot = ExecProject(projInfo, &isDone);
			if (isDone != ExprEndResult)
			{
				parent_accepts_tuples = pushTuple(resultSlot, node,
												  (PlanState *) pusher);
				while (parent_accepts_tuples && isDone == ExprMultipleResult)
				{
					resultSlot = ExecProject(projInfo, &isDone);
					parent_accepts_tuples = pushTuple(resultSlot, node,
													  (PlanState *) pusher);
				}
			}
		}
		else
		{
			/*
			 * Here, we aren't projecting, so just push scan tuple.
			 */
			return pushTuple(slot, node, (PlanState *) pusher);
		}
	}
	else
		InstrCountFiltered1(pusher, 1);

	return parent_accepts_tuples;
}


/* ----------------
 * Fetch tuples, check quals and push them. Modified from heapgettup_pagemode.
 * This function in fact doesn't care about pusher type, although SeqScanState
 * is hardcoded for now
 * ----------------
 */
void
heappushtups_pagemode(HeapScanDesc scan,
					  ScanDirection dir,
					  int nkeys,
					  ScanKey key,
					  PlanState *node,
					  SeqScanState *pusher)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	int			lines;
	int			lineindex;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;
	bool (*pushFunc)(HeapTuple tuple, PlanState *node, SeqScanState *pusher)
		= &seqPushHeapTuple;

	/* no movement is not supported for now */
	Assert(!ScanDirectionIsNoMovement(dir));

	/*
	 * calculate next starting lineindex, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				(*pushFunc)(&(scan->rs_ctup), node, pusher);
				return;
			}
			if (scan->rs_parallel != NULL)
			{
				page = heap_parallelscan_nextpage(scan);

				/* Other processes might have already finished the scan. */
				if (page == InvalidBlockNumber)
				{
					Assert(!BufferIsValid(scan->rs_cbuf));
					tuple->t_data = NULL;
					(*pushFunc)(&(scan->rs_ctup), node, pusher);
					return;
				}
			}
			else
				page = scan->rs_startblock;		/* first page */
			heapgetpage(scan, page);
			lineindex = 0;
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
			lineindex = scan->rs_cindex + 1;
		}

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(scan->rs_snapshot, scan->rs_rd, dp);
		lines = scan->rs_ntuples;
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineindex;
	}
	else /* backward */
	{
		/* backward parallel scan not supported */
		Assert(scan->rs_parallel == NULL);

		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				(*pushFunc)(&(scan->rs_ctup), node, pusher);
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_syncscan = false;
			/* start from last page of the scan */
			if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
			heapgetpage(scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
		}

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(scan->rs_snapshot, scan->rs_rd, dp);
		lines = scan->rs_ntuples;

		if (!scan->rs_inited)
		{
			lineindex = lines - 1;
			scan->rs_inited = true;
		}
		else
		{
			lineindex = scan->rs_cindex - 1;
		}
		/* page and lineindex now reference the previous visible tid */

		linesleft = lineindex + 1;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		while (linesleft > 0)
		{
			bool tuple_qualifies = false;

			lineoff = scan->rs_vistuples[lineindex];
			lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsNormal(lpp));

			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(tuple->t_self), page, lineoff);

			/*
			 * if current tuple qualifies, push it.
			 */
			if (key != NULL)
			{
				HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
							nkeys, key, tuple_qualifies);
			}
			else
			{
				tuple_qualifies = true;
			}

			if (tuple_qualifies)
			{
				/* Push tuple */
				scan->rs_cindex = lineindex;
				pgstat_count_heap_getnext(scan->rs_rd);
				if (!(*pushFunc)(&(scan->rs_ctup), node, pusher))
					return;
			}

			/*
			 * and carry on to the next one anyway
			 */
			--linesleft;
			if (backward)
				--lineindex;
			else
				++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else if (scan->rs_parallel != NULL)
		{
			page = heap_parallelscan_nextpage(scan);
			finished = (page == InvalidBlockNumber);
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_syncscan)
				ss_report_location(scan->rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			(*pushFunc)(&(scan->rs_ctup), node, pusher);
			return;
		}

		heapgetpage(scan, page);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(scan->rs_snapshot, scan->rs_rd, dp);
		lines = scan->rs_ntuples;
		linesleft = lines;
		if (backward)
			lineindex = lines - 1;
		else
			lineindex = 0;
	}
}

/* Copied from vanilla Postgres without changes */

/* ----------------
 *		heap_parallelscan_nextpage - get the next page to scan
 *
 *		Get the next page to scan.  Even if there are no pages left to scan,
 *		another backend could have grabbed a page to scan and not yet finished
 *		looking at it, so it doesn't follow that the scan is done when the
 *		first backend gets an InvalidBlockNumber return.
 * ----------------
 */
static BlockNumber
heap_parallelscan_nextpage(HeapScanDesc scan)
{
	BlockNumber page = InvalidBlockNumber;
	BlockNumber sync_startpage = InvalidBlockNumber;
	BlockNumber report_page = InvalidBlockNumber;
	ParallelHeapScanDesc parallel_scan;

	Assert(scan->rs_parallel);
	parallel_scan = scan->rs_parallel;

retry:
	/* Grab the spinlock. */
	SpinLockAcquire(&parallel_scan->phs_mutex);

	/*
	 * If the scan's startblock has not yet been initialized, we must do so
	 * now.  If this is not a synchronized scan, we just start at block 0, but
	 * if it is a synchronized scan, we must get the starting position from
	 * the synchronized scan machinery.  We can't hold the spinlock while
	 * doing that, though, so release the spinlock, get the information we
	 * need, and retry.  If nobody else has initialized the scan in the
	 * meantime, we'll fill in the value we fetched on the second time
	 * through.
	 */
	if (parallel_scan->phs_startblock == InvalidBlockNumber)
	{
		if (!parallel_scan->phs_syncscan)
			parallel_scan->phs_startblock = 0;
		else if (sync_startpage != InvalidBlockNumber)
			parallel_scan->phs_startblock = sync_startpage;
		else
		{
			SpinLockRelease(&parallel_scan->phs_mutex);
			sync_startpage = ss_get_location(scan->rs_rd, scan->rs_nblocks);
			goto retry;
		}
		parallel_scan->phs_cblock = parallel_scan->phs_startblock;
	}

	/*
	 * The current block number is the next one that needs to be scanned,
	 * unless it's InvalidBlockNumber already, in which case there are no more
	 * blocks to scan.  After remembering the current value, we must advance
	 * it so that the next call to this function returns the next block to be
	 * scanned.
	 */
	page = parallel_scan->phs_cblock;
	if (page != InvalidBlockNumber)
	{
		parallel_scan->phs_cblock++;
		if (parallel_scan->phs_cblock >= scan->rs_nblocks)
			parallel_scan->phs_cblock = 0;
		if (parallel_scan->phs_cblock == parallel_scan->phs_startblock)
		{
			parallel_scan->phs_cblock = InvalidBlockNumber;
			report_page = parallel_scan->phs_startblock;
		}
	}

	/* Release the lock. */
	SpinLockRelease(&parallel_scan->phs_mutex);

	/*
	 * Report scan location.  Normally, we report the current page number.
	 * When we reach the end of the scan, though, we report the starting page,
	 * not the ending page, just so the starting positions for later scans
	 * doesn't slew backwards.  We only report the position at the end of the
	 * scan once, though: subsequent callers will have report nothing, since
	 * they will have page == InvalidBlockNumber.
	 */
	if (scan->rs_syncscan)
	{
		if (report_page == InvalidBlockNumber)
			report_page = page;
		if (report_page != InvalidBlockNumber)
			ss_report_location(scan->rs_rd, report_page);
	}

	return page;
}
