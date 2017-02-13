/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAMPUSH_H
#define HEAPAMPUSH_H

#include "access/heapam.h"
#include "nodes/execnodes.h"

extern void heappushtups_pagemode(HeapScanDesc scan,
								  ScanDirection dir,
								  int nkeys,
								  ScanKey key,
								  PlanState *node,
								  SeqScanState *pusher);


#endif   /* HEAPAMPUSH_H */
