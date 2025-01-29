/*-------------------------------------------------------------------------
 *
 * fasttab.h
 *	  virtual catalog and fast temporary tables
 *
 * FOR INTERNAL USAGE ONLY. Backward compatability is not guaranteed.
 * Don't use in extensions!
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/fasttab.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FASTTAB_H
#define FASTTAB_H

#include "c.h"
#include "postgres_ext.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/sdir.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

/*
 * Flag stored in ItemPointerData.ip_posid to mark tuple as virtual. We can
 * safely store a flag in higher bits of ip_posid since it's maximum value is
 * very limited. See MaxHeapTuplesPerPage.
 *
 * This constant better be not too large since MAX_TUPLES_PER_PAGE depends on
 * its value.
 */
#define FASTTAB_ITEM_POINTER_BIT 0x0800

/* Determine whether ItemPointer is virtual */
#define IsFasttabItemPointer(ptr) \
	( ((ptr)->ip_posid & FASTTAB_ITEM_POINTER_BIT) != 0 )

typedef struct FasttabIndexMethodsData FasttabIndexMethodsData;

typedef FasttabIndexMethodsData const *FasttabIndexMethods;

extern bool IsFasttabHandledRelationId(Oid relId);

extern bool IsFasttabHandledIndexId(Oid indexId);

extern void fasttab_set_relpersistence_hint(char relpersistence);

extern char fasttab_get_relpersistence_hint(void);

extern void fasttab_clear_relpersistence_hint(void);

extern void fasttab_begin_transaction(void);

extern void fasttab_end_transaction(void);

extern void fasttab_abort_transaction(void);

extern void fasttab_define_savepoint(const char *name);

extern void fasttab_rollback_to_savepoint(const char *name);

extern void fasttab_beginscan(HeapScanDesc scan);

extern void fasttab_serialize(int *query_tup_cnt, char **dest);
extern void fasttab_deserialize(int query_tup_cnt, char *dest);

extern HeapTuple fasttab_getnext(HeapScanDesc scan, ScanDirection direction);

extern bool fasttab_hot_search_buffer(ItemPointer tid, Relation relation,
						  HeapTuple heapTuple, bool *all_dead, bool *result);

extern bool fasttab_insert(Relation relation, HeapTuple tup, HeapTuple heaptup,
			   Oid *result, bool import_mode);

extern bool fasttab_delete(Relation relation, ItemPointer tid);

extern bool fasttab_update(Relation relation, ItemPointer otid,
			   HeapTuple newtup);

extern bool fasttab_inplace_update(Relation relation, HeapTuple tuple);

extern bool fasttab_index_insert(Relation indexRelation,
					 ItemPointer heap_t_ctid, bool *result);

extern void fasttab_index_beginscan(IndexScanDesc scan);

extern void fasttab_index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
					 ScanKey orderbys, int norderbys);

extern bool fasttab_simple_heap_fetch(Relation relation, Snapshot snapshot,
						  HeapTuple tuple);

extern bool fasttab_index_getnext_tid_merge(IndexScanDesc scan,
								ScanDirection direction);

extern bool fasttab_index_getbitmap(IndexScanDesc scan, Node **bitmap,
						int64 *result);

extern void fasttab_index_endscan(IndexScanDesc scan);

#endif   /* FASTTAB_H */
