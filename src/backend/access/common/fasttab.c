/*-------------------------------------------------------------------------
 *
 * fasttab.c
 *	  virtual catalog and fast temporary tables
 *
 * This file contents imlementation of special type of temporary tables ---
 * fast temporary tables (FTT). From user perspective they work exactly as
 * regular temporary tables. However there are no records about FTTs in
 * pg_catalog. These records are stored in backend's memory instead and mixed
 * with regular records during scans of catalog tables. We refer to
 * corresponding tuples of catalog tables as "in-memory" or "virtual" tuples
 * and to all these tuples together --- as "in-memory" or "virtual" catalog.
 *
 * Note that since temporary tables are visiable only in one session there is
 * no need to use shared memory or locks for FTTs. Transactions support is
 * very simple too. There is no need to track xmin/xmax, etc.
 *
 * FTTs are designed to to solve pg_catalog bloating problem. The are
 * applications that create and delete a lot of temporary tables. It causes
 * bloating of pg_catalog and running auto vacuum on it. It's quite an
 * expensive operation that affects entire database performance.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/fasttab.c
 *
 *-------------------------------------------------------------------------
 */

// #define FASTTAB_DEBUG

#include "c.h"
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "access/fasttab.h"
#include "access/relscan.h"
#include "access/valid.h"
#include "access/memtup.h"
#include "access/sysattr.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_statistic.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "utils/memutils.h"

/*****************************************************************************
		  TYPEDEFS, MACRO DECLARATIONS AND CONST STATIC VARIABLES
 *****************************************************************************/

/* #define FASTTAB_DEBUG 1 */

#ifdef FASTTAB_DEBUG
static int32 fasttab_scan_tuples_counter = -1;
#endif

/* List of in-memory tuples. */
typedef struct
{
	dlist_node	node;
	HeapTuple	tup;
}	DListHeapTupleData;

typedef DListHeapTupleData *DListHeapTuple;

/* Like strcmp but for integer types --- int, uint32, Oid, etc. */
#define FasttabCompareInts(x, y) ( (x) == (y) ? 0 : ( (x) > (y) ? 1 : -1 ))

#define SCAN_CHECKS \
( \
	AssertMacro(IndexScanIsValid(scan)), \
	AssertMacro(RelationIsValid(scan->indexRelation)), \
	AssertMacro(PointerIsValid(scan->indexRelation->rd_am)) \
)

#define GET_SCAN_PROCEDURE(pname) \
do { \
	procedure = &scan->indexRelation->rd_aminfo->pname; \
	if (!OidIsValid(procedure->fn_oid)) \
	{ \
		RegProcedure	procOid = scan->indexRelation->rd_am->pname; \
		if (!RegProcedureIsValid(procOid)) \
			elog(ERROR, "invalid %s regproc", CppAsString(pname)); \
		fmgr_info_cxt(procOid, procedure, scan->indexRelation->rd_indexcxt); \
	} \
} while(0)

/* Forward declaration is required for relation_is_inmem_tuple_function */
struct FasttabSnapshotData;
typedef struct FasttabSnapshotData *FasttabSnapshot;

/* Predicate that determines whether given tuple should be stored in-memory */
typedef bool (*relation_is_inmem_tuple_function)
			(Relation relation, HeapTuple tup, FasttabSnapshot fasttab_snapshot,
						 int tableIdx);

/* Capacity of FasttabRelationMethods->attrNumbers, see below */
#define FasttabRelationMaxOidAttributes 2

/* FasttabRelationMethodsTable entry */
typedef const struct
{
	/* relation oid */
	Oid			relationId;
	/* predicate that determines whether tuple should be stored in-memory */
	relation_is_inmem_tuple_function is_inmem_tuple_fn;
	/* number of attributes in attrNumbers array */
	AttrNumber	noidattr;
	/* attributes that reference to pg_class records */
	AttrNumber	attrNumbers[FasttabRelationMaxOidAttributes];
}	FasttabRelationMethodsData;

typedef FasttabRelationMethodsData const *FasttabRelationMethods;

/* Forward declaration of all possible is_inmem_tuple_fn values */
static bool generic_is_inmem_tuple(Relation relation, HeapTuple tup,
					   FasttabSnapshot fasttab_snapshot, int tableIdx);
static bool pg_class_is_inmem_tuple(Relation relation, HeapTuple tup,
						FasttabSnapshot fasttab_snapshot, int tableIdx);

/*
 * Static information necessary to determine whether given tuple of given
 * relation should be stored in-memory or not.
 *
 * NB: Keep this array sorted by relationId.
 */
static FasttabRelationMethodsData FasttabRelationMethodsTable[] =
{
	/* 1247 */
	{TypeRelationId, &generic_is_inmem_tuple, 1,
		{Anum_pg_type_typrelid, 0}
	},
	/* 1249 */
	{AttributeRelationId, &generic_is_inmem_tuple, 1,
		{Anum_pg_attribute_attrelid, 0}
	},
	/* 1259 */
	{RelationRelationId, &pg_class_is_inmem_tuple, 0,
		{0, 0}
	},
	/* 2608 */
	{DependRelationId, &generic_is_inmem_tuple, 2,
		{Anum_pg_depend_objid, Anum_pg_depend_refobjid}
	},
	/* 2611 */
	{InheritsRelationId, &generic_is_inmem_tuple, 2,
		{Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhparent}
	},
	/* 2619 */
	{StatisticRelationId, &generic_is_inmem_tuple, 1,
		{Anum_pg_statistic_starelid, 0}
	},
};

/* Number of tables that can have a virtual part */
#define FasttabSnapshotTablesNumber (lengthof(FasttabRelationMethodsTable))

/* Possible values of FasttabIndexMethods->attrCompareMethod[], see below */
typedef enum FasttabCompareMethod
{
	CompareInvalid,				/* invalid value */
	CompareOid,					/* compare attributes as oids */
	CompareCString,				/* compare attributes as strings */
	CompareInt16,				/* compare attributes as int16's */
	CompareInt64,				/* compare attributes as int64's */
	CompareBoolean,				/* compare attributes as booleans */
}	FasttabCompareMethod;

/* Capacity of FasttabIndexMethods->attrNumbers, see below */
#define FasttabIndexMaxAttributes 3

/*
 * FasttabIndexMethodsTable entry.
 *
 * NB: typedef is located in fasttab.h
 */
struct FasttabIndexMethodsData
{
	/* index oid */
	Oid			indexId;
	/* number of indexed attributes */
	AttrNumber	nattr;
	/* indexed attributes (NB: attribute number can be negative) */
	AttrNumber	attrNumbers[FasttabIndexMaxAttributes];
	/* how to compare attributes */
	FasttabCompareMethod attrCompareMethod[FasttabIndexMaxAttributes];
};

/*
 * Static information required for sorting virtual tuples during index scans.
 *
 * NB: Keep this array sorted by indexId.
 *
 * NB: Uniqueness information is currently not used. Still please keep
 * comments regarding uniqueness, for possible use in the future.
 */
static FasttabIndexMethodsData FasttabIndexMethodsTable[] =
{
	/* 2187, non-unique */
	{InheritsParentIndexId, 1,
		{Anum_pg_inherits_inhparent, 0, 0},
		{CompareOid, CompareInvalid, CompareInvalid}
	},
	/* 2658, unique */
	{AttributeRelidNameIndexId, 2,
		{Anum_pg_attribute_attrelid, Anum_pg_attribute_attname, 0},
		{CompareOid, CompareCString, CompareInvalid}
	},
	/* 2659, unique */
	{AttributeRelidNumIndexId, 2,
		{Anum_pg_attribute_attrelid, Anum_pg_attribute_attnum, 0},
		{CompareOid, CompareInt16, CompareInvalid}
	},
	/* 2662, unique */
	{ClassOidIndexId, 1,
		{ObjectIdAttributeNumber, 0, 0},
		{CompareOid, CompareInvalid, CompareInvalid}
	},
	/* 2663, unique */
	{ClassNameNspIndexId, 2,
		{Anum_pg_class_relname, Anum_pg_class_relnamespace, 0},
		{CompareCString, CompareOid, CompareInvalid}
	},
	/* 2673, non-unique */
	{DependDependerIndexId, 3,
		{Anum_pg_depend_classid, Anum_pg_depend_objid, Anum_pg_depend_objsubid},
		{CompareOid, CompareOid, CompareInt64}
	},
	/* 2674, non-unique */
	{DependReferenceIndexId, 3,
		{Anum_pg_depend_refclassid, Anum_pg_depend_refobjid,
		Anum_pg_depend_refobjsubid},
		{CompareOid, CompareOid, CompareInt64}
	},
	/* 2680, unique */
	{InheritsRelidSeqnoIndexId, 2,
		{Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhseqno, 0},
		{CompareOid, CompareOid, CompareInvalid}
	},
	/* 2696, unique */
	{StatisticRelidAttnumInhIndexId, 3,
		{Anum_pg_statistic_starelid, Anum_pg_statistic_staattnum,
		Anum_pg_statistic_stainherit},
		{CompareOid, CompareInt16, CompareBoolean}
	},
	/* 2703, unique */
	{TypeOidIndexId, 1,
		{ObjectIdAttributeNumber, 0, 0},
		{CompareOid, CompareInvalid, CompareInvalid}
	},
	/* 2704, unique */
	{TypeNameNspIndexId, 2,
		{Anum_pg_type_typname, Anum_pg_type_typnamespace, 0},
		{CompareCString, CompareOid, CompareInvalid}
	},
	/* 3455, non-unique */
	{ClassTblspcRelfilenodeIndexId, 2,
		{Anum_pg_class_reltablespace, Anum_pg_class_relfilenode, 0},
		{CompareOid, CompareOid, CompareInvalid}
	},
};

/* List of virtual tuples of single relation */
typedef struct
{
	int			tuples_num;		/* number of virtual tuples */
	dlist_head	tuples;			/* list of virtual tuples */
}	FasttabSnapshotRelationData;

/*
 * Snapshot represents state of virtual heap for current transaction or
 * savepoint.
 */
struct FasttabSnapshotData
{
	/* Previous snapshot to rollback to. */
	struct FasttabSnapshotData *prev;
	/* Optional name of a savepoint. Can be NULL. */
	char	   *name;
	/* State of relations that can contain virtual tuples */
	FasttabSnapshotRelationData relationData[FasttabSnapshotTablesNumber];
}	FasttabSnapshotData;

/* Determine whether given snapshot is a root snapshot. */
#define FasttabSnapshotIsRoot(sn) ( !PointerIsValid((sn)->prev) )

/* Determine whether given snapshot is anonymous. */
#define FasttabSnapshotIsAnonymous(sn) ( !PointerIsValid((sn)->name) )

/* Determine whether there is a transaction in progress. */
#define FasttabTransactionInProgress() \
	( PointerIsValid(FasttabSnapshotGetCurrent()->prev))

/*****************************************************************************
							 GLOBAL VARIABLES
 *****************************************************************************/

/* Memory context used to store virtual catalog */
static MemoryContext LocalMemoryContextPrivate = NULL;

/* Counters used to generate unique virtual ItemPointers */
static uint32 CurrentFasttabBlockId = 0;
static uint16 CurrentFasttabOffset = 1; /* NB: 0 is considered invalid */

/* Current snapshot */
static FasttabSnapshot CurrentFasttabSnapshotPrivate = NULL;

/* Current relpersistence hint value */
static char CurrentRelpersistenceHint = RELPERSISTENCE_UNDEFINED;

/*****************************************************************************
							UTILITY PROCEDURES
 *****************************************************************************/

/*
 * Set relpersistence hint.
 *
 * Usualy to figure out wheter tuple should be stored in-memory or not we use
 * in-memory part of pg_class table. Unfortunately during table creation some
 * tuples are stored in catalog tables _before_ modification of pg_class table.
 * So there is no way to tell that these tuples should be in-memory.
 *
 * In these rare cases we set a hint with relperistence value of a table we
 * about to create. This not only solves a problem described above but also
 * allows to run described check much faster.
 */
void
fasttab_set_relpersistence_hint(char relpersistence)
{
	CurrentRelpersistenceHint = relpersistence;
}


char
fasttab_get_relpersistence_hint()
{
	return CurrentRelpersistenceHint;
}

/*
 * Clear relpersisntence hint.
 */
void
fasttab_clear_relpersistence_hint(void)
{
	CurrentRelpersistenceHint = RELPERSISTENCE_UNDEFINED;
}

/*
 * Get memory context for storing virtual catalog. Create one if necessary.
 */
static MemoryContext
GetLocalMemoryContext(void)
{
	if (!PointerIsValid(LocalMemoryContextPrivate))
	{
		LocalMemoryContextPrivate = AllocSetContextCreate(
														  NULL,
											"Virtual catalog memory context",
													ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);
	}

	return LocalMemoryContextPrivate;
}

/*
 * Generate unique virtual ItemPointer
 */
static ItemPointerData
GenFasttabItemPointerData(void)
{
	ItemPointerData res;

	BlockIdSet(&(res.ip_blkid), CurrentFasttabBlockId);
	res.ip_posid = CurrentFasttabOffset | FASTTAB_ITEM_POINTER_BIT;

	CurrentFasttabOffset++;

	if (CurrentFasttabOffset > MaxHeapTuplesPerPage)
	{
		CurrentFasttabOffset = 1;
		CurrentFasttabBlockId++;

#ifdef FASTTAB_DEBUG
		elog(NOTICE, "FASTTAB: GenFasttabItemPointerData, CurrentFasttabOffset > MaxHeapTuplesPerPage (%d), new values - CurrentFasttabOffset = %d, CurrentFasttabBlockId = %d",
		  MaxHeapTuplesPerPage, CurrentFasttabOffset, CurrentFasttabBlockId);
#endif
	}

	return res;
}

/*
 * Find FasttabRelationMethodsTable index by relation oid using binary search.
 *
 * Not for direct usage. GetSnapshotRelationIdxByOid should be used instead.
 *
 * Return values:
 * == -1 - not found
 * >=  0 - found on N-th position
 */
static int
GetSnapshotRelationIdxByOidInternal(Oid relId)
{
	int			begin = 0;
	int			end = FasttabSnapshotTablesNumber - 1;

#ifdef USE_ASSERT_CHECKING
	/* Test that FasttabRelationMethodsTable is properly sorted */
	int			i;

	for (i = 0; i <= end; i++)
	{
		Assert(PointerIsValid(FasttabRelationMethodsTable[i].is_inmem_tuple_fn));
		if (i > 0)
			Assert(FasttabRelationMethodsTable[i - 1].relationId < FasttabRelationMethodsTable[i].relationId);
	}
#endif

	while (begin < end)
	{
		int			test = (begin + end) / 2;

		if (FasttabRelationMethodsTable[test].relationId == relId)
		{
			begin = test;
			break;
		}

		if (FasttabRelationMethodsTable[test].relationId < relId)
			begin = test + 1;	/* go right */
		else
			end = test - 1;		/* go left */
	}

	if (FasttabRelationMethodsTable[begin].relationId == relId)
		return begin;			/* found */
	else
		return -1;				/* not found */
}

/*
 * Determine FasttabRelationMethodsTable index by relation oid.
 */
static inline int
GetSnapshotRelationIdxByOid(Oid relId)
{
	int			result;

	Assert(IsFasttabHandledRelationId(relId));
	result = GetSnapshotRelationIdxByOidInternal(relId);
	Assert(result >= 0 && result < FasttabSnapshotTablesNumber);
	return result;
}

/*
 * Determine whether relation with given oid can have virtual tuples.
 */
bool
IsFasttabHandledRelationId(Oid relId)
{
	return (GetSnapshotRelationIdxByOidInternal(relId) >= 0);
}

/*
 * Find FasttabIndexMethodsTable entry by index oid using binary search.
 *
 * Not for direct usage. GetFasttabIndexMethods should be used instead.
 *
 * Return values:
 * == NULL - not found
 * != NULL - found
 */
static FasttabIndexMethods
GetFasttabIndexMethodsInternal(Oid indexId)
{
	int			begin = 0;
	int			end = (sizeof(FasttabIndexMethodsTable) /
					   sizeof(FasttabIndexMethodsTable[0]) - 1);

#ifdef USE_ASSERT_CHECKING
	/* Test that FasttabIndexMethodsTable is properly sorted. */
	int			i;

	for (i = 0; i <= end; i++)
	{
		if (i > 0)
			Assert(FasttabIndexMethodsTable[i - 1].indexId < FasttabIndexMethodsTable[i].indexId);
	}
#endif

	while (begin < end)
	{
		int			test = (begin + end) / 2;

		if (FasttabIndexMethodsTable[test].indexId == indexId)
		{
			begin = test;
			break;
		}

		if (FasttabIndexMethodsTable[test].indexId < indexId)
			begin = test + 1;	/* go right */
		else
			end = test - 1;		/* go left */
	}

	if (FasttabIndexMethodsTable[begin].indexId == indexId)
		return &FasttabIndexMethodsTable[begin];		/* found */
	else
		return NULL;			/* not found */
}

/*
 * Determine whether index with given oid has a virtual part.
 */
bool
IsFasttabHandledIndexId(Oid indexId)
{
	return (GetFasttabIndexMethodsInternal(indexId) != NULL);
}

/*
 * Find FasttabIndexMethodsTable entry by index oid using binary search.
 */
static inline FasttabIndexMethods
GetFasttabIndexMethods(Oid indexId)
{
	Assert(IsFasttabHandledIndexId(indexId));
	return GetFasttabIndexMethodsInternal(indexId);
}

/*
 * Free single DListHeapTuple
 */
static void
DListHeapTupleFree(DListHeapTuple dlist_tup)
{
	heap_freetuple(dlist_tup->tup);
	pfree(dlist_tup);
}

/*
 * Free list of DListHeapTuple's
 */
static void
FasttabDListFree(dlist_head *head)
{
	while (!dlist_is_empty(head))
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) dlist_pop_head_node(head);

		DListHeapTupleFree(dlist_tup);
	}
}

/*
 * Create a new empty snapshot.
 */
static FasttabSnapshot
FasttabSnapshotCreateEmpty(void)
{
	FasttabSnapshot result;
	MemoryContext oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());

	result = palloc0(sizeof(FasttabSnapshotData));
	MemoryContextSwitchTo(oldctx);
	return result;
}

/*
 * Create a snapshot copy.
 */
static FasttabSnapshot
FasttabSnapshotCopy(FasttabSnapshot src, const char *dst_name)
{
	int			idx;
	dlist_iter	iter;
	MemoryContext oldctx;
	FasttabSnapshot dst = FasttabSnapshotCreateEmpty();

	oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());
	dst->name = dst_name ? pstrdup(dst_name) : NULL;

	for (idx = 0; idx < FasttabSnapshotTablesNumber; idx++)
	{
		dst->relationData[idx].tuples_num = src->relationData[idx].tuples_num;
		dlist_foreach(iter, &src->relationData[idx].tuples)
		{
			DListHeapTuple src_dlist_tup = (DListHeapTuple) iter.cur;
			DListHeapTuple dst_dlist_tup = palloc(sizeof(DListHeapTupleData));

			dst_dlist_tup->tup = heap_copytuple(src_dlist_tup->tup);
			dlist_push_tail(&dst->relationData[idx].tuples,
							&dst_dlist_tup->node);
		}
	}

	MemoryContextSwitchTo(oldctx);
	return dst;
}

/*
 * Free snapshot.
 */
static void
FasttabSnapshotFree(FasttabSnapshot fasttab_snapshot)
{
	int			idx;

	for (idx = 0; idx < FasttabSnapshotTablesNumber; idx++)
		FasttabDListFree(&fasttab_snapshot->relationData[idx].tuples);

	if (PointerIsValid(fasttab_snapshot->name))
		pfree(fasttab_snapshot->name);

	pfree(fasttab_snapshot);
}

/*
 * Get current snapshot. Create one if necessary.
 */
static FasttabSnapshot
FasttabSnapshotGetCurrent(void)
{
	if (!PointerIsValid(CurrentFasttabSnapshotPrivate))
		CurrentFasttabSnapshotPrivate = FasttabSnapshotCreateEmpty();

	return CurrentFasttabSnapshotPrivate;
}

/*
 * Places a snapshot on top of snapshots stack. Placed snapshot becomes
 * current.
 */
static inline void
FasttabSnapshotPushBack(FasttabSnapshot fasttab_snapshot)
{
	fasttab_snapshot->prev = FasttabSnapshotGetCurrent();
	CurrentFasttabSnapshotPrivate = fasttab_snapshot;
}

/*
 * Removes snapshot from top of snapshots stack.
 *
 * Returns valid FasttabSnapshot or NULL if only root snapshot left.
 */
static FasttabSnapshot
FasttabSnapshotPopBack(void)
{
	FasttabSnapshot curr = FasttabSnapshotGetCurrent();

	if (FasttabSnapshotIsRoot(curr))
		return NULL;

	CurrentFasttabSnapshotPrivate = curr->prev;
	curr->prev = NULL;
	return curr;
}

/*
 * Creates a copy of current snapshot with given name (can be NULL) and places
 * it on top of snapshots stack. This copy becomes current snapshot.
 */
static void
FasttabSnapshotCreate(const char *name)
{
	FasttabSnapshot src = FasttabSnapshotGetCurrent();
	FasttabSnapshot dst = FasttabSnapshotCopy(src, name);

	FasttabSnapshotPushBack(dst);
}

/*
 * Makes given snapshot a root one.
 */
static void
FasttabSnapshotPushFront(FasttabSnapshot fasttab_snapshot)
{
	FasttabSnapshot temp = FasttabSnapshotGetCurrent();

	while (!FasttabSnapshotIsRoot(temp))
		temp = temp->prev;

	temp->prev = fasttab_snapshot;
	fasttab_snapshot->prev = NULL;
}

/*****************************************************************************
							 MAIN PROCEDURES
 *****************************************************************************/

/*
 * Make preparations related to virtual catalog on transaction begin.
 *
 * NB: There could be already a transaction in progress.
 */
void
fasttab_begin_transaction(void)
{
#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_begin_transaction, transaction is already in progress: %u",
		 FasttabTransactionInProgress());
#endif

	if (FasttabTransactionInProgress())
		return;

	/* begin transaction */
	FasttabSnapshotCreate(NULL);
	Assert(FasttabTransactionInProgress());
	Assert(FasttabSnapshotIsAnonymous(FasttabSnapshotGetCurrent()));
}

/*
 * Perform actions related to virtual catalog on transaction commit.
 *
 * NB: There could be actually no transaction in progress.
 */
void
fasttab_end_transaction(void)
{
#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_end_transaction result (1 - commit, 0 - rollback)"
		 ", transaction is in progress: %u", FasttabTransactionInProgress());
#endif

	if (!FasttabTransactionInProgress())
		return;

	Assert(FasttabSnapshotIsAnonymous(FasttabSnapshotGetCurrent()));

	/* Commit transaction. 1) Save top snapshot to the bottom of the stack. */
	FasttabSnapshotPushFront(FasttabSnapshotPopBack());
	/* 2) get rid of all snapshots except the root one */
	fasttab_abort_transaction();
}

/*
 * Perform actions related to virtual catalog on transaction abort.
 *
 * NB: There could be in fact no transaction running.
 */
void
fasttab_abort_transaction(void)
{
	FasttabSnapshot fasttab_snapshot;

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_abort_transaction, transaction is in progress: %u (it's OK if this procedure is called from fasttab_end_transaction - see the code)",
		 FasttabTransactionInProgress());
#endif

	for (;;)
	{
		fasttab_snapshot = FasttabSnapshotPopBack();
		if (!fasttab_snapshot)	/* root snapshot reached */
			break;

		FasttabSnapshotFree(fasttab_snapshot);
	}

	Assert(!FasttabTransactionInProgress());
}

/*
 * Perform actions related to virtual catalog on savepoint creation.
 */
void
fasttab_define_savepoint(const char *name)
{
	Assert(FasttabTransactionInProgress());
	Assert(FasttabSnapshotIsAnonymous(FasttabSnapshotGetCurrent()));

	/*
	 * Value of `name` argument can be NULL in 'rollback to savepoint' case.
	 * This case is already handled by fasttab_rollback_to_savepoint.
	 */
	if (!PointerIsValid(name))
		return;

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_define_safepoint, name = '%s'", name);
#endif

	FasttabSnapshotCreate(name);	/* savepoint to rollback to */
	FasttabSnapshotCreate(NULL);	/* current snapshot to store changes */

	Assert(FasttabTransactionInProgress());
}

/*
 * Perform actions related to virtual catalog on `rollback to savepoint`.
 *
 * NB: There is no need to re-check case of savepoint name (upper / lower) or
 * that savepoint exists.
 */
void
fasttab_rollback_to_savepoint(const char *name)
{
	Assert(PointerIsValid(name));
	Assert(FasttabTransactionInProgress());
	Assert(FasttabSnapshotIsAnonymous(FasttabSnapshotGetCurrent()));

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_rollback_to_savepoint, name = '%s'", name);
#endif

	/*
	 * Pop snapshots from the stack and free them until a snapshot with given
	 * name will be reached.
	 */
	for (;;)
	{
		FasttabSnapshot fasttab_snapshot = FasttabSnapshotGetCurrent();

		Assert(!FasttabSnapshotIsRoot(fasttab_snapshot));

		if ((!FasttabSnapshotIsAnonymous(fasttab_snapshot)) &&
			(strcmp(fasttab_snapshot->name, name) == 0))
			break;

		FasttabSnapshotFree(FasttabSnapshotPopBack());
	}

	/* Create a new current snapshot to store changes. */
	FasttabSnapshotCreate(NULL);
}

/*
 * (Re)initialize part of `scan` related to virtual catalog during heap
 * (re)scan.
 */
void
fasttab_beginscan(HeapScanDesc scan)
{
	int			idx;
	Oid			relid = RelationGetRelid(scan->rs_rd);
	FasttabSnapshot fasttab_snapshot;

	if (!IsFasttabHandledRelationId(relid))
		return;

	fasttab_snapshot = FasttabSnapshotGetCurrent();

	idx = GetSnapshotRelationIdxByOid(relid);
	if (dlist_is_empty(&fasttab_snapshot->relationData[idx].tuples))
		scan->rs_curr_inmem_tupnode = NULL;
	else
		scan->rs_curr_inmem_tupnode = dlist_head_node(&fasttab_snapshot->relationData[idx].tuples);

#ifdef FASTTAB_DEBUG
	fasttab_scan_tuples_counter = 0;
	elog(NOTICE, "FASTTAB: fasttab_beginscan, returning scan = %p, rs_curr_inmem_tupnode = %p", scan, scan->rs_curr_inmem_tupnode);
#endif
}

void
fasttab_deserialize(int queryTupleLen, char *ser)
{
	bool			tup_isnull[MaxHeapAttributeNumber];
	Datum			tup_values[MaxHeapAttributeNumber];
	Relation		tupRel;
	MemTupleBinding	*mt_bind;
	MemTuple		memtup;
	HeapTuple		ht;
	int idx;
	int srSz;
	int srOff;
	int itemLen;
	int lastIndex;
	char * ptr = ser;
	char * currMemTuple;
	Oid tmp;
	char prevRelpers;

	if (queryTupleLen == 0)
		return;

	srOff = 0;
	lastIndex = -1;
	tupRel = NULL;
	mt_bind = NULL;

	prevRelpers = CurrentRelpersistenceHint;

	CurrentRelpersistenceHint = RELPERSISTENCE_FAST_TEMP;


	while (srOff < queryTupleLen)
	{
		memcpy(&idx, ptr, sizeof(idx));
		ptr += sizeof(idx);
		
		memcpy(&itemLen, ptr, sizeof(itemLen));
		ptr += sizeof(itemLen);

		currMemTuple = palloc(itemLen * sizeof(char));
		memcpy(currMemTuple, ptr, itemLen);

		ptr += itemLen;
		srOff += sizeof(idx) + sizeof(itemLen) + itemLen;

		if (idx != lastIndex) {
			if (tupRel != NULL) {
				relation_close(tupRel, NoLock);
				pfree(mt_bind);
			}
			
			tupRel = relation_open(FasttabRelationMethodsTable[idx].relationId, NoLock);
			mt_bind = create_memtuple_binding(RelationGetDescr(tupRel));
		}

		memtuple_deform(currMemTuple, mt_bind, tup_values, tup_isnull);

		ht = heap_form_tuple(RelationGetDescr(tupRel), tup_values, tup_isnull);

		if (mtbind_has_oid(mt_bind))
			HeapTupleSetOid(ht, MemTupleGetOid(currMemTuple, mt_bind));

#ifdef FASTTAB_DEBUG
		elog(NOTICE, "FASTTAB: fasttab_deserialize tuple, memtup oid = %d, heaptup oid = %d, relation relid = %d",
			MemTupleGetOid(currMemTuple, mt_bind), HeapTupleGetOid(ht), RelationGetRelid(tupRel));
#endif

		fasttab_insert(tupRel, ht, ht, &tmp, true);

		lastIndex = idx;
	}

	if (tupRel != NULL) {
		relation_close(tupRel, NoLock);
		pfree(mt_bind);
	}

	Assert(srOff == queryTupleLen);

	CurrentRelpersistenceHint = prevRelpers;
}

void
fasttab_serialize(int *query_tup_len, char **dest)
{
	FasttabSnapshot fasttab_snapshot;
	int idx;
	int srSz;
	int srOff;
	int itemLen;
	dlist_node *cur_node;
	DListHeapTuple dlist_tup;
	bool			tup_isnull[MaxHeapAttributeNumber];
	Datum			tup_values[MaxHeapAttributeNumber];
	Relation		tupRel;
	MemTupleBinding	*mt_bind;
	MemTuple		memtup;

	fasttab_snapshot = FasttabSnapshotGetCurrent();

	srSz = 1024;
	srOff = 0;
	*dest = palloc0(srSz);

	for (idx = 0; idx < FasttabSnapshotTablesNumber; ++ idx) {
		if (dlist_is_empty(&fasttab_snapshot->relationData[idx].tuples))
			continue;
		/*
		* Simple strategy - first return all in-memory tuples, then proceed with
		* others.
		*/
		tupRel = relation_open(FasttabRelationMethodsTable[idx].relationId, NoLock);

		cur_node = &fasttab_snapshot->relationData[idx].tuples.head;

		mt_bind = create_memtuple_binding(RelationGetDescr(tupRel));


		do /* inmemory tuples enumiration is still in progress? */
		{
			cur_node = dlist_next_node(&fasttab_snapshot->relationData[idx].tuples, cur_node);

			dlist_tup = (DListHeapTuple) cur_node;

#ifdef FASTTAB_DEBUG
			fasttab_scan_tuples_counter++;
			elog(NOTICE, "FASTTAB: fasttab_serialize,  counter = %u, return tuple t_self = %08X/%04X, relation = %s, oid = %d",
				fasttab_scan_tuples_counter,
				BlockIdGetBlockNumber(&dlist_tup->tup->t_self.ip_blkid), dlist_tup->tup->t_self.ip_posid, RelationGetRelationName(tupRel), HeapTupleGetOid(dlist_tup->tup)
				);
#endif

			heap_deform_tuple(dlist_tup->tup,  RelationGetDescr(tupRel), tup_values, tup_isnull);

			memtup = memtuple_form_to(mt_bind, tup_values, tup_isnull, NULL, 0, false);

			if(mtbind_has_oid(mt_bind))
				MemTupleSetOid(memtup, mt_bind, HeapTupleGetOid(dlist_tup->tup));

			/*
			* get space to insert our next item (tuple)
			*/
			itemLen = memtuple_get_size(memtup);

			while (srOff + sizeof(idx) + sizeof(itemLen) + itemLen > srSz) {
				srSz *= 2;
				*dest = repalloc(*dest, srSz);
			}

			memcpy((*dest + srOff), &idx, sizeof(idx));
			srOff += sizeof(idx);

			memcpy((*dest + srOff), &itemLen, sizeof(itemLen));
			srOff += sizeof(itemLen);

			memcpy((*dest + srOff), (const char*) memtup, itemLen);
			srOff += itemLen;

			/* HeapKeyTest is a macro, it changes `match` variable */
		} while (dlist_has_next(&fasttab_snapshot->relationData[idx].tuples, cur_node));

		relation_close(tupRel, NoLock);
	}

	*query_tup_len = srOff;
}

/*
 * Returns next virtual tuple during heap scan or NULL if there are no more
 * virtual tuples. Basically heap_getnext implementation for virtual catalog.
 */
HeapTuple
fasttab_getnext(HeapScanDesc scan, ScanDirection direction)
{
	bool		match;
	int			idx;
	FasttabSnapshot fasttab_snapshot;
	DListHeapTuple dlist_tup;
	dlist_node *ret_node;

	if (!IsFasttabHandledRelationId(RelationGetRelid(scan->rs_rd)))
		return NULL;

	/* Other directions are never used for pg_catalog. */
	Assert(ScanDirectionIsForward(direction));

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(scan->rs_rd));

	/*
	 * Simple strategy - first return all in-memory tuples, then proceed with
	 * others.
	 */
	while (scan->rs_curr_inmem_tupnode) /* inmemory tuples enumiration is
										 * still in progress? */
	{
		ret_node = scan->rs_curr_inmem_tupnode;

		if (dlist_has_next(&fasttab_snapshot->relationData[idx].tuples, ret_node))
			scan->rs_curr_inmem_tupnode = dlist_next_node(&fasttab_snapshot->relationData[idx].tuples, ret_node);
		else
			scan->rs_curr_inmem_tupnode = NULL;

		dlist_tup = (DListHeapTuple) ret_node;

#ifdef FASTTAB_DEBUG
		fasttab_scan_tuples_counter++;
		elog(NOTICE, "FASTTAB: fasttab_getnext, scan = %p, counter = %u, direction = %d, return tuple t_self = %08X/%04X, oid = %d",
			 scan, fasttab_scan_tuples_counter, direction,
			 BlockIdGetBlockNumber(&dlist_tup->tup->t_self.ip_blkid), dlist_tup->tup->t_self.ip_posid, HeapTupleGetOid(dlist_tup->tup)
			);
#endif

		/* HeapKeyTest is a macro, it changes `match` variable */
		HeapKeyTest(dlist_tup->tup, RelationGetDescr(scan->rs_rd), scan->rs_nkeys, scan->rs_key, match);
		if (!match)
			continue;

		return dlist_tup->tup;
	}

	/* There are not more virtual tuples. */
	return NULL;
}

/*
 * Pretend searching HOT chain for virtual tuple.
 *
 * Basically heap_hot_search_buffer implementation for virtual catalog.
 */
bool
fasttab_hot_search_buffer(ItemPointer tid, Relation relation,
						  HeapTuple heapTuple, bool *all_dead, bool *result)
{
	FasttabSnapshot fasttab_snapshot;
	dlist_iter	iter;
	int			idx;
	bool		found = false;

	if (!IsFasttabItemPointer(tid))
		return false;

	Assert(IsFasttabHandledRelationId(RelationGetRelid(relation)));

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(relation));
	dlist_foreach(iter, &fasttab_snapshot->relationData[idx].tuples)
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, tid))
		{
			memcpy(heapTuple, dlist_tup->tup, sizeof(HeapTupleData));
			found = true;
			break;
		}
	}

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_hot_search_buffer, tid = %08X/%04X, found = %u",
		 BlockIdGetBlockNumber(&tid->ip_blkid), tid->ip_posid, found);
#endif

	/* `all_dead` can be NULL during bitmap scan */
	if (all_dead)
		*all_dead = false;

	/* `result` can be false in ALTER TABLE case */
	*result = found;
	return true;
}

/*
 * Insert a tuple. Basically heap_insert implementation for virtual tuples.
 * Returns true if tuple was inserted, false otherwise.
 */
bool
fasttab_insert(Relation relation, HeapTuple tup, HeapTuple heaptup, Oid *result, bool import_mode)
{
	FasttabSnapshot fasttab_snapshot;
	MemoryContext oldctx;
	DListHeapTuple dlist_tup;
	int			idx = GetSnapshotRelationIdxByOidInternal(RelationGetRelid(relation));

	if (idx < 0)				/* i.e. `!IsFasttabHandledRelationId` */
		return false;

	fasttab_snapshot = FasttabSnapshotGetCurrent();

	/*
	 * Check whether tuple should be stored in-memory.
	 *
	 * NB: passing `idx` is kind of optimization, it could be actually
	 * re-calculated from `relation` argument.
	 */
	if (!import_mode && !FasttabRelationMethodsTable[idx].is_inmem_tuple_fn(relation,
												 tup, fasttab_snapshot, idx))
	{
#ifdef FASTTAB_DEBUG
		elog(NOTICE, "FASTTAB: fasttab_insert skip, inmemory tuples num = %d, heaptup oid = %d, idx = %d, relation relid = %d",
			fasttab_snapshot->relationData[idx].tuples_num,
			HeapTupleGetOid(heaptup), idx, RelationGetRelid(relation));
#endif
		return false;
	}

	oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());
	heaptup->t_self = GenFasttabItemPointerData();
	dlist_tup = palloc(sizeof(DListHeapTupleData));
	dlist_tup->tup = heap_copytuple(heaptup);
	MemoryContextSwitchTo(oldctx);

	dlist_push_tail(&fasttab_snapshot->relationData[idx].tuples,
					&dlist_tup->node);
	fasttab_snapshot->relationData[idx].tuples_num++;

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_insert, dlist_tup->tup->t_self = %08X/%04X, oid = %d, inmemory tuples num = %d, heaptup oid = %d, idx = %d, relation relid = %d",
		 BlockIdGetBlockNumber(&dlist_tup->tup->t_self.ip_blkid),
		 dlist_tup->tup->t_self.ip_posid, HeapTupleGetOid(dlist_tup->tup),
		 fasttab_snapshot->relationData[idx].tuples_num,
		 HeapTupleGetOid(heaptup), idx, RelationGetRelid(relation)
		);
#endif

	if (!import_mode)
	{
		CacheInvalidateHeapTuple(relation, dlist_tup->tup, NULL);
		pgstat_count_heap_insert(relation, 1);
	}
	
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	*result = HeapTupleGetOid(tup);
	return true;
}

/*
 * Remove pg_depend and pg_type records that would be kept in memory otherwise
 * when relation with given Oid is deleted. Basically here we are solving the
 * same issue that is solved by relpersistence hint, but during table deletion,
 * not creation.
 *
 * Used in fasttab_delete.
 */
static void
fasttab_clean_catalog_on_relation_delete(Oid reloid)
{
	Oid			curroid = reloid;
	FasttabSnapshot fasttab_snapshot = FasttabSnapshotGetCurrent();
	int			dependIdx = GetSnapshotRelationIdxByOid(DependRelationId);
	int			typeIdx = GetSnapshotRelationIdxByOid(TypeRelationId);
	Relation	dependRel = relation_open(DependRelationId, AccessShareLock);
	Relation	typeRel = relation_open(TypeRelationId, AccessShareLock);
	ItemPointerData itemPointerData;

	for (;;)
	{
		dlist_iter	iter;
		bool		isnull,
					found = false;

		/* Find pg_depend tuple with refobjid == curroid. */
		dlist_foreach(iter, &fasttab_snapshot->relationData[dependIdx].tuples)
		{
			DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;
			Oid			refobjid = DatumGetObjectId(heap_getattr(dlist_tup->tup, Anum_pg_depend_refobjid,
									  RelationGetDescr(dependRel), &isnull));

			if (refobjid == curroid)
			{
				found = true;
				/* curroid := tuple.objid */
				curroid = DatumGetObjectId(heap_getattr(dlist_tup->tup, Anum_pg_depend_objid,
									  RelationGetDescr(dependRel), &isnull));

				/*
				 * Delete found tuple. Can't pass dlist_tup->tup->t_self as an
				 * argument - this memory is about to be freed.
				 */
				itemPointerData = dlist_tup->tup->t_self;
				fasttab_delete(dependRel, &itemPointerData);
				break;
			}
		}

		/* If not found - cleanup is done, end of loop */
		if (!found)
			break;

		/* Find pg_type tuple with oid == curroid */
		found = false;
		dlist_foreach(iter, &fasttab_snapshot->relationData[typeIdx].tuples)
		{
			DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;
			Oid			oid = DatumGetObjectId(heap_getattr(dlist_tup->tup, ObjectIdAttributeNumber,
										RelationGetDescr(typeRel), &isnull));

			if (oid == curroid)
			{
				found = true;

				/*
				 * Delete found tuple. Can't pass dlist_tup->tup->t_self as an
				 * argument - this memory is about to be freed.
				 */
				itemPointerData = dlist_tup->tup->t_self;
				fasttab_delete(typeRel, &itemPointerData);
				break;
			}
		}

		Assert(found);
	}

	relation_close(typeRel, AccessShareLock);
	relation_close(dependRel, AccessShareLock);
}

/*
 * Delete tuple. Basically heap_delete implementation for virtual tuples.
 * Returns true if tuple was deleted, false otherwise.
 */
bool
fasttab_delete(Relation relation, ItemPointer tid)
{
	FasttabSnapshot fasttab_snapshot;
	dlist_iter	iter;
	int			idx;

	if (!IsFasttabItemPointer(tid))
		return false;

	Assert(IsFasttabHandledRelationId(RelationGetRelid(relation)));

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(relation));
	dlist_foreach(iter, &fasttab_snapshot->relationData[idx].tuples)
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, tid))
		{
			/*
			 * If it's a tuple from pg_class, delete tuples that might still
			 * reference to it.
			 */
			if (RelationGetRelid(relation) == RelationRelationId)
			{
				bool		isnull;
				Oid			reloid = DatumGetObjectId(heap_getattr(dlist_tup->tup, ObjectIdAttributeNumber,
									   RelationGetDescr(relation), &isnull));

				fasttab_clean_catalog_on_relation_delete(reloid);
			}

			pgstat_count_heap_delete(relation);
			CacheInvalidateHeapTuple(relation, dlist_tup->tup, NULL);

			dlist_delete(&dlist_tup->node);
			DListHeapTupleFree(dlist_tup);
			fasttab_snapshot->relationData[idx].tuples_num--;

#ifdef FASTTAB_DEBUG
			elog(NOTICE, "FASTTAB: fasttab_delete, tid = %08X/%04X - entry found and deleted, tuples_num = %d, idx = %d, rd_id = %d",
				 BlockIdGetBlockNumber(&tid->ip_blkid), tid->ip_posid,
				 fasttab_snapshot->relationData[idx].tuples_num, idx, relation->rd_id
				);
#endif

			return true;
		}
	}

	elog(ERROR, "in-memory tuple not found during delete");
	return false;				/* will be never reached */
}

/*
 * Update tuple. Basically heap_update implementation for virtual tuples.
 * Returns true if tuple was updated, false otherwise.
 */
bool
fasttab_update(Relation relation, ItemPointer otid, HeapTuple newtup)
{
	FasttabSnapshot fasttab_snapshot;
	dlist_iter	iter;
	int			idx;

	if (!IsFasttabItemPointer(otid))
		return false;

	Assert(IsFasttabHandledRelationId(RelationGetRelid(relation)));

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_update, looking for otid = %08X/%04X",
		 BlockIdGetBlockNumber(&otid->ip_blkid), otid->ip_posid);
#endif

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(relation));
	dlist_foreach(iter, &fasttab_snapshot->relationData[idx].tuples)
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, otid))
		{
			MemoryContext oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());

			CacheInvalidateHeapTuple(relation, dlist_tup->tup, newtup);
			heap_freetuple(dlist_tup->tup);
			newtup->t_self = GenFasttabItemPointerData();
			dlist_tup->tup = heap_copytuple(newtup);
			MemoryContextSwitchTo(oldctx);

			pgstat_count_heap_update(relation, false);

#ifdef FASTTAB_DEBUG
			elog(NOTICE, "FASTTAB: fasttab_update - entry found and updated, newtup->t_self = %08X/%04X, oid = %d, tuples_num = %d, idx = %d",
				 BlockIdGetBlockNumber(&newtup->t_self.ip_blkid), newtup->t_self.ip_posid,
				 HeapTupleGetOid(dlist_tup->tup),
				 fasttab_snapshot->relationData[idx].tuples_num, idx);
#endif
			return true;
		}
	}

	elog(ERROR, "in-memory tuple not found during update");
	return false;				/* will be never reached */
}

/*
 * Update tuple "in place". Basically heap_inplace_update implementation for
 * virtual tuples. Returns true if tuple was updated, false otherwise.
 */
bool
fasttab_inplace_update(Relation relation, HeapTuple tuple)
{
	FasttabSnapshot fasttab_snapshot;
	dlist_iter	iter;
	int			idx;

	if (!IsFasttabItemPointer(&tuple->t_self))
		return false;

	Assert(IsFasttabHandledRelationId(RelationGetRelid(relation)));

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_heap_inplace_update, looking for tuple with tid = %08X/%04X, oid = %d...",
	  BlockIdGetBlockNumber(&tuple->t_self.ip_blkid), tuple->t_self.ip_posid,
		 HeapTupleGetOid(tuple));
#endif

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(relation));
	dlist_foreach(iter, &fasttab_snapshot->relationData[idx].tuples)
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, &tuple->t_self))
		{
			MemoryContext oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());

			if (!IsBootstrapProcessingMode())
				CacheInvalidateHeapTuple(relation, tuple, NULL);

			heap_freetuple(dlist_tup->tup);
			dlist_tup->tup = heap_copytuple(tuple);
			MemoryContextSwitchTo(oldctx);

#ifdef FASTTAB_DEBUG
			elog(NOTICE, "FASTTAB: fasttab_inplace_update - entry found and updated, tuples_num = %d, idx = %d",
				 fasttab_snapshot->relationData[idx].tuples_num, idx);
#endif
			return true;
		}
	}

	elog(ERROR, "in-memory tuple not found during \"in place\" update");
	return false;				/* will be never reached */
}

/*
 * Insert an index tuple into a relation. Basically index_insert implementation
 * for virtual tuples. Returns true if tuple was inserted, false otherwise.
 *
 * Current FFTs implementation builds indexes "on the fly" when index scan
 * begins. Thus for now we do almost nothing here.
 */
bool
fasttab_index_insert(Relation indexRelation, ItemPointer heap_t_ctid,
					 bool *result)
{
	Oid			indexId = RelationGetRelid(indexRelation);

	if (!IsFasttabItemPointer(heap_t_ctid))
		return false;

	Assert(IsFasttabHandledIndexId(indexId));

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_index_insert, indexRelation relid = %u, heap_t_ctid = %08X/%04X",
		 RelationGetRelid(indexRelation),
		 BlockIdGetBlockNumber(&heap_t_ctid->ip_blkid),
		 heap_t_ctid->ip_posid);
#endif

	if (IsFasttabHandledIndexId(indexId))
	{
		*result = true;
		return true;			/* don't actually modify an index */
	}

	return false;
}

/*
 * Compare two tuples during index scan.
 *
 * Returns:
 * > 0 - first tuple is greater
 * = 0 - tuples are equal
 * < 0 - first tuple is lesser
 */
static int
fasttab_index_compare_tuples(HeapTuple first, HeapTuple second,
							 IndexScanDesc scan)
{
	TupleDesc	tupledesc = RelationGetDescr(scan->heapRelation);
	Datum		datum1,
				datum2;
	bool		isnull1,
				isnull2;
	int			i,
				result = 0;

	for (i = 0; i < scan->indexMethods->nattr; i++)
	{
		Assert(scan->indexMethods->attrCompareMethod[i] != CompareInvalid);
		datum1 = heap_getattr(first, scan->indexMethods->attrNumbers[i], tupledesc,
							  &isnull1);
		datum2 = heap_getattr(second, scan->indexMethods->attrNumbers[i], tupledesc,
							  &isnull2);
		Assert((!isnull1) && (!isnull2));

		switch (scan->indexMethods->attrCompareMethod[i])
		{
			case CompareOid:
				result = FasttabCompareInts(DatumGetObjectId(datum1),
											DatumGetObjectId(datum2));
				break;
			case CompareCString:
				result = strcmp(DatumGetCString(datum1),
								DatumGetCString(datum2));
				break;
			case CompareInt16:
				result = FasttabCompareInts(DatumGetInt16(datum1),
											DatumGetInt16(datum2));
				break;
			case CompareInt64:
				result = FasttabCompareInts(DatumGetInt64(datum1),
											DatumGetInt64(datum2));
				break;
			case CompareBoolean:
				result = FasttabCompareInts(DatumGetBool(datum1),
											DatumGetBool(datum2));
				break;
			default:			/* should never happen, can be useful during
								 * development though */
				elog(ERROR, "Unexpected compare method: %d",
					 scan->indexMethods->attrCompareMethod[i]);
		}

		if (result != 0)
			break;
	}

	return result;
}

/*
 * Form index tuple from virtual heap tuple during index-only scan.
 */
static IndexTuple
fasttab_index_form_tuple(HeapTuple tup, IndexScanDesc scan)
{
	TupleDesc	heaptupledesc = RelationGetDescr(scan->heapRelation);
	TupleDesc	indextupledesc = RelationGetDescr(scan->indexRelation);
	Datum		values[FasttabIndexMaxAttributes];
	bool		isnull[FasttabIndexMaxAttributes];
	int			i;

	for (i = 0; i < scan->indexMethods->nattr; i++)
	{
		/*
		 * NB: heap_getattr prcesses negative attribute numbers like
		 * ObjectIdAttributeNumber just fine
		 */
		values[i] = heap_getattr(tup, scan->indexMethods->attrNumbers[i],
								 heaptupledesc, &(isnull[i]));
	}

	return index_form_tuple(indextupledesc, values, isnull);
}

/*
 * Convert index attribute number to heap attribute number.
 */
static inline AttrNumber
fasttab_convert_index_attno_to_heap_attno(IndexScanDesc scan,
										  AttrNumber indexAttno)
{
	Assert(indexAttno > 0);
	Assert(indexAttno <= FasttabIndexMaxAttributes);
	Assert(indexAttno <= scan->indexMethods->nattr);
	return scan->indexMethods->attrNumbers[indexAttno - 1];
}

/*
 * Determine whether virtual heap tuple matches WHERE condition during index
 * scan.
 */
static bool
fasttab_index_tuple_matches_where_condition(IndexScanDesc scan, HeapTuple tup)
{
	int			i;
	bool		insert;
	AttrNumber	attrNumbersBackup[FasttabIndexMaxAttributes];

	/* If WHERE condition is empty all tuples match */
	if (scan->numberOfKeys == 0)
		return true;

	/* NB: scan->keyData[0].sk_strategy can be InvalidStrategy */
	Assert(scan->keyData != NULL);
	Assert(scan->keyData[0].sk_attno != InvalidAttrNumber);

	/* Convert index attribute numbers to tuple attribute numbers. */
	for (i = 0; i < scan->numberOfKeys; i++)
	{
		attrNumbersBackup[i] = scan->keyData[i].sk_attno;
		scan->keyData[i].sk_attno = fasttab_convert_index_attno_to_heap_attno(scan, scan->keyData[i].sk_attno);
	}

	/* NB: HeapKeyTest is a macro, it changes `insert` variable */
	HeapKeyTest(tup, RelationGetDescr(scan->heapRelation), scan->numberOfKeys,
				scan->keyData, insert);

	/* Restore original attribute numbers. */
	for (i = 0; i < scan->numberOfKeys; i++)
		scan->keyData[i].sk_attno = attrNumbersBackup[i];

	return insert;
}

/*
 * Add tuple to scan->xs_inmem_tuplist at proper position.
 *
 * Returs:
 * true - tuple added
 * false - tuple not added (filtered by WHERE condition)
 */
static bool
fasttab_index_insert_tuple_in_sorted_list(IndexScanDesc scan, HeapTuple tup)
{
	DListHeapTuple dlist_tup;
	dlist_node *insert_after = &scan->xs_inmem_tuplist.head;
	dlist_iter	iter;

	/* scan->orderByData is never used in index scans over catalog tables */
	Assert(scan->numberOfOrderBys == 0);
	Assert(scan->numberOfKeys >= 0 && scan->numberOfKeys <= FasttabIndexMaxAttributes);

	if (!fasttab_index_tuple_matches_where_condition(scan, tup))
		return false;

	/* Using regular transaction memory context here. */
	dlist_tup = palloc(sizeof(DListHeapTupleData));
	dlist_tup->tup = heap_copytuple(tup);

	dlist_foreach(iter, &scan->xs_inmem_tuplist)
	{
		DListHeapTuple dlist_curr = (DListHeapTuple) iter.cur;

		if (fasttab_index_compare_tuples(dlist_curr->tup, tup, scan) >= 0)
			break;

		insert_after = iter.cur;
	}

	dlist_insert_after(insert_after, &dlist_tup->node);

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_index_insert_tuple_in_sorted_list scan = %p, tup oid = %d, tuple added to list",
		 scan, HeapTupleGetOid(tup));
#endif

	return true;
}

/*
 * Initialize part of `scan` related to virtual catalog. Basically
 * index_beginscan implementation for virtual tuples.
 *
 * NB: scan->keyData is not initialized here (usually filled with 0x7f's)
 */
void
fasttab_index_beginscan(IndexScanDesc scan)
{
	Oid			indexId = RelationGetRelid(scan->indexRelation);

	Assert(PointerIsValid(scan->indexRelation));

	if (!IsFasttabHandledIndexId(indexId))
		return;

	scan->xs_regular_tuple_enqueued = false;
	scan->xs_regular_scan_finished = false;
	scan->xs_scan_finish_returned = false;

	/* indexMethods is accessed quite often so we memoize it */
	scan->indexMethods = GetFasttabIndexMethods(indexId);

	/*
	 * xs_inmem_tuplist is initialized when fasttab_index_getnext_tid_merge is
	 * called first time. We are not doing it here because:
	 *
	 * 1) It's more efficient this way, since sometimes beginscan/rescan are
	 * called without any actual scanning
	 *
	 * 2) Sometimes `scan` passed to beginscan is not fully initilized so we
	 * can't filter tuples by WHERE condition here
	 *
	 * 3) We would like to filter tuples by WHERE condition ASAP, otherwise
	 * memory will be wasted on tuples that will be filtered anyway
	 */
	scan->xs_inmem_tuplist_init_done = false;
	dlist_init(&scan->xs_inmem_tuplist);

	/*
	 * Make sure scan->xs_ctup.t_self has proper initial value (required in
	 * index_getnext_tid)
	 */
	ItemPointerSetInvalid(&scan->xs_ctup.t_self);

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_index_beginscan (could be called from rescan), scan = %p, indexId = %u "
		 "scan->numberOfKeys = %d, scan->keyData = %p, scan->numberOfOrderBys = %d, scan->orderByData = %p",
	scan, indexId, scan->numberOfKeys, scan->keyData, scan->numberOfOrderBys,
		 scan->orderByData
		);
#endif

}

/*
 * Free part of `scan` related to virtual catalog. Basically index_endscan
 * implementation for virtual tuples.
 */
void
fasttab_index_endscan(IndexScanDesc scan)
{
	Assert(PointerIsValid(scan->indexRelation));

	if (!IsFasttabHandledIndexId(RelationGetRelid(scan->indexRelation)))
		return;

	/* Free in-memory tuples left. */
	FasttabDListFree(&scan->xs_inmem_tuplist);

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_index_endscan (could be called from rescan), scan = %p, scan->indexRelation relid = %u",
		 scan, RelationGetRelid(scan->indexRelation)
		);
#endif

}

/*
 * Reinitialize part of `scan` related to virtual catalog. Basically
 * index_rescan implementation for virtual tuples.
 *
 * NB: scan->keyData is not initialized here (usually filled with 0x7f's)
 */
void
fasttab_index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
					 ScanKey orderbys, int norderbys)
{
	fasttab_index_endscan(scan);
	fasttab_index_beginscan(scan);
}

/*
 * Fetch virtual or regular tuple from heap. Almost as heap_fetch, but also
 * understands HOT chains.
 *
 * Returns true if tuple was found, false otherwise.
 */
bool
fasttab_simple_heap_fetch(Relation relation, Snapshot snapshot,
						  HeapTuple tuple)
{
	Page		page;
	bool		found;
	Buffer		buffer = InvalidBuffer;
	ItemPointer tid = &(tuple->t_self);

	/*
	 * No need to lock any buffers for in-memory tuple, they could not even
	 * exist!
	 */
	if (IsFasttabItemPointer(tid))
		return heap_hot_search_buffer(tid, relation, buffer, snapshot, tuple, NULL, true);

	/* Fetch and pin the appropriate page of the relation. */
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

	/* Need share lock on buffer to examine tuple commit status. */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	found = heap_hot_search_buffer(tid, relation, buffer, snapshot, tuple,
								   NULL, true);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);

	return found;
}

/*
 * Make sure scan->xs_inmem_tuplist is initialized.
 */
static void
fasttab_index_make_sure_inmem_tuplist_init_done(IndexScanDesc scan)
{
	FasttabSnapshot fasttab_snapshot;
	dlist_iter	iter;
	int			idx;

	Assert(PointerIsValid(scan->indexRelation));

	/* initialize scan->xs_inmem_tuplist during first call */
	if (scan->xs_inmem_tuplist_init_done)
		return;

	idx = GetSnapshotRelationIdxByOid(RelationGetRelid(scan->heapRelation));

	fasttab_snapshot = FasttabSnapshotGetCurrent();
	dlist_foreach(iter, &fasttab_snapshot->relationData[idx].tuples)
	{
		DListHeapTuple dlist_curr = (DListHeapTuple) iter.cur;

		(void) fasttab_index_insert_tuple_in_sorted_list(scan, dlist_curr->tup);
	}

	scan->xs_inmem_tuplist_init_done = true;
}

/*
 * Get next virtual or regular TID from a scan. Basically a wrapper around
 * indexRelation->rd_amroutine->amgettuple procedure.
 *
 * NB: we filter tuples using scan->keyData _here_ since keyData is not always
 * initialized when fasttab_index_beginscan or _rescan are called (usually
 * filled with 0x7f's)
 */
bool
fasttab_index_getnext_tid_merge(IndexScanDesc scan, ScanDirection direction)
{
	bool		fetched;
	DListHeapTuple ret_node;
	FmgrInfo   *procedure;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amgettuple);

	Assert(PointerIsValid(scan->indexRelation));

	if (!IsFasttabHandledIndexId(RelationGetRelid(scan->indexRelation)))
	{
		/*
		 * Regular logic.
		 *
		 * The AM's amgettuple proc finds the next index entry matching the scan
		 * keys, and puts the TID into scan->xs_ctup.t_self.  It should also set
		 * scan->xs_recheck and possibly scan->xs_itup, though we pay no attention
		 * to those fields here.
		 */
		return DatumGetBool(FunctionCall2(procedure,
									   PointerGetDatum(scan),
									   Int32GetDatum(direction)));
	}

	/* Initialize scan->xs_inmem_tuplist during first call. */
	fasttab_index_make_sure_inmem_tuplist_init_done(scan);

	if (dlist_is_empty(&scan->xs_inmem_tuplist))		/* in-memory tuples
														 * enumiration is over? */
	{
#ifdef FASTTAB_DEBUG
		elog(NOTICE, "FASTTAB: fasttab_index_getnext_tid_merge, scan = %p, fake tuples list is empty, xs_regular_scan_finished = %u, xs_scan_finish_returned = %u",
		scan, scan->xs_regular_scan_finished, scan->xs_scan_finish_returned);
#endif

		/*
		 * If ->amgettuple() already returned false we should not call it once
		 * again.  In this case btree index will start a scan all over again,
		 * see btgettuple implementation.  Still if user will call this
		 * procedure once again dispite of returned 'false' value she probably
		 * knows what she is doing.
		 */
		if (scan->xs_regular_scan_finished && (!scan->xs_scan_finish_returned))
		{
			scan->xs_scan_finish_returned = true;
			return false;
		}

		/* regular logic */
		return DatumGetBool(FunctionCall2(procedure,
									   PointerGetDatum(scan),
									   Int32GetDatum(direction)));
	}

	/*
	 * Other directions are not used in index-only scans for catalog tables.
	 * No need to check direction above this point since only here
	 * scan->xs_inmem_tuplist is both initialized and non-empty.
	 */
	Assert(ScanDirectionIsForward(direction));

	/* If there is no regular tuple in in-memory queue, we should load one. */
	while ((!scan->xs_regular_tuple_enqueued) && (!scan->xs_regular_scan_finished))
	{
		bool found;
		found = DatumGetBool(FunctionCall2(procedure,
									   PointerGetDatum(scan),
									   Int32GetDatum(direction)));
		if (found)
		{
			HeapTupleData regular_tup;

			regular_tup.t_self = scan->xs_ctup.t_self;
			fetched = fasttab_simple_heap_fetch(scan->heapRelation, scan->xs_snapshot,
												&regular_tup);

			if (!fetched)
			{
#ifdef FASTTAB_DEBUG
				elog(NOTICE, "FASTTAB: fasttab_index_getnext_tid_merge, scan = %p, indexed tuple not found, 'continue;'",
					 scan);
#endif
				continue;
			}
			scan->xs_regular_tuple_enqueued = fasttab_index_insert_tuple_in_sorted_list(scan, &regular_tup);
		}
		else
			scan->xs_regular_scan_finished = true;
	}

	Assert(scan->xs_regular_scan_finished || scan->xs_regular_tuple_enqueued);

	ret_node = (DListHeapTuple) dlist_pop_head_node(&scan->xs_inmem_tuplist);
	Assert(PointerIsValid(ret_node));

	scan->xs_recheck = false;
	ItemPointerCopy(&ret_node->tup->t_self, &scan->xs_ctup.t_self);

	if (!IsFasttabItemPointer(&scan->xs_ctup.t_self))
		scan->xs_regular_tuple_enqueued = false;

#ifdef FASTTAB_DEBUG
	elog(NOTICE, "FASTTAB: fasttab_index_getnext_tid_merge, scan = %p, direction = %d, scan->indexRelation relid = %u, return tuple tid = %08X/%04X",
		 scan, direction, RelationGetRelid(scan->indexRelation),
		 BlockIdGetBlockNumber(&scan->xs_ctup.t_self.ip_blkid),
		 scan->xs_ctup.t_self.ip_posid
		);
#endif

	scan->xs_itup = fasttab_index_form_tuple(ret_node->tup, scan);
	DListHeapTupleFree(ret_node);
	return true;
}

/*
 * Get all tuples, virtual and regular, at once from an index scan. Basically
 * index_getbitmap implementation for virtual tuples.
 *
 * Returns true on success and false if relation doesn't have a virtual part.
 */
bool
fasttab_index_getbitmap(IndexScanDesc scan, Node **bitmap, int64 *result)
{
	int64		ntids = 0;
	bool		heap_opened = false;


	/* XXX should we use less than work_mem for this? */
	*bitmap = tbm_create(work_mem * 1024L);

	Assert(PointerIsValid(scan->indexRelation));

	if (!IsFasttabHandledIndexId(RelationGetRelid(scan->indexRelation)))
		return false;

	/* Fill heapRelation if it's NULL, we require it in fasttab_* procedures */
	if (!scan->heapRelation)
	{
		scan->heapRelation = heap_open(scan->indexRelation->rd_index->indrelid,
									   NoLock);
		heap_opened = true;
	}

	/* Initialize scan->xs_inmem_tuplist during first call. */
	fasttab_index_make_sure_inmem_tuplist_init_done(scan);

	/* There are in fact no in-memory tuples? */
	if (dlist_is_empty(&scan->xs_inmem_tuplist))
	{
		if (heap_opened)		/* cleanup */
		{
			heap_close(scan->heapRelation, NoLock);
			scan->heapRelation = NULL;
		}
		return false;
	}

	while (fasttab_index_getnext_tid_merge(scan, ForwardScanDirection))
	{
		tbm_add_tuples(bitmap, &scan->xs_ctup.t_self, 1, false);
		ntids++;
	}

	if (heap_opened)			/* cleanup */
	{
		heap_close(scan->heapRelation, NoLock);
		scan->heapRelation = NULL;
	}

	*result = ntids;
	return true;
}


/*****************************************************************************
			   PROCEDURES USED IN FasttabRelationMethodsTable
 *****************************************************************************/

/*
 * Determine wheter given tuple of pg_class relation should be stored in-memory.
 *
 * If tuple's relpersistence = RELPERSISTENCE_FAST_TEMP it should be virtual.
 */
static bool
pg_class_is_inmem_tuple(Relation relation, HeapTuple tup,
						FasttabSnapshot fasttab_snapshot, int tableIdx)
{
	bool		isnull;
	Datum		relpersistencedat;
	TupleDesc	tupledesc;

	Assert(RelationGetRelid(relation) == RelationRelationId);

	tupledesc = RelationGetDescr(relation);
	relpersistencedat = heap_getattr(tup, Anum_pg_class_relpersistence,
									 tupledesc, &isnull);
	Assert(!isnull);
	return ((char) relpersistencedat == RELPERSISTENCE_FAST_TEMP);
}

/*
 * Determine wheter given tuple of relations other than pg_class should be
 * stored in-memory.
 *
 * If tuple references to virtual pg_class tuple it should be virtual as well.
 */
static bool
generic_is_inmem_tuple(Relation relation, HeapTuple tup,
					   FasttabSnapshot fasttab_snapshot, int tableIdx)
{
	dlist_iter	iter;
	TupleDesc	tupledesc;
	Oid			values[FasttabRelationMaxOidAttributes];
	bool		isnull;
	int			i,
				pg_class_idx,
				noidattr = FasttabRelationMethodsTable[tableIdx].noidattr;

	Assert(IsFasttabHandledRelationId(RelationGetRelid(relation)));
	Assert(tableIdx >= 0 && tableIdx < FasttabSnapshotTablesNumber);
	Assert(noidattr > 0 && noidattr <= FasttabRelationMaxOidAttributes);

	/*
	 * Special case. During table creation pg_type and pg_depend are modified
	 * before pg_class (see heap_create_with_catalog implementation) so there
	 * is no way to tell wheter tuples are in-memory without using
	 * relperistence hint. Also this check could be considered as an
	 * optimization.
	 */
	if ((RelationGetRelid(relation) == TypeRelationId) || (RelationGetRelid(relation) == DependRelationId))
		return (CurrentRelpersistenceHint == RELPERSISTENCE_FAST_TEMP);

	tupledesc = RelationGetDescr(relation);

	for (i = 0; i < noidattr; i++)
	{
		values[i] = DatumGetObjectId(heap_getattr(tup,
						FasttabRelationMethodsTable[tableIdx].attrNumbers[i],
												  tupledesc, &isnull));
		Assert(!isnull);
	}

	/*
	 * Check whether there is an in-memory pg_class tuple with oid from
	 * values[] array
	 */
	pg_class_idx = GetSnapshotRelationIdxByOid(RelationRelationId);
	dlist_foreach(iter, &fasttab_snapshot->relationData[pg_class_idx].tuples)
	{
		DListHeapTuple dlist_tup = (DListHeapTuple) iter.cur;
		Oid			oid = HeapTupleGetOid(dlist_tup->tup);

		for (i = 0; i < noidattr; i++)
		{
			if (oid == values[i])
				return true;
		}
	}

	return false;
}

