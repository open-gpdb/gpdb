/*-------------------------------------------------------------------------
 *
 * cdbtempresult.c
 *	  POC: session-level registry for "catalogless" temporary tables
 *	  created via CREATE TEMP TABLE ... AS SELECT.
 *
 *	  See src/include/cdb/cdbtempresult.h for an overview.
 *
 * Portions Copyright (c) 2026, Open GPDB POC
 *
 * IDENTIFICATION
 *	  src/backend/cdb/cdbtempresult.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cdb/cdbtempresult.h"
#include "cdb/cdbvars.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/tuplestorenew.h"

/* GUC: defined here, registered in guc_gp.c */
bool		gp_enable_catalogless_temp = false;

/* Per-session state, all in TopMemoryContext */
static HTAB *tempResultHash = NULL;
static int	tempResultIdCounter = 0;
static bool tempResultXactCbRegistered = false;

/*
 * Reader-side NTupleStores opened during this transaction; closed at end
 * of transaction.  (List cells live in TopMemoryContext.)
 */
static List *tempResultOpenReaders = NIL;

static void TempResultXactCallback(XactEvent event, void *arg);
static void TempResultEnsureHash(void);

static void
TempResultEnsureHash(void)
{
	if (tempResultHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(TempResultEntry);
		ctl.hcxt = TopMemoryContext;
		tempResultHash = hash_create("catalogless temp result registry",
									 16, &ctl, HASH_ELEM | HASH_CONTEXT);
	}

	if (!tempResultXactCbRegistered)
	{
		RegisterXactCallback(TempResultXactCallback, NULL);
		tempResultXactCbRegistered = true;
	}
}

/*
 * Look up a catalogless temp table by (unqualified) name.
 * Returns NULL if not found or if the feature is disabled.
 */
TempResultEntry *
TempResultLookup(const char *name)
{
	if (!gp_enable_catalogless_temp || tempResultHash == NULL || name == NULL)
		return NULL;

	return (TempResultEntry *) hash_search(tempResultHash, name,
										   HASH_FIND, NULL);
}

/*
 * Register a new catalogless temp table.  tupdesc and policy are copied
 * into TopMemoryContext.
 */
TempResultEntry *
TempResultRegister(const char *name, int vid, TupleDesc tupdesc,
				   GpPolicy *policy)
{
	TempResultEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	Assert(name != NULL);

	/* POC restriction: no subtransaction support */
	if (GetCurrentTransactionNestLevel() > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("catalogless temp tables inside subtransactions are not implemented in this POC")));

	TempResultEnsureHash();

	entry = (TempResultEntry *) hash_search(tempResultHash, name,
											HASH_ENTER, &found);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("catalogless temp table \"%s\" already exists", name)));

	entry->vid = vid;
	entry->rowcount = 0;
	entry->store = NULL;
	entry->writeacc = NULL;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	entry->tupdesc = CreateTupleDescCopy(tupdesc);
	entry->policy = policy ? GpPolicyCopy(policy) : NULL;
	MemoryContextSwitchTo(oldcxt);

	return entry;
}

/*
 * Drop a single entry, releasing its tuplestore (which also removes the
 * files of the writer side).
 */
static void
TempResultReleaseEntry(TempResultEntry *entry)
{
	if (entry->writeacc)
	{
		ntuplestore_destroy_accessor(entry->writeacc);
		entry->writeacc = NULL;
	}
	if (entry->store)
	{
		ntuplestore_destroy(entry->store);
		entry->store = NULL;
	}
	if (entry->tupdesc)
	{
		FreeTupleDesc(entry->tupdesc);
		entry->tupdesc = NULL;
	}
	if (entry->policy)
	{
		pfree(entry->policy);
		entry->policy = NULL;
	}
}

void
TempResultRemove(const char *name)
{
	TempResultEntry *entry;

	if (tempResultHash == NULL)
		return;

	entry = (TempResultEntry *) hash_search(tempResultHash, name,
											HASH_FIND, NULL);
	if (entry == NULL)
		return;

	TempResultReleaseEntry(entry);
	hash_search(tempResultHash, name, HASH_REMOVE, NULL);
}

/*
 * Assign a new per-session virtual id (QD only).
 */
int
TempResultAssignId(void)
{
	return ++tempResultIdCounter;
}

/*
 * Deterministic tuplestore name shared between the writing CTAS and later
 * readers on the same segment.  Modeled on shareinput_create_bufname_prefix.
 */
char *
TempResultStoreName(int vid)
{
	return psprintf("CTMPRES_%d_%d", gp_session_id, vid);
}

/*
 * Open the local tuplestore of a catalogless temp table for reading.
 * The store is tracked and closed at end of transaction.
 */
struct NTupleStore *
TempResultOpenReader(int vid)
{
	NTupleStore *store;

	store = ntuplestore_create_readerwriter(TempResultStoreName(vid), 0,
											false /* reader */ );
	TempResultTrackReader(store);
	return store;
}

void
TempResultTrackReader(struct NTupleStore *store)
{
	MemoryContext oldcxt;

	TempResultEnsureHash();

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tempResultOpenReaders = lappend(tempResultOpenReaders, store);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * End-of-transaction cleanup: drop all registered temp results and close
 * all reader stores.  POC scope: catalogless temp tables only live until
 * the end of the top-level transaction.
 */
static void
TempResultXactCallback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS status;
	TempResultEntry *entry;
	ListCell   *lc;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
			break;
		default:
			return;
	}

	foreach(lc, tempResultOpenReaders)
	{
		NTupleStore *store = (NTupleStore *) lfirst(lc);

		ntuplestore_destroy(store);
	}
	list_free(tempResultOpenReaders);
	tempResultOpenReaders = NIL;

	if (tempResultHash != NULL)
	{
		hash_seq_init(&status, tempResultHash);
		while ((entry = (TempResultEntry *) hash_seq_search(&status)) != NULL)
		{
			TempResultReleaseEntry(entry);
			hash_search(tempResultHash, entry->name, HASH_REMOVE, NULL);
		}
	}
}
