/*-------------------------------------------------------------------------
 *
 * cdbtempresult.c
 *	  POC: session-level registry for "catalogless" temp tables
 *	  created via CREATE TEMP TABLE ... WITH (catalogless) AS SELECT.
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
#include "catalog/namespace.h"
#include "cdb/cdbtempresult.h"
#include "cdb/cdbvars.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/tuplestorenew.h"

/*
 * GUC: defined here, registered in guc_gp.c.  Superuser-only kill-switch
 * for creation (default on); the per-object trigger is the CTAS
 * WITH (catalogless) option.  Lookups do not depend on it, so turning it
 * off never changes what an existing name resolves to.
 */
bool		gp_enable_catalogless_temp = true;

/* Per-session state, all in TopMemoryContext */
static HTAB *tempResultHash = NULL;
static int32 tempResultIdCounter = 0;
static bool tempResultXactCbRegistered = false;

/*
 * Reader-side NTupleStores currently open in this process.  A scan closes
 * its reader in ExecEnd; whatever is left here (error paths) is closed at
 * end of transaction.  (List cells live in TopMemoryContext.)
 */
static List *tempResultOpenReaders = NIL;

static void TempResultXactCallback(XactEvent event, void *arg);
static void TempResultEnsureHash(void);
static void TempResultReleaseEntry(TempResultEntry *entry);
static void TempResultCheckNotSubxact(const char *action);

static void
TempResultEnsureHash(void)
{
	if (tempResultHash == NULL)
	{
		HASHCTL		ctl;

		/* key: NUL-terminated table name (default string hash) */
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
 * The registry is not transactional at the subtransaction level: a
 * ROLLBACK TO SAVEPOINT could not undo a create or drop.  Refuse both.
 */
static void
TempResultCheckNotSubxact(const char *action)
{
	if (GetCurrentTransactionNestLevel() > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot %s a catalogless temp table inside a subtransaction", action),
				 errdetail("Subtransactions are not supported by catalogless temp tables in this POC.")));
}

bool
TempResultHasEntries(void)
{
	return tempResultHash != NULL && hash_get_num_entries(tempResultHash) > 0;
}

/*
 * Look up a catalogless temp table by name, ignoring the search path.
 */
TempResultEntry *
TempResultLookup(const char *name)
{
	if (!TempResultHasEntries() || name == NULL)
		return NULL;

	return (TempResultEntry *) hash_search(tempResultHash, name,
										   HASH_FIND, NULL);
}

/*
 * Look up a catalogless temp table by name and verify that it is still the
 * same table (virtual id) that the caller resolved earlier.
 */
TempResultEntry *
TempResultLookupId(const char *name, int32 vid)
{
	TempResultEntry *entry = TempResultLookup(name);

	if (entry == NULL || entry->vid != vid)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("catalogless temp table \"%s\" no longer exists", name),
				 errdetail("The table was dropped or recreated after the statement was parsed.")));
	return entry;
}

/*
 * Resolve a RangeVar as a reference to a catalogless temp table.
 *
 * Catalogless temp tables behave as if they lived in pg_temp: an explicit
 * pg_temp qualification always finds them, an unqualified name only if
 * pg_temp comes first for that name in the search path.  Any other schema
 * qualification never does.
 */
TempResultEntry *
TempResultResolve(const RangeVar *rv)
{
	TempResultEntry *entry;

	if (!TempResultHasEntries())
		return NULL;

	if (rv->schemaname != NULL && strcmp(rv->schemaname, "pg_temp") != 0)
		return NULL;

	entry = TempResultLookup(rv->relname);
	if (entry == NULL)
		return NULL;

	if (rv->schemaname == NULL && !RelnameTempNamespaceFirst(rv->relname))
		return NULL;

	return entry;
}

/*
 * Assign a new per-session virtual id (QD only).
 */
static int32
TempResultAssignId(void)
{
	if (tempResultIdCounter == PG_INT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of catalogless temp table ids in this session")));
	return ++tempResultIdCounter;
}

/*
 * Execution-time part of CREATE TEMP TABLE ... WITH (catalogless) on the QD:
 * check the kill-switch and name conflicts, and return a copy of the
 * IntoClause carrying a fresh virtual id.  The input is not modified, since
 * it may belong to a cached plan that is executed again.
 */
IntoClause *
TempResultPrepareInto(IntoClause *into)
{
	IntoClause *result;
	const char *name = into->rel->relname;
	Oid			tempns;

	Assert(into->isTempResult);

	if (!gp_enable_catalogless_temp)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("catalogless temp tables are disabled"),
				 errhint("A superuser can enable them with gp_enable_catalogless_temp.")));

	if (TempResultLookup(name) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", name)));

	/* a regular temp relation of the same name would be shadowed */
	tempns = LookupExplicitNamespace("pg_temp", true);
	if (OidIsValid(tempns) && OidIsValid(get_relname_relid(name, tempns)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", name)));

	result = copyObject(into);
	result->tempResultId = TempResultAssignId();
	return result;
}

/*
 * Register a new catalogless temp table.  tupdesc and policy are copied
 * into TopMemoryContext.
 */
TempResultEntry *
TempResultRegister(const char *name, int32 vid, TupleDesc tupdesc,
				   GpPolicy *policy)
{
	TempResultEntry *entry;
	TupleDesc	tupdesc_copy;
	GpPolicy   *policy_copy;
	bool		found;
	MemoryContext oldcxt;

	Assert(name != NULL);

	TempResultCheckNotSubxact("create");
	TempResultEnsureHash();

	if (hash_get_num_entries(tempResultHash) >= TEMPRESULT_MAX_ENTRIES)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many catalogless temp tables in this transaction"),
				 errdetail("At most %d catalogless temp tables can exist at the same time.",
						   TEMPRESULT_MAX_ENTRIES)));

	/* allocate everything first, so a failure cannot leave a bogus entry */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tupdesc_copy = CreateTupleDescCopy(tupdesc);
	policy_copy = policy ? GpPolicyCopy(policy) : NULL;
	MemoryContextSwitchTo(oldcxt);

	entry = (TempResultEntry *) hash_search(tempResultHash, name,
											HASH_ENTER, &found);
	if (found)
	{
		FreeTupleDesc(tupdesc_copy);
		if (policy_copy)
			pfree(policy_copy);
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", name)));
	}

	entry->vid = vid;
	entry->tupdesc = tupdesc_copy;
	entry->policy = policy_copy;
	entry->rowcount = 0;
	entry->store = NULL;
	entry->writeacc = NULL;

	/* cached plans may have resolved this name to something else */
	if (Gp_role != GP_ROLE_EXECUTE)
		ResetPlanCache();

	return entry;
}

/*
 * Drop a single entry's resources (the writer side also removes the files).
 */
static void
TempResultReleaseEntry(TempResultEntry *entry)
{
	NTupleStoreAccessor *writeacc = entry->writeacc;
	NTupleStore *store = entry->store;
	TupleDesc	tupdesc = entry->tupdesc;
	GpPolicy   *policy = entry->policy;

	/* forget the pointers first, so a failure below cannot free twice */
	entry->writeacc = NULL;
	entry->store = NULL;
	entry->tupdesc = NULL;
	entry->policy = NULL;

	if (writeacc)
		ntuplestore_destroy_accessor(writeacc);
	if (store)
		ntuplestore_destroy(store);
	if (tupdesc)
		FreeTupleDesc(tupdesc);
	if (policy)
		pfree(policy);
}

/*
 * DROP TABLE of a catalogless temp table.  Runs on the QD and, through the
 * dispatched DROP statement, on the QE writers.
 */
void
TempResultRemove(const char *name)
{
	TempResultEntry *entry;

	entry = TempResultLookup(name);
	if (entry == NULL)
		return;

	TempResultCheckNotSubxact("drop");

	TempResultReleaseEntry(entry);
	hash_search(tempResultHash, name, HASH_REMOVE, NULL);

	if (Gp_role != GP_ROLE_EXECUTE)
		ResetPlanCache();
}

/*
 * Deterministic tuplestore name shared between the writing CTAS and later
 * readers on the same segment.  Modeled on shareinput_create_bufname_prefix.
 */
char *
TempResultStoreName(int32 vid)
{
	return psprintf("CTMPRES_%d_%d", gp_session_id, vid);
}

/*
 * Open the local tuplestore of a catalogless temp table for reading.
 * The caller closes it with TempResultCloseReader when the scan ends.
 */
struct NTupleStore *
TempResultOpenReader(int32 vid)
{
	NTupleStore *store;
	MemoryContext oldcxt;

	TempResultEnsureHash();

	/*
	 * The files are interXact (the writer keeps them across statements),
	 * so resource owners will not close them on error; track the store and
	 * keep it out of the per-query context until it is closed.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	store = ntuplestore_create_readerwriter_xact(TempResultStoreName(vid), 0,
												 false /* reader */ );
	tempResultOpenReaders = lappend(tempResultOpenReaders, store);
	MemoryContextSwitchTo(oldcxt);

	return store;
}

/*
 * Close a reader store opened by TempResultOpenReader.  Readers never delete
 * the files, only the writer does.
 */
void
TempResultCloseReader(struct NTupleStore *store)
{
	tempResultOpenReaders = list_delete_ptr(tempResultOpenReaders, store);
	ntuplestore_destroy(store);
}

/*
 * End-of-transaction cleanup: drop all registered temp results and close
 * all reader stores.  POC scope: catalogless temp tables only live until
 * the end of the top-level transaction.
 *
 * This runs at PRE_COMMIT / PRE_PREPARE rather than at COMMIT: there an
 * error still aborts the transaction cleanly, while an error after the
 * commit record has been written would be a PANIC.  PRE_PREPARE covers the
 * QEs of a two-phase commit, where the backend leaves the transaction at
 * PREPARE time.
 */
static void
TempResultXactCallback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS status;
	TempResultEntry *entry;
	bool		had_entries;

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_ABORT:
			break;
		default:
			return;
	}

	while (tempResultOpenReaders != NIL)
		TempResultCloseReader((NTupleStore *) linitial(tempResultOpenReaders));

	had_entries = TempResultHasEntries();
	if (had_entries)
	{
		hash_seq_init(&status, tempResultHash);
		while ((entry = (TempResultEntry *) hash_seq_search(&status)) != NULL)
		{
			TempResultReleaseEntry(entry);
			hash_search(tempResultHash, entry->name, HASH_REMOVE, NULL);
		}

		/* plans referring to the dropped tables must be re-analyzed */
		if (Gp_role != GP_ROLE_EXECUTE)
			ResetPlanCache();
	}
}
