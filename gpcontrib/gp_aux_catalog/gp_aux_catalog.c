
#include "postgres.h"

#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"

PG_MODULE_MAGIC;
void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_event_trigger_ddl_commands);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_oid);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_reason);

PG_FUNCTION_INFO_V1(gpdb_binary_upgrade_catalog_1_0_to_1_1);

Datum
pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_ddl_commands_internal(fcinfo);
}

Datum
pg_event_trigger_table_rewrite_oid(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_table_rewrite_oid_internal(fcinfo);
}

Datum
pg_event_trigger_table_rewrite_reason(PG_FUNCTION_ARGS)
{
    return pg_event_trigger_table_rewrite_reason_internal(fcinfo);
}

static void
gpdb_binary_upgrade_insert_pro_tup(
	Relation rel,
	Oid oid,
	TupleDesc tupDesc,
	const char * proname,
	Oid prorettype,
	uint16 nargs,
	oidvector	*parameterTypes)
{
    bool		nulls[Natts_pg_proc];
	Datum		values[Natts_pg_proc];
    HeapTuple tuple;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

    values[Anum_pg_proc_proname - 1] = NameGetDatum(proname);
	values[Anum_pg_proc_pronamespace - 1] = ObjectIdGetDatum(PG_CATALOG_NAMESPACE);
	values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID);
	values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(INTERNALlanguageId);
	values[Anum_pg_proc_procost - 1] = Float4GetDatum(1);
	values[Anum_pg_proc_prorows - 1] = Float4GetDatum(0);
	values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_proc_protransform - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_proc_proisagg - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proiswindow - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(true);
	values[Anum_pg_proc_proretset - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_provolatile - 1] = CharGetDatum(PROVOLATILE_VOLATILE);
	values[Anum_pg_proc_pronargs - 1] = UInt16GetDatum(nargs);
	values[Anum_pg_proc_pronargdefaults - 1] = UInt16GetDatum(0);
	values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(prorettype);
	values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes);
	nulls[Anum_pg_proc_proallargtypes - 1] = true;
	nulls[Anum_pg_proc_proargmodes - 1] = true;
	nulls[Anum_pg_proc_proargnames - 1] = true;
	nulls[Anum_pg_proc_proargdefaults - 1] = true;
	values[Anum_pg_proc_prosrc - 1] = CStringGetTextDatum(proname);
	nulls[Anum_pg_proc_probin - 1] = true;
	nulls[Anum_pg_proc_proconfig - 1] = true;
	nulls[Anum_pg_proc_proacl - 1] = true;
	/* proacl will be determined later */
	values[Anum_pg_proc_prodataaccess - 1] = CharGetDatum(PRODATAACCESS_NONE);
	values[Anum_pg_proc_proexeclocation - 1] = CharGetDatum(PROEXECLOCATION_ANY);

	tuple = heap_form_tuple(tupDesc, values, nulls);

	if (tupDesc->tdhasoid)
		HeapTupleSetOid(tuple, oid);
	else
		elog(ERROR, "failed to upgrade");

	simple_heap_insert(rel, tuple);

	CatalogUpdateIndexes(rel, tuple);
	heap_freetuple(tuple);
}

static void
gpdb_binary_upgrade_insert_am_tup(
	Relation rel,
	TupleDesc tupDesc
)
{
    bool		nulls[Natts_pg_am];
	Datum		values[Natts_pg_am];
    HeapTuple tuple;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_am_amname - 1] = NameGetDatum("bloom");
	values[Anum_pg_am_amstrategies - 1] = Int16GetDatum(0);
	values[Anum_pg_am_amsupport - 1] = Int16GetDatum(0);
	values[Anum_pg_am_amcanorder - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amcanorderbyop - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amcanbackward - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amcanunique - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amcanmulticol - 1] = BoolGetDatum(true);
	values[Anum_pg_am_amoptionalkey - 1] = BoolGetDatum(true);
	values[Anum_pg_am_amsearcharray - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amsearchnulls - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amstorage - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amclusterable - 1] = BoolGetDatum(false);
	values[Anum_pg_am_ampredlocks - 1] = BoolGetDatum(false);
	values[Anum_pg_am_amkeytype - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_am_aminsert - 1] = ObjectIdGetDatum(F_BLINSERT);
	values[Anum_pg_am_ambeginscan - 1] = ObjectIdGetDatum(F_BLBEGINSCAN);
	values[Anum_pg_am_amgettuple - 1] = ObjectIdGetDatum(F_BLGETTUPLE);;
	values[Anum_pg_am_amgetbitmap - 1] = ObjectIdGetDatum(F_BLGETBITMAP);
	values[Anum_pg_am_amrescan - 1] = ObjectIdGetDatum(F_BLRESCAN);
	values[Anum_pg_am_amendscan - 1] = ObjectIdGetDatum(F_BLENDSCAN);
	values[Anum_pg_am_ammarkpos - 1] = ObjectIdGetDatum(F_BLMARKPOS);
	values[Anum_pg_am_amrestrpos - 1] = ObjectIdGetDatum(F_BLRESTRPOS);
	values[Anum_pg_am_ambuild - 1] = ObjectIdGetDatum(F_BLBUILD);
	values[Anum_pg_am_ambuildempty - 1] = ObjectIdGetDatum(F_BLBUILDEMPTY);
	values[Anum_pg_am_ambulkdelete - 1] = ObjectIdGetDatum(F_BLBULKDELETE);
	values[Anum_pg_am_amvacuumcleanup - 1] = ObjectIdGetDatum(F_BLVACUUMCLEANUP);
	values[Anum_pg_am_amcanreturn - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_am_amcostestimate - 1] = ObjectIdGetDatum(F_BLCOSTESTIMATE);
	values[Anum_pg_am_amoptions - 1] = ObjectIdGetDatum(F_BLOPTIONS);


	tuple = heap_form_tuple(tupDesc, values, nulls);
	simple_heap_insert(rel, tuple);

	CatalogUpdateIndexes(rel, tuple);
	heap_freetuple(tuple);
}

/*
*
*	extern Datum blbuild(PG_FUNCTION_ARGS);
*	extern Datum blbuildempty(PG_FUNCTION_ARGS);
*	extern Datum blinsert(PG_FUNCTION_ARGS);
*	extern Datum blbeginscan(PG_FUNCTION_ARGS);
*	extern Datum blgettuple(PG_FUNCTION_ARGS);
*	extern Datum blgetbitmap(PG_FUNCTION_ARGS);
*	extern Datum blrescan(PG_FUNCTION_ARGS);
*	extern Datum blendscan(PG_FUNCTION_ARGS);
*	extern Datum blmarkpos(PG_FUNCTION_ARGS);
*	extern Datum blrestrpos(PG_FUNCTION_ARGS);
*	extern Datum blbulkdelete(PG_FUNCTION_ARGS);
*	extern Datum blvacuumcleanup(PG_FUNCTION_ARGS);
*	extern Datum bloptions(PG_FUNCTION_ARGS);
*/

Datum
gpdb_binary_upgrade_catalog_1_0_to_1_1(PG_FUNCTION_ARGS)
{
    Relation pgprocrel;
    Relation pgamrel;

    TupleDesc tupDesc;
	
	pgprocrel = relation_open(ProcedureRelationId, RowExclusiveLock);
	pgamrel = relation_open(AccessMethodRelationId, RowExclusiveLock);

	tupDesc = RelationGetDescr(pgprocrel);

	{
#define BLBUILD_NARGS 3
		Oid			procArgTypes[BLBUILD_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blbuild";

		for (int i = 0; i  < BLBUILD_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;
	
		parameterTypes = buildoidvector(procArgTypes, BLBUILD_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLBUILD, tupDesc, proname, INTERNALOID, BLBUILD_NARGS, parameterTypes);
	}

	{

#define BLBUILDEMPTY_NARGS 1
		Oid			procArgTypes[BLBUILDEMPTY_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blbuildempty";
	
		for (int i = 0; i  < BLBUILDEMPTY_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLBUILDEMPTY_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLBUILDEMPTY, tupDesc, proname, INTERNALOID, BLBUILDEMPTY_NARGS, parameterTypes);
	}

	{
#define BLINSERT_NARGS 6
		Oid			procArgTypes[BLINSERT_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blinsert";
	
		for (int i = 0; i  < BLINSERT_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLINSERT_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLINSERT, tupDesc, proname, BOOLOID, BLINSERT_NARGS, parameterTypes);
	}


	{
#define BLBEGINSCAN_NARGS 3
		Oid			procArgTypes[BLBEGINSCAN_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blbeginscan";
	
		for (int i = 0; i  < BLBEGINSCAN_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLBEGINSCAN_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLBEGINSCAN, tupDesc, proname, INTERNALOID, BLBEGINSCAN_NARGS, parameterTypes);
	}

	{
#define BLGETTUPLE_NARGS 2
		Oid			procArgTypes[BLGETTUPLE_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blgettuple";
	
		for (int i = 0; i  < BLGETTUPLE_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLGETTUPLE_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLGETTUPLE, tupDesc, proname, BOOLOID, BLGETTUPLE_NARGS, parameterTypes);
	}

		{
#define BLGETBITMAP_NARGS 2
		Oid			procArgTypes[BLGETBITMAP_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blgetbitmap";
	
		for (int i = 0; i  < BLGETBITMAP_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLGETBITMAP_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLGETBITMAP, tupDesc, proname, BOOLOID, BLGETBITMAP_NARGS, parameterTypes);
	}

	{
#define BLRESCAN_NARGS 2
		Oid			procArgTypes[BLRESCAN_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blrescan";

		for (int i = 0; i  < BLRESCAN_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLRESCAN_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLRESCAN, tupDesc, proname, VOIDOID, BLRESCAN_NARGS, parameterTypes);
	}

	{
#define BLENDSCAN_NARGS 2
		Oid			procArgTypes[BLENDSCAN_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blendscan";

		for (int i = 0; i  < BLENDSCAN_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLENDSCAN_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLENDSCAN, tupDesc, proname, VOIDOID, BLENDSCAN_NARGS, parameterTypes);
	}


	{
#define BLMARKPOS_NARGS 1
		Oid			procArgTypes[BLMARKPOS_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blmarkpos";

		for (int i = 0; i  < BLMARKPOS_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLMARKPOS_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLMARKPOS, tupDesc, proname, VOIDOID, BLMARKPOS_NARGS, parameterTypes);
	}

	{
#define BLRESTRPOS_NARGS 1
		Oid			procArgTypes[BLRESTRPOS_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blrestrpos";

		for (int i = 0; i  < BLRESTRPOS_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLRESTRPOS_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLRESTRPOS, tupDesc, proname, VOIDOID, BLRESTRPOS_NARGS, parameterTypes);
	}


	{
#define BLBULKDELETE_NARGS 4
		Oid			procArgTypes[BLBULKDELETE_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blbulkdelete";

		for (int i = 0; i  < BLBULKDELETE_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLBULKDELETE_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLBULKDELETE, tupDesc, proname, INTERNALOID, BLBULKDELETE_NARGS, parameterTypes);
	}

	{
#define BLVACUUMCLEANUP_NARGS 2
		Oid			procArgTypes[BLVACUUMCLEANUP_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blvacuumcleanup";

		for (int i = 0; i  < BLVACUUMCLEANUP_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLVACUUMCLEANUP_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLVACUUMCLEANUP, tupDesc, proname, INTERNALOID, BLVACUUMCLEANUP_NARGS, parameterTypes);
	}

	{
#define BLOPTIONS_NARGS 2
		Oid			procArgTypes[BLOPTIONS_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "bloptions";

		procArgTypes[0] = TEXTARRAYOID;
		procArgTypes[1] = BOOLOID;

		parameterTypes = buildoidvector(procArgTypes, BLOPTIONS_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLOPTIONS, tupDesc, proname, BYTEAOID, BLOPTIONS_NARGS, parameterTypes);
	}

	{
#define BLCOSTESTIMATE_NARGS 7
		Oid			procArgTypes[BLCOSTESTIMATE_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "blcostestimate";

		for (int i = 0; i  < BLCOSTESTIMATE_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;

		parameterTypes = buildoidvector(procArgTypes, BLCOSTESTIMATE_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_BLCOSTESTIMATE, tupDesc, proname, VOIDOID, BLCOSTESTIMATE_NARGS, parameterTypes);
	}

	gpdb_binary_upgrade_insert_am_tup(pgamrel, RelationGetDescr(pgamrel));

    relation_close(pgprocrel, RowExclusiveLock);
    relation_close(pgamrel, RowExclusiveLock);

    PG_RETURN_VOID();
}