
#include "postgres.h"

#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_amop.h"
#include "access/htup_details.h"
#include "../backend/commands/analyzefuncs.c"
PG_MODULE_MAGIC;
void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_event_trigger_ddl_commands);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_oid);
PG_FUNCTION_INFO_V1(pg_event_trigger_table_rewrite_reason);
PG_FUNCTION_INFO_V1(gpdb_binary_upgrade_catalog_1_0_to_1_1);
PG_FUNCTION_INFO_V1(gp_acquire_sample_rows_vac);

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
	values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(ClanguageId);
	values[Anum_pg_proc_procost - 1] = Float4GetDatum(1);
	values[Anum_pg_proc_prorows - 1] = Float4GetDatum(1000);
	values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_proc_protransform - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_proc_proisagg - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proiswindow - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(false);
	values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(true);
	values[Anum_pg_proc_proretset - 1] = BoolGetDatum(true);
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
	values[Anum_pg_proc_probin - 1] = CStringGetTextDatum("$libdir/gp_aux_catalog");
	
	nulls[Anum_pg_proc_proconfig - 1] = true;
	nulls[Anum_pg_proc_proacl - 1] = true;
	/* proacl will be determined later */
	values[Anum_pg_proc_prodataaccess - 1] = CharGetDatum(PRODATAACCESS_NONE);
	values[Anum_pg_proc_proexeclocation - 1] = CharGetDatum(PROEXECLOCATION_ALL_SEGMENTS);

	tuple = heap_form_tuple(tupDesc, values, nulls);

	if (tupDesc->tdhasoid)
		HeapTupleSetOid(tuple, oid);
	else
		elog(ERROR, "failed to upgrade");

	simple_heap_insert(rel, tuple);

	CatalogUpdateIndexes(rel, tuple);
	heap_freetuple(tuple);
}





Datum
gpdb_binary_upgrade_catalog_1_0_to_1_1(PG_FUNCTION_ARGS)
{
    Relation pgprocrel;
    Relation pgamrel;
	Relation pgopcrel;
	Relation pgopfrel;
	Relation pgamprocrel;
	Relation pgamoprel;

    TupleDesc tupDesc;

	pgprocrel = relation_open(ProcedureRelationId, RowExclusiveLock);
	pgamrel = relation_open(AccessMethodRelationId, RowExclusiveLock);
	pgopcrel = relation_open(OperatorClassRelationId, RowExclusiveLock);
	pgopfrel = relation_open(OperatorFamilyRelationId, RowExclusiveLock);
	pgamprocrel = relation_open(AccessMethodProcedureRelationId, RowExclusiveLock);
	pgamoprel = relation_open(AccessMethodOperatorRelationId, RowExclusiveLock);

	tupDesc = RelationGetDescr(pgprocrel);
    {
#define F_GP_ASR 7214
#define F_GP_NARGS 4
		Oid			procArgTypes[F_GP_NARGS];
		oidvector	*parameterTypes;

		char proname[NAMEDATALEN] = "gp_acquire_sample_rows_vac";

		for (int i = 0; i  < F_GP_NARGS; ++i) 
			procArgTypes[i] = INTERNALOID;
		procArgTypes[0] = OIDOID;
		procArgTypes[1] =  INT4OID;
		procArgTypes[2] =  BOOLOID;
		procArgTypes[3] = INT4OID;
		parameterTypes = buildoidvector(procArgTypes, F_GP_NARGS);
		gpdb_binary_upgrade_insert_pro_tup(pgprocrel, F_GP_ASR, tupDesc, proname, RECORDOID, F_GP_NARGS, parameterTypes);
	}


	relation_close(pgopcrel, RowExclusiveLock);
	relation_close(pgopfrel, RowExclusiveLock);
    relation_close(pgprocrel, RowExclusiveLock);
    relation_close(pgamrel, RowExclusiveLock);
	relation_close(pgamprocrel, RowExclusiveLock);
	relation_close(pgamoprel, RowExclusiveLock);

    PG_RETURN_VOID();
}

Datum
gp_acquire_sample_rows_vac(PG_FUNCTION_ARGS)
{
	
	Oid			relOid = PG_GETARG_OID(0);
	int32		targrows = PG_GETARG_INT32(1);
	bool		inherited = PG_GETARG_BOOL(2);
	int32		vacopts = PG_GETARG_INT32(3);
	return gp_acquire_sample_rows_int(fcinfo,relOid,targrows,inherited,vacopts);
}