/*-------------------------------------------------------------------------
 *
 * vacuum_stats.c
 *		Expose the vacuum counters accumulated by the statistics collector
 *		for relations (tables and indexes) and databases.
 *
 * The counters themselves are gathered by (auto)vacuum and delivered to
 * the statistics collector via PGSTAT_MTYPE_VACSTATS messages; here we
 * only read them back through the regular pgstat fetch API, so no system
 * catalog changes are required.
 *
 * contrib/vacuum_stats/vacuum_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "pgstat.h"

PG_MODULE_MAGIC;

/*
 * Fetch the vacuum counters for a relation (table or index), or NULL if
 * the statistics collector has no entry for it.
 */
static PgStat_VacuumStats *
fetch_rel_vacuum_stats(Oid relid)
{
	PgStat_StatTabEntry *tabentry;

	tabentry = pgstat_fetch_stat_tabentry(relid);
	if (tabentry == NULL)
		return NULL;

	return &tabentry->vacuum_stats;
}

/*
 * Fetch the per-database vacuum counters, or NULL if the statistics
 * collector has no entry for the database.
 */
static PgStat_VacuumStats *
fetch_db_vacuum_stats(Oid dbid)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_fetch_stat_dbentry(dbid);
	if (dbentry == NULL)
		return NULL;

	return &dbentry->n_vacuum_stats;
}

#define DEFINE_REL_VACSTAT_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			relid = PG_GETARG_OID(0); \
	PgStat_VacuumStats *stats = fetch_rel_vacuum_stats(relid); \
\
	PG_RETURN_INT64(stats ? (int64) stats->field : 0); \
}

#define DEFINE_DB_VACSTAT_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			dbid = PG_GETARG_OID(0); \
	PgStat_VacuumStats *stats = fetch_db_vacuum_stats(dbid); \
\
	PG_RETURN_INT64(stats ? (int64) stats->field : 0); \
}

/*
 * The "rev" counters live directly in the relation/database entries, not
 * in the embedded PgStat_VacuumStats, since they are fed from the regular
 * relation statistics rather than from the vacuum report.
 */
#define DEFINE_REL_ENTRY_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			relid = PG_GETARG_OID(0); \
	PgStat_StatTabEntry *tabentry = pgstat_fetch_stat_tabentry(relid); \
\
	PG_RETURN_INT64(tabentry ? (int64) tabentry->field : 0); \
}

#define DEFINE_DB_ENTRY_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			dbid = PG_GETARG_OID(0); \
	PgStat_StatDBEntry *dbentry = pgstat_fetch_stat_dbentry(dbid); \
\
	PG_RETURN_INT64(dbentry ? (int64) dbentry->field : 0); \
}

DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_tuples_deleted, tuples_deleted)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_dead_tuples, dead_tuples)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_deleted, pages_deleted)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_dead_pages, dead_pages)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_frozen, pages_frozen)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_all_visible, pages_all_visible)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_wraparound_count, wraparound_vacuum_count)
DEFINE_REL_ENTRY_FUNC(pg_stat_get_vacuum_rev_all_frozen_pages, rev_all_frozen_pages)
DEFINE_REL_ENTRY_FUNC(pg_stat_get_vacuum_rev_all_visible_pages, rev_all_visible_pages)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_total_time, total_time)

DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_tuples_deleted, tuples_deleted)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_dead_tuples, dead_tuples)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_deleted, pages_deleted)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_dead_pages, dead_pages)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_frozen, pages_frozen)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_all_visible, pages_all_visible)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_wraparound_count, wraparound_vacuum_count)
DEFINE_DB_ENTRY_FUNC(pg_stat_get_db_vacuum_rev_all_frozen_pages, n_rev_all_frozen_pages)
DEFINE_DB_ENTRY_FUNC(pg_stat_get_db_vacuum_rev_all_visible_pages, n_rev_all_visible_pages)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_total_time, total_time)
