/* contrib/vacuum_stats/vacuum_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vacuum_stats" to load this file. \quit

--
-- Per-relation accessor functions (tables and indexes).
--
CREATE FUNCTION pg_stat_get_vacuum_tuples_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_tuples_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_dead_tuples(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_dead_tuples'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_dead_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_dead_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_frozen(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_frozen'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_all_visible(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_all_visible'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_wraparound_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_wraparound_count'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_rev_all_frozen_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_rev_all_frozen_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_rev_all_visible_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_rev_all_visible_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_total_time(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_total_time'
LANGUAGE C STABLE STRICT;

--
-- Per-database accessor functions.
--
CREATE FUNCTION pg_stat_get_db_vacuum_tuples_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_tuples_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_dead_tuples(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_dead_tuples'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_dead_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_dead_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_frozen(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_frozen'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_all_visible(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_all_visible'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_wraparound_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_wraparound_count'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_rev_all_frozen_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_rev_all_frozen_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_rev_all_visible_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_rev_all_visible_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_total_time(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_total_time'
LANGUAGE C STABLE STRICT;

--
-- Local (per-node) views.  total_time is exposed in milliseconds, like
-- the other timing columns of the cumulative statistics views.
--
CREATE VIEW pg_stat_vacuum_tables AS
    SELECT
        c.oid AS relid,
        n.nspname AS schemaname,
        c.relname AS relname,
        pg_stat_get_vacuum_tuples_deleted(c.oid) AS tuples_deleted,
        pg_stat_get_vacuum_dead_tuples(c.oid) AS dead_tuples,
        pg_stat_get_vacuum_pages_deleted(c.oid) AS pages_deleted,
        pg_stat_get_vacuum_dead_pages(c.oid) AS dead_pages,
        pg_stat_get_vacuum_pages_frozen(c.oid) AS pages_frozen,
        pg_stat_get_vacuum_pages_all_visible(c.oid) AS pages_all_visible,
        pg_stat_get_vacuum_rev_all_frozen_pages(c.oid) AS rev_all_frozen_pages,
        pg_stat_get_vacuum_rev_all_visible_pages(c.oid) AS rev_all_visible_pages,
        pg_stat_get_vacuum_wraparound_count(c.oid) AS wraparound_vacuum_count,
        pg_stat_get_vacuum_total_time(c.oid) / 1000.0 AS total_time
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm');

CREATE VIEW pg_stat_vacuum_indexes AS
    SELECT
        c.oid AS relid,
        i.oid AS indexrelid,
        n.nspname AS schemaname,
        c.relname AS relname,
        i.relname AS indexrelname,
        pg_stat_get_vacuum_tuples_deleted(i.oid) AS tuples_deleted,
        pg_stat_get_vacuum_dead_tuples(i.oid) AS dead_tuples,
        pg_stat_get_vacuum_pages_deleted(i.oid) AS pages_deleted,
        pg_stat_get_vacuum_dead_pages(i.oid) AS dead_pages,
        pg_stat_get_vacuum_pages_frozen(i.oid) AS pages_frozen,
        pg_stat_get_vacuum_pages_all_visible(i.oid) AS pages_all_visible,
        pg_stat_get_vacuum_rev_all_frozen_pages(i.oid) AS rev_all_frozen_pages,
        pg_stat_get_vacuum_rev_all_visible_pages(i.oid) AS rev_all_visible_pages,
        pg_stat_get_vacuum_wraparound_count(i.oid) AS wraparound_vacuum_count,
        pg_stat_get_vacuum_total_time(i.oid) / 1000.0 AS total_time
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i';

CREATE VIEW pg_stat_vacuum_database AS
    SELECT
        d.oid AS datid,
        d.datname AS datname,
        pg_stat_get_db_vacuum_tuples_deleted(d.oid) AS tuples_deleted,
        pg_stat_get_db_vacuum_dead_tuples(d.oid) AS dead_tuples,
        pg_stat_get_db_vacuum_pages_deleted(d.oid) AS pages_deleted,
        pg_stat_get_db_vacuum_dead_pages(d.oid) AS dead_pages,
        pg_stat_get_db_vacuum_pages_frozen(d.oid) AS pages_frozen,
        pg_stat_get_db_vacuum_pages_all_visible(d.oid) AS pages_all_visible,
        pg_stat_get_db_vacuum_rev_all_frozen_pages(d.oid) AS rev_all_frozen_pages,
        pg_stat_get_db_vacuum_rev_all_visible_pages(d.oid) AS rev_all_visible_pages,
        pg_stat_get_db_vacuum_wraparound_count(d.oid) AS wraparound_vacuum_count,
        pg_stat_get_db_vacuum_total_time(d.oid) / 1000.0 AS total_time
    FROM pg_catalog.pg_database d;

--
-- Cluster-wide views: the same information from the master and from
-- every segment, following the gp_stat_replication pattern.
--
-- NB: the queries are spelled out instead of referencing the local views,
-- because a function executing on a QE slice may only access catalog
-- relations.
--
CREATE FUNCTION gp_stat_get_master_vacuum_tables() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid, n.nspname, c.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(c.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_frozen_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_visible_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_wraparound_count(c.oid),
        @extschema@.pg_stat_get_vacuum_total_time(c.oid) / 1000.0
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm')
$$
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION gp_stat_get_segment_vacuum_tables() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid, n.nspname, c.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(c.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_frozen_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_visible_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_wraparound_count(c.oid),
        @extschema@.pg_stat_get_vacuum_total_time(c.oid) / 1000.0
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm')
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_tables AS
    SELECT * FROM gp_stat_get_master_vacuum_tables() AS T
        (gp_segment_id int, relid oid, schemaname name, relname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_tables() AS T
        (gp_segment_id int, relid oid, schemaname name, relname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric);

CREATE FUNCTION gp_stat_get_master_vacuum_indexes() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid, i.oid, n.nspname, c.relname, i.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(i.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_frozen_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_visible_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_wraparound_count(i.oid),
        @extschema@.pg_stat_get_vacuum_total_time(i.oid) / 1000.0
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i'
$$
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION gp_stat_get_segment_vacuum_indexes() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid, i.oid, n.nspname, c.relname, i.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(i.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_frozen_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_rev_all_visible_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_wraparound_count(i.oid),
        @extschema@.pg_stat_get_vacuum_total_time(i.oid) / 1000.0
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i'
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_indexes AS
    SELECT * FROM gp_stat_get_master_vacuum_indexes() AS T
        (gp_segment_id int, relid oid, indexrelid oid, schemaname name,
         relname name, indexrelname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_indexes() AS T
        (gp_segment_id int, relid oid, indexrelid oid, schemaname name,
         relname name, indexrelname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric);

CREATE FUNCTION gp_stat_get_master_vacuum_database() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        d.oid, d.datname,
        @extschema@.pg_stat_get_db_vacuum_tuples_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_tuples(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_frozen(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_all_visible(d.oid),
        @extschema@.pg_stat_get_db_vacuum_rev_all_frozen_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_rev_all_visible_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_wraparound_count(d.oid),
        @extschema@.pg_stat_get_db_vacuum_total_time(d.oid) / 1000.0
    FROM pg_catalog.pg_database d
$$
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION gp_stat_get_segment_vacuum_database() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        d.oid, d.datname,
        @extschema@.pg_stat_get_db_vacuum_tuples_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_tuples(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_frozen(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_all_visible(d.oid),
        @extschema@.pg_stat_get_db_vacuum_rev_all_frozen_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_rev_all_visible_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_wraparound_count(d.oid),
        @extschema@.pg_stat_get_db_vacuum_total_time(d.oid) / 1000.0
    FROM pg_catalog.pg_database d
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_database AS
    SELECT * FROM gp_stat_get_master_vacuum_database() AS T
        (gp_segment_id int, datid oid, datname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_database() AS T
        (gp_segment_id int, datid oid, datname name,
         tuples_deleted int8, dead_tuples int8,
         pages_deleted int8, dead_pages int8,
         pages_frozen int8, pages_all_visible int8,
         rev_all_frozen_pages int8, rev_all_visible_pages int8,
         wraparound_vacuum_count int8, total_time numeric);

GRANT SELECT ON pg_stat_vacuum_tables, pg_stat_vacuum_indexes,
    pg_stat_vacuum_database, gp_stat_vacuum_tables,
    gp_stat_vacuum_indexes, gp_stat_vacuum_database TO PUBLIC;
