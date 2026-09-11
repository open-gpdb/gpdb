CREATE EXTENSION vacuum_stats;

SET client_min_messages = warning;

CREATE TABLE vstat_heap (id int, val text) DISTRIBUTED BY (id);
CREATE INDEX vstat_heap_idx ON vstat_heap (val);
INSERT INTO vstat_heap SELECT g, 'val_' || g FROM generate_series(1, 1000) g;
DELETE FROM vstat_heap WHERE id % 2 = 0;
VACUUM vstat_heap;

-- The statistics collector is fed asynchronously (UDP), and every QE
-- caches its stats snapshot for the transaction, so poll until the
-- counters become visible, resetting the snapshots on each iteration.
CREATE FUNCTION vstat_clear_segment_snapshots() RETURNS SETOF void AS
$$ SELECT pg_catalog.pg_stat_clear_snapshot() $$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION wait_for_vacuum_stats(cond text) RETURNS bool AS
$$
DECLARE
    ok bool;
BEGIN
    FOR i IN 1..120 LOOP
        PERFORM pg_stat_clear_snapshot();
        PERFORM * FROM vstat_clear_segment_snapshots();
        EXECUTE cond INTO ok;
        IF ok THEN
            RETURN true;
        END IF;
        PERFORM pg_sleep(0.25);
    END LOOP;
    RETURN false;
END
$$ LANGUAGE plpgsql;

-- table counters: the deleted tuples must show up
SELECT wait_for_vacuum_stats($$
    SELECT sum(tuples_deleted) > 0
    FROM gp_stat_vacuum_tables WHERE relname = 'vstat_heap' $$);

SELECT sum(tuples_deleted) > 0 AS tuples_deleted_ok,
       sum(dead_tuples) >= 0 AS dead_tuples_ok,
       sum(pages_deleted) >= 0 AS pages_deleted_ok,
       sum(dead_pages) >= 0 AS dead_pages_ok,
       sum(pages_frozen) >= 0 AS pages_frozen_ok,
       sum(wraparound_vacuum_count) >= 0 AS wraparound_ok,
       sum(rev_all_frozen_pages) = 0 AS rev_all_frozen_pages_ok,
       sum(total_time) > 0 AS total_time_ok
FROM gp_stat_vacuum_tables WHERE relname = 'vstat_heap';

-- vacuum marks the remaining pages all-visible
SELECT wait_for_vacuum_stats($$
    SELECT sum(pages_all_visible) > 0
    FROM gp_stat_vacuum_tables WHERE relname = 'vstat_heap' $$);

-- index counters: the index entries removed by vacuum must show up
SELECT wait_for_vacuum_stats($$
    SELECT sum(tuples_deleted) > 0
    FROM gp_stat_vacuum_indexes WHERE indexrelname = 'vstat_heap_idx' $$);

SELECT sum(tuples_deleted) > 0 AS tuples_deleted_ok,
       sum(pages_deleted) >= 0 AS pages_deleted_ok,
       sum(dead_pages) >= 0 AS dead_pages_ok,
       sum(total_time) >= 0 AS total_time_ok
FROM gp_stat_vacuum_indexes WHERE indexrelname = 'vstat_heap_idx';

-- inserting into all-visible pages revokes their all-visible status,
-- which is delivered with the regular relation statistics
INSERT INTO vstat_heap SELECT g, 'again_' || g FROM generate_series(1, 30) g;

SELECT wait_for_vacuum_stats($$
    SELECT sum(rev_all_visible_pages) > 0
    FROM gp_stat_vacuum_tables WHERE relname = 'vstat_heap' $$);

-- database counters accumulate the per-relation ones
SELECT wait_for_vacuum_stats($$
    SELECT sum(tuples_deleted) > 0
    FROM gp_stat_vacuum_database WHERE datname = current_database() $$);

-- local (per-node) views also work; on the master the table is empty,
-- so just check that the relations are visible there
SELECT count(*) = 1 AS heap_visible FROM pg_stat_vacuum_tables
WHERE relname = 'vstat_heap';
SELECT count(*) = 1 AS index_visible FROM pg_stat_vacuum_indexes
WHERE indexrelname = 'vstat_heap_idx';
SELECT count(*) = 1 AS db_visible FROM pg_stat_vacuum_database
WHERE datname = current_database();

DROP FUNCTION wait_for_vacuum_stats(text);
DROP FUNCTION vstat_clear_segment_snapshots();
DROP TABLE vstat_heap;
DROP EXTENSION vacuum_stats;
