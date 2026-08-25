# Test for checking dead_tuples, tuples_deleted and pages_frozen in
# pg_stat_vacuum_tables (provided by the vacuum_stats extension).
#
# Dead_tuples values are counted when vacuum cannot clean up unused tuples
# while a snapshot is held by another transaction.  Dead_tuples aren't
# increased after releasing the snapshot, unlike tuples_deleted, which is
# increased by the number of tuples that the vacuum managed to remove.
# Pages_frozen stays zero here: with the default vacuum_freeze_min_age
# these tuples are far too young to be frozen.
#
# The counters are summed over the whole cluster with the
# gp_stat_vacuum_tables view, since in Greenplum the data lives on the
# segments.  The statistics collector is fed asynchronously, so before
# printing the counters the wait steps poll until the vacuum report
# becomes visible, resetting the cached statistics snapshots on the
# master and the segments on every iteration.

setup
{
    CREATE EXTENSION vacuum_stats;
    CREATE TABLE test_vacuum_stat_isolation(id int, ival int) WITH (autovacuum_enabled = off);

    CREATE FUNCTION clear_stats_snapshot_on_segments() RETURNS SETOF void AS
    'SELECT pg_catalog.pg_stat_clear_snapshot()' LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

    CREATE FUNCTION wait_vacuum_stats(cond text) RETURNS bool AS '
    DECLARE
        ok bool;
    BEGIN
        FOR i IN 1..120 LOOP
            PERFORM pg_stat_clear_snapshot();
            PERFORM * FROM clear_stats_snapshot_on_segments();
            EXECUTE cond INTO ok;
            IF ok THEN
                RETURN true;
            END IF;
            PERFORM pg_sleep(0.25);
        END LOOP;
        RETURN false;
    END' LANGUAGE plpgsql;
}

teardown
{
    DROP TABLE test_vacuum_stat_isolation CASCADE;
    DROP FUNCTION wait_vacuum_stats(text);
    DROP FUNCTION clear_stats_snapshot_on_segments();
    DROP EXTENSION vacuum_stats;
}

session "s1"
step "s1_begin_repeatable_read" {
    BEGIN transaction ISOLATION LEVEL REPEATABLE READ;
    select count(ival) from test_vacuum_stat_isolation where id>900;
}
step "s1_commit"                { COMMIT; }

session "s2"
step "s2_insert"                { INSERT INTO test_vacuum_stat_isolation(id, ival) SELECT ival, ival%10 FROM generate_series(1,1000) As ival; }
step "s2_update"                { UPDATE test_vacuum_stat_isolation SET ival = ival + 2 where id > 900; }
step "s2_insert_interrupt"      { INSERT INTO test_vacuum_stat_isolation values (1,1); }
step "s2_vacuum"                { VACUUM test_vacuum_stat_isolation; }
step "s2_checkpoint"            { CHECKPOINT; }
step "s2_wait_dead_tuples"
{
    SELECT wait_vacuum_stats('SELECT sum(dead_tuples) = 100 FROM gp_stat_vacuum_tables WHERE relname = ''test_vacuum_stat_isolation''');
}
step "s2_wait_tuples_deleted"
{
    SELECT wait_vacuum_stats('SELECT sum(tuples_deleted) = 100 FROM gp_stat_vacuum_tables WHERE relname = ''test_vacuum_stat_isolation''');
}
step "s2_print_vacuum_stats_table"
{
    SELECT relname, sum(tuples_deleted) AS tuples_deleted,
           sum(dead_tuples) AS dead_tuples,
           sum(pages_frozen) AS pages_frozen
    FROM gp_stat_vacuum_tables
    WHERE relname = 'test_vacuum_stat_isolation'
    GROUP BY relname;
}

permutation
    "s2_insert"
    "s2_print_vacuum_stats_table"
    "s1_begin_repeatable_read"
    "s2_update"
    "s2_insert_interrupt"
    "s2_vacuum"
    "s2_wait_dead_tuples"
    "s2_print_vacuum_stats_table"
    "s1_commit"
    "s2_checkpoint"
    "s2_vacuum"
    "s2_wait_tuples_deleted"
    "s2_print_vacuum_stats_table"
