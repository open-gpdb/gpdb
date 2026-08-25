# vacuum_stats

Exposes vacuum counters accumulated by the statistics collector for
relations (tables and indexes) and databases, without any system catalog
changes: the counters live in the statistics collector's per-relation and
per-database entries, and all SQL objects are created by this extension.

## Why

Vacuum computes all of this while it works, and then throws it away.  The
numbers reach the server log — one block of text per run, from `VACUUM
VERBOSE` or `log_autovacuum_min_duration` — and are not available to SQL
afterwards.  What the server does keep about vacuum in
`pg_stat_all_tables` is `vacuum_count`, `last_vacuum` and the current
`n_dead_tup` estimate: that tells you how often vacuum ran, not what work
it had to do or what the work cost.  There is no `pg_stat_progress_vacuum`
in this release, and progress views only describe a vacuum that is running
right now anyway, not the picture over a period.

The gap matters because an administrator has to balance the useful effect
of vacuum against the overhead it puts on the system, and that balance is
different for every relation.  It is near zero for an append-only table
and highest for a frequently updated one.  Indexes have no visibility map,
so vacuum scans them in full: the cost grows with the number and the size
of the indexes, and the worst case is a bloating index on a small table.
In Greenplum this is entirely the administrator's problem — autovacuum is
disabled here except for the anti-wraparound vacuum of `template0` and
other non-connectable databases, so all VACUUMing of user tables is done
by hand or on a schedule, with no feedback loop to correct a bad guess.

Questions the counters answer:

- **Where does the vacuum budget actually go?**  `total_time`,
  `tuples_deleted` and `pages_deleted` per table and per index show which
  relations the maintenance window is being spent on, instead of a
  cluster-wide impression.
- **Is vacuum running but unable to do its job?**  A `dead_tuples` value
  that stays high means vacuum did visit the table but the dead rows are
  still visible to an old snapshot — a long-running transaction, an
  idle-in-transaction session, an old distributed snapshot.  Vacuuming
  more often will not help; the blocker has to be found and removed.
- **How quickly is the work undone?**  Comparing `rev_all_visible_pages`
  with `pages_all_visible` shows whether the pages vacuum marks
  all-visible keep the mark or lose it again immediately.  A table that
  constantly revokes it makes the next vacuum redo the same scan, and is
  a candidate for a lower `fillfactor` (to get HOT updates instead of
  page-spanning ones) or for being vacuumed on a different schedule.
- **When did a routine VACUUM turn into a full-table scan?**
  `wraparound_vacuum_count` counts the runs that crossed
  `vacuum_freeze_table_age` and therefore could not skip a single page
  through the visibility map.  That escalation is invisible otherwise and
  is usually the reason a nightly VACUUM suddenly takes hours.  It also
  measures the freezing pressure: this release has no autovacuum for user
  tables, so a manual VACUUM is the only thing that ever advances
  `relfrozenxid`, and a steadily growing counter means the schedule is
  only just keeping up with the wraparound horizon.  The counter reports
  only genuine freeze-age escalation: on a young cluster the freeze limits
  are clamped to their minimum values, which makes every relation match
  them, and those full-table scans are deliberately not counted.
- **Is vacuum freezing anything at all?**  `pages_frozen` says on how many
  pages the run actually froze tuples.  A table that accumulates vacuum
  runs with `pages_frozen` staying at zero is never getting its tuples
  frozen — the freezing is being deferred to a later, much more expensive
  full-table run, and `vacuum_freeze_min_age` is worth revisiting.
- **Is the load skewed?**  The `gp_segment_id` breakdown in the
  `gp_stat_vacuum_*` views shows a segment doing much more vacuum work
  than its peers, which usually means unevenly distributed data rather
  than a vacuum problem.

## Counters

For every heap relation, index and database the following counters are
accumulated by each (auto)vacuum run since the last statistics reset:

| column                  | meaning                                                                 |
|-------------------------|-------------------------------------------------------------------------|
| `tuples_deleted`        | tuples (index entries) removed by vacuum                                |
| `dead_tuples`           | dead tuples found but not yet removable (visible to old snapshots)      |
| `pages_deleted`         | pages truncated from a heap / pages deleted in an index                 |
| `dead_pages`            | pages left with unremovable dead tuples (heap) or deleted-but-not-yet-reusable pages (index) |
| `pages_frozen`          | pages on which vacuum froze at least one tuple — shows whether vacuum is freezing at all |
| `pages_all_visible`     | pages vacuum marked all-visible in the visibility map                   |
| `rev_all_frozen_pages`  | pages whose all-frozen status was revoked (always 0: the visibility map has no all-frozen bit in this release) |
| `rev_all_visible_pages` | pages whose all-visible bit was cleared by ordinary DML (delivered with the regular relation statistics, not the vacuum report) |
| `wraparound_vacuum_count` | vacuum runs that had to scan the whole relation because its `relfrozenxid`/`relminmxid` reached the freeze table age |
| `total_time`            | total vacuum time spent on the relation, in milliseconds                |

For indexes only the tuple-deletion, page and timing counters are
meaningful; the dead-tuple counter, the visibility-map counters and
`wraparound_vacuum_count` stay zero, since those describe the heap
relation the index belongs to.

Like the rest of the collected statistics, the counters are written to
the permanent statistics files when the statistics collector exits,
which happens on a clean shutdown and also when the postmaster dies
unexpectedly, so they survive a restart of the cluster.  After crash
recovery the server resets all collected statistics (including these),
as PostgreSQL considers them invalid after a crash.

## Tests

- `sql/vacuum_stats.sql` — a pg_regress smoke test of the extension
  objects and the cluster-wide views (`make installcheck`).
- `t/001_vacuum_statistics.pl` — a TAP test with the table, index and
  database scenarios of the upstream vacuum statistics patch, plus the
  restart-persistence and crash-reset checks (`make installcheck-tap`,
  requires a build with `--enable-tap-tests`).
- `src/test/isolation/specs/vacuum-extending-in-repetable-read.spec` —
  an isolation test checking that dead tuples held back by a repeatable
  read snapshot show up in `dead_tuples` and move to `tuples_deleted`
  once the snapshot is released.

## Views

Local (current node) views:

- `pg_stat_vacuum_tables`
- `pg_stat_vacuum_indexes`
- `pg_stat_vacuum_database`

Cluster-wide views (master plus every segment, with a `gp_segment_id`
column; in Greenplum vacuum does its real work on the segments, so these
are usually the interesting ones):

- `gp_stat_vacuum_tables`
- `gp_stat_vacuum_indexes`
- `gp_stat_vacuum_database`

## Usage

```sql
CREATE EXTENSION vacuum_stats;

VACUUM my_table;

SELECT gp_segment_id, tuples_deleted, dead_tuples, pages_deleted,
       pages_all_visible, rev_all_visible_pages,
       wraparound_vacuum_count, total_time
FROM gp_stat_vacuum_tables
WHERE relname = 'my_table';
```

Summed over the segments, to rank the tables by the vacuum time they
cost:

```sql
SELECT schemaname, relname,
       round(sum(total_time), 2) AS total_time_ms,
       sum(tuples_deleted) AS tuples_deleted,
       sum(dead_tuples) AS dead_tuples
FROM gp_stat_vacuum_tables
GROUP BY schemaname, relname
HAVING sum(total_time) > 0
ORDER BY total_time_ms DESC
LIMIT 10;
```

The counters are reset together with the rest of the collected statistics
(`pg_stat_reset()`, `pg_stat_reset_single_table_counters()`).
