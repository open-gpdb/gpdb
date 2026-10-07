# The custom_scan_test toy extension

Everything this specific extension does — its GUCs, CSV modes, locus
menu, per-segment metadata routing, and how to run it. For the
generic "how would I build something like this from scratch" guide,
see [README-howto.md](README-howto.md).

Contents:

1. [What the extension does](#1-what-the-extension-does)
2. [GUCs](#2-gucs)
3. [Heap mode: replacing SeqScan](#3-heap-mode-replacing-seqscan)
4. [CSV mode: reading a file instead of the heap](#4-csv-mode-reading-a-file-instead-of-the-heap)
5. [Metadata-driven per-segment files](#6-metadata-driven-per-segment-files)
6. [Build, install, run](#7-build-install-run)
7. [Regression test](#8-regression-test)

---

## 1. What the extension does

When loaded, it installs a planner hook that replaces add path
`CustomScan` called **`TestCustomScan`**. The new node
can read rows from source:

- a CSV file — path picked either from a GUC or from a metadata
  table the extension manages, which can pin a different file to
  each segment.

It also exposes a knob that selects every `CdbPathLocus_Make*`
constructor, so you can watch what each one does to the plan and to
the result set.

---

## 2. GUCs

Both are `PGC_USERSET`.

| GUC                                    | Type   | Default   | What it controls                                                                                     |
|----------------------------------------|--------|-----------|-----------------------------------------------------------------------------------------------------|
| `custom_scan_test.enabled`             | bool   | `off`     | Master switch. If off, the planner hook is a no-op.                                                  |
| `custom_scan_test.metadata_file`       | string | `''`      | Path (on the coordinator) to a CSV file describing per-segment scan routing. Each non-empty non-comment line is `relname,seg_id,csv_path,ncols`. `seg_id = -1` means "any segment". The QD reads this file at planning time and ships the matching rows to every segment via the plan's `custom_private`. |

Note there is **no GUC for the data CSV path**: the data path is
always resolved through `metadata_file` and then shipped from the
master to the segments inside the plan. Locus of the custom path is
always inherited from the cheapest existing path (`cheapest->locus`),
and CSV data files are read as-is (no header skipping).

---

## 3. Heap mode

```sql
LOAD 'custom_scan_test';
SET custom_scan_test.enabled = on;

EXPLAIN SELECT * FROM some_table;
--   Custom Scan (TestCustomScan)        ← instead of Seq Scan
```

Behavior is identical to `SeqScan`: the node opens the relation with
`heap_beginscan`, pulls tuples with `heap_getnext`, and delegates
qual/projection to `ExecScan`. This is the trivial path, there to
demonstrate that the full `set_rel_pathlist_hook → CustomPath →
PlanCustomPath → CustomScan → CustomScanState` round trip actually
runs end to end.

---

## 4. CSV mode: reading a file instead of the heap

Rows come from CSVs **only** when the metadata file has a routing
entry for the scanned relation — the QD parses that file at planning
time, freezes the per-segment map in `custom_private`, and dispatches.
Segments never open the metadata file themselves.

```sql
CREATE TABLE employees (
    id int, name text, department text, salary numeric, hired_on date);

-- Write the metadata file on the coordinator (relname,seg_id,path,ncols).
COPY (VALUES
    ('employees', -1, '/abs/path/sample.csv', 5))
TO '/tmp/meta.csv' WITH (FORMAT csv);

SET custom_scan_test.metadata_file = '/tmp/meta.csv';

SELECT * FROM employees;   -- rows come from the CSV, not the table
```

```
# comments start with #
relname,seg_id,csv_path,ncols
employees,-1,/tmp/shared.csv,5       # "any segment" row
orders,0,/tmp/orders_seg0.csv,7      # specific segments
orders,1,/tmp/orders_seg1.csv,7
orders,2,/tmp/orders_seg2.csv,7
```

---

---

## 5. Metadata-driven per-segment files

The more realistic pattern than slicing one big CSV is
*distributed storage*: each segment already has its own file on disk,
and the QD's job is to tell every segment where **its** file lives.
This extension implements the whole pipeline for that — from a
plain metadata CSV on the coordinator all the way through to
per-segment file opens on the QEs.

### 5.1. The metadata file

A CSV on the coordinator pointed to by
`custom_scan_test.metadata_file`. Each non-empty, non-comment line:

```
relname,seg_id,csv_path,ncols
```

- `seg_id = -1` — the path applies to any segment.
- `seg_id = 0..N-1` — the path is pinned to that specific primary.
- All rows for the same `relname` must agree on `ncols`.

### 5.2. The pipeline

```
  ┌────────────────────────────┐
  │ QD (coordinator)           │
  │  set_rel_pathlist_hook     │
  │  └─ read_metadata_file()   │  fopen() + CSV parse of
  │        ↓                   │     custom_scan_test.metadata_file
  │  List [(seg_id, path)]     │
  │        ↓                   │
  │  custom_private =          │  <── nodeToString, dispatched to all QEs
  │   [mode, ncols, entries]   │
  └────────────────────────────┘
               │   dispatch
               ▼
  ┌────────────────────────────┐   ┌────────────────────────────┐
  │ Segment 0                  │   │ Segment 1                  │
  │  BeginCustomScan           │   │  BeginCustomScan           │
  │   ↓ pick entry where       │   │   ↓ pick entry where       │
  │     seg_id = 0             │   │     seg_id = 1             │
  │  AllocateFile(seg0.csv)    │   │  AllocateFile(seg1.csv)    │
  └────────────────────────────┘   └────────────────────────────┘
```

Segments never open the metadata file themselves — they act on what
the master dispatched.

### 5.3. Try it

Ready-to-run in [toy_demo.sql](toy_demo.sql). Summary:

```sql
CREATE EXTENSION custom_scan_test;
LOAD 'custom_scan_test';
SET custom_scan_test.enabled = on;

CREATE TABLE employees (
    id int, name text, department text, salary numeric, hired_on date);

-- Write the routing metadata file on the coordinator.
COPY (VALUES
    ('employees', 0, '/abs/path/seg0.csv', 5),
    ('employees', 1, '/abs/path/seg1.csv', 5),
    ('employees', 2, '/abs/path/seg2.csv', 5))
TO '/tmp/meta.csv' WITH (FORMAT csv);

SET custom_scan_test.metadata_file = '/tmp/meta.csv';

-- The actual read.  Each row comes from a different segment's CSV.
SELECT gp_segment_id, * FROM employees ORDER BY id;
```

`EXPLAIN VERBOSE` prints the full per-segment map the QD shipped —
look for `CSV Metadata Entries` and `CSV File` properties on the
plan node.

### 5.4. Where in the code the data flow lives

| Step                                                          | Where                                                  |
|---------------------------------------------------------------|--------------------------------------------------------|
| Parse the metadata file on the QD                             | [`read_metadata_file()`](custom_scan_test.c)           |
| Freeze the result into the CustomPath's `custom_private`      | [`test_set_rel_pathlist()`](custom_scan_test.c)        |
| Copy into the CustomScan plan                                 | [`TestPlanCustomPath()`](custom_scan_test.c)           |
| Pick the right row per segment using `GpIdentity.segindex`    | [`TestBeginCustomScan()`](custom_scan_test.c)          |

This is sections 2 ("passing data") and 3 ("using it on segments")
of [README-howto.md](README-howto.md), made concrete in about a
hundred lines of C.

---

## 6. Build, install, run

```sh
cd contrib/custom_scan_test
make
make install
```

Then, from psql:

```sql
LOAD 'custom_scan_test';
SET  custom_scan_test.enabled = on;
-- optionally: SET custom_scan_test.metadata_file = '/path/to/meta.csv';

EXPLAIN SELECT * FROM your_table;      -- expect "Custom Scan (TestCustomScan)"
```

---

## 7. Regression test

Automated test:

- [sql/custom_scan_test.sql](sql/custom_scan_test.sql) — input.
- [expected/custom_scan_test.out](expected/custom_scan_test.out) — golden output.