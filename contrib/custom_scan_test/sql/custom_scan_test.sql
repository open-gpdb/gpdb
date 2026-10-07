-- ----------------------------------------------------------------------
-- Regression test for contrib/custom_scan_test.
--
-- Exercises the full pipeline:
--   * The QD reads a plain CSV metadata file from its filesystem.
--   * The planner hook parses it, resolves every matching row for the
--     relation being scanned, and freezes the per-segment map into
--     the CustomScan plan.
--   * The plan is dispatched; every segment picks its own CSV file
--     via GpIdentity.segindex and reads rows from it.
--
-- The test assumes the gpdemo layout: 3 primary segments on the same
-- host, which lets us write both the metadata file and per-segment
-- data files into /tmp and have the QD/QEs read them back.
-- ----------------------------------------------------------------------

\set VERBOSITY terse
SET client_min_messages = WARNING;

-- Force the PG planner so the set_rel_pathlist_hook fires (ORCA has its
-- own planning path and ignores this hook).
SET optimizer = off;

LOAD 'custom_scan_test';
SET custom_scan_test.enabled = on;

-- ---------------------------------------------------------------
-- Materialize three per-segment CSV files on the coordinator host.
-- COPY TO '/tmp/...' runs on the QD; gpdemo co-locates all segments
-- on the same host, so each QE can read the right file.
-- ---------------------------------------------------------------
COPY (VALUES
    (1, 'Alice',       'Engineering', 95000.00::numeric, '2021-03-14'::date),
    (2, 'Bob',         'Engineering', 88500.50::numeric, '2019-07-01'::date),
    (3, 'Carol, Jr.',  'Sales',       72000.00::numeric, '2022-11-23'::date))
TO '/tmp/custom_scan_test_seg0.csv' WITH (FORMAT csv, HEADER false);

COPY (VALUES
    (4, 'Dave',        'Marketing',   NULL::numeric,      '2023-01-15'::date),
    (5, 'Eve A',       'Engineering', 120000.00::numeric, '2018-05-30'::date),
    (6, 'Frank',       'Support',     54000.00::numeric,  '2024-02-02'::date),
    (7, 'Grace',       'Research',    110250.75::numeric, '2017-09-10'::date))
TO '/tmp/custom_scan_test_seg1.csv' WITH (FORMAT csv, HEADER false);

COPY (VALUES
    (8, 'Heidi',       'Sales',       67500.00::numeric, '2020-12-05'::date),
    (9, 'Ivan',        'Engineering', 99800.00::numeric, '2016-04-18'::date),
   (10, 'Judy',        'Marketing',   81000.00::numeric, '2022-08-08'::date))
TO '/tmp/custom_scan_test_seg2.csv' WITH (FORMAT csv, HEADER false);

-- ---------------------------------------------------------------
-- Write the routing metadata file on the QD.  Layout is
--     relname,seg_id,csv_path,ncols
-- One row per primary segment.
-- ---------------------------------------------------------------
COPY (VALUES
    ('employees', 0, '/tmp/custom_scan_test_seg0.csv', 5),
    ('employees', 1, '/tmp/custom_scan_test_seg1.csv', 5),
    ('employees', 2, '/tmp/custom_scan_test_seg2.csv', 5))
TO '/tmp/custom_scan_test.meta' WITH (FORMAT csv);

SET custom_scan_test.metadata_file = '/tmp/custom_scan_test.meta';

-- Target relation: schema must match the CSVs (5 columns).
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id         int,
    name       text,
    department text,
    salary     numeric,
    hired_on   date
) DISTRIBUTED BY (id);

-- Show the routing file the master will read.  pg_read_file() only
-- accepts paths under the data directory, so we rely on the COPY above
-- having produced a CSV with 3 lines of the form
--     "employees",N,"/tmp/custom_scan_test_segN.csv",5
-- one per primary segment.

-- EXPLAIN: the plan must use the Custom Scan node.
EXPLAIN (COSTS OFF) SELECT * FROM employees;

-- EXPLAIN VERBOSE: the extension's own properties — Locus Mode,
-- CSV Metadata Entries, CSV Expected Columns, Source — are printed
-- by ExplainCustomScan.  CSV Metadata Entries shows how many routing
-- rows the QD shipped to segments.
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM employees;

-- The actual scan: each row comes from a different segment's CSV.
-- The QD built the routing on its side, dispatched the plan, and each
-- segment opened exactly the file it was told to open.  The 10 rows
-- are the union of 3+4+3.
SELECT id, name, department, salary, hired_on
  FROM employees
 ORDER BY id;

-- Qual pushdown: the WHERE clause is carried in cscan->scan.plan.qual
-- and checked by ExecScan's generic filter loop.
EXPLAIN (COSTS OFF) SELECT id, name FROM employees WHERE department = 'Engineering';
SELECT id, name FROM employees WHERE department = 'Engineering' ORDER BY id;

-- Compound qual: AND + IN both end up on the CustomScan's Filter line,
-- evaluated per-row by ExecScan.
EXPLAIN (COSTS OFF)
SELECT id, name, salary
  FROM employees
 WHERE salary > 70000
   AND department IN ('Engineering', 'Sales');
SELECT id, name, salary
  FROM employees
 WHERE salary > 70000
   AND department IN ('Engineering', 'Sales')
 ORDER BY id;

-- Projection pushdown: the targetlist shrinks the output; salary and
-- hired_on are still read from CSV but not shipped upstream.
EXPLAIN (COSTS OFF) SELECT name FROM employees WHERE id = 5;
SELECT name FROM employees WHERE id = 5;

-- --------------------------------------------------------------------
-- Motion demo: join employees to a second relation distributed on a
-- different key.  Motion is computed entirely on the coordinator's
-- planner; segments just execute whatever motion slices the master
-- built for them.  Look for "Broadcast Motion" or "Redistribute
-- Motion" above the inner Custom Scan.
-- --------------------------------------------------------------------
DROP TABLE IF EXISTS departments;
CREATE TABLE departments (
    dept_name text,
    budget    numeric
) DISTRIBUTED BY (dept_name);

INSERT INTO departments VALUES
    ('Engineering', 5000000),
    ('Sales',       1200000),
    ('Marketing',    900000),
    ('Support',      600000),
    ('Research',    3000000);

EXPLAIN (COSTS OFF)
SELECT e.name, e.department, d.budget
  FROM employees e
  JOIN departments d ON e.department = d.dept_name
 WHERE e.salary > 80000;

SELECT e.name, e.department, d.budget
  FROM employees e
  JOIN departments d ON e.department = d.dept_name
 WHERE e.salary > 80000
 ORDER BY e.id;

DROP TABLE departments;

-- ---------------------------------------------------------------
-- Column-count mismatch: rewrite the metadata file claiming 7
-- columns; the segments must refuse to scan.
-- ---------------------------------------------------------------
COPY (VALUES
    ('employees', 0, '/tmp/custom_scan_test_seg0.csv', 7),
    ('employees', 1, '/tmp/custom_scan_test_seg1.csv', 7),
    ('employees', 2, '/tmp/custom_scan_test_seg2.csv', 7))
TO '/tmp/custom_scan_test.meta' WITH (FORMAT csv);

SELECT * FROM employees ORDER BY id;    -- expect ERROR from BeginCustomScan

-- Cleanup.
DROP TABLE employees;
