-- -----------------------------------------------------------------------
-- Toy scenario: the master reads per-segment scan routing from a plain
-- metadata file on its filesystem, and tells every segment which CSV
-- to open via the plan's custom_private.  Segments never open the
-- metadata file themselves; they just execute what the QD dispatched.
--
-- The gpdemo cluster has 3 primary segments (seg_id 0..2).  We pin a
-- different data file to each one.
-- -----------------------------------------------------------------------

DROP EXTENSION IF EXISTS custom_scan_test CASCADE;
CREATE EXTENSION custom_scan_test;

LOAD 'custom_scan_test';
SET custom_scan_test.enabled        = on;
SET custom_scan_test.locus          = 'strewn';
SET custom_scan_test.csv_has_header = on;

-- 3 data files, one per primary segment.
COPY (VALUES
    (1, 'Alice', 'Engineering', 95000.00::numeric, '2021-03-14'::date),
    (2, 'Bob',   'Engineering', 88500.50::numeric, '2019-07-01'::date),
    (3, 'Carol', 'Sales',       72000.00::numeric, '2022-11-23'::date))
TO '/tmp/toy_seg0.csv' WITH (FORMAT csv, HEADER true);
COPY (VALUES
    (4, 'Dave',  'Marketing',   60000.00::numeric, '2023-01-15'::date),
    (5, 'Eve',   'Engineering', 120000.00::numeric,'2018-05-30'::date))
TO '/tmp/toy_seg1.csv' WITH (FORMAT csv, HEADER true);
COPY (VALUES
    (6, 'Frank', 'Support',     54000.00::numeric, '2024-02-02'::date),
    (7, 'Grace', 'Research',    110250.75::numeric,'2017-09-10'::date))
TO '/tmp/toy_seg2.csv' WITH (FORMAT csv, HEADER true);

-- The routing file — readable by the master at planning time.
-- Format: relname,seg_id,csv_path,ncols
COPY (VALUES
    ('employees', 0, '/tmp/toy_seg0.csv', 5),
    ('employees', 1, '/tmp/toy_seg1.csv', 5),
    ('employees', 2, '/tmp/toy_seg2.csv', 5))
TO '/tmp/toy_meta.csv' WITH (FORMAT csv);

SET custom_scan_test.metadata_file = '/tmp/toy_meta.csv';

CREATE TABLE IF NOT EXISTS employees (
    id        int,
    name      text,
    department text,
    salary    numeric,
    hired_on  date
) DISTRIBUTED BY (id);

\echo
\echo --- Per-segment routing as planned by the master -----------------------
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM employees;

\echo
\echo --- Result of SELECT * FROM employees ----------------------------------
SELECT gp_segment_id, * FROM employees ORDER BY id;

DROP TABLE employees;
DROP EXTENSION custom_scan_test CASCADE;
