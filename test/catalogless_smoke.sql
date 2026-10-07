-- ============================================================
-- Smoke test for the catalogless temp table POC
-- (per-object WITH (catalogless) option).  Run in a single psql session:
--   psql -p <master_port> pocdb -e -f test/catalogless_smoke.sql
--
-- Segment-side catalog counters are read through gp_dist_random(),
-- which runs the query on every primary segment.
-- ============================================================

-- Setup: an ordinary heap table for the join test, created BEFORE the
-- baseline snapshot so it does not disturb the deltas.
DROP TABLE IF EXISTS poc_heap;
CREATE TABLE poc_heap(id int, hval int) DISTRIBUTED BY (id);
INSERT INTO poc_heap SELECT g, g*10 FROM generate_series(1,100) g;

-- Warm up the per-session temp namespace with a throwaway *regular* temp
-- table, so that pg_temp_N schema creation (a one-off session side
-- effect) does not pollute the catalog deltas of the experiment.
CREATE TEMP TABLE poc_warmup(x int);
DROP TABLE poc_warmup;

-- ============================================================
-- (1) BASELINE catalog counters, QD + all segments
-- ============================================================
SELECT 'QD baseline' AS phase,
       (SELECT count(*) FROM pg_class)     AS pg_class,
       (SELECT count(*) FROM pg_attribute) AS pg_attribute,
       (SELECT count(*) FROM pg_type)      AS pg_type,
       (SELECT count(*) FROM pg_depend)    AS pg_depend;

SELECT 'seg baseline pg_class' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_class') GROUP BY 2 ORDER BY 2;
SELECT 'seg baseline pg_attribute' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_attribute') GROUP BY 2 ORDER BY 2;
SELECT 'seg baseline pg_type' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_type') GROUP BY 2 ORDER BY 2;
SELECT 'seg baseline pg_depend' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_depend') GROUP BY 2 ORDER BY 2;

-- ============================================================
-- (2) The experiment: catalogless CTAS inside a transaction
--     (per-object WITH option; no session GUC required)
-- ============================================================
BEGIN;
CREATE TEMP TABLE poc_t WITH (catalogless=true) AS
  SELECT g AS id, g*2 AS val FROM generate_series(1,1000) g
  DISTRIBUTED BY (id);

-- ============================================================
-- (3) Reads inside the same transaction
-- ============================================================
-- expected: 1000 / 1001000
SELECT count(*) AS cnt, sum(val) AS sumval FROM poc_t;

SELECT * FROM poc_t WHERE id < 5 ORDER BY id;

-- join with a co-distributed heap table: expect TempResultScan and NO
-- Redistribute Motion on the poc_t side (hashed locus from GpPolicy)
EXPLAIN SELECT count(*) FROM poc_t JOIN poc_heap USING (id);
SELECT count(*) AS join_cnt, sum(val + hval) AS join_sum
  FROM poc_t JOIN poc_heap USING (id);

-- repeated read of the tuplestore within one query (self-join)
SELECT count(*) AS selfjoin_cnt
  FROM poc_t a JOIN poc_t b USING (id);

-- catalog is untouched while the table is alive:
SELECT count(*) AS qd_pgclass_poc_t
  FROM pg_class WHERE relname = 'poc_t';        -- expect 0
SELECT gp_segment_id, count(*) AS seg_pgclass_poc_t
  FROM gp_dist_random('pg_class') WHERE relname = 'poc_t'
 GROUP BY 1 ORDER BY 1;                         -- expect no rows

SELECT 'QD during txn' AS phase,
       (SELECT count(*) FROM pg_class)     AS pg_class,
       (SELECT count(*) FROM pg_attribute) AS pg_attribute,
       (SELECT count(*) FROM pg_type)      AS pg_type,
       (SELECT count(*) FROM pg_depend)    AS pg_depend;
SELECT 'seg during pg_class' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_class') GROUP BY 2 ORDER BY 2;
SELECT 'seg during pg_attribute' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_attribute') GROUP BY 2 ORDER BY 2;
SELECT 'seg during pg_type' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_type') GROUP BY 2 ORDER BY 2;
SELECT 'seg during pg_depend' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_depend') GROUP BY 2 ORDER BY 2;

-- tuplestore files exist on the segments while the txn is open:
\! find $DATADIRS -name "*CTMPRES*" 2>/dev/null | sed "s|.*/datadirs/||"

-- ============================================================
-- (4) End of transaction: table and files are gone
-- ============================================================
COMMIT;

-- files must be gone now:
\! find $DATADIRS -name "*CTMPRES*" 2>/dev/null | sed "s|.*/datadirs/||"; echo "(no CTMPRES files means cleanup worked)"

-- expect: ERROR relation "poc_t" does not exist
SELECT count(*) FROM poc_t;

SELECT 'QD after txn' AS phase,
       (SELECT count(*) FROM pg_class)     AS pg_class,
       (SELECT count(*) FROM pg_attribute) AS pg_attribute,
       (SELECT count(*) FROM pg_type)      AS pg_type,
       (SELECT count(*) FROM pg_depend)    AS pg_depend;

-- ============================================================
-- (5) CONTROL GROUP: same CTAS without the option, and with
--     catalogless=false (the DefElem must be consumed, not passed to
--     reloptions validation)
-- ============================================================
CREATE TEMP TABLE poc_control WITH (catalogless=false) AS
  SELECT g AS id, g*2 AS val FROM generate_series(1,1000) g
  DISTRIBUTED BY (id);

SELECT 'QD control (catalogless=false)' AS phase,
       (SELECT count(*) FROM pg_class)     AS pg_class,
       (SELECT count(*) FROM pg_attribute) AS pg_attribute,
       (SELECT count(*) FROM pg_type)      AS pg_type,
       (SELECT count(*) FROM pg_depend)    AS pg_depend;
SELECT 'seg control pg_class' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_class') GROUP BY 2 ORDER BY 2;
SELECT 'seg control pg_attribute' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_attribute') GROUP BY 2 ORDER BY 2;
SELECT 'seg control pg_type' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_type') GROUP BY 2 ORDER BY 2;
SELECT 'seg control pg_depend' AS what, gp_segment_id, count(*)
  FROM gp_dist_random('pg_depend') GROUP BY 2 ORDER BY 2;

SELECT count(*) AS control_pgclass_rows
  FROM pg_class WHERE relname = 'poc_control';  -- expect 1: ordinary temp table
DROP TABLE poc_control;

-- ============================================================
-- (6) NEGATIVE tests (each in its own txn; POC has no savepoints)
-- ============================================================

-- catalogless on a non-TEMP table: clean error
CREATE TABLE poc_neg0 WITH (catalogless=true) AS SELECT 1 AS id DISTRIBUTED BY (id);

-- kill-switch off: the option raises an error
SET gp_enable_catalogless_temp = off;
CREATE TEMP TABLE poc_neg00 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED BY (id);
RESET gp_enable_catalogless_temp;

BEGIN;
CREATE TEMP TABLE poc_neg1 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED BY (id);
-- expect a clean error, not a crash:
INSERT INTO poc_neg1 VALUES (2);
ROLLBACK;

BEGIN;
CREATE TEMP TABLE poc_neg2 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED BY (id);
-- expect a clean error, not a crash:
CREATE INDEX poc_neg2_idx ON poc_neg2(id);
ROLLBACK;

-- DROP TABLE on a live catalogless temp table
BEGIN;
CREATE TEMP TABLE poc_neg3 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED BY (id);
DROP TABLE poc_neg3;
-- expect: does not exist
SELECT * FROM poc_neg3;
ROLLBACK;

-- cleanup
DROP TABLE poc_heap;
