-- ============================================================
-- Plan-correctness test for catalogless temp tables (POC).
--
-- For several query shapes over WITH (catalogless) temp tables we show
-- the EXPLAIN plan and compare count/checksum results bit-for-bit with
-- the same query over ordinary temp tables holding identical data.
-- For every created table (catalogless and its ordinary twin) we also
-- show the per-node catalog delta (pg_class/pg_attribute/pg_type/
-- pg_depend on the QD and both segments): catalogless must be 0
-- everywhere.
--
-- Catalogless queries are planned by the Postgres planner (forced ORCA
-- fallback); control queries may be planned by ORCA — the point of
-- comparison is Motion structure and identical results.
--
-- Run in a single psql session:
--   psql -p <master_port> pocdb -e -f test/catalogless_plans.sql
-- ============================================================

-- heap probe tables for joins, created before all snapshots
DROP TABLE IF EXISTS plans_probe;
CREATE TABLE plans_probe(id int, pv bigint) DISTRIBUTED BY (id);
INSERT INTO plans_probe SELECT g, g*5 FROM generate_series(1,20000) g;

-- warm up the session temp namespace (one-off catalog side effect)
CREATE TEMP TABLE plans_warmup(x int);
DROP TABLE plans_warmup;

-- ------------------------------------------------------------
-- catalog counter snapshot helper (12 values), repeated via \gset
-- ------------------------------------------------------------
SELECT (SELECT count(*) FROM pg_class)     AS qc,
       (SELECT count(*) FROM pg_attribute) AS qa,
       (SELECT count(*) FROM pg_type)      AS qt,
       (SELECT count(*) FROM pg_depend)    AS qd,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=0) AS c0,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=0) AS a0,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=0) AS t0,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=0) AS d0,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=1) AS c1,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=1) AS a1,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=1) AS t1,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=1) AS d1
\gset s0_

BEGIN;

-- ============================================================
-- table 1: ct_a, catalogless, DISTRIBUTED BY (id)
-- ============================================================
CREATE TEMP TABLE ct_a WITH (catalogless) AS
  SELECT g AS id, g % 97 AS grp, (g*11)::bigint AS v
  FROM generate_series(1,100000) g
  DISTRIBUTED BY (id);

SELECT (SELECT count(*) FROM pg_class)     AS qc,
       (SELECT count(*) FROM pg_attribute) AS qa,
       (SELECT count(*) FROM pg_type)      AS qt,
       (SELECT count(*) FROM pg_depend)    AS qd,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=0) AS c0,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=0) AS a0,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=0) AS t0,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=0) AS d0,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=1) AS c1,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=1) AS a1,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=1) AS t1,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=1) AS d1
\gset s1_

-- ============================================================
-- table 2: ct_b, catalogless, DISTRIBUTED BY (fk) (different key)
-- ============================================================
CREATE TEMP TABLE ct_b WITH (catalogless) AS
  SELECT g AS id, g % 1000 AS fk, (g*13)::bigint AS w
  FROM generate_series(1,50000) g
  DISTRIBUTED BY (fk);

SELECT (SELECT count(*) FROM pg_class)     AS qc,
       (SELECT count(*) FROM pg_attribute) AS qa,
       (SELECT count(*) FROM pg_type)      AS qt,
       (SELECT count(*) FROM pg_depend)    AS qd,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=0) AS c0,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=0) AS a0,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=0) AS t0,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=0) AS d0,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=1) AS c1,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=1) AS a1,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=1) AS t1,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=1) AS d1
\gset s2_

-- ============================================================
-- control twins: ordinary temp tables with identical data
-- ============================================================
CREATE TEMP TABLE ht_a AS
  SELECT g AS id, g % 97 AS grp, (g*11)::bigint AS v
  FROM generate_series(1,100000) g
  DISTRIBUTED BY (id);

SELECT (SELECT count(*) FROM pg_class)     AS qc,
       (SELECT count(*) FROM pg_attribute) AS qa,
       (SELECT count(*) FROM pg_type)      AS qt,
       (SELECT count(*) FROM pg_depend)    AS qd,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=0) AS c0,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=0) AS a0,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=0) AS t0,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=0) AS d0,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=1) AS c1,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=1) AS a1,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=1) AS t1,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=1) AS d1
\gset s3_

CREATE TEMP TABLE ht_b AS
  SELECT g AS id, g % 1000 AS fk, (g*13)::bigint AS w
  FROM generate_series(1,50000) g
  DISTRIBUTED BY (fk);

SELECT (SELECT count(*) FROM pg_class)     AS qc,
       (SELECT count(*) FROM pg_attribute) AS qa,
       (SELECT count(*) FROM pg_type)      AS qt,
       (SELECT count(*) FROM pg_depend)    AS qd,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=0) AS c0,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=0) AS a0,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=0) AS t0,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=0) AS d0,
       (SELECT count(*) FROM gp_dist_random('pg_class')     WHERE gp_segment_id=1) AS c1,
       (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE gp_segment_id=1) AS a1,
       (SELECT count(*) FROM gp_dist_random('pg_type')      WHERE gp_segment_id=1) AS t1,
       (SELECT count(*) FROM gp_dist_random('pg_depend')    WHERE gp_segment_id=1) AS d1
\gset s4_

-- ============================================================
-- CATALOG DELTA SUMMARY: rows added per node by each CREATE
-- (catalogless tables must be 0 everywhere)
-- ============================================================
SELECT 'ct_a  catalogless (id)'  AS created_table,
       :s1_qc - :s0_qc AS qd_class, :s1_qa - :s0_qa AS qd_attr,
       :s1_qt - :s0_qt AS qd_type,  :s1_qd - :s0_qd AS qd_depend,
       :s1_c0 - :s0_c0 AS s0_class, :s1_a0 - :s0_a0 AS s0_attr,
       :s1_t0 - :s0_t0 AS s0_type,  :s1_d0 - :s0_d0 AS s0_depend,
       :s1_c1 - :s0_c1 AS s1_class, :s1_a1 - :s0_a1 AS s1_attr,
       :s1_t1 - :s0_t1 AS s1_type,  :s1_d1 - :s0_d1 AS s1_depend
UNION ALL
SELECT 'ct_b  catalogless (fk)',
       :s2_qc - :s1_qc, :s2_qa - :s1_qa, :s2_qt - :s1_qt, :s2_qd - :s1_qd,
       :s2_c0 - :s1_c0, :s2_a0 - :s1_a0, :s2_t0 - :s1_t0, :s2_d0 - :s1_d0,
       :s2_c1 - :s1_c1, :s2_a1 - :s1_a1, :s2_t1 - :s1_t1, :s2_d1 - :s1_d1
UNION ALL
SELECT 'ht_a  ordinary temp (id)',
       :s3_qc - :s2_qc, :s3_qa - :s2_qa, :s3_qt - :s2_qt, :s3_qd - :s2_qd,
       :s3_c0 - :s2_c0, :s3_a0 - :s2_a0, :s3_t0 - :s2_t0, :s3_d0 - :s2_d0,
       :s3_c1 - :s2_c1, :s3_a1 - :s2_a1, :s3_t1 - :s2_t1, :s3_d1 - :s2_d1
UNION ALL
SELECT 'ht_b  ordinary temp (fk)',
       :s4_qc - :s3_qc, :s4_qa - :s3_qa, :s4_qt - :s3_qt, :s4_qd - :s3_qd,
       :s4_c0 - :s3_c0, :s4_a0 - :s3_a0, :s4_t0 - :s3_t0, :s4_d0 - :s3_d0,
       :s4_c1 - :s3_c1, :s4_a1 - :s3_a1, :s4_t1 - :s3_t1, :s4_d1 - :s3_d1
ORDER BY 1;

-- ============================================================
-- (1) join ON the distribution key: expect Temp Result Scan with NO
--     Redistribute Motion on the temp side
-- ============================================================
EXPLAIN SELECT count(*), sum(v + pv) FROM ct_a JOIN plans_probe USING (id);
SELECT count(*) AS q1_cnt, sum(v + pv) AS q1_sum FROM ct_a JOIN plans_probe USING (id);
SELECT count(*) AS q1_cnt_ctl, sum(v + pv) AS q1_sum_ctl FROM ht_a JOIN plans_probe USING (id);

-- ============================================================
-- (2) join on a NON-distribution key: the planner MUST add a Motion
--     (Redistribute or Broadcast); sides are not co-located
-- ============================================================
EXPLAIN SELECT count(*), sum(v + pv) FROM ct_a JOIN plans_probe p ON ct_a.grp = p.id;
SELECT count(*) AS q2_cnt, sum(v + pv) AS q2_sum FROM ct_a JOIN plans_probe p ON ct_a.grp = p.id;
SELECT count(*) AS q2_cnt_ctl, sum(v + pv) AS q2_sum_ctl FROM ht_a JOIN plans_probe p ON ht_a.grp = p.id;

-- ============================================================
-- (3) GROUP BY a non-distribution column: two-phase aggregation with a
--     Redistribute Motion above the Temp Result Scan
-- ============================================================
EXPLAIN SELECT grp, count(*), sum(v) FROM ct_a GROUP BY grp;
SELECT count(*) AS q3_groups, sum(cnt) AS q3_rows, sum(s) AS q3_sum
  FROM (SELECT grp, count(*) AS cnt, sum(v) AS s FROM ct_a GROUP BY grp) x;
SELECT count(*) AS q3_groups_ctl, sum(cnt) AS q3_rows_ctl, sum(s) AS q3_sum_ctl
  FROM (SELECT grp, count(*) AS cnt, sum(v) AS s FROM ht_a GROUP BY grp) x;

-- ============================================================
-- (4) join of two catalogless tables with different distribution keys
--     ON ct_a.id = ct_b.id: ct_a (hashed by id) stays put, ct_b
--     (hashed by fk) must be moved
-- ============================================================
EXPLAIN SELECT count(*), sum(v + w) FROM ct_a a JOIN ct_b b ON a.id = b.id;
SELECT count(*) AS q4_cnt, sum(v + w) AS q4_sum FROM ct_a a JOIN ct_b b ON a.id = b.id;
SELECT count(*) AS q4_cnt_ctl, sum(v + w) AS q4_sum_ctl FROM ht_a a JOIN ht_b b ON a.id = b.id;

-- ============================================================
-- (5) semi-join: EXISTS with a temp result inside
-- ============================================================
EXPLAIN SELECT count(*) FROM plans_probe p WHERE EXISTS (SELECT 1 FROM ct_b b WHERE b.id = p.id);
SELECT count(*) AS q5_cnt FROM plans_probe p WHERE EXISTS (SELECT 1 FROM ct_b b WHERE b.id = p.id);
SELECT count(*) AS q5_cnt_ctl FROM plans_probe p WHERE EXISTS (SELECT 1 FROM ht_b b WHERE b.id = p.id);

-- ============================================================
-- (6) ORDER BY + LIMIT over the temp result: Limit + Gather Motion
--     with the sort pushed to the segments
-- ============================================================
EXPLAIN SELECT id, v FROM ct_a ORDER BY v DESC LIMIT 10;
SELECT id, v FROM ct_a ORDER BY v DESC LIMIT 10;
SELECT id, v FROM ht_a ORDER BY v DESC LIMIT 10;

COMMIT;

DROP TABLE ht_a;
DROP TABLE ht_b;
DROP TABLE plans_probe;
