-- ============================================================
-- Regression scenarios for the review fixes of the catalogless temp
-- table POC.  Run as a superuser in a single psql session:
--   psql -p <master_port> pocdb -e -f test/catalogless_fixes.sql
-- Every block states the expected outcome in a comment.
-- ============================================================

DROP TABLE IF EXISTS fx_heap, fx_shadow, fx_other;
DROP FUNCTION IF EXISTS fx_ctas();
DROP FUNCTION IF EXISTS fx_count();
DROP FUNCTION IF EXISTS fx_drop();
DROP FUNCTION IF EXISTS fx_loop(int);
DROP ROLE IF EXISTS fx_user;
CREATE TABLE fx_heap(id int, v int) DISTRIBUTED BY (id);
INSERT INTO fx_heap SELECT g, g FROM generate_series(1,100) g;

-- ------------------------------------------------------------
-- (1) Option parsing is not done by mutating a cached statement.
--     A plpgsql CTAS runs from a cached plan: every call must create a
--     catalogless table (no pg_class row), and the kill-switch must be
--     honored on later calls.
-- ------------------------------------------------------------
CREATE FUNCTION fx_ctas() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE fx_f WITH (catalogless) AS
    SELECT g AS id FROM generate_series(1,10) g DISTRIBUTED BY (id);
  RETURN (SELECT count(*) FROM fx_f);
END $$;

BEGIN;
SELECT fx_ctas() AS first_call;                                   -- 10
SELECT count(*) AS pg_class_rows FROM pg_class WHERE relname = 'fx_f'; -- 0
COMMIT;
BEGIN;
SELECT fx_ctas() AS second_call;                                  -- 10
SELECT count(*) AS pg_class_rows FROM pg_class WHERE relname = 'fx_f'; -- 0
COMMIT;
SET gp_enable_catalogless_temp = off;
BEGIN;
SELECT fx_ctas() AS third_call;       -- ERROR: catalogless temp tables are disabled
ROLLBACK;
RESET gp_enable_catalogless_temp;

-- the kill-switch is superuser-only
CREATE ROLE fx_user;
SET ROLE fx_user;
SET gp_enable_catalogless_temp = off; -- ERROR: permission denied
RESET ROLE;

-- ------------------------------------------------------------
-- (2) EXPLAIN of a catalogless CTAS shows its plan; EXPLAIN ANALYZE is
--     rejected instead of silently creating a regular table.
-- ------------------------------------------------------------
EXPLAIN CREATE TEMP TABLE fx_e WITH (catalogless) AS SELECT * FROM fx_heap;
EXPLAIN ANALYZE CREATE TEMP TABLE fx_e WITH (catalogless) AS SELECT * FROM fx_heap; -- ERROR
-- duplicated option
CREATE TEMP TABLE fx_e WITH (catalogless, catalogless=false) AS SELECT 1; -- ERROR

-- ------------------------------------------------------------
-- (3) Name resolution follows pg_temp's place in search_path.
-- ------------------------------------------------------------
CREATE TABLE fx_shadow(src text) DISTRIBUTED RANDOMLY;
INSERT INTO fx_shadow VALUES ('catalog');
BEGIN;
CREATE TEMP TABLE fx_shadow WITH (catalogless) AS SELECT 'catalogless'::text AS src;
SELECT src AS default_path FROM fx_shadow;          -- catalogless (pg_temp implicit, first)
SET LOCAL search_path = public, pg_temp;
SELECT src AS pg_temp_last FROM fx_shadow;          -- catalog
SELECT src AS pg_temp_qualified FROM pg_temp.fx_shadow; -- catalogless
SET LOCAL search_path = pg_temp, public;
SELECT src AS pg_temp_first FROM fx_shadow;         -- catalogless
COMMIT;

-- ------------------------------------------------------------
-- (4) DML/DDL on the name never falls through to the shadowed table.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_shadow WITH (catalogless) AS SELECT 'catalogless'::text AS src;
INSERT INTO fx_shadow VALUES ('oops');  -- ERROR: is a catalogless temp table
ROLLBACK;
BEGIN;
CREATE TEMP TABLE fx_shadow WITH (catalogless) AS SELECT 'catalogless'::text AS src;
TRUNCATE fx_shadow;                     -- ERROR: is a catalogless temp table
ROLLBACK;
SELECT src AS catalog_table_intact FROM fx_shadow;  -- catalog

-- ------------------------------------------------------------
-- (5) Views and rules cannot capture a catalogless temp table.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_v WITH (catalogless) AS SELECT 1 AS a;
CREATE VIEW fx_view AS SELECT * FROM fx_v;  -- ERROR
ROLLBACK;

-- ------------------------------------------------------------
-- (6) Catalogless and regular temp tables share the pg_temp name space.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_n WITH (catalogless) AS SELECT 1 AS a;
CREATE TEMP TABLE fx_n (a int);         -- ERROR: already exists
ROLLBACK;
BEGIN;
CREATE TEMP TABLE fx_n (a int);
CREATE TEMP TABLE fx_n WITH (catalogless) AS SELECT 1 AS a; -- ERROR: already exists
ROLLBACK;

-- ------------------------------------------------------------
-- (7) DROP reaches the QE writers: drop and recreate the same name with
--     a different row type in one transaction.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_r WITH (catalogless) AS
  SELECT g AS id FROM generate_series(1,50) g DISTRIBUTED BY (id);
DROP TABLE fx_r;
CREATE TEMP TABLE fx_r WITH (catalogless) AS
  SELECT g AS id, repeat('x', g) AS s FROM generate_series(1,20) g DISTRIBUTED BY (id);
SELECT count(*) AS cnt, sum(length(s)) AS len FROM fx_r;  -- 20 / 210
COMMIT;

-- a DROP of a mixed list removes both kinds
CREATE TABLE fx_other(a int) DISTRIBUTED BY (a);
BEGIN;
CREATE TEMP TABLE fx_m WITH (catalogless) AS SELECT 1 AS a;
DROP TABLE fx_m, fx_other;
SELECT count(*) AS fx_other_gone FROM pg_class WHERE relname = 'fx_other'; -- 0
COMMIT;

-- ------------------------------------------------------------
-- (8) Subtransactions: DROP inside a savepoint is refused, so ROLLBACK
--     TO SAVEPOINT can never "lose" a table.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_s WITH (catalogless) AS SELECT 1 AS a;
SAVEPOINT sp;
DROP TABLE fx_s;                        -- ERROR: inside a subtransaction
ROLLBACK TO SAVEPOINT sp;
SELECT count(*) AS still_there FROM fx_s;  -- 1
COMMIT;

-- ------------------------------------------------------------
-- (9) Cached plans are invalidated when a name is rebound: a plpgsql
--     SELECT sees each transaction's own table.
-- ------------------------------------------------------------
CREATE FUNCTION fx_count() RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  RETURN (SELECT count(*) FROM fx_c);
END $$;
BEGIN;
CREATE TEMP TABLE fx_c WITH (catalogless) AS SELECT g AS id FROM generate_series(1,5) g;
SELECT fx_count() AS xact1;             -- 5
COMMIT;
BEGIN;
CREATE TEMP TABLE fx_c WITH (catalogless) AS SELECT g AS id FROM generate_series(1,7) g;
SELECT fx_count() AS xact2;             -- 7
COMMIT;

-- plpgsql DROP from a cached plan works on every call
CREATE FUNCTION fx_drop() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  DROP TABLE fx_d;
END $$;
BEGIN;
CREATE TEMP TABLE fx_d WITH (catalogless) AS SELECT 1 AS a;
SELECT fx_drop();
CREATE TEMP TABLE fx_d WITH (catalogless) AS SELECT 1 AS a;
SELECT fx_drop();
SELECT * FROM fx_d;                     -- ERROR: relation "fx_d" does not exist
ROLLBACK;

-- ------------------------------------------------------------
-- (10) Many scans in one transaction do not accumulate reader stores
--      (each scan closes its reader in ExecEnd).
-- ------------------------------------------------------------
CREATE FUNCTION fx_loop(n int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE s bigint := 0;
BEGIN
  FOR i IN 1..n LOOP
    s := s + (SELECT count(*) FROM fx_l WHERE id = i % 100);
  END LOOP;
  RETURN s;
END $$;
BEGIN;
CREATE TEMP TABLE fx_l WITH (catalogless) AS
  SELECT g AS id FROM generate_series(0,99) g DISTRIBUTED BY (id);
SELECT fx_loop(3000) AS loop_sum;       -- 3000
COMMIT;

-- ------------------------------------------------------------
-- (11) Other distribution policies and skewed data.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_rep WITH (catalogless) AS
  SELECT g AS id FROM generate_series(1,10) g DISTRIBUTED REPLICATED;
SELECT count(*) AS rep_cnt FROM fx_rep;                         -- 10
-- join with an entry-locus relation (catalog): the replicated side must be
-- read on a segment and moved, never scanned on the QD (no file there)
SELECT count(*) AS rep_join FROM fx_rep r JOIN pg_class c ON c.relnatts = r.id;
SELECT count(*) AS rep_join_expected FROM pg_class WHERE relnatts BETWEEN 1 AND 10;
EXPLAIN SELECT count(*) FROM fx_rep r JOIN pg_class c ON c.relnatts = r.id;
CREATE TEMP TABLE fx_rnd WITH (catalogless) AS
  SELECT g AS id FROM generate_series(1,1000) g DISTRIBUTED RANDOMLY;
SELECT count(*) AS rnd_cnt, sum(id) AS rnd_sum FROM fx_rnd;     -- 1000 / 500500
-- all rows on one segment: the other segment reads an empty store
CREATE TEMP TABLE fx_one WITH (catalogless) AS
  SELECT * FROM fx_heap WHERE id = 1 DISTRIBUTED BY (id);
SELECT count(*) AS one_cnt FROM fx_one;                          -- 1
SELECT count(*) AS one_join FROM fx_one JOIN fx_heap USING (id); -- 1
-- a query filtered on the key must not be dispatched to one segment only
SELECT count(*) AS dd FROM fx_rnd r JOIN fx_heap h ON h.id = r.id WHERE h.id = 7; -- 1
COMMIT;

-- ------------------------------------------------------------
-- (12) Two-phase commit together with a real write: files are cleaned.
-- ------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE fx_2pc WITH (catalogless) AS
  SELECT g AS id FROM generate_series(1,100) g DISTRIBUTED BY (id);
INSERT INTO fx_heap SELECT id + 1000, 0 FROM fx_2pc;
COMMIT;
SELECT count(*) AS heap_after_2pc FROM fx_heap;                   -- 200

DROP TABLE fx_heap, fx_shadow;
DROP FUNCTION fx_ctas();
DROP FUNCTION fx_count();
DROP FUNCTION fx_drop();
DROP FUNCTION fx_loop(int);
DROP ROLE fx_user;
