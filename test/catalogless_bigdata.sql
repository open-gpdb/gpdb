-- ============================================================
-- Big-data test for the catalogless temp table POC: page
-- eviction/reload far beyond the 512KB in-memory window (16 pages
-- x 32KB), the LOB path, and join performance vs a heap temp table.
--
-- Run in a single psql session (DATADIRS must point at the demo
-- cluster's datadirs directory):
--   DATADIRS=.../gpAux/gpdemo/datadirs \
--     psql -p 16432 pocdb -e -f test/catalogless_bigdata.sql
--
-- Volumes: big1 = 20M narrow rows (~280MB spill per segment),
-- big2 = 20k rows x 40KB incompressible-enough pads (~400MB LOB per
-- segment; NTS_MAX_ENTRY_SIZE is ~32700 bytes with BLCKSZ=32768, so a
-- 40KB tuple is guaranteed to take the LOB path -- the 2KB originally
-- suggested would stay inline).  ~2GB total, checked against free disk
-- beforehand (407GB available).
-- ============================================================

\timing on

\! df -h $DATADIRS | tail -1

-- co-distributed heap table for the join phase (created before the
-- experiment so it is out of the measurement)
DROP TABLE IF EXISTS poc_big_heap;
CREATE TABLE poc_big_heap(id int, hval bigint) DISTRIBUTED BY (id);
INSERT INTO poc_big_heap SELECT g, g*3 FROM generate_series(1,1000000) g;

-- ============================================================
-- (1) narrow tuples, 20M rows
-- ============================================================
BEGIN;

\! echo "--- QE RSS(KB) before write ---"; ps -axo rss,command | grep "[p]ostgres" | grep -E "con[0-9]+ seg[0-9]" | sort -rn | head -4

CREATE TEMP TABLE big1 WITH (catalogless=true) AS
  SELECT g AS id, (g*7)::bigint AS v FROM generate_series(1,20000000) g
  DISTRIBUTED BY (id);

\! echo "--- QE RSS(KB) after write ---"; ps -axo rss,command | grep "[p]ostgres" | grep -E "con[0-9]+ seg[0-9]" | sort -rn | head -4
\! echo "--- tuplestore files after write ---"; ls -lh $DATADIRS/dbfast1/demoDataDir0/base/pgsql_tmp/ $DATADIRS/dbfast2/demoDataDir1/base/pgsql_tmp/ 2>/dev/null

-- expect 20000000 | 1400000070000000  (7*n*(n+1)/2, n=20M)
SELECT count(*) AS cnt, sum(v) AS sumv FROM big1;

SELECT * FROM big1 WHERE id IN (1, 10000000, 20000000) ORDER BY id;

-- second full read: the pages were evicted long ago, comes from disk
SELECT count(*) AS cnt_again FROM big1;

\! echo "--- QE RSS(KB) after reads ---"; ps -axo rss,command | grep "[p]ostgres" | grep -E "con[0-9]+ seg[0-9]" | sort -rn | head -4

-- ============================================================
-- (2) wide tuples: LOB path (tuple > NTS_MAX_ENTRY_SIZE ~ 32700 B)
-- ============================================================
CREATE TEMP TABLE big2 WITH (catalogless=true) AS
  SELECT g AS id, repeat('x', 40000) AS pad FROM generate_series(1,20000) g
  DISTRIBUTED BY (id);

\! echo "--- tuplestore files incl LOB ---"; ls -lh $DATADIRS/dbfast1/demoDataDir0/base/pgsql_tmp/ $DATADIRS/dbfast2/demoDataDir1/base/pgsql_tmp/ 2>/dev/null

-- expect 20000 | 800000000
SELECT count(*) AS lob_cnt, sum(length(pad)) AS lob_bytes FROM big2;
SELECT id, length(pad) AS padlen FROM big2 WHERE id IN (1, 9999, 20000) ORDER BY id;

-- ============================================================
-- (3) join at volume: 20M-row temp result vs 1M-row heap
-- ============================================================
EXPLAIN ANALYZE SELECT count(*) FROM big1 JOIN poc_big_heap USING (id);
SELECT count(*) AS join_cnt FROM big1 JOIN poc_big_heap USING (id);

-- ============================================================
-- (4) COMMIT: files removed, space released
-- ============================================================
COMMIT;
\! echo "--- pgsql_tmp after COMMIT ---"; ls -lh $DATADIRS/dbfast1/demoDataDir0/base/pgsql_tmp/ $DATADIRS/dbfast2/demoDataDir1/base/pgsql_tmp/ 2>/dev/null; find $DATADIRS -name "*CTMPRES*" | wc -l
\! df -h $DATADIRS | tail -1

-- ============================================================
-- (5) control: same join with an ordinary (heap) temp table
-- ============================================================
BEGIN;
CREATE TEMP TABLE big1_heap AS
  SELECT g AS id, (g*7)::bigint AS v FROM generate_series(1,20000000) g
  DISTRIBUTED BY (id);
EXPLAIN ANALYZE SELECT count(*) FROM big1_heap JOIN poc_big_heap USING (id);
SELECT count(*) AS join_cnt_heap FROM big1_heap JOIN poc_big_heap USING (id);
ROLLBACK;

DROP TABLE poc_big_heap;
