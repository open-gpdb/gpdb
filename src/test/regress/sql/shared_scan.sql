--
-- Queries that lead to hanging (not dead lock) when we don't handle synchronization properly in shared scan
-- Queries that lead to wrong result when we don't finish executing the subtree below the shared scan being squelched.
--

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
-- end_ignore

CREATE SCHEMA shared_scan;

SET search_path = shared_scan;

CREATE TABLE foo (a int, b int);
CREATE TABLE bar (c int, d int);
CREATE TABLE jazz(e int, f int);

INSERT INTO foo values (1, 2);
INSERT INTO bar SELECT i, i from generate_series(1, 100)i;
INSERT INTO jazz VALUES (2, 2), (3, 3);

ANALYZE foo;
ANALYZE bar;
ANALYZE jazz;

SELECT $query$
SELECT * FROM
        (
        WITH cte AS (SELECT * FROM foo)
        SELECT * FROM (SELECT * FROM cte UNION ALL SELECT * FROM cte)
        AS X
        JOIN bar ON b = c
        ) AS XY
        JOIN jazz on c = e AND b = f;
$query$ AS qry \gset

-- We are very particular about this plan shape and data distribution with ORCA:
-- 1. `jazz` has to be the inner table of the outer HASH JOIN, so that on a
-- segment which has zero tuples in `jazz`, the Sequence node that contains the
-- Shared Scan will be squelched on that segment. If `jazz` is not on the inner
-- side, the above mentioned "hang" scenario will not be covered.
-- 2. The Shared Scan producer has to be on a different slice from consumers,
-- and some tuples coming out of the Share Scan producer on one segments are
-- redistributed to a different segment over Motion. If not, the above mentioned
-- "wrong result" scenario will not be covered.
EXPLAIN (COSTS OFF)
:qry ;

SET statement_timeout = '15s';

:qry ;

RESET statement_timeout;

SELECT COUNT(*)
FROM (SELECT *,
        (
        WITH cte AS (SELECT * FROM jazz WHERE jazz.e = bar.c)
        SELECT 1 FROM cte c1, cte c2
        )
      FROM bar) as s;

CREATE TABLE t1 (a int, b int);
CREATE TABLE t2 (a int);

-- ORCA plan contains a Shared Scan producer with a unsorted Motion below it
EXPLAIN (COSTS OFF)
WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 LIMIT 10) SELECT a, 1, 1 FROM cte JOIN t2 USING (a);
-- This functions returns one more column than expected.
CREATE OR REPLACE FUNCTION col_mismatch_func1() RETURNS TABLE (field1 int, field2 int)
LANGUAGE 'plpgsql' VOLATILE STRICT AS
$$
DECLARE
   v_qry text;
BEGIN
   v_qry := 'WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 LIMIT 10) SELECT a, 1 , 1 FROM cte JOIN t2 USING (a)';
  RETURN QUERY EXECUTE v_qry;
END
$$;

-- This should only ERROR and should not SIGSEGV
SELECT col_mismatch_func1();

-- ORCA plan contains a Shared Scan producer with a sorted Motion below it
EXPLAIN (COSTS OFF)
WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 ORDER BY b LIMIT 10) SELECT a, 1, 1 FROM cte JOIN t2 USING (a);
--- This functions returns one more column than expected.
CREATE OR REPLACE FUNCTION col_mismatch_func2() RETURNS TABLE (field1 int, field2 int)
    LANGUAGE 'plpgsql' VOLATILE STRICT AS
$$
DECLARE
    v_qry text;
BEGIN
    v_qry := 'WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 ORDER BY b LIMIT 10) SELECT a, 1 , 1 FROM cte JOIN t2 USING (a)';
    RETURN QUERY EXECUTE v_qry;
END
$$;

-- This should only ERROR and should not SIGSEGV
SELECT col_mismatch_func2();

-- https://github.com/greenplum-db/gpdb/issues/12701
-- Disable cte sharing in subquery
drop table if exists pk_list;
create table pk_list (id int, schema_name varchar, table_name varchar) distributed by (id);
drop table if exists calender;
create table calender (id int, data_hour timestamp) distributed by (id);

explain (costs off)
with
	tbls as (select distinct schema_name, table_name as table_nm from pk_list),
	tbls_daily_report_23 as (select unnest(string_to_array('mart_cm.card' ,',')) as table_nm_23),
	tbls_w_onl_actl_data as (select unnest(string_to_array('mart_cm.cont_resp,mart_cm.card', ',')) as table_nm_onl_act)
select  data_hour, stat.schema_name as schema_nm, dt.table_nm
from (
	select * from calender c
	cross join tbls
) dt
inner join (
	select tbls.schema_name, tbls.table_nm as table_name
	from tbls tbls
) stat on dt.table_nm = stat.table_name
where
	(data_hour = date_trunc('day',data_hour) and stat.schema_name || '.' ||stat.table_name not in (select table_nm_23 from tbls_daily_report_23))
	and (stat.schema_name || '.' ||stat.table_name not in (select table_nm_onl_act from tbls_w_onl_actl_data))
	or (stat.schema_name || '.' ||stat.table_name in (select table_nm_onl_act from tbls_w_onl_actl_data));

-- A CTE over a replicated table referenced from multiple scalar subqueries
-- used to hang: ORCA placed the SharedScan consumer on a different slice than
-- the producer and the cross-slice temp-file protocol cannot handle that
-- topology. ORCA now force-inlines a replicated-table CTE (the data is on
-- every segment, so a local copy per consumer is equivalent), producing a
-- correct native plan instead of a cross-slice shared scan.
-- ss_t1 needs enough rows (40000) to push ORCA to the cross-slice plan;
-- with fewer rows the bug does not manifest and the test would silently
-- pass even without the fix.
CREATE TABLE ss_t1 AS
  SELECT generate_series(1, 40000) id
  DISTRIBUTED BY (id);
CREATE TABLE ss_t2 AS
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS v(id, v)
  DISTRIBUTED REPLICATED;
ANALYZE ss_t1;
ANALYZE ss_t2;
-- Plan: the replicated CTE is materialized once into a local Shared Scan
-- co-located with its consumers, and the repeated reference reuses that copy,
-- so ss_t2 is scanned once per CTE -- no cross-slice SharedScan, no duplicates.
EXPLAIN (COSTS OFF) WITH
    cte1 AS (SELECT v FROM ss_t2 WHERE id = 1),
    cte2 AS (SELECT v FROM ss_t2 WHERE id = 2)
  SELECT (SELECT v FROM cte1) + (SELECT v FROM cte2) +
         (SELECT v FROM cte1) + (SELECT v FROM cte2) AS result
  FROM ss_t1
  LIMIT 1;
-- Run it under a timeout to prove it no longer hangs.
SET statement_timeout = '15s';
WITH
    cte1 AS (SELECT v FROM ss_t2 WHERE id = 1),
    cte2 AS (SELECT v FROM ss_t2 WHERE id = 2)
  SELECT (SELECT v FROM cte1) + (SELECT v FROM cte2) +
         (SELECT v FROM cte1) + (SELECT v FROM cte2) AS result
  FROM ss_t1
  LIMIT 1;
RESET statement_timeout;
DROP TABLE ss_t1, ss_t2;

-- ORCA should also fall back when the replicated CTE is referenced from
-- *correlated* scalar subqueries. These become correlated NL joins whose
-- inner side runs as a SubPlan in its own slice. Counting only Motion
-- nodes misses this, so the ShareInputScan writer hangs waiting for a
-- DONE ack from the reader slice. The walk must treat the correlated-join
-- inner side as a separate slice.
CREATE TABLE ss_c2 (id numeric, refrcode varchar(255), referenceid numeric)
  DISTRIBUTED REPLICATED;
CREATE TABLE ss_c1 (id bigint, iscalctrg varchar(15) NOT NULL,
                    iscalcdetail varchar(15))
  DISTRIBUTED REPLICATED;
INSERT INTO ss_c2 SELECT i, 'A'||(i%5), 101991 FROM generate_series(1, 50000) i;
INSERT INTO ss_c1
  SELECT i, 'A'||(i%5), 'A'||(i%7) FROM generate_series(1, 50000) i;
ANALYZE ss_c1;
ANALYZE ss_c2;

SET statement_timeout = '15s';
WITH cte AS (
    SELECT id, refrcode FROM ss_c2 WHERE referenceid = 101991 AND id <  25000
    UNION ALL
    SELECT id, refrcode FROM ss_c2 WHERE referenceid = 101991 AND id >= 25000
)
  SELECT (SELECT refrcode FROM cte WHERE refrcode = p.iscalctrg    LIMIT 1) = 'A1'
     AND (SELECT refrcode FROM cte WHERE refrcode = p.iscalcdetail LIMIT 1) = 'A1' AS ok
  FROM ss_c1 p WHERE p.id = 1;
RESET statement_timeout;
DROP TABLE ss_c1, ss_c2;

-- Test the scenario which already opened many fds
-- start_ignore
RESET search_path;
-- end_ignore
\! mkdir -p /tmp/_gpdb_fault_inject_tmp_dir/

select gp_inject_fault('inject_many_fds_for_shareinputscan', 'skip', dbid) from gp_segment_configuration where role = 'p' and content = 0;
-- borrow the test query in gp_aggregates
select case when ten < 5 then ten else ten * 2 end, count(distinct two), count(distinct four) from tenk1 group by 1;
select gp_inject_fault('inject_many_fds_for_shareinputscan', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;

\! rm -rf /tmp/_gpdb_fault_inject_tmp_dir/
