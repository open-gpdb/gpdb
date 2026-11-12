\set VERBOSITY terse

-- start_ignore
SELECT pg_file_unlink('pg_log/view.log');
-- end_ignore

LOAD '$libdir/view_log.so';

CREATE TABLE base_table(id int, val text) DISTRIBUTED RANDOMLY;
INSERT INTO base_table VALUES (1, 'a'), (2, 'b');

CREATE VIEW my_view AS SELECT * FROM base_table;

CREATE OR REPLACE FUNCTION show_view_log_simple(before_query text)
RETURNS TABLE(viewname text)
AS $$
BEGIN
    RETURN QUERY
    SELECT split_part(line, E'\t', 4)
    FROM (
        SELECT unnest(string_to_array(pg_read_file('pg_log/view.log'), E'\n')) AS line
    ) AS lines
    WHERE line LIKE E'%\t%'
      AND split_part(line, E'\t', 1)::timestamptz >= before_query::timestamptz;
EXCEPTION
    WHEN undefined_file THEN
        RETURN;
END;
$$ LANGUAGE plpgsql;

-- Test 1: Direct SELECT from a view
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM my_view;
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 2: View in a subquery in WHERE clause
CREATE VIEW filtered_view AS SELECT * FROM base_table WHERE id > 1;
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM base_table WHERE id IN (SELECT id FROM filtered_view);
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 3: View in a subquery in SELECT target list
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT (SELECT count(*) FROM my_view) AS cnt;
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 4: View in a subquery in HAVING clause
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT max(id) FROM base_table HAVING max(id) > (SELECT avg(id) FROM my_view);
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 5: View inside an operator in WHERE
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM base_table WHERE id = (SELECT max(id) FROM my_view);
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 6: View in a subquery in FROM
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM (SELECT * FROM my_view) AS sub;
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 7: Query with a function in FROM
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM base_table, generate_series(1,1) AS g;
SELECT count(*) = 0 AS no_views_logged FROM show_view_log_simple(:'before_query');

-- Test 8: Query with no views (plain table)
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM base_table;
SELECT count(*) = 0 AS no_views_logged FROM show_view_log_simple(:'before_query');

-- Test 9: Materialized view is NOT logged
CREATE MATERIALIZED VIEW test_matview AS SELECT * FROM base_table DISTRIBUTED BY (id);
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM test_matview;
SELECT count(*) = 0 AS matview_not_logged FROM show_view_log_simple(:'before_query');
DROP MATERIALIZED VIEW test_matview;

-- Test 10: CTE with view (covers cteList)
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
WITH c AS (SELECT * FROM my_view) SELECT * FROM c;
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 11: LIMIT with subquery (covers limitCount)
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT 1 LIMIT (SELECT max(id) FROM my_view);
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 12: SubLink testexpr with view (covers testexpr)
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT (SELECT max(id) FROM my_view) IN (SELECT 1);
SELECT * FROM show_view_log_simple(:'before_query');

-- Test 13: View name with comma (covers TSV separator)
CREATE VIEW "comma,view" AS SELECT * FROM base_table;
SELECT pg_file_unlink('pg_log/view.log');
SELECT now() AS before_query \gset
SELECT * FROM "comma,view";
SELECT * FROM show_view_log_simple(:'before_query');
DROP VIEW "comma,view";

-- Cleanup
DROP VIEW my_view;
DROP VIEW filtered_view;
DROP TABLE base_table;
DROP FUNCTION show_view_log_simple(text);

SELECT pg_file_unlink('pg_log/view.log');
