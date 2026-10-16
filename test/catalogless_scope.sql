-- Run on a disposable database with this branch installed:
-- psql -X -v ON_ERROR_STOP=1 -f test/catalogless_scope.sql <database>
-- Compare SQLSTATE, message and alias/scope hints with ordinary temp tables.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL optimizer = off;
SET LOCAL gp_enable_catalogless_temp = on;
SET LOCAL search_path = pg_temp, public;

CREATE TEMP TABLE scope_heap0 AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE scope_heap1 AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE scope_heap2 AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE scope_result0 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE scope_result1 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE scope_result2 WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED RANDOMLY;

DO $$
DECLARE
  query text;
  heap_state text;
  heap_message text;
  heap_hint text;
  result_state text;
  result_message text;
  result_hint text;
BEGIN
  FOREACH query IN ARRAY ARRAY[
    -- Forward JOIN reference, as in the SQLancer finding.
    'SELECT 1 FROM %1$s0 FULL JOIN %1$s1 ON %1$s2.id = %1$s1.id FULL JOIN %1$s2 ON true',
    -- The real name is hidden by an alias.
    'SELECT %1$s0.id FROM %1$s0 AS a',
    -- A comma-separated FROM item is outside the JOIN condition's scope.
    'SELECT 1 FROM %1$s0, %1$s1 JOIN %1$s2 ON %1$s0.id = %1$s2.id',
    -- Search parent parse states when producing the alias hint.
    'SELECT (SELECT %1$s0.id) FROM %1$s0 AS a',
    -- A CTE of the same name must take precedence over the registry.
    'WITH %1$s0 AS (SELECT 1 AS id) SELECT %1$s0.id FROM %1$s0 AS a',
    -- Explicit pg_temp qualification still finds the aliased RTE.
    'SELECT pg_temp.%1$s0.id FROM pg_temp.%1$s0 AS a'
  ] LOOP
    heap_state := NULL;
    result_state := NULL;
    BEGIN
      EXECUTE format(query, 'scope_heap');
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS heap_state = RETURNED_SQLSTATE,
        heap_message = MESSAGE_TEXT, heap_hint = PG_EXCEPTION_HINT;
    END;
    BEGIN
      EXECUTE format(query, 'scope_result');
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS result_state = RETURNED_SQLSTATE,
        result_message = MESSAGE_TEXT, result_hint = PG_EXCEPTION_HINT;
    END;
    IF heap_state IS DISTINCT FROM '42P01'
       OR result_state IS DISTINCT FROM heap_state
       OR replace(result_message, 'scope_result', 'scope_heap') IS DISTINCT FROM heap_message
       OR replace(result_hint, 'scope_result', 'scope_heap') IS DISTINCT FROM heap_hint THEN
      RAISE EXCEPTION 'scope diagnostic mismatch for %: heap=(%, %, %), catalogless=(%, %, %)',
        query, heap_state, heap_message, heap_hint,
        result_state, result_message, result_hint;
    END IF;
  END LOOP;
END $$;

-- The diagnostic-only lookup must not open forbidden catalog operations.
DO $$
DECLARE
  query text;
BEGIN
  FOREACH query IN ARRAY ARRAY[
    'INSERT INTO scope_result0 VALUES (2)',
    'TRUNCATE scope_result0',
    'CREATE INDEX scope_result_idx ON scope_result0(id)'
  ] LOOP
    BEGIN
      EXECUTE query;
      RAISE EXCEPTION 'catalog operation unexpectedly accepted: %', query;
    EXCEPTION WHEN feature_not_supported THEN NULL;
    END;
  END LOOP;
  IF (SELECT count(*) FROM scope_result0) <> 1 THEN
    RAISE EXCEPTION 'scope diagnostics changed the result';
  END IF;
END $$;
ROLLBACK;
