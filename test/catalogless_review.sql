-- Run on a disposable database, as superuser, with this branch installed:
-- psql -X -v ON_ERROR_STOP=1 -f test/catalogless_review.sql <database>
-- Database ACL and table changes are rolled back; the test role is dropped.
-- The role is committed before testing SET ROLE so reader gangs can see it.
\set ON_ERROR_STOP on
CREATE ROLE catalogless_review_user;
BEGIN;
SET LOCAL optimizer = off;
SET LOCAL gp_enable_catalogless_temp = on;

-- Force out-of-line, incompressible input. Matching distribution avoids
-- relying on Motion serialization to flatten the values for the CTAS.
CREATE TEMP TABLE cr_source(id int, payload text) DISTRIBUTED BY (id);
ALTER TABLE cr_source ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO cr_source
SELECT id, (SELECT string_agg(md5(g::text), '') FROM generate_series(1,2000) g)
FROM generate_series(1,10) id;
CREATE TEMP TABLE cr_expected AS
SELECT id, md5(payload) AS digest FROM cr_source DISTRIBUTED BY (id);
EXPLAIN CREATE TEMP TABLE cr_result WITH (catalogless) AS
SELECT * FROM cr_source DISTRIBUTED BY (id);
CREATE TEMP TABLE cr_result WITH (catalogless) AS
SELECT * FROM cr_source DISTRIBUTED BY (id);
TRUNCATE cr_source;
DO $$
BEGIN
  IF (SELECT count(*) FROM cr_result r JOIN cr_expected e USING (id)
      WHERE md5(r.payload) = e.digest) <> 10 THEN
    RAISE EXCEPTION 'catalogless result did not retain its TOAST values';
  END IF;
END $$;

-- Greenplum rejects FETCH BACKWARD globally. TempResultScan must nevertheless
-- advertise its actual executor capabilities (forward-only) to the planner.
EXPLAIN DECLARE cr_cursor SCROLL CURSOR FOR SELECT id FROM cr_result;

PREPARE cr_prepared AS SELECT count(*) FROM cr_result;
EXECUTE cr_prepared;
SET LOCAL ROLE catalogless_review_user;
DO $$
BEGIN
  BEGIN
    PERFORM count(*) FROM cr_result;
    RAISE EXCEPTION 'reading another role''s result was allowed';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
  BEGIN
    EXECUTE 'EXECUTE cr_prepared';
    RAISE EXCEPTION 'cached plan bypassed the owner check';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
  BEGIN
    EXECUTE 'DROP TABLE cr_result';
    RAISE EXCEPTION 'dropping another role''s result was allowed';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
END $$;
RESET ROLE;
DEALLOCATE cr_prepared;

-- Check TEMP privilege even when the session already has a temp namespace.
DO $$
BEGIN
  EXECUTE 'REVOKE TEMP ON DATABASE ' || quote_ident(current_database()) || ' FROM PUBLIC';
END $$;
SET LOCAL ROLE catalogless_review_user;
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE TEMP TABLE cr_denied WITH (catalogless) AS SELECT 1 AS id DISTRIBUTED RANDOMLY';
    RAISE EXCEPTION 'creating without TEMP privilege was allowed';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
END $$;
RESET ROLE;

-- Validate before registration, so the checks also precede the POC's
-- prohibition on creating a result inside a subtransaction.
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE TEMP TABLE cr_duplicate WITH (catalogless) AS SELECT 1 AS x, 2 AS x DISTRIBUTED RANDOMLY';
    RAISE EXCEPTION 'duplicate column names were allowed';
  EXCEPTION WHEN duplicate_column THEN NULL;
  END;
  BEGIN
    EXECUTE 'CREATE TEMP TABLE cr_record WITH (catalogless) AS SELECT ROW(1,2) AS x DISTRIBUTED RANDOMLY';
    RAISE EXCEPTION 'a pseudo-type column was allowed';
  EXCEPTION WHEN invalid_table_definition THEN NULL;
  END;
END $$;

CREATE FUNCTION cr_restricted() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE cr_hidden WITH (catalogless)
  AS SELECT 1 AS id DISTRIBUTED RANDOMLY;
  RETURN 1;
END $$;
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW cr_mv AS SELECT cr_restricted() AS id DISTRIBUTED RANDOMLY';
    RAISE EXCEPTION 'creating inside a security-restricted operation was allowed';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
END $$;
ROLLBACK;
DROP ROLE catalogless_review_user;
