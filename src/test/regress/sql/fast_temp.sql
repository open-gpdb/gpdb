--
-- FAST TEMP
-- Test fast temporary tables
--

-- basic test

CREATE FAST TEMP TABLE fasttab_test1(x int, s text);

INSERT INTO fasttab_test1 VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (4, 'ddd');

UPDATE fasttab_test1 SET s = 'eee' WHERE x = 4;

UPDATE fasttab_test1 SET x = 5 WHERE s = 'bbb';

DELETE FROM fasttab_test1 WHERE x = 3;

SELECT * FROM fasttab_test1 ORDER BY x;

DROP TABLE fasttab_test1;

-- kind of load test

do $$
declare
  count_fast_table integer = 150;
  count_attr integer = 20;
  i integer;
  j integer;
  t_sql text;
begin
  for i in 1 .. count_fast_table
  loop
    t_sql = 'CREATE FAST TEMP TABLE fast_table_' || i :: text;
    t_sql = t_sql || '  (';
    for j in 1 .. count_attr
    loop
      t_sql = t_sql || ' attr' || j || ' text';
      if j <> count_attr then
        t_sql = t_sql || ', ';
      end if;
    end loop;
    t_sql = t_sql || ' );';
    execute t_sql;
    -- raise info 't_sql %', t_sql;
  end loop;
end $$;

SELECT * FROM fast_table_1;

-- test bitmap index scan

SELECT count(*) FROM pg_class WHERE relname = 'fast_table_1' OR relname = 'fast_table_2';

-- create / delete / create test

CREATE FAST TEMP TABLE fasttab_test1(x int, s text);

-- check index only scan

SELECT COUNT(*) FROM pg_class WHERE relname = 'fasttab_test1';
SELECT relname FROM pg_class WHERE relname = 'fasttab_test1';

DROP TABLE fasttab_test1;

-- select from non-existend temp table

SELECT COUNT(*) FROM fasttab_test1;

CREATE FAST TEMP TABLE fasttab_test1(x int, s text);
CREATE FAST TEMP TABLE fasttab_test2(x int, s text);
SELECT * FROM fasttab_test1;

-- check that ALTER works as expected

ALTER TABLE fasttab_test1 ADD COLUMN y int;
SELECT * FROM fasttab_test1;

ALTER TABLE fasttab_test1 ADD COLUMN z int;
SELECT * FROM fasttab_test1;

ALTER TABLE fasttab_test1 DROP COLUMN x;
SELECT * FROM fasttab_test1;

ALTER TABLE fasttab_test1 DROP COLUMN y;
SELECT * FROM fasttab_test1;

-- check tat ALTER TABLE ... RENAME TO ... works as expected

CREATE FAST TEMP TABLE fast_temp_1 (x int);
ALTER TABLE fast_temp_1 RENAME TO fast_temp_2;
CREATE FAST TEMP TABLE fast_temp_1 (x int);
DROP TABLE fast_temp_1;
DROP TABLE fast_temp_2;

-- test transactions and savepoints

BEGIN;

INSERT INTO fasttab_test2 VALUES (1, 'aaa'), (2, 'bbb');
SELECT * FROM fasttab_test2;

ROLLBACK;

SELECT * FROM fasttab_test2;

BEGIN;

INSERT INTO fasttab_test2 VALUES (3, 'ccc'), (4, 'ddd');
SELECT * FROM fasttab_test2;

COMMIT;

SELECT * FROM fasttab_test2;


BEGIN;

SAVEPOINT sp1;

ALTER TABLE fasttab_test2 ADD COLUMN y int;
SELECT * FROM fasttab_test2;

SAVEPOINT sp2;

INSERT INTO fasttab_test2 VALUES (5, 'eee', 6);
SELECT * FROM fasttab_test2;
ROLLBACK TO SAVEPOINT sp2;

INSERT INTO fasttab_test2 VALUES (55, 'EEE', 66);
SELECT * FROM fasttab_test2;
ROLLBACK TO SAVEPOINT sp2;

SELECT * FROM fasttab_test2;
COMMIT;

DROP TABLE fasttab_test1;
DROP TABLE fasttab_test2;

-- test that exceptions are handled properly

DO $$
DECLARE
BEGIN
    CREATE FAST TEMP TABLE fast_exception_test(x int, y int, z int);
    RAISE EXCEPTION 'test error';
END $$;

CREATE FAST TEMP TABLE fast_exception_test(x int, y int, z int);
DROP TABLE fast_exception_test;

-- test that inheritance works as expected
-- OK:

CREATE TABLE cities (name text, population float, altitude int);
CREATE TABLE capitals (state char(2)) INHERITS (cities);
DROP TABLE capitals;
DROP TABLE cities;

-- OK:

CREATE TABLE cities2 (name text, population float, altitude int);
CREATE FAST TEMPORARY TABLE capitals2 (state char(2)) INHERITS (cities2);
INSERT INTO capitals2 VALUES ('Moscow', 123.45, 789, 'RU');
INSERT INTO capitals2 VALUES ('Paris', 543.21, 987, 'FR');
SELECT * FROM capitals2;
SELECT * FROM cities2;
DELETE FROM cities2 WHERE name = 'Moscow';
SELECT * FROM capitals2;
SELECT * FROM cities2;
DROP TABLE capitals2;
DROP TABLE cities2;

-- ERROR:

CREATE FAST TEMPORARY TABLE cities3 (name text, population float, altitude int);
-- cannot inherit from temporary relation "cities3"
CREATE TABLE capitals3 (state char(2)) INHERITS (cities3);
DROP TABLE cities3;

-- OK:

CREATE FAST TEMPORARY TABLE cities4 (name text, population float, altitude int);
CREATE FAST TEMPORARY TABLE capitals4 (state char(2)) INHERITS (cities4);
INSERT INTO capitals4 VALUES ('Moscow', 123.45, 789, 'RU');
INSERT INTO capitals4 VALUES ('Paris', 543.21, 987, 'FR');
SELECT * FROM capitals4;
SELECT * FROM cities4;
DELETE FROM cities4 WHERE name = 'Moscow';
SELECT * FROM capitals4;
SELECT * FROM cities4;
DROP TABLE capitals4;
DROP TABLE cities4;

-- OK:

CREATE TEMPORARY TABLE cities5 (name text, population float, altitude int);
CREATE FAST TEMPORARY TABLE capitals5 (state char(2)) INHERITS (cities5);
INSERT INTO capitals5 VALUES ('Moscow', 123.45, 789, 'RU');
INSERT INTO capitals5 VALUES ('Paris', 543.21, 987, 'FR');
SELECT * FROM capitals5;
SELECT * FROM cities5;
DELETE FROM cities5 WHERE name = 'Moscow';
SELECT * FROM capitals5;
SELECT * FROM cities5;
DROP TABLE capitals5;
DROP TABLE cities5;

-- OK:

CREATE FAST TEMPORARY TABLE cities6 (name text, population float, altitude int);
CREATE TEMPORARY TABLE capitals6 (state char(2)) INHERITS (cities6);
INSERT INTO capitals6 VALUES ('Moscow', 123.45, 789, 'RU');
INSERT INTO capitals6 VALUES ('Paris', 543.21, 987, 'FR');
SELECT * FROM capitals6;
SELECT * FROM cities6;
DELETE FROM cities6 WHERE name = 'Moscow';
SELECT * FROM capitals6;
SELECT * FROM cities6;
DROP TABLE capitals6;
DROP TABLE cities6;

-- test index-only scan

CREATE FAST TEMP TABLE fasttab_unique_prefix_beta(x int);
CREATE TABLE fasttab_unique_prefix_alpha(x int);
CREATE FAST TEMP TABLE fasttab_unique_prefix_delta(x int);
CREATE TABLE fasttab_unique_prefix_epsilon(x int);
CREATE TABLE fasttab_unique_prefix_gamma(x int);
SELECT relname FROM pg_class WHERE relname > 'fasttab_unique_prefix_' ORDER BY relname LIMIT 5;
DROP TABLE fasttab_unique_prefix_alpha;
DROP TABLE fasttab_unique_prefix_beta;
DROP TABLE fasttab_unique_prefix_gamma;
DROP TABLE fasttab_unique_prefix_delta;
DROP TABLE fasttab_unique_prefix_epsilon;

-- test VACUUM / VACUUM FULL

VACUUM;
VACUUM FULL;
SELECT * FROM fast_table_1;

-- test ANALYZE

CREATE FAST TEMP TABLE fasttab_analyze_test(x int, s text);
INSERT INTO fasttab_analyze_test SELECT x, '--> ' || x FROM generate_series(1,100) as x;
ANALYZE fasttab_analyze_test;
SELECT count(*) FROM pg_statistic WHERE starelid = (SELECT oid FROM pg_class WHERE relname = 'fasttab_analyze_test');
DROP TABLE fasttab_analyze_test;
SELECT count(*) FROM pg_statistic WHERE starelid = (SELECT oid FROM pg_class WHERE relname = 'fasttab_analyze_test');

-- cleanup after load test

do $$
declare
  count_fast_table integer = 150;
  t_sql text;
begin
  for i in 1 .. count_fast_table
  loop
    t_sql = 'DROP TABLE fast_table_' || i || ';';
    execute t_sql;
  end loop;
end $$;

