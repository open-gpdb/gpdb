-- start_matchsubs
-- m/:relid \d+/
-- s/:relid \d+/:relid XXXX/g
-- m/:resorigtbl \d+/
-- s/:resorigtbl \d+/:resorigtbl XXXX/g
-- m/:constvalue \d+ \[.*\]/
-- s/:constvalue \d+ \[.*\]/:constvalue XXXX [XXXX]/g
-- m/,\d+,\d+,/
-- s/,\d+,\d+,/,_ID_,_SID_,/
-- m/<not logged>/
-- s/<not logged>/<none>/
-- m/pg_temp_\d+/
-- s/pg_temp_\d+/pg_temp_XX/
-- m/\{r \d+ [a-zA-Z_]+\.[a-zA-Z_]+ \{[^}]*\}\}/
-- s/\{r \d+ [a-zA-Z_]+\.[a-zA-Z_]+ \{[^}]*\}\}/{r XXXX schema.table {cols}}/g
-- m/\{f \d+ [a-zA-Z_]+\.[a-zA-Z_]+ \{\}\}/
-- s/\{f \d+ [a-zA-Z_]+\.[a-zA-Z_]+ \{\}\}/{f XXXX schema.func {}}/g
-- m/\(seg\d+ .*\)$/
-- s/\(seg\d+ .*\)$//g
-- end_matchsubs

\set VERBOSITY terse

-- start_ignore
CREATE EXTENSION IF NOT EXISTS ast_log;
-- end_ignore

SET ast_log.log = 'ast_sel';
SET ast_log.log_client = ON;
SET ast_log.log_level = 'notice';

CREATE TABLE test_ast (id int, data text) DISTRIBUTED BY (id);
SELECT * FROM test_ast;
SELECT id FROM test_ast WHERE id = 1;

SET ast_log.log = 'ast_mod';

INSERT INTO test_ast VALUES (1, 'hello');
UPDATE test_ast SET data = 'world' WHERE id = 1;
DELETE FROM test_ast WHERE id = 1;

SET ast_log.log = 'ast_ctas';

CREATE TABLE test_ctas AS SELECT * FROM test_ast DISTRIBUTED BY (id);
CREATE TEMP TABLE test_ctas_tmp AS SELECT id FROM test_ast DISTRIBUTED BY (id);

SET ast_log.log = 'ast_sel, ast_mod, ast_ctas';

CREATE TABLE test_multi (a int) DISTRIBUTED BY (a);
INSERT INTO test_multi SELECT generate_series(1, 5);
CREATE TABLE test_multi_copy AS SELECT * FROM test_multi DISTRIBUTED BY (a);
SELECT * FROM test_multi_copy;
UPDATE test_multi_copy SET a = a + 10;
DELETE FROM test_multi_copy WHERE a > 15;

SET ast_log.log = 'ast_sel';
SET ast_log.log_parameter = ON;

PREPARE test_stmt(int) AS SELECT * FROM test_multi WHERE a = $1;
EXECUTE test_stmt(1);
DEALLOCATE test_stmt;

DROP TABLE test_ast;
DROP TABLE test_multi;
DROP TABLE test_multi_copy;

RESET ast_log.log;
RESET ast_log.log_client;
RESET ast_log.log_level;
RESET ast_log.log_parameter;
