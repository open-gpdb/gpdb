CREATE EXTENSION gp_log_tools;

SHOW ycmdb.yc_allow_copy_from_logs;

SELECT count(*) != 0 as is_logs_selected FROM gp_master_logs;

CREATE USER test_user1;

GRANT SELECT ON gp_master_logs TO PUBLIC;

SET SESSION AUTHORIZATION test_user1;

SELECT count(*) != 0 as is_logs_selected FROM gp_master_logs;

\c -

DROP USER test_user1;

DROP EXTENSION gp_log_tools;
