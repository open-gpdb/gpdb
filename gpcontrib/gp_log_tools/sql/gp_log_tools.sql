CREATE EXTENSION gp_log_tools;

SHOW ycmdb.yc_allow_copy_from_logs;

SELECT count(*) != 0 as is_logs_selected FROM gp_master_logs;

DROP EXTENSION gp_log_tools;
