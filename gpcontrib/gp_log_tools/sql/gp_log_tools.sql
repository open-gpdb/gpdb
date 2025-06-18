-- start_matchsubs
-- m/^\s+logtime.*/
-- s/^\s+logtime.*/ log_table/
-- m/^\(\d+ row[s]?\)/
-- s/^\(\d+ row[s]?\)/(XXX rows)/
-- end_matchsubs
-- start_matchignore
-- m/^([\s\S]+\|){10}/
-- end_matchignore

CREATE EXTENSION gp_log_tools;

SELECT * FROM gp_master_logs;

DROP EXTENSION gp_log_tools;
