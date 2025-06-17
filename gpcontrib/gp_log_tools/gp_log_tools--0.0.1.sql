\echo Use "CREATE EXTENSION gp_log_tools" to load this file. \quit

CREATE OR REPLACE FUNCTION get_logs() RETURNS TABLE(
    logtime timestamp with time zone,
    loguser text,
    logdatabase text,
    logpid text,
    logthread text,
    loghost text,
    logport text,
    logsessiontime timestamp with time zone,
    logtransaction int,
    logsession text,
    logcmdcount text,
    logsegment text,
    logslice text,
    logdistxact text,
    loglocalxact text,
    logsubxact text,
    logseverity text,
    logstate text,
    logmessage text,
    logdetail text,
    loghint text,
    logquery text,
    logquerypos int,
    logcontext text,
    logdebug text,
    logcursorpos int,
    logfunction text,
    logfile text,
    logline int,
    logstack text
) AS
$BODY$
DECLARE
	r RECORD;
BEGIN
	CREATE TEMP TABLE my_logs(
        logtime timestamp with time zone,
        loguser text,
        logdatabase text,
        logpid text,
        logthread text,
        loghost text,
        logport text,
        logsessiontime timestamp with time zone,
        logtransaction int,
        logsession text,
        logcmdcount text,
        logsegment text,
        logslice text,
        logdistxact text,
        loglocalxact text,
        logsubxact text,
        logseverity text,
        logstate text,
        logmessage text,
        logdetail text,
        loghint text,
        logquery text,
        logquerypos int,
        logcontext text,
        logdebug text,
        logcursorpos int,
        logfunction text,
        logfile text,
        logline int,
        logstack text
    ) DISTRIBUTED RANDOMLY;

	FOR r IN
		SELECT pg_ls_dir AS log_file FROM pg_ls_dir('pg_log') WHERE pg_ls_dir LIKE '%.csv' OR pg_ls_dir LIKE '%.log'
	LOOP
		EXECUTE $$COPY my_logs FROM 'pg_log/$$ || r.log_file || $$' CSV DELIMITER ',' QUOTE '"'$$;
	END LOOP;
	RETURN QUERY SELECT * FROM my_logs;
    DROP TABLE my_logs;
END
$BODY$
LANGUAGE plpgsql
SECURITY DEFINER;

CREATE VIEW gp_master_logs AS
SELECT * FROM get_logs();

CREATE VIEW gp_segments_logs AS
SELECT * FROM gp_dist_random('gp_master_logs');
