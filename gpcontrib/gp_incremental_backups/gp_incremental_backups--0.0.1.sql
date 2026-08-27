\echo Use "CREATE EXTENSION gp_incremental_backups" to load this file. \quit

CREATE TABLE gp_incremental_backups.possible_vacuum_list(
    filenode OID,
    vacuum_lsn pg_lsn
)
DISTRIBUTED REPLICATED;
