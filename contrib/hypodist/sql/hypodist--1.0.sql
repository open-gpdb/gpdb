/* hypodist--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hypodist" to load this file. \quit


CREATE FUNCTION create_hypodist(table_name NAME, column_name NAME)
RETURNS VOID
AS 'MODULE_PATHNAME', 'create_hypodist'
LANGUAGE C STRICT EXECUTE ON MASTER;

CREATE FUNCTION drop_hypodist(table_name NAME)
RETURNS VOID
AS 'MODULE_PATHNAME', 'drop_hypodist'
LANGUAGE C STRICT EXECUTE ON MASTER;