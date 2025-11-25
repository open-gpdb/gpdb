
CREATE OR REPLACE FUNCTION
pg_catalog.pg_event_trigger_ddl_commands(
	OUT classid oid,
	OUT objid oid,
	OUT objsubid integer,
	OUT command_tag text,
	OUT object_type text,
	OUT schema_name text,
	OUT object_identity text,
	OUT in_extension boolean
)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION
pg_catalog.pg_event_trigger_table_rewrite_oid(
	OUT oid oid
)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION
pg_catalog.pg_event_trigger_table_rewrite_reason()
RETURNS INT
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;