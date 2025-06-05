
CREATE FUNCTION
gpdb_binary_upgrade_catalog_1_0_to_1_1_m()
RETURNS VOID
AS 'MODULE_PATHNAME','gpdb_binary_upgrade_catalog_1_0_to_1_1'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE FUNCTION
gpdb_binary_upgrade_catalog_1_0_to_1_1_seg()
RETURNS VOID
AS 'MODULE_PATHNAME','gpdb_binary_upgrade_catalog_1_0_to_1_1'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE FUNCTION
gp_acquire_sample_rows_vac(oid,int4,bool,int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME','gp_acquire_sample_rows_vac'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


 CREATE FUNCTION gp_acquire_sample_rows(oid, int4, bool) RETURNS SETOF record LANGUAGE internal VOLATILE STRICT EXECUTE ON ALL SEGMENTS AS 'gp_acquire_sample_rows' WITH (OID=6038, DESCRIPTION="Collect a random sample of rows from table" );
 

SELECT gpdb_binary_upgrade_catalog_1_0_to_1_1_seg();
SELECT gpdb_binary_upgrade_catalog_1_0_to_1_1_m();

DROP FUNCTION gpdb_binary_upgrade_catalog_1_0_to_1_1_seg();
DROP FUNCTION gpdb_binary_upgrade_catalog_1_0_to_1_1_m();
