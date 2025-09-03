
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

 

SELECT gpdb_binary_upgrade_catalog_1_0_to_1_1_seg();
SELECT gpdb_binary_upgrade_catalog_1_0_to_1_1_m();

DROP FUNCTION gpdb_binary_upgrade_catalog_1_0_to_1_1_seg();
DROP FUNCTION gpdb_binary_upgrade_catalog_1_0_to_1_1_m();

CREATE OR REPLACE FUNCTION
gp_add_segment_aux_mirror(int2, text, text, int4, text) 
RETURNS int2
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;
