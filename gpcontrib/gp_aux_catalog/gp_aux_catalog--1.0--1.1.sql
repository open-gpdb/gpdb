
CREATE FUNCTION
gpdb_binary_upgrade_catalog_1_0_to_1_1()
RETURNS VOID
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


SELECT gpdb_binary_upgrade_catalog_1_0_to_1_1();

DROP FUNCTION gpdb_binary_upgrade_catalog_1_0_to_1_1();