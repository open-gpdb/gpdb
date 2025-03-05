 
 
CREATE FUNCTION blgettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'bloomgettuple' WITH (OID=7050, DESCRIPTION="bloom(internal)");

CREATE FUNCTION bmgetbitmap(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmgetbitmap' WITH (OID=7051, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bminsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'bminsert' WITH (OID=7187, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbeginscan' WITH (OID=7188, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmrescan' WITH (OID=7189, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmendscan' WITH (OID=7190, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmmarkpos' WITH (OID=7191, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmrestrpos' WITH (OID=7192, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbuild' WITH (OID=7193, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmbuildempty(internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbuildempty' WITH (OID=7011, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbulkdelete' WITH (OID=7194, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmvacuumcleanup' WITH (OID=7195, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmcostestimate(internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmcostestimate' WITH (OID=7196, DESCRIPTION="bitmap(internal)");

CREATE FUNCTION bmoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'bmoptions' WITH (OID=7197, DESCRIPTION="btree(internal)");
