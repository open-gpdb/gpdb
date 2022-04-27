--
-- Tests for password verifiers
--

-- Tests for GUC password_encryption
SET password_hash_algorithm = 'novalue'; -- error
SET password_encryption = on; -- ok
SET password_hash_algorithm = 'md5'; -- ok
SET password_encryption = off; -- ok

-- consistency of password entries
SET password_encryption = off;
CREATE ROLE regress_passwd1 PASSWORD 'role_pwd1';
SET password_encryption = 'on';
SET password_hash_algorithm = 'md5';
CREATE ROLE regress_passwd2 PASSWORD 'role_pwd2';
SET password_encryption = 'on';
CREATE ROLE regress_passwd3 PASSWORD 'role_pwd3';
SET password_encryption = 'off';
CREATE ROLE regress_passwd5 PASSWORD NULL;

-- check list of created entries
-- Since the salt is random, the exact value stored will be different on every test
-- run. Use a regular expression to mask the changing parts.
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;

-- Rename a role
ALTER ROLE regress_passwd3 RENAME TO regress_passwd3_new;
-- md5 entry should have been removed
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd3_new'
    ORDER BY rolname, rolpassword;
ALTER ROLE regress_passwd3_new RENAME TO regress_passwd3;

-- ENCRYPTED and UNENCRYPTED passwords
ALTER ROLE regress_passwd1 UNENCRYPTED PASSWORD 'foo'; -- unencrypted
ALTER ROLE regress_passwd2 UNENCRYPTED PASSWORD 'md5dfa155cadd5f4ad57860162f3fab9cdb'; -- encrypted with MD5
SET password_encryption = 'on';
SET password_hash_algorithm = 'md5';
ALTER ROLE regress_passwd3 ENCRYPTED PASSWORD 'foo'; -- encrypted with MD5
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;

-- An empty password is not allowed, in any form
CREATE ROLE regress_passwd_empty PASSWORD '';
ALTER ROLE regress_passwd_empty PASSWORD 'md585939a5ce845f1a1b620742e3c659e0a';
ALTER ROLE regress_passwd_empty PASSWORD 'SCRAM-SHA-256$4096:hpFyHTUsSWcR7O9P$LgZFIt6Oqdo27ZFKbZ2nV+vtnYM995pDh9ca6WSi120=:qVV5NeluNfUPkwm7Vqat25RjSPLkGeoZBQs6wVv+um4=';
SELECT rolpassword FROM pg_authid WHERE rolname='regress_passwd_empty';

DROP ROLE regress_passwd1;
DROP ROLE regress_passwd2;
DROP ROLE regress_passwd3;
DROP ROLE regress_passwd5;
DROP ROLE regress_passwd_empty;

-- all entries should have been removed
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;
