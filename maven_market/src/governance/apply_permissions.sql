-- Tiered Access Control for Unity Catalog
-- ========================================
-- This script documents the deployed permission model.
-- Run manually by an admin when setting up a new environment.
--
-- Group            | Catalog          | Bronze   | Silver   | Gold
-- -----------------+------------------+----------+----------+----------
-- maven_admins     | ALL PRIVILEGES   | inherited| inherited| inherited
-- maven_engineers  | USE CATALOG      | ALL PRIV | ALL PRIV | ALL PRIV
-- maven_analysts   | USE CATALOG      | denied   | denied   | SELECT
-- maven_executives | USE CATALOG      | denied   | denied   | SELECT
--
-- Analysts and executives also have USE SCHEMA on gold.
-- Bronze and silver are explicitly revoked from analysts and executives.
--
-- To apply, replace <catalog> with the target catalog name and run each
-- statement individually in a SQL editor as a catalog owner.

-- 1. Admins: full catalog ownership
--    ALL PRIVILEGES ON CATALOG <catalog> TO maven_admins

-- 2. Engineers: full schema access + catalog usage
--    USE CATALOG ON CATALOG <catalog> TO maven_engineers
--    ALL PRIVILEGES ON SCHEMA <catalog>.bronze TO maven_engineers
--    ALL PRIVILEGES ON SCHEMA <catalog>.silver TO maven_engineers
--    ALL PRIVILEGES ON SCHEMA <catalog>.gold   TO maven_engineers

-- 3. Analysts: gold read-only
--    USE CATALOG ON CATALOG <catalog> TO maven_analysts
--    USE SCHEMA  ON SCHEMA  <catalog>.gold TO maven_analysts
--    SELECT      ON SCHEMA  <catalog>.gold TO maven_analysts

-- 4. Executives: gold read-only
--    USE CATALOG ON CATALOG <catalog> TO maven_executives
--    USE SCHEMA  ON SCHEMA  <catalog>.gold TO maven_executives
--    SELECT      ON SCHEMA  <catalog>.gold TO maven_executives

-- 5. Deny bronze/silver to business users
--    REVOKE ALL ON SCHEMA <catalog>.bronze FROM maven_analysts
--    REVOKE ALL ON SCHEMA <catalog>.silver FROM maven_analysts
--    REVOKE ALL ON SCHEMA <catalog>.bronze FROM maven_executives
--    REVOKE ALL ON SCHEMA <catalog>.silver FROM maven_executives
