-- =============================================================================
-- ClickHouse users for Atlas. Run this ONCE, as `default` (or any admin), in the
-- ClickHouse Cloud SQL console.
--
-- Why bother, when `default` already works?
--
-- Because the whole point of the session is that schema changes should be
-- governed. Handing a migration tool a full-admin credential is the same
-- category of mistake as editing production from a console. Two scoped users
-- cost you five minutes and they make the CI story defensible:
--
--   atlas_admin  applies migrations. Scoped to the adtech database only.
--   atlas_drift  detects drift. Read-only. Cannot change anything, ever.
--
-- The drift job in ci/schema-ci.yml runs on a schedule against
-- production. It must be structurally incapable of applying a plan. That is what
-- atlas_drift is for.
--
-- ALL GRANTS BELOW WERE VERIFIED against ClickHouse 26.8 (the scenarios in
-- SCENARIOS.md were separately measured on Cloud 26.2): atlas_admin can build
-- the full baseline schema, insert, alter and read the system tables the helper
-- scripts need, and is refused outside `adtech`. atlas_drift can inspect
-- everything Atlas needs to compute a diff, and is refused on ALTER, INSERT
-- and DROP.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. Replace these before running. Do not reuse the demo values.
--    On ClickHouse Cloud, generate real passwords and store them in your secret
--    manager, not in this file.
-- -----------------------------------------------------------------------------
--    atlas_admin  -> 'CHANGE_ME_ADMIN'
--    atlas_drift  -> 'CHANGE_ME_DRIFT'

-- -----------------------------------------------------------------------------
-- 1. The database Atlas manages.
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS adtech;

-- -----------------------------------------------------------------------------
-- 2. atlas_admin — plans and applies migrations.
--
-- Deliberately NOT granted:
--   * anything outside adtech.*         (blast radius stays inside one database)
--   * CREATE/DROP DATABASE              (a migration should never drop a database)
--   * access management                 (it cannot grant itself more)
--
-- SELECT ON system.* is required, not optional. Atlas inspects system.tables,
-- system.columns and friends to work out current state. Without it, every diff
-- comes back looking like an empty database.
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS atlas_admin
    IDENTIFIED WITH sha256_password BY 'CHANGE_ME_ADMIN';

GRANT SHOW, SELECT, INSERT, ALTER,
      CREATE TABLE, CREATE VIEW,
      DROP TABLE, DROP VIEW,
      TRUNCATE, OPTIMIZE
    ON adtech.* TO atlas_admin;

GRANT SELECT ON system.* TO atlas_admin;

-- -----------------------------------------------------------------------------
-- 3. atlas_drift — read-only, for the scheduled drift check.
--
-- This is the credential that goes in CI as a repository secret. If someone
-- later adds an "apply on merge" job and wires this user into it by mistake, the
-- job fails instead of mutating production. That is the design goal.
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS atlas_drift
    IDENTIFIED WITH sha256_password BY 'CHANGE_ME_DRIFT';

GRANT SHOW, SELECT ON adtech.* TO atlas_drift;
GRANT SELECT ON system.* TO atlas_drift;

-- -----------------------------------------------------------------------------
-- 4. Confirm what you actually granted. Read this output, do not assume.
-- -----------------------------------------------------------------------------
SHOW GRANTS FOR atlas_admin;
SHOW GRANTS FOR atlas_drift;

-- -----------------------------------------------------------------------------
-- 5. Prove the read-only user really is read-only.
--
-- Connect as atlas_drift and run this. It MUST fail with:
--   Code: 497. DB::Exception: atlas_drift: Not enough privileges.
--
-- If it succeeds, stop and fix the grants before wiring anything into CI. A
-- "read-only" credential you have not tested is not a control, it is a hope.
-- -----------------------------------------------------------------------------
-- ALTER TABLE adtech.ad_events ADD COLUMN should_not_work UInt8;

-- -----------------------------------------------------------------------------
-- 6. Dev database note.
--
-- Atlas needs a scratch "dev database" it can wipe on every command. If you
-- point CH_DEV_URL at a second ClickHouse Cloud service (recommended, because it
-- gives exact version and engine parity), the user there needs MORE than
-- atlas_admin above: Atlas creates and drops databases in the dev instance.
--
-- Use the `default` user on the dev service, or grant broadly there. It holds no
-- real data, and treating it as disposable is the point.
-- -----------------------------------------------------------------------------
