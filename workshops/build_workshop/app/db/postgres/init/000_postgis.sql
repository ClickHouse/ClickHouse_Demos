-- Enable PostGIS in the default database created on first container init, IF the
-- image provides it. The workshop compose runs the official multi-arch
-- `postgres:16` image (native on Apple Silicon; no platform-emulation warning),
-- which does NOT ship PostGIS -- and the workshop path needs no geo types
-- (centroids are plain double precision, the CDC table has no geo columns). So a
-- missing extension must be non-fatal: without the guard below, a failed
-- CREATE EXTENSION aborts container init under ON_ERROR_STOP. The DO block swallows
-- that error so this file is portable across `postgres:16` and a PostGIS image.
-- Note: docker-entrypoint-initdb.d scripts only run on a fresh data directory.
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS postgis;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'PostGIS not available in this image; skipping (not required for the workshop).';
END $$;

