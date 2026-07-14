-- Enable PostGIS in the default database created on first container init.
-- Note: docker-entrypoint-initdb.d scripts only run on a fresh data directory.
CREATE EXTENSION IF NOT EXISTS postgis;

