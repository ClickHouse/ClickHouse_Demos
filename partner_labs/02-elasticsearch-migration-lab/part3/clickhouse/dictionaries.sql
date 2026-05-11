-- ============================================================
-- GeoIP Dictionaries — Replaces the ES "geoip" ingest processor
-- ============================================================
-- In Elasticsearch, geoip enrichment is a black-box processor built
-- into the ingest pipeline. In ClickHouse, you use a dictionary backed
-- by the same MaxMind data (or equivalent), giving you full control over
-- the data source, refresh interval, and lookup behavior.
--
-- The IP_TRIE layout is purpose-built for CIDR range lookups:
--   dictGet('otel.geoip_country', 'country', toIPv4('1.0.0.1'))
--   → 'Australia'
-- This is a longest-prefix match over CIDR ranges — equivalent to what
-- ES's geoip processor does internally.
--
-- For this lab, data is loaded from geoip-sample-data.csv (shipped with
-- the lab), so no MaxMind account is required. In production, use the
-- full GeoLite2 database or a commercial MaxMind license.
--
-- Run order: load geoip-sample-data.csv first, then run this file.
-- ============================================================

CREATE DATABASE IF NOT EXISTS otel;
USE otel;


-- ── Step 1: Source table for GeoIP data ──────────────────────────────
-- Stores CIDR ranges with their geographic attributes.
-- The IP_TRIE dictionary layout reads from this table.
CREATE TABLE IF NOT EXISTS geoip_data
(
    `prefix`  String,   -- CIDR notation, e.g. '1.0.0.0/24'
    `country` String,
    `city`    String
)
ENGINE = MergeTree
ORDER BY prefix;

-- Load sample data (run this from the directory containing geoip-sample-data.csv):
-- clickhouse client --query "INSERT INTO geoip_data FORMAT CSVWithNames" < geoip-sample-data.csv


-- ── Step 2: Country-level dictionary ────────────────────────────────
CREATE DICTIONARY IF NOT EXISTS geoip_country
(
    `prefix`  String,
    `country` String DEFAULT ''
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'geoip_data' DB 'otel'))
LAYOUT(IP_TRIE)
LIFETIME(3600);  -- reload from source table every hour


-- ── Step 3: City-level dictionary ───────────────────────────────────
CREATE DICTIONARY IF NOT EXISTS geoip_city
(
    `prefix`  String,
    `city`    String DEFAULT ''
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'geoip_data' DB 'otel'))
LAYOUT(IP_TRIE)
LIFETIME(3600);


-- ── Verification queries ──────────────────────────────────────────────
-- Run these to confirm the dictionaries are loaded and working:
--
--   SELECT dictGet('otel.geoip_country', 'country', toIPv4('1.0.0.1'));
--   -- Expected: 'Australia'
--
--   SELECT dictGet('otel.geoip_city', 'city', toIPv4('8.8.8.8'));
--   -- Expected: 'Mountain View' (or 'United States' depending on CSV coverage)
--
--   SELECT status, element_count, bytes_allocated
--   FROM system.dictionaries
--   WHERE name IN ('geoip_country', 'geoip_city');
--   -- Both should show status = 'LOADED'
