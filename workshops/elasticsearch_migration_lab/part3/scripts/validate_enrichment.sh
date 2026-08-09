#!/usr/bin/env bash
# ============================================================
# validate_enrichment.sh — Enrichment column verification
# ============================================================
# Verifies that the materialized enrichment columns in otel_logs_v2
# are populated correctly (GeoIP, user-agent parsing, severity derivation).
#
# Usage:
#   source ../../common/env.sh   # fill in common/env.sh.example first
#   bash validate_enrichment.sh
# ============================================================

set -euo pipefail

CH_HOST="${CH_HOST:?Set CH_HOST}"
CH_PASSWORD="${CH_PASSWORD:?Set CH_PASSWORD}"
CH_DATABASE="${CH_DATABASE:-otel}"
CH_CONN="clickhouse client --host ${CH_HOST} --port 9440 --user default --password ${CH_PASSWORD} --secure --database ${CH_DATABASE}"

echo "═══════════════════════════════════════════════════════════"
echo "  Enrichment Column Verification"
echo "═══════════════════════════════════════════════════════════"

echo ""
echo "── GeoIP enrichment (top 10 countries) ──────────────────────"
$CH_CONN --query "
SELECT GeoCountry, count() AS rows
FROM otel_logs_v2
WHERE RequestType != '' AND GeoCountry != ''
GROUP BY GeoCountry
ORDER BY rows DESC
LIMIT 10
FORMAT Pretty"

echo ""
echo "── User-agent parsing (top browsers) ────────────────────────"
$CH_CONN --query "
SELECT BrowserFamily, OSFamily, count() AS rows
FROM otel_logs_v2
WHERE RequestType != '' AND BrowserFamily != ''
GROUP BY BrowserFamily, OSFamily
ORDER BY rows DESC
LIMIT 10
FORMAT Pretty"

echo ""
echo "── Bot detection ─────────────────────────────────────────────"
$CH_CONN --query "
SELECT
    if(IsBot = 1, 'Bot', 'Human') AS traffic_type,
    count()                       AS rows,
    round(count() / sum(count()) OVER () * 100, 1) AS pct
FROM otel_logs_v2
WHERE RequestType != ''
GROUP BY IsBot
ORDER BY IsBot
FORMAT Pretty"

echo ""
echo "── Severity derivation (status-based) ───────────────────────"
$CH_CONN --query "
SELECT DerivedSeverity, count() AS rows
FROM otel_logs_v2
GROUP BY DerivedSeverity
ORDER BY rows DESC
FORMAT Pretty"

echo ""
echo "── Sample enriched rows ─────────────────────────────────────"
$CH_CONN --query "
SELECT
    RemoteAddr,
    GeoCountry,
    GeoCity,
    BrowserFamily,
    OSFamily,
    IsBot,
    StatusCode,
    DerivedSeverity
FROM otel_logs_v2
WHERE RemoteAddr != '' AND GeoCountry != '' AND BrowserFamily != ''
LIMIT 5
FORMAT Pretty"

echo ""
echo "── Coverage summary ─────────────────────────────────────────"
$CH_CONN --query "
SELECT
    count()                                        AS total_web_rows,
    countIf(GeoCountry    != '')                   AS with_geo,
    countIf(BrowserFamily != '')                   AS with_browser,
    countIf(DerivedSeverity != 'info')             AS with_derived_severity,
    round(with_geo     / total_web_rows * 100, 1)  AS geo_pct,
    round(with_browser / total_web_rows * 100, 1)  AS browser_pct,
    round(with_derived_severity / total_web_rows * 100, 1) AS severity_pct
FROM otel_logs_v2
WHERE RequestType != ''
FORMAT Pretty"
