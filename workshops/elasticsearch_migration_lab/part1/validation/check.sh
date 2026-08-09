#!/bin/bash
set -euo pipefail

# Part 1 Validation Checkpoint
# Usage: ./check.sh [ES_URL] [KIBANA_URL] [OTELCOL_URL]
# Defaults to localhost if not provided

ES_URL="${1:-http://localhost:9200}"
KIBANA_URL="${2:-http://localhost:5601}"
OTELCOL_URL="${3:-http://localhost:8888}"

PASS=0
FAIL=0
SKIP=0

pass() { echo "[PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL + 1)); }
skip() { echo "[SKIP] $1"; SKIP=$((SKIP + 1)); }

echo "============================================"
echo " Part 1 Validation — Source Environment"
echo " ES:      $ES_URL"
echo " Kibana:  $KIBANA_URL"
echo " OTelCol: $OTELCOL_URL"
echo "============================================"
echo ""

# Check 1: Elasticsearch reachable
if curl -sf "${ES_URL}/_cluster/health" > /dev/null 2>&1; then
  pass "Elasticsearch is reachable"
else
  fail "Elasticsearch is not reachable at ${ES_URL}"
fi

# Check 2: Data streams exist
DS_COUNT=$(curl -sf "${ES_URL}/_data_stream/logs-*" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data_streams',[])))" 2>/dev/null || echo "0")
if [ "$DS_COUNT" -ge 3 ]; then
  pass "Data streams exist ($DS_COUNT found: logs-web_access-lab, logs-application-lab, logs-infrastructure-lab)"
else
  fail "Expected 3 data streams, found $DS_COUNT. Run: curl ${ES_URL}/_data_stream/logs-*"
fi

# Check 3: ILM policy exists
if curl -sf "${ES_URL}/_ilm/policy/lab-observability-policy" > /dev/null 2>&1; then
  pass "ILM policy 'lab-observability-policy' exists"
else
  fail "ILM policy 'lab-observability-policy' not found"
fi

# Check 4: Ingest pipelines exist
for pipeline in web-access-enrichment app-log-enrichment infra-log-parsing default-enrichment; do
  if curl -sf "${ES_URL}/_ingest/pipeline/${pipeline}" > /dev/null 2>&1; then
    pass "Ingest pipeline '${pipeline}' exists"
  else
    fail "Ingest pipeline '${pipeline}' not found"
  fi
done

# Check 5: Documents are being indexed
for stream in logs-web_access-lab logs-application-lab logs-infrastructure-lab; do
  DOC_COUNT=$(curl -sf "${ES_URL}/${stream}/_count" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
  if [ "$DOC_COUNT" -gt 100 ]; then
    pass "Data stream '${stream}' has data ($DOC_COUNT docs)"
  else
    fail "Data stream '${stream}' has too few docs ($DOC_COUNT) — generators may not be running"
  fi
done

# Check 6: GeoIP enrichment working (spot check web access logs)
GEO_COUNTRY=$(curl -sf "${ES_URL}/logs-web_access-lab/_search?size=1" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
hits = data.get('hits', {}).get('hits', [])
if hits:
    src = hits[0].get('_source', {})
    geo = src.get('geo', {})
    print(geo.get('country_name', 'MISSING'))
else:
    print('NO_DOCS')
" 2>/dev/null || echo "ERROR")
if [ "$GEO_COUNTRY" != "MISSING" ] && [ "$GEO_COUNTRY" != "NO_DOCS" ] && [ "$GEO_COUNTRY" != "ERROR" ]; then
  pass "GeoIP enrichment working (country: $GEO_COUNTRY)"
else
  fail "GeoIP enrichment not working — geo.country_name missing from web access docs"
fi

# Check 7: Kibana reachable
if curl -sf "${KIBANA_URL}/api/status" > /dev/null 2>&1; then
  pass "Kibana is reachable"
else
  fail "Kibana is not reachable at ${KIBANA_URL}"
fi

# Check 8: Kibana dashboards imported
DASHBOARD_COUNT=$(curl -sf "${KIBANA_URL}/api/saved_objects/_find?type=dashboard" -H "kbn-xsrf: true" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('total',0))" 2>/dev/null || echo "0")
if [ "$DASHBOARD_COUNT" -ge 3 ]; then
  pass "Kibana dashboards imported ($DASHBOARD_COUNT found)"
else
  skip "Kibana dashboards count is $DASHBOARD_COUNT (expected >=3) — may still be importing"
fi

# Check 9: OTel Collector (demo) health
if curl -sf "${OTELCOL_URL}/metrics" > /dev/null 2>&1; then
  pass "OTel Collector (otelcol-demo) is reachable"
else
  skip "OTel Collector not reachable at ${OTELCOL_URL} — start Workload 2 first"
fi

# Check 10: APM traces are flowing from OTel Demo
APM_COUNT=$(curl -sf "${ES_URL}/traces-apm-*/_count" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
if [ "$APM_COUNT" -gt 100 ]; then
  pass "APM traces indexed from OTel Demo ($APM_COUNT spans)"
else
  skip "APM traces not yet indexed ($APM_COUNT) — Workload 2 may still be starting (~5 min)"
fi

# Check 11: OTel Demo storefront reachable
if curl -sf "http://localhost:8090" > /dev/null 2>&1; then
  pass "OTel Demo storefront reachable at http://localhost:8090"
else
  skip "OTel Demo storefront not reachable — Workload 2 may still be starting"
fi

echo ""
echo "============================================"
echo " Results: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "Some checks failed. Troubleshooting tips:"
  echo "  * Check container logs: docker compose -f docker-compose.source.yml logs -f"
  echo "  * Re-run bootstrap: docker compose -f docker-compose.source.yml restart es-bootstrap"
  exit 1
fi
exit 0
