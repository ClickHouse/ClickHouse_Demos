#!/usr/bin/env bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
container="pricing-otel-test-$$"
directory=$(mktemp -d)
cleanup() { docker rm --force "$container" >/dev/null 2>&1 || true; rm -rf "$directory"; }
trap cleanup EXIT
cat > "$directory/collector.yaml" <<'YAML'
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
    logs:
      receivers: [otlp]
      exporters: [debug]
YAML
docker run --detach --name "$container" -p 127.0.0.1::4318 \
  -v "$directory/collector.yaml:/etc/otelcol/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.139.0@sha256:faf125d656fa47cea568b2f3b4494efd2525083bc75c1e96038bc23f05cd68fd \
  --config=/etc/otelcol/config.yaml >/dev/null
address=$(docker port "$container" 4318/tcp)
export PRICING_TEST_OTEL_ENDPOINT="http://$address"
.venv/bin/python - <<'PY'
import os,time,urllib.request
from pricing_lab.observability import configure,start_span
endpoint=os.environ['PRICING_TEST_OTEL_ENDPOINT']
for attempt in range(40):
    try:
        urllib.request.urlopen(endpoint+'/v1/traces',timeout=1)
    except urllib.error.HTTPError:
        break # An OTLP receiver rejects GET; that still confirms it is listening.
    except (OSError,urllib.error.URLError):
        time.sleep(.1)
else:
    raise RuntimeError('Local OTLP receiver did not start.')
runtime=configure('pricing-collector-smoke',endpoint)
with start_span('pricing.snapshot',{'pricing.owner_id':'smoke-learner','pricing.generation':3,'pricing.snapshot_id':'smoke-snapshot-001'}):
    pass
assert runtime.force_flush(timeout_millis=5000)
runtime.shutdown()
PY
docker logs "$container" > "$directory/received.log" 2>&1
.venv/bin/python - "$directory/received.log" <<'PY'
import sys
from pathlib import Path
text=Path(sys.argv[1]).read_text()
for marker in ('smoke-learner','smoke-snapshot-001','pricing.generation','pricing.snapshot','pricing.snapshot completed'):
    assert marker in text, f'Missing correlation marker: {marker}'
print('Local OTLP receiver verified traces, operation logs, and correlation IDs. Cloud delivery remains unverified.')
PY
