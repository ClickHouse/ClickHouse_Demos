#!/usr/bin/env bash
# Verifies that every dashboard panel's SQL is byte-identical to the harness's query files.
# This is the mechanical form of the workshop's central claim: the application's SQL does
# not change across the migration. If a panel and its query file drift apart, the "before"
# and "after" benchmark numbers stop measuring the same thing, and the claim that nothing
# in the application changed stops being checkable.
#
# The check runs in both directions on purpose. Forwards, so no query file is missing from
# the dashboard; backwards, so no panel carries SQL that exists nowhere on disk.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dashboard="$root/grafana/dashboards/shop-ops.json"
status=0

if [ ! -f "$dashboard" ]; then
  echo "Dashboard not found: $dashboard" >&2
  exit 1
fi

for file in "$root"/bench/queries/*.sql; do
  if ! python3 - "$dashboard" "$file" <<'PY'
import json, sys
dashboard, query_file = sys.argv[1], sys.argv[2]
wanted = open(query_file).read().strip()
panels = json.load(open(dashboard)).get("panels", [])
found = any((t.get("rawSql") or "").strip() == wanted
            for p in panels for t in p.get("targets", []))
sys.exit(0 if found else 1)
PY
  then
    echo "Panel SQL missing or altered for $(basename "$file")" >&2
    status=1
  fi
done

if ! python3 - "$dashboard" "$root/bench/queries" <<'PY'
import json, sys
from pathlib import Path
dashboard, query_dir = sys.argv[1], Path(sys.argv[2])
on_disk = {p.read_text().strip() for p in query_dir.glob("*.sql")}
panels = json.load(open(dashboard)).get("panels", [])
status = 0
seen = 0
for panel in panels:
    for target in panel.get("targets", []):
        sql = (target.get("rawSql") or "").strip()
        if not sql:
            continue
        seen += 1
        if sql not in on_disk:
            print("Panel %r carries SQL that is in no bench/queries file" % panel.get("title"),
                  file=sys.stderr)
            status = 1
if seen != len(on_disk):
    print("Panel SQL count %d does not match query file count %d" % (seen, len(on_disk)),
          file=sys.stderr)
    status = 1
sys.exit(status)
PY
then
  status=1
fi

exit $status
