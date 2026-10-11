#!/usr/bin/env bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
module=${1:-setup}
exec "$lab_root/.venv/bin/workshop" preflight --module "$module"
