#!/usr/bin/env bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
test -x .venv/bin/python || { echo 'Install the Python 3.12 lab dependencies first.' >&2; exit 1; }
exec .venv/bin/python -u -m infra.setup_cloud "$@"
