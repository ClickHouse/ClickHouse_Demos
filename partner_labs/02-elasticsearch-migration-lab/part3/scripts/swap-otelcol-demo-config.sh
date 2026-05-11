#!/bin/bash
# ============================================================
# swap-otelcol-demo-config.sh — swap otelcol-demo container config
# ============================================================
# Recreates the otelcol-demo container with a Part 3 config (parallel-run
# or cutover) without editing the Part 1 baseline file. Both phases use
# `docker compose up -d --force-recreate --no-deps` with a Part 3 compose
# override that mounts the appropriate config and overrides `command:`.
#
# Usage (from part3/):
#   source ../common/env.sh
#   bash scripts/swap-otelcol-demo-config.sh parallel
#   bash scripts/swap-otelcol-demo-config.sh cutover
# ============================================================

set -euo pipefail

PHASE="${1:-}"

if [[ "$PHASE" != "parallel" && "$PHASE" != "cutover" ]]; then
    echo "Usage: $0 [parallel|cutover]"
    exit 1
fi

if [[ -z "${CH_HOST:-}" ]]; then
    echo "Error: CH_HOST not set. Run 'source ../common/env.sh' first."
    exit 1
fi
if [[ -z "${CH_PASSWORD:-}" ]]; then
    echo "Error: CH_PASSWORD not set. Run 'source ../common/env.sh' first."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PART3_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PART3_DIR"

# Compose resolves relative paths in overlay files against the project
# directory (part1/docker/), not against the overlay file's own directory.
# Pass an absolute path to part3/configs/ via env var so the overlay's
# volume mount works regardless of which directory the user invokes from.
export PART3_CONFIG_DIR="${PART3_DIR}/configs"

echo "→ Recreating otelcol-demo with ${PHASE} config..."
docker compose \
    -f ../part1/docker/docker-compose.source.yml \
    -f ../part1/docker/docker-compose.otel-demo.yml \
    -f "docker/docker-compose.otel-demo.${PHASE}.yml" \
    up -d --force-recreate --no-deps otelcol-demo

echo
echo "✓ otelcol-demo recreated with ${PHASE} config."
echo "  Verify with: docker logs docker-otelcol-demo-1 --tail=20"
