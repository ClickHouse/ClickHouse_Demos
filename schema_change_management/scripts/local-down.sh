#!/usr/bin/env bash
# =============================================================================
# Stops and removes the local ClickHouse container started by local-up.sh.
#
#     ./scripts/local-down.sh          # keep the data volume
#     ./scripts/local-down.sh --wipe   # also delete the data
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

NAME="${LOCAL_CH_NAME:-atlas-demo-ch}"
CH_VERSION="${LOCAL_CH_VERSION:-26.6}"

say "Stopping container '${NAME}'"
ARGS=(down)
[[ "${1:-}" == "--wipe" ]] && { ARGS+=(-v); echo "  --wipe: the data volume goes too"; }

LOCAL_CH_VERSION="${CH_VERSION}" LOCAL_CH_NAME="${NAME}" \
  docker compose -f "${REPO_ROOT}/docker-compose.yml" "${ARGS[@]}" 2>/dev/null \
  || docker rm -f "${NAME}" >/dev/null 2>&1 \
  || echo "  nothing to remove"
echo "  done"
