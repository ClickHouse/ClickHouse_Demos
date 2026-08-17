#!/usr/bin/env bash
# =============================================================================
# Starts a local ClickHouse OSS container to rehearse against, using
# docker-compose.yml, and prints the exact .env values that match it.
#
#     ./scripts/local-up.sh
#     ./scripts/local-down.sh
#
# WHY COMPOSE AND NOT `atlas tool docker`
#
# This used to wrap `atlas tool docker`. It does not any more, and the reason is
# worth knowing because it is easy to reach for the wrong one:
#
#   `atlas tool docker` exists to hand ATLAS a throwaway database. Atlas owns the
#   connection, so it publishes only the native protocol port, on a RANDOM host
#   port, with a RANDOM generated password. Verified on this repo: it bound 9000
#   to host port 32845 and never published 8123 at all.
#
#   Every helper script here (bootstrap, seed, inject-drift, reset) talks to
#   ClickHouse over HTTP on 8123, because that avoids a clickhouse-client
#   dependency. Against an Atlas-managed container there is no HTTP port to reach
#   and the password is not the one lib.sh expects, so none of them work.
#
# So: `atlas tool docker` stays where it belongs, behind CH_DEV_URL
# (docker://clickhouse/<ver>/dev), where Atlas is the only thing connecting. A
# rehearsal TARGET, which you drive yourself, needs fixed ports and a known
# password. That is what docker-compose.yml gives you.
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

CH_VERSION="${LOCAL_CH_VERSION:-26.6}"
NAME="${LOCAL_CH_NAME:-atlas-demo-ch}"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker daemon is not reachable." >&2
  exit 1
fi

say "Starting local ClickHouse ${CH_VERSION} as container '${NAME}'"
echo "Pin this version close to your ClickHouse Cloud version. Mismatch here is"
echo "the parity risk SETUP.md step 6 talks about, so it is better to see it than"
echo "to let it hide."
echo

LOCAL_CH_VERSION="${CH_VERSION}" LOCAL_CH_NAME="${NAME}" \
  docker compose -f "${REPO_ROOT}/docker-compose.yml" up -d

say "Waiting for ClickHouse to accept queries"
READY=0
for i in $(seq 1 60); do
  if curl --silent --fail --user "default:localpass" \
       --data-binary "SELECT 1" "http://localhost:8123/" >/dev/null 2>&1; then
    echo "  ready after ${i}s"
    READY=1
    break
  fi
  sleep 1
done
if [[ "${READY}" -ne 1 ]]; then
  echo "  ERROR: not accepting queries after 60s." >&2
  echo "  Check: docker compose -f docker-compose.yml logs" >&2
  exit 1
fi

hr
echo "These are already the defaults in .env.example, so there is normally"
echo "nothing to edit:"
echo
echo "    CH_LOCAL_URL=clickhouse://default:localpass@localhost:9000/adtech"
echo "    CH_LOCAL_HTTP=http://localhost:8123"
echo
echo "Both ports are published and the password is fixed, so the helper scripts"
echo "and Atlas agree on how to reach this container."
hr
echo "Rehearse the whole demo here before you point anything at Cloud:"
echo
echo "    ./scripts/bootstrap.sh local"
echo "    ./scripts/seed.sh local"
echo "    ./scripts/use-step.sh 1 && atlas migrate diff add_device_type --env local"
echo
echo "Tear down when you are done:  ./scripts/local-down.sh"
