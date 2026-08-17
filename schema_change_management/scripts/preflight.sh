#!/usr/bin/env bash
# =============================================================================
# Run this BEFORE the meeting. It checks every moving part and, when CH_DEV_URL
# is a docker:// image, prints the version gap between that dev database and the
# ClickHouse Cloud service plans will actually be applied to.
#
#     ./scripts/preflight.sh
#     ./scripts/preflight.sh local
#
# Safe to run at any point in setup, including before the adtech database
# exists. A missing database is a WARN with the exact command that fixes it, not
# a FAIL — you are meant to be able to run this the moment .env is filled in.
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
resolve_target "${1:-cloud}"

FAIL=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; FAIL=1; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$*"; }

say "1. Tooling"

HAS_ATLAS=0
if command -v atlas >/dev/null 2>&1; then
  HAS_ATLAS=1
  ok "atlas CLI: $(atlas version 2>/dev/null | head -1)"
else
  bad "atlas CLI not on PATH. Install: curl -sSf https://atlasgo.sh | sh"
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ok "docker daemon reachable"
else
  if [[ "${CH_DEV_URL:-}" == docker://* ]]; then
    bad "docker not reachable, but CH_DEV_URL is a docker:// URL"
  else
    warn "docker not reachable (fine, CH_DEV_URL is not docker://)"
  fi
fi

if command -v curl >/dev/null 2>&1; then
  ok "curl present"
else
  bad "curl not found (helper scripts need it)"
fi

say "2. Atlas login  (the ClickHouse driver needs an Atlas Pro entitlement)"
if [[ "${HAS_ATLAS}" -ne 1 ]]; then
  bad "skipped: atlas CLI not installed (see step 1 above)"
elif atlas whoami >/dev/null 2>&1; then
  ok "logged in: $(atlas whoami 2>/dev/null | tr '\n' ' ')"
else
  bad "not logged in. Run: atlas login    <- ClickHouse will not work without this"
fi

say "3. Target database: ${TARGET_LABEL}"
# Deliberately unscoped: the adtech database may not exist yet on a fresh
# service, and scoping this query to it would report UNKNOWN_DATABASE, which
# looks like a connectivity problem and wastes ten minutes.
if TARGET_VER=$(ch_query_nodb "SELECT version()" 2>/dev/null); then
  ok "reachable, server version ${TARGET_VER}"
else
  bad "cannot reach ${TARGET_HTTP} over HTTP. Check host, port 8443, credentials."
  TARGET_VER=""
fi

# Three states, not two. "unknown" matters because the query below reads
# system.databases, which ClickHouse SILENTLY ROW-FILTERS for a user without
# SELECT ON system.* (see the long note in section 3b). Telling someone to
# CREATE DATABASE when the real problem is a missing grant is the same category
# of misdiagnosis this script exists to prevent.
DBEXISTS=unknown
if [[ -n "${TARGET_VER}" ]]; then
  # ch_query_nodb uses --fail-with-body, so an error prints the body on stdout.
  # Only accept a value that is literally 0 or 1.
  DBRAW=$(ch_query_nodb "SELECT count() FROM system.databases WHERE name = '${TARGET_DB}'" 2>/dev/null || true)
  DBRAW="${DBRAW//[[:space:]]/}"
  case "${DBRAW}" in
    1) DBEXISTS=1 ;;
    0) DBEXISTS=0 ;;
    *) DBEXISTS=unknown ;;
  esac

  if [[ "${DBEXISTS}" == "1" ]]; then
    ok "database '${TARGET_DB}' exists"
  elif [[ "${DBEXISTS}" == "unknown" ]]; then
    warn "could not determine whether '${TARGET_DB}' exists. See the grant check below."
  else
    # Not a failure. This check is meant to be runnable before the database is
    # created. But name the command that actually creates it: bootstrap.sh does
    # NOT, and sending people there costs a round trip.
    warn "database '${TARGET_DB}' does not exist yet. Nothing below can inspect it."
    if [[ "${1:-cloud}" == "local" ]]; then
      echo "        The container should have been started with it. Recreate it:"
      echo "            ./scripts/local-down.sh && ./scripts/local-up.sh"
      echo "        Then: ./scripts/bootstrap.sh local"
    else
      echo "        Create it once, as an admin user, in the Cloud SQL console:"
      echo "            CREATE DATABASE ${TARGET_DB};"
      echo "        setup/01-users-and-grants.sql does this and creates the scoped"
      echo "        users (SETUP.md step 5). Then: ./scripts/bootstrap.sh cloud"
    fi
  fi
fi

if [[ -n "${TARGET_VER}" ]]; then
  say "3b. Permissions  (GRANT SELECT ON system.* — the one people forget)"
  #
  # Why this check needs care. ClickHouse treats these two groups differently,
  # verified on ClickHouse 26.8:
  #
  #   HARD DENIED without the grant:
  #     system.parts, system.mutations, system.data_skipping_indices, system.clusters
  #
  #   SILENTLY ROW-FILTERED without the grant:
  #     system.tables, system.columns, system.databases
  #     A user missing the grant saw 1 table where an authorised user saw 127.
  #     The query SUCCEEDS and returns almost nothing.
  #
  # So probing system.tables proves nothing. We probe system.parts, which fails
  # loudly, and then separately report how much the user can actually see.
  #
  # This matters because the row-filtered case is the nastiest failure mode in the
  # whole setup: Atlas reads a nearly-empty database, concludes nothing exists, and
  # generates a plan that creates everything from scratch. No error anywhere.
  #
  if ch_query_nodb_retry "SELECT count() FROM system.parts" >/dev/null 2>&1; then
    ok "SELECT ON system.* is present"
  else
    bad "missing the system grant. Run: GRANT SELECT ON system.* TO <your user>"
    echo "        See SETUP.md step 5."
    echo "        Symptom if you skip it: no error, but every diff proposes CREATE for"
    echo "        objects that already exist, because Atlas cannot see them."
  fi

  if ch_query_nodb_retry "SELECT count() FROM system.data_skipping_indices" >/dev/null 2>&1; then
    ok "can read system.data_skipping_indices (scenario 6 needs this)"
  else
    warn "cannot read system.data_skipping_indices; scenario 6 will show less"
  fi

  VISIBLE=$(ch_query_nodb "SELECT count() FROM system.tables WHERE database = '${TARGET_DB}'" 2>/dev/null || echo "?")
  echo "  connected as: $(ch_query_nodb "SELECT currentUser()" 2>/dev/null || echo unknown)"
  echo "  tables visible in '${TARGET_DB}': ${VISIBLE}"
  if [[ "${VISIBLE}" == "0" && "${DBEXISTS}" == "1" ]]; then
    warn "database exists but you can see 0 tables in it."
    echo "        Either it is not bootstrapped yet (fine, run bootstrap), or your"
    echo "        grants hide it (not fine). Check the grants printed below."
  fi
  echo "  grants:"
  ch_query_nodb "SHOW GRANTS FORMAT TSVRaw" 2>/dev/null | sed 's/^/    /' \
    || echo "    (could not read own grants; not fatal)"
fi

say "4. Atlas connectivity over the native protocol"
#
# The database name in a ClickHouse URL travels in the native-protocol handshake,
# not per query, so connecting with a database that does not exist is refused at
# connect time with UNKNOWN_DATABASE (code 81). That is indistinguishable, from
# the exit code alone, from a wrong port or a blocked IP.
#
# So: whenever the scoped inspect fails, re-probe against `default` — always
# present on ClickHouse Cloud, and the database Atlas's own Cloud examples connect
# to. The re-probe uses the same credentials over the same port, so it cannot mask
# a real auth or network problem; it can only separate "cannot connect" from
# "connected fine, that one database is not reachable".
#
# It is deliberately NOT gated on DBEXISTS. DBEXISTS is derived from CH_CLOUD_DB,
# while the connection uses whatever database CH_CLOUD_URL happens to name, and
# those two can disagree.
#
if [[ "${1:-cloud}" == "local" ]]; then ATLAS_URL="${CH_LOCAL_URL:-}"; else ATLAS_URL="${CH_CLOUD_URL:-}"; fi
URL_DB="$(atlas_url_db "${ATLAS_URL:-}")"
if [[ -z "${ATLAS_URL}" ]]; then
  bad "connection URL for target is empty. Check .env"
elif [[ "${HAS_ATLAS}" -ne 1 ]]; then
  bad "skipped: atlas CLI not installed (see step 1 above)"
elif atlas schema inspect --url "${ATLAS_URL}" >/dev/null 2>&1; then
  ok "atlas schema inspect succeeded (native protocol + TLS path is good)"
elif atlas schema inspect --url "$(atlas_url_with_db "${ATLAS_URL}" default)" >/dev/null 2>&1; then
  ok "native protocol + TLS path is good (probed against 'default')"
  warn "atlas connected, but cannot inspect '${URL_DB:-${TARGET_DB}}'. Not a connection problem."
  if [[ "${DBEXISTS}" == "0" ]]; then
    echo "        That database does not exist yet — see step 3 above, then re-run this script."
  elif [[ -n "${URL_DB}" && "${URL_DB}" != "${TARGET_DB}" ]]; then
    echo "        Note the mismatch: CH_CLOUD_URL names '${URL_DB}', CH_CLOUD_DB is '${TARGET_DB}'."
    echo "        Make them agree in .env."
  else
    echo "        Either it does not exist, or your user cannot see it. Check the grants above."
  fi
else
  bad "atlas cannot connect at all — this is not just a missing database."
  echo "        The probe against 'default' failed too, so the connection itself is bad."
  echo "        Three causes, in order of likelihood:"
  echo "          1. IP access list does not include you (SETUP.md step 4)."
  echo "          2. URL is not native 9440 with ?secure=true (Cloud rejects otherwise)."
  echo "          3. Not logged in, or no Atlas Pro entitlement (see step 2 above)."
  echo "        Reproduce with the error text visible:"
  echo "            atlas schema inspect --url \"\$CH_CLOUD_URL\""
fi

say "5. Dev database"
echo "  CH_DEV_URL = $(redact_url "${CH_DEV_URL:-<unset>}")"
DEV_DB="$(atlas_url_db "${CH_DEV_URL:-}")"
if [[ -z "${CH_DEV_URL:-}" ]]; then
  bad "CH_DEV_URL unset. Atlas needs a dev database to plan and validate."
elif [[ "${HAS_ATLAS}" -ne 1 ]]; then
  bad "skipped: atlas CLI not installed (see step 1 above)"
elif atlas schema inspect --url "${CH_DEV_URL}" >/dev/null 2>&1; then
  ok "dev database usable"
  if [[ "${CH_DEV_URL}" == docker://clickhouse/* ]]; then
    DEVVER="${CH_DEV_URL#docker://clickhouse/}"; DEVVER="${DEVVER%%/*}"
    if [[ -n "${TARGET_VER}" ]]; then
      warn "VERSION PARITY: dev is OSS ${DEVVER}, target is ${TARGET_VER}."
      echo "        Plans are validated against ${DEVVER}, then applied to ${TARGET_VER}."
      echo "        Say this out loud in the demo. To remove the gap entirely, point"
      echo "        CH_DEV_URL at a second ClickHouse Cloud service."
    fi
  fi
elif [[ "${CH_DEV_URL}" == docker://* ]]; then
  bad "cannot use dev database. Check the image tag exists on Docker Hub, and that"
  echo "        the docker daemon is reachable (see step 1 above)."
else
  # Same handshake trap as section 4. Atlas wipes and rebuilds objects INSIDE the
  # dev database; it does not create the database itself.
  bad "cannot use dev database '${DEV_DB:-<none>}'."
  if [[ -n "${DEV_DB}" ]]; then
    echo "        Most likely it does not exist yet. Atlas creates and drops objects"
    echo "        inside the dev database, but the database itself must already be"
    echo "        there. On the dev service, as an admin:"
    echo "            CREATE DATABASE ${DEV_DB};"
  fi
  echo "        Otherwise check credentials, port 9440 and ?secure=true."
fi

hr
if [[ "${FAIL}" -eq 0 ]]; then
  printf '\033[32mPreflight passed.\033[0m Read any WARN lines above before you present.\n'
  echo
  if [[ "${DBEXISTS}" != "1" ]]; then
    if [[ "${1:-cloud}" == "local" ]]; then
      echo "Next: start the container (./scripts/local-up.sh), re-run this script, then:"
    else
      echo "Next: create the database (SETUP.md step 5), re-run this script, then:"
    fi
  else
    echo "Next:"
  fi
  echo "    ./scripts/bootstrap.sh ${1:-cloud} && ./scripts/seed.sh ${1:-cloud}"
  echo "    ./scripts/use-step.sh 0"
  echo "    then baseline the migration directory — SETUP.md step 9. Skipping that"
  echo "    makes scenario 1 generate the whole schema instead of one ADD COLUMN."
else
  printf '\033[31mPreflight failed.\033[0m Fix the FAIL lines before the meeting.\n'
  exit 1
fi
