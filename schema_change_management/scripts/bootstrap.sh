#!/usr/bin/env bash
# =============================================================================
# Applies the BASELINE schema to an EXISTING adtech database, directly, without
# Atlas. It does not create the database — setup/01-users-and-grants.sql does
# that, once, as an admin (SETUP.md step 5). See the comment above the guard below.
#
# Bypassing Atlas is on purpose: it simulates the realistic starting point, an
# existing ClickHouse database that nobody has under version control yet.
#
# The first thing you then do in the demo is bring it UNDER control by
# inspecting it, which is a much more relatable story than starting from empty.
#
#     ./scripts/bootstrap.sh          # cloud
#     ./scripts/bootstrap.sh local
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
resolve_target "${1:-cloud}"

say "Bootstrapping baseline schema on ${TARGET_LABEL}"

# The database is NOT created here, on purpose.
#
# setup/01-users-and-grants.sql creates it, as an admin, once. The atlas_admin
# user this demo connects with deliberately has no CREATE DATABASE grant, because
# a schema migration tool should never be able to create or drop a database.
# If bootstrap created it, we would have to widen that grant and lose the point.
if [[ "$(ch_query_nodb "SELECT count() FROM system.databases WHERE name = '${TARGET_DB}'" 2>/dev/null || echo 0)" != "1" ]]; then
  echo "ERROR: database '${TARGET_DB}' does not exist." >&2
  echo >&2
  echo "Create it once, as an admin user (SETUP.md step 5):" >&2
  echo "    CREATE DATABASE ${TARGET_DB};" >&2
  echo >&2
  echo "Running setup/01-users-and-grants.sql does this for you." >&2
  exit 1
fi
echo "  database ${TARGET_DB} present"

ch_file "${REPO_ROOT}/steps/00-baseline.sql"

say "Objects now present"
ch_query "SELECT name, engine FROM system.tables WHERE database = '${TARGET_DB}' ORDER BY name FORMAT PrettyCompactMonoBlock"

hr
echo "Note the engine names above."
echo "On ClickHouse Cloud you asked for MergeTree and got SharedMergeTree."
echo "That promotion is automatic, and it is why the schema file must be"
echo "written in OSS engine terms if you want a local dev database to work."
echo
echo "Next: ./scripts/seed.sh ${1:-cloud}"
