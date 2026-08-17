#!/usr/bin/env bash
# =============================================================================
# Puts the demo back to its starting state so you can run it twice, or recover
# if something goes sideways mid-meeting.
#
#     ./scripts/reset.sh          # cloud
#     ./scripts/reset.sh local
#
# Drops every table and view inside the adtech database (the database itself
# survives), restores the baseline schema file, and clears migrations/.
#
# Because migrations/ is cleared, re-run the baselining block in SETUP.md step 9
# after bootstrap + seed, or the next `atlas migrate diff` regenerates the whole
# schema instead of the one change you meant to show.
#
# Destructive. It only ever touches the `adtech` database.
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
resolve_target "${1:-cloud}"

read -r -p "Drop every table and view in '${TARGET_DB}' on ${TARGET_LABEL}? [y/N] " ans
[[ "${ans:-n}" =~ ^[Yy]$ ]] || { echo "aborted"; exit 0; }

# Objects are dropped individually rather than dropping the whole database, so
# this works with the least-privilege atlas_admin user from
# setup/01-users-and-grants.sql, which has DROP TABLE / DROP VIEW on adtech.* but
# deliberately no DROP DATABASE. The database itself survives.
#
# Materialized views are dropped first: dropping a target table out from under a
# live MV leaves the MV pointing at nothing.
for kind in "MaterializedView" "View" ""; do
  if [[ -n "$kind" ]]; then
    FILTER="AND engine = '${kind}'"
  else
    FILTER="AND engine NOT IN ('MaterializedView','View')"
  fi
  OBJS=$(ch_query "SELECT name FROM system.tables WHERE database = '${TARGET_DB}' ${FILTER} FORMAT TSV" 2>/dev/null || true)
  for o in ${OBJS}; do
    if [[ "$kind" == "MaterializedView" || "$kind" == "View" ]]; then
      ch_query "DROP VIEW IF EXISTS \`${TARGET_DB}\`.\`${o}\`" >/dev/null 2>&1 \
        || ch_query "DROP TABLE IF EXISTS \`${TARGET_DB}\`.\`${o}\`" >/dev/null 2>&1 || true
    else
      ch_query "DROP TABLE IF EXISTS \`${TARGET_DB}\`.\`${o}\`" >/dev/null 2>&1 || true
    fi
    echo "  dropped ${o}"
  done
done

REMAIN=$(ch_query "SELECT count() FROM system.tables WHERE database = '${TARGET_DB}'" 2>/dev/null || echo "?")
echo "  objects remaining in ${TARGET_DB}: ${REMAIN}"

cp "${REPO_ROOT}/steps/00-baseline.sql" "${REPO_ROOT}/schema/sql/schema.sql"
echo "  schema/sql/schema.sql restored to baseline"

rm -f "${REPO_ROOT}"/migrations/*.sql "${REPO_ROOT}"/migrations/atlas.sum
echo "  migrations/ cleared"

rm -f "${REPO_ROOT}/schema/hcl/schema.generated.hcl" \
      "${REPO_ROOT}/schema/sql/schema.inspected.sql"
echo "  generated files removed"

hr
echo "Clean. Next: ./scripts/bootstrap.sh ${1:-cloud} && ./scripts/seed.sh ${1:-cloud}"
echo "            ./scripts/use-step.sh 0, then re-baseline migrations/ (SETUP.md step 9)"
