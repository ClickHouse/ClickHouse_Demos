#!/usr/bin/env bash
# =============================================================================
# Swaps the desired-state file to a given scenario. This is standing in for
# "an engineer edits schema.sql and opens a pull request".
#
#     ./scripts/use-step.sh 0    # baseline
#     ./scripts/use-step.sh 1    # scenario 1: additive column
#     ./scripts/use-step.sh 2    # scenario 2: create a table
#     ./scripts/use-step.sh 3    # scenario 3: drop a table
#     ./scripts/use-step.sh 4    # scenario 4: dangerous change
#     ./scripts/use-step.sh 5    # scenario 5: MV chain evolution
#
# Scenario 6 (drift) has no desired-state file: it is applied straight to the
# database by ./scripts/inject-drift.sh, which is the whole point of it.
#
# It prints the diff itself, so there is nothing to remember and no dependency on
# this directory being a git working tree.
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

STEP="${1:-}"
case "$STEP" in
  0) SRC="${REPO_ROOT}/steps/00-baseline.sql";              NAME="baseline" ;;
  1) SRC="${REPO_ROOT}/steps/01-additive/schema.sql";       NAME="additive column" ;;
  2) SRC="${REPO_ROOT}/steps/02-new-table/schema.sql";      NAME="create a table" ;;
  3) SRC="${REPO_ROOT}/steps/03-drop-table/schema.sql";     NAME="drop a table" ;;
  4) SRC="${REPO_ROOT}/steps/04-dangerous/schema.sql";      NAME="dangerous change" ;;
  5) SRC="${REPO_ROOT}/steps/05-mv-chain/schema.sql";       NAME="materialized view chain" ;;
  *) echo "Usage: $0 {0|1|2|3|4|5}   (scenario 6 is ./scripts/inject-drift.sh)" >&2; exit 1 ;;
esac

DST="${REPO_ROOT}/schema/sql/schema.sql"

# Show the change before making it, the way a reviewer would see it in a PR.
# Comments and blank lines are stripped: the step files carry long explanatory
# headers that differ completely and would bury the one line that matters.
if [[ -f "$DST" ]]; then
  say "What changes in the desired state (DDL only, comments stripped)"
  if diff -u --label "current" --label "step ${STEP}" \
       <(grep -vE '^[[:space:]]*(--|$)' "$DST") \
       <(grep -vE '^[[:space:]]*(--|$)' "$SRC"); then
    echo "  (no change — the desired state was already step ${STEP})"
  fi
fi

cp "$SRC" "$DST"
say "Desired state is now: step ${STEP} (${NAME})"
echo "Read the full file with its reasoning:"
echo
echo "    less ${SRC#"${REPO_ROOT}"/}"
echo
echo "Then ask Atlas what it would do:"
echo
echo "    atlas schema diff --env cloud --from \"\$CH_CLOUD_URL\" \\"
echo "                      --to file://schema/sql/schema.sql"
