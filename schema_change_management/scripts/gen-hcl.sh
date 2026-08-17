#!/usr/bin/env bash
# =============================================================================
# Regenerates schema/hcl/schema.generated.hcl and schema/sql/schema.inspected.sql
# from a live database, so the HCL you show is Atlas's own canonical output rather
# than something hand-written and possibly wrong for your Atlas version.
#
#     ./scripts/gen-hcl.sh          # from cloud
#     ./scripts/gen-hcl.sh local
#
# Also runs the round-trip parity test: anything that does not survive
# database -> Atlas -> SQL is something Atlas does not model, and therefore
# something it cannot defend against drift.
#
# Both inspects are written to temp files first and only moved into place if they
# succeed. Without that, a `>` redirect truncates the previous good output before
# Atlas has said whether it can connect, and an idle ClickHouse Cloud service
# (i/o timeout on the first call) leaves you with an empty HCL file next to a
# stale .inspected.sql from a DIFFERENT database. Diffing those two produces
# confident nonsense, which is the worst possible outcome for a parity test.
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

TARGET="${1:-cloud}"
if [[ "$TARGET" == "local" ]]; then URL="${CH_LOCAL_URL:?}"; else URL="${CH_CLOUD_URL:?}"; fi

HCL_OUT="${REPO_ROOT}/schema/hcl/schema.generated.hcl"
SQL_OUT="${REPO_ROOT}/schema/sql/schema.inspected.sql"
TMP_HCL="$(mktemp)"; TMP_SQL="$(mktemp)"
trap 'rm -f "${TMP_HCL}" "${TMP_SQL}"' EXIT

inspect_to() {
  local fmt="$1" dest="$2" label="$3"
  if ! atlas schema inspect --url "${URL}" --format "${fmt}" > "${dest}" 2>"${dest}.err"; then
    echo "  ERROR: could not inspect ${TARGET} as ${label}." >&2
    sed 's/^/    /' "${dest}.err" >&2
    rm -f "${dest}.err"
    echo >&2
    echo "  If that is an i/o timeout, the service was idle. Wake it and retry:" >&2
    echo "      curl --user \"\$CH_CLOUD_USER:\$CH_CLOUD_PASSWORD\" \\" >&2
    echo "           --data-binary 'SELECT 1' \"\$CH_CLOUD_HTTP/\"" >&2
    echo >&2
    echo "  Existing ${HCL_OUT##*/} / ${SQL_OUT##*/} were left untouched, so you are" >&2
    echo "  not about to diff stale output against fresh output." >&2
    exit 1
  fi
  rm -f "${dest}.err"
  [[ -s "${dest}" ]] || { echo "  ERROR: ${label} inspect returned nothing." >&2; exit 1; }
}

say "Inspecting ${TARGET} as HCL"
inspect_to '{{ hcl . }}' "${TMP_HCL}" "HCL"
say "Same database, inspected as SQL"
inspect_to '{{ sql . }}' "${TMP_SQL}" "SQL"

mv "${TMP_HCL}" "${HCL_OUT}"
mv "${TMP_SQL}" "${SQL_OUT}"
echo "  wrote schema/hcl/schema.generated.hcl"
echo "  wrote schema/sql/schema.inspected.sql"

# -----------------------------------------------------------------------------
# The parity test, normalised.
#
# A plain `diff <(sort a) <(sort b)` is unusable: schema.sql is written for humans
# with aligned columns and long comment banners, Atlas emits one normalised space
# and no comments. Measured on this repo that diff was 107 lines against 41 with
# essentially every line differing, so a genuinely dropped attribute is invisible.
#
# Stripping comments and collapsing whitespace leaves the differences that mean
# something.
# -----------------------------------------------------------------------------
hr
say "Round-trip parity: schema/sql/schema.sql vs what Atlas read back"
# Collapse each SQL statement onto ONE line before sorting. Sorting raw lines
# compares a multi-line human-formatted CREATE against Atlas's single-line output
# and every line looks different. Statement-level is the only granularity where
# the comparison means anything.
norm() {
  grep -vE '^[[:space:]]*(--|$)' "$1" \
    | tr '\n' ' ' \
    | sed -e 's/;/;\n/g' -e 's/  */ /g' -e 's/^ //' -e 's/ $//' \
    | grep -vE '^[[:space:]]*$' \
    | sort
}
if diff <(norm "${REPO_ROOT}/schema/sql/schema.sql") <(norm "${SQL_OUT}") > /dev/null; then
  echo "  identical after normalisation. Everything you wrote survived the round trip."
else
  # `|| true` is required, not defensive: diff exits 1 when it finds differences,
  # and under `set -euo pipefail` (from lib.sh) that aborts the script mid-report.
  # Without it, everything below this line silently never prints and gen-hcl.sh
  # exits 1 on the completely normal "there are differences" path.
  diff <(norm "${REPO_ROOT}/schema/sql/schema.sql") <(norm "${SQL_OUT}") | sed 's/^/    /' || true
  echo
  echo "  Read that as three separate things:"
  echo "    1. Objects the database has and your file does not. Expected if you have"
  echo "       applied migrations (atlas_schema_revisions) or injected drift."
  echo "    2. Rendering differences. Atlas normalises: CODEC(Delta, ...) comes back"
  echo "       as CODEC(Delta(4), ...), a skipping index as ((expr)), and PRIMARY KEY"
  echo "       is made explicit. Cosmetic, but know them so you can dismiss them."
  echo "    3. Anything ELSE missing from the right-hand side is the finding: an"
  echo "       attribute Atlas does not model, and therefore will not defend."
fi
hr
echo "Put schema.generated.hcl next to schema/sql/schema.sql on screen and ask the"
echo "room which they would rather review in a pull request. For a ClickHouse-only"
echo "team the answer is usually SQL."

# Differences are the expected outcome of this test, not a failure of it.
exit 0
