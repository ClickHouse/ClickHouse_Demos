#!/usr/bin/env bash
# =============================================================================
# Manufactures the 2am hotfix. Applies steps/06-drift/hotfix.sql directly to the
# target, bypassing Atlas and bypassing git, exactly as a real on-call engineer
# would from the ClickHouse Cloud SQL console.
#
#     ./scripts/inject-drift.sh          # cloud
#     ./scripts/inject-drift.sh local
# =============================================================================
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
resolve_target "${1:-cloud}"

say "Applying out-of-band hotfix to ${TARGET_LABEL}"
echo "No pull request. No migration file. No record anywhere but the database."
echo
ch_file "${REPO_ROOT}/steps/06-drift/hotfix.sql"

say "Production now has objects the repository has never heard of"
ch_query "
SELECT name, type, default_expression
FROM system.columns
WHERE database = '${TARGET_DB}' AND table = 'ad_events'
ORDER BY position
FORMAT PrettyCompactMonoBlock"

echo
echo "  TTL is now 6 months, not the 13 the repo asks for:"
# system.tables has no ttl_expression column, so pull it out of the DDL text.
ch_query "
SELECT extractAll(create_table_query, 'TTL [a-zA-Z_]+ \+ [a-zA-Z0-9_()]+')[1] AS current_ttl
FROM system.tables
WHERE database = '${TARGET_DB}' AND name = 'ad_events'
FORMAT PrettyCompactMonoBlock"

echo
echo "  And an undocumented skipping index nobody will remember adding:"
ch_query "
SELECT table, name, type_full, expr, granularity
FROM system.data_skipping_indices
WHERE database = '${TARGET_DB}'
FORMAT PrettyCompactMonoBlock"

hr
echo "Now ask Atlas to compare reality against the repo:"
echo
echo "    atlas schema diff \\"
echo "      --from \"\$CH_CLOUD_URL\" \\"
echo "      --to file://schema/sql/schema.sql \\"
echo "      --dev-url \"\$CH_DEV_URL\" \\"
echo "      --exclude atlas_schema_revisions"
echo
echo "The --exclude matters: atlas migrate apply keeps its revision history in"
echo "adtech.atlas_schema_revisions, which schema.sql does not describe. Without"
echo "it, every drift check also proposes dropping Atlas own bookkeeping table."
echo
echo "Read the output carefully with the room. It proposes reverting all three:"
echo "DROP COLUMN debug_trace_id, DROP INDEX idx_creative, and MODIFY TTL back to"
echo "13 months - because as far as the repo is concerned none of it should exist."
echo "Atlas models skipping indexes, so the index is caught too."
echo
echo "That is the correct behaviour AND the dangerous behaviour. The lesson is"
echo "that drift detection has to run continuously, not at apply time, so the"
echo "conversation happens the next morning instead of during the next deploy."
