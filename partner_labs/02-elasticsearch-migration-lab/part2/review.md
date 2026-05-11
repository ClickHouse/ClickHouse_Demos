New Issue A — High | ReplacingMergeTree syntax is invalid in the backfill escape hatch
solutions/adr-solution.md, Decision 7:


Dedup strategy: swap the target engine to ReplacingMergeTree((cityHash64(Body), Timestamp))
ReplacingMergeTree takes a single version column name as its optional parameter — not a tuple or expression. ReplacingMergeTree((cityHash64(Body), Timestamp)) is invalid syntax and would error at CREATE TABLE time. The correct approach is to add a computed column first, then reference it:


-- Add a stable row hash so ReplacingMergeTree has a version column to work with
ALTER TABLE otel_logs ADD COLUMN RowHash UInt64 DEFAULT cityHash64(ServiceName, toUnixTimestamp(Timestamp), Body);
-- Engine declaration:
ENGINE = ReplacingMergeTree(RowHash)
ORDER BY (ServiceName, Timestamp, RowHash)
Then OPTIMIZE TABLE otel_logs FINAL deduplicates rows with the same ORDER BY key, keeping the one with the highest RowHash (identical content → same hash → same row wins, so it's stable). Alternatively, for a simpler escape hatch, skip ReplacingMergeTree entirely and deduplicate at query-read time with SELECT DISTINCT.

New Issue B — Low | _field_caps?fields=* inflates the field count with ES meta-fields
README.md, field count command:


curl -s "http://localhost:9200/logs-web_access-lab/_field_caps?fields=*" \
  | jq '.fields | length'
The _field_caps API includes ES system/meta-fields (_id, _index, _source, _type, _routing, _seq_no, _primary_term) in the response. These inflate the count by ~7. The solution says ~83 fields, but this command will return ~90 on most clusters. Filter them out:


curl -s "http://localhost:9200/logs-web_access-lab/_field_caps?fields=*" \
  | jq '[.fields | to_entries[] | select(.key | startswith("_") | not)] | length'