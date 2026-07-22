#!/usr/bin/env bash
# End-to-end workshop stack provisioning via clickhousectl (clickhousectl):
# managed Postgres -> ClickHouse Cloud service -> Postgres CDC ClickPipe -> data sync.
#
# Instructor-side tool. It stands up the SHARED Postgres every participant's
# ClickPipe reads from, provisions the per-participant slots, and can bring up
# a complete demo stack (instructor's own ClickHouse service + CDC pipe) to
# prove the whole chain end to end — the same chain participants build by hand.
#
# Subcommands:
#   PG:   create-pg | wait-pg | configure-pg | verify-pg
#   Slots: provision (participants 01..N) | provision-demo (slot 00) | slips
#   CH:   create-ch | schema
#   CDC:  create-pipe | wait-pipe | create-mv | verify-sync
#   All:  e2e   (create-pg -> wait-pg -> configure-pg -> provision-demo -> create-ch
#                -> schema -> create-pipe -> wait-pipe -> create-mv -> verify-sync)
#   Down: teardown (participant slots) | delete-pipe | delete-ch | delete-pg
#
# E2E-verified 2026-07-14 against a live org (see infra/README.md for findings).
# For the reliable ClickHouse query path, export CH_HOST and CH_PASSWORD from the
# create-ch output (the clickhousectl Query API fails on orgs not migrated to Custom Roles).
#
# Auth: org API key (write ops; browser OAuth is read-only):
#   export CLICKHOUSE_CLOUD_API_KEY=...  CLICKHOUSE_CLOUD_API_SECRET=...
#
# Admin Postgres connection (after create-pg, or your own RDS):
#   ADMIN_PGHOST (required) ADMIN_PGPASSWORD (required)
#   ADMIN_PGPORT=5432 ADMIN_PGUSER=postgres ADMIN_PGDATABASE=postgres ADMIN_PGSSLMODE=require
#
# Identifiers (exported after the create steps print them):
#   PG_SERVICE_ID   managed Postgres service id   (clickhousectl cloud postgres list --json)
#   CH_SERVICE_ID   demo ClickHouse service id    (clickhousectl cloud service list --json)
#   PIPE_ID         demo ClickPipe id             (printed by create-pipe)
#
# Tunables:
#   PARTICIPANTS=35  START_SLOT=1  DB_PREFIX=taxi_p  PUB_PREFIX=pub_  OUT_DIR=./out
#   (provision does slots START_SLOT..PARTICIPANTS — raise START_SLOT to add capacity
#    later without re-touching lower slots)
#   PG_SERVICE_NAME=build-workshop-shared-pg  PG_REGION=us-east-1  PG_SIZE=m8gd.large  PG_VERSION=17
#   CH_SERVICE_NAME=build-workshop-demo-ch    CH_REGION=$PG_REGION
#   SCHEMA_FILE=../app/db/cloud/001_cloud_schema.sql
#
# Requirements: bash 3.2+, psql (brew install libpq), openssl, python3,
#               clickhousectl 0.3.x (curl https://clickhouse.com/cli | sh).
set -euo pipefail

PARTICIPANTS="${PARTICIPANTS:-35}"
START_SLOT="${START_SLOT:-1}"
DB_PREFIX="${DB_PREFIX:-taxi_p}"
PUB_PREFIX="${PUB_PREFIX:-pub_}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${OUT_DIR:-$HERE/out}"
CSV="$OUT_DIR/participants.csv"
SCHEMA_FILE="${SCHEMA_FILE:-$HERE/../app/db/cloud/001_cloud_schema.sql}"

ADMIN_PGPORT="${ADMIN_PGPORT:-5432}"
ADMIN_PGUSER="${ADMIN_PGUSER:-postgres}"
ADMIN_PGDATABASE="${ADMIN_PGDATABASE:-postgres}"
ADMIN_PGSSLMODE="${ADMIN_PGSSLMODE:-require}"

PG_SERVICE_NAME="${PG_SERVICE_NAME:-build-workshop-shared-pg}"
PG_REGION="${PG_REGION:-us-east-1}"
PG_SIZE="${PG_SIZE:-m8gd.large}"
PG_VERSION="${PG_VERSION:-17}"

CH_SERVICE_NAME="${CH_SERVICE_NAME:-build-workshop-demo-ch}"
CH_REGION="${CH_REGION:-$PG_REGION}"

DEMO_SLOT="00"
PIPE_NAME="${PIPE_NAME:-taxi-cdc-demo}"

TARGET_SLOTS=50
TARGET_WAL_SENDERS=50
TARGET_CONNECTIONS=300
TARGET_SLOT_WAL_KEEP_MB=20480

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: '$1' not found on PATH" >&2; exit 1; }; }
admin_env_required() {
  : "${ADMIN_PGHOST:?set ADMIN_PGHOST to the shared Postgres endpoint}"
  : "${ADMIN_PGPASSWORD:?set ADMIN_PGPASSWORD}"
}

# psqla [dbname] [extra psql args...] — admin connection, quiet/tuples/no-align.
psqla() {
  local db="${1:-$ADMIN_PGDATABASE}"; shift || true
  PGPASSWORD="$ADMIN_PGPASSWORD" PGSSLMODE="$ADMIN_PGSSLMODE" psql \
    --host "$ADMIN_PGHOST" --port "$ADMIN_PGPORT" --username "$ADMIN_PGUSER" \
    --dbname "$db" --no-psqlrc --quiet --tuples-only --no-align \
    --set ON_ERROR_STOP=1 "$@"
}

# chq <sql> — run a query on the demo ClickHouse service.
# Preferred path: clickhouse client against the service endpoint (set CH_HOST +
# CH_PASSWORD from the create-ch output). Fallback: the clickhousectl Query API — but
# note (verified live) clickhousectl 0.3.1's auto-provisioning fails on organizations
# that have not migrated to Custom Roles ("Use 'roles' instead of
# 'assignedRoleIds'"), so the client path is the reliable one.
chq() {
  if [ -n "${CH_HOST:-}" ] && [ -n "${CH_PASSWORD:-}" ]; then
    clickhouse client --host "$CH_HOST" --secure --password "$CH_PASSWORD" \
      --format TabSeparated --query "$1"
  else
    clickhousectl cloud service query --id "${CH_SERVICE_ID:?set CH_SERVICE_ID or CH_HOST+CH_PASSWORD}" \
      --format TabSeparated --query "$1"
  fi
}

# json_field <key> — best-effort extraction of a field from JSON on stdin.
json_field() {
  python3 -c '
import json, sys
key = sys.argv[1]
def walk(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if k.lower() == key.lower() and isinstance(v, (str, int)):
                yield v
            yield from walk(v)
    elif isinstance(o, list):
        for i in o:
            yield from walk(i)
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
vals = list(walk(data))
print(vals[0] if vals else "")
' "$1"
}

gen_password() { openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 20; }
pnum() { printf '%02d' "$1"; }

csv_row() { # csv_row <slot> -> participant,db,user,pass,pub (or empty)
  [ -f "$CSV" ] && awk -F, -v n="$1" '$1 == n {print; exit}' "$CSV" || true
}

# --------------------------------------------------------------------------
# Postgres (shared, managed by ClickHouse)
# --------------------------------------------------------------------------
cmd_create_pg() {
  need clickhousectl
  echo ">> Creating Postgres service managed by ClickHouse: $PG_SERVICE_NAME ($PG_REGION, $PG_SIZE, pg$PG_VERSION)"
  echo ">> The admin password is shown ONCE below — save it as ADMIN_PGPASSWORD."
  clickhousectl cloud postgres create \
    --name "$PG_SERVICE_NAME" --region "$PG_REGION" \
    --size "$PG_SIZE" --pg-version "$PG_VERSION" --ha-type none
  echo ""
  echo ">> Copy id/hostname/password from the JSON above into your environment:"
  echo "     export PG_SERVICE_ID=<id>  ADMIN_PGHOST=<hostname>  ADMIN_PGPASSWORD=<password>"
  echo ">> VERIFIED LIVE (beta caveat): 'postgres get/list/config patch/delete' can"
  echo "   return FORBIDDEN or empty on orgs without the managed-Postgres read"
  echo "   entitlement — the create response above is the source of truth. The"
  echo "   instance accepts connections within ~a minute: $0 wait-pg"
}

cmd_wait_pg() {
  need psql; admin_env_required
  echo ">> Waiting for Postgres to accept connections (up to 10 minutes)"
  local i
  for i in $(seq 1 40); do
    if psqla postgres -c "SELECT 1" >/dev/null 2>&1; then
      echo ">> Postgres is up: $(psqla postgres -c "SELECT version()" | cut -c1-60)"
      return 0
    fi
    echo "   [$i] not ready yet"
    sleep 15
  done
  echo "ERROR: Postgres did not become reachable" >&2; exit 1
}

cmd_configure_pg() {
  need clickhousectl
  : "${PG_SERVICE_ID:?set PG_SERVICE_ID (clickhousectl cloud postgres list --json)}"
  echo ">> Patching runtime config on $PG_SERVICE_ID"
  if clickhousectl cloud postgres config patch "$PG_SERVICE_ID" \
    --set "max_replication_slots=$TARGET_SLOTS" \
    --set "max_wal_senders=$TARGET_WAL_SENDERS" \
    --set "max_connections=$TARGET_CONNECTIONS" \
    --set "max_slot_wal_keep_size=$TARGET_SLOT_WAL_KEEP_MB"; then
    echo ">> If verify-pg shows old values, restart: clickhousectl cloud postgres restart $PG_SERVICE_ID"
  else
    echo ">> WARNING: config patch failed (VERIFIED LIVE: returns FORBIDDEN on orgs"
    echo "   without the managed-Postgres config entitlement, and ALTER SYSTEM is"
    echo "   blocked in this environment). Raise max_replication_slots/max_wal_senders"
    echo "   via the Cloud console Settings tab for the Postgres service, or via"
    echo "   ClickHouse support. Instance defaults observed live: wal_level=logical,"
    echo "   slots=10, senders=10, max_connections=500 — 10 slots is enough for a"
    echo "   demo/e2e but NOT for a full workshop; this is the D1 provider gate."
  fi
}

cmd_verify_pg() {
  need psql; admin_env_required
  echo ">> Instance settings:"
  local k ok=1 slot_gap=0
  for k in wal_level max_replication_slots max_wal_senders max_connections max_slot_wal_keep_size; do
    echo "   $k = $(psqla postgres -c "SHOW $k")"
  done
  [ "$(psqla postgres -c "SHOW wal_level")" = "logical" ] || { echo "   FAIL: wal_level must be logical"; ok=0; }
  [ "$(psqla postgres -c "SHOW max_replication_slots")" -ge "$TARGET_SLOTS" ] || { echo "   FAIL: max_replication_slots < $TARGET_SLOTS"; ok=0; slot_gap=1; }
  [ "$(psqla postgres -c "SHOW max_wal_senders")" -ge "$TARGET_WAL_SENDERS" ] || { echo "   FAIL: max_wal_senders < $TARGET_WAL_SENDERS"; ok=0; slot_gap=1; }
  if [ "$slot_gap" = "1" ]; then
    echo "   NOTE: this slot/sender shortfall is a known managed-Postgres beta gap"
    echo "     (config patch returns FORBIDDEN, ALTER SYSTEM is blocked). It is needed"
    echo "     ONLY for the SHARED fallback pool at 30+ participants — a participant's"
    echo "     OWN instance needs just one slot and the 10 defaults are plenty. Raise"
    echo "     these via the Cloud console Postgres Settings tab or ClickHouse support"
    echo "     before a full shared run."
  fi
  echo ">> Replication slots:"
  psqla postgres --field-separator ' | ' -c \
    "SELECT slot_name, database, active,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
     FROM pg_replication_slots ORDER BY slot_name" || true
  echo "   participant databases: $(psqla postgres -c "SELECT count(*) FROM pg_database WHERE datname LIKE '${DB_PREFIX}%'")"
  [ "$ok" = "1" ] && echo ">> VERIFY-PG PASS" || { echo ">> VERIFY-PG FAIL"; exit 1; }
}

# --------------------------------------------------------------------------
# Participant slots (databases + roles + publications)
# --------------------------------------------------------------------------
provision_slot() { # provision_slot <NN>
  local n="$1" db role pub pass
  db="${DB_PREFIX}${n}"; role="${DB_PREFIX}${n}"; pub="${PUB_PREFIX}${DB_PREFIX}${n}"

  # One role per slot, used by BOTH the participant's data generator (INSERT)
  # and their CDC ClickPipe (SELECT + REPLICATION). Existing passwords are kept.
  if [ "$(psqla postgres -c "SELECT 1 FROM pg_roles WHERE rolname = '$role'")" != "1" ]; then
    pass="$(gen_password)"
    psqla postgres -c "CREATE ROLE $role LOGIN REPLICATION PASSWORD '$pass'" >/dev/null
    echo "$n,$db,$role,$pass,$pub" >> "$CSV"
  fi
  if [ "$(psqla postgres -c "SELECT 1 FROM pg_database WHERE datname = '$db'")" != "1" ]; then
    psqla postgres -c "CREATE DATABASE $db OWNER $role" >/dev/null
  fi
  # CDC source table (same DDL as the app's local fallback) + pre-created
  # publication, so participants never need CREATE PUBLICATION rights.
  psqla "$db" <<SQL >/dev/null
CREATE TABLE IF NOT EXISTS public.realtime_trips (
  id bigserial PRIMARY KEY,
  pickup_datetime timestamptz NOT NULL,
  dropoff_datetime timestamptz NOT NULL,
  pickup_location_id integer NOT NULL,
  dropoff_location_id integer NOT NULL,
  passenger_count smallint NOT NULL,
  trip_distance double precision NOT NULL,
  fare_amount double precision NOT NULL,
  tip_amount double precision NOT NULL,
  total_amount double precision NOT NULL,
  payment_type smallint NOT NULL,
  vendor_id smallint NOT NULL,
  car_type text NOT NULL DEFAULT 'yellow',
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.realtime_trips OWNER TO $role;
GRANT USAGE, CREATE ON SCHEMA public TO $role;
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '$pub') THEN
    CREATE PUBLICATION $pub FOR TABLE public.realtime_trips;
  END IF;
END \$\$;
SQL
  echo "   $db ready (role=$role, publication=$pub)"
}

ensure_csv() {
  mkdir -p "$OUT_DIR"; chmod 700 "$OUT_DIR"
  if [ ! -f "$CSV" ]; then
    echo "participant,pgdatabase,pguser,pgpassword,publication" > "$CSV"; chmod 600 "$CSV"
  fi
}

cmd_provision() {
  need psql; need openssl; admin_env_required; ensure_csv
  local i
  # START_SLOT (default 1) lets you add capacity later without re-touching lower
  # slots: e.g. START_SLOT=36 PARTICIPANTS=50 provisions 36..50 only. provision_slot
  # is idempotent, so overlapping ranges keep existing passwords.
  for i in $(seq "$START_SLOT" "$PARTICIPANTS"); do provision_slot "$(pnum "$i")"; done
  echo ">> participant slots ${START_SLOT}..${PARTICIPANTS} ready. Credentials: $CSV (mode 600)"
  cmd_slips
}

cmd_provision_demo() {
  need psql; need openssl; admin_env_required; ensure_csv
  provision_slot "$DEMO_SLOT"
  echo ">> Demo slot ${DB_PREFIX}${DEMO_SLOT} ready (used by create-pipe / verify-sync)"
}

cmd_slips() {
  admin_env_required
  [ -f "$CSV" ] || { echo "ERROR: $CSV not found — run provision first" >&2; exit 1; }
  local slips="$OUT_DIR/slips.txt"
  : > "$slips"; chmod 600 "$slips"
  tail -n +2 "$CSV" | while IFS=, read -r n db user pass pub; do
    [ "$n" = "$DEMO_SLOT" ] && continue
    cat >> "$slips" <<EOF
==================== BUILD Workshop — participant $n ====================
Your .env.workshop values (data generator -> shared Postgres):
  PGHOST=$ADMIN_PGHOST
  PGPORT=$ADMIN_PGPORT
  PGDATABASE=$db
  PGUSER=$user
  PGPASSWORD=$pass
  PGSSLMODE=require
  PG_PUBLICATION=$pub

Your ClickPipe (console: Data Sources -> Set up a ClickPipe -> Postgres CDC):
  Host: $ADMIN_PGHOST   Port: $ADMIN_PGPORT   Database: $db
  User: $user   Password: (same as above)   Publication: $pub
  Tables: public.realtime_trips -> destination database nyc_tlc_data, keep table name

Or via CLI (needs an API key in YOUR trial org):
  clickhousectl cloud clickpipe create postgres <your-service-id> \\
    --name taxi-cdc --host $ADMIN_PGHOST --pg-database $db \\
    --username $user --password '$pass' --publication-name $pub \\
    --table-mapping "public.realtime_trips:realtime_trips"

EOF
  done
  echo ">> Slips file: $slips"
}

# --------------------------------------------------------------------------
# ClickHouse (instructor demo service)
# --------------------------------------------------------------------------
cmd_create_ch() {
  need clickhousectl
  echo ">> Creating ClickHouse Cloud service: $CH_SERVICE_NAME ($CH_REGION, 1 x 8GB)"
  echo ">> The default-user password is shown ONCE below — save it if you plan to"
  echo "   run the app against this service (the pipe and schema need only the id)."
  clickhousectl cloud service create \
    --name "$CH_SERVICE_NAME" --provider aws --region "$CH_REGION" \
    --min-replica-memory-gb 8 --max-replica-memory-gb 8 --num-replicas 1 \
    --idle-scaling true --idle-timeout-minutes 15
  echo ""
  echo ">> Next: from the JSON above export CH_SERVICE_ID (service.id), and for the"
  echo "   reliable query path also CH_HOST (https endpoint host) and CH_PASSWORD"
  echo "   (top-level password, shown once). Then: $0 schema"
}

cmd_schema() {
  need clickhousectl
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID (clickhousectl cloud service list --json)}"
  [ -f "$SCHEMA_FILE" ] || { echo "ERROR: schema file not found: $SCHEMA_FILE" >&2; exit 1; }
  echo ">> Applying $SCHEMA_FILE (base tables + views)"
  # 001 now contains only base objects, so it applies cleanly on a fresh service
  # (the CDC materialized view was split into 003_cdc_mv.sql precisely because it
  # referenced the not-yet-existing pipe table). set -e makes a genuine failure
  # fatal, which is what we want now that no statement is expected to fail.
  if [ -n "${CH_HOST:-}" ] && [ -n "${CH_PASSWORD:-}" ]; then
    need clickhouse
    clickhouse client --host "$CH_HOST" --secure --password "$CH_PASSWORD" \
      --multiquery < "$SCHEMA_FILE"
  else
    clickhousectl cloud service query --id "$CH_SERVICE_ID" --queries-file "$SCHEMA_FILE"
  fi
  echo ">> Schema applied. The CDC materialized view is separate: run '$0 create-mv'"
  echo "   after wait-pipe (it detects the pipe's ACTUAL destination table -- CLI-created"
  echo "   pipes land it in the 'default' database, the console wizard in nyc_tlc_data)."
}

cmd_create_mv() {
  # VERIFIED LIVE: a CLI-created pipe always lands its destination table in the
  # 'default' database (the --table-mapping target does NOT accept a database
  # qualifier — a qualified name becomes a literal table name). The console
  # wizard is the only place to choose the destination database. So the MV is
  # created here against the DETECTED location instead of assuming
  # nyc_tlc_data.realtime_trips.
  local loc db tbl
  loc="$(chq "SELECT database || ' ' || name FROM system.tables WHERE name = 'realtime_trips' AND database != 'nyc_tlc_data' LIMIT 1")"
  if [ -z "$loc" ]; then
    loc="$(chq "SELECT database || ' ' || name FROM system.tables WHERE name = 'realtime_trips' LIMIT 1")"
  fi
  [ -n "$loc" ] || { echo "ERROR: realtime_trips not found — has the pipe finished its snapshot? ($0 wait-pipe)" >&2; exit 1; }
  read -r db tbl <<< "$loc"
  echo ">> CDC destination detected: $db.$tbl — creating MV into nyc_tlc_data.taxi_trips"
  chq "
CREATE MATERIALIZED VIEW IF NOT EXISTS nyc_tlc_data.realtime_trips_to_taxi_trips_mv
TO nyc_tlc_data.taxi_trips
AS SELECT
  'yellow' AS car_type,
  pickup_datetime AS pickup_datetime,
  dropoff_datetime AS dropoff_datetime,
  toInt16(vendor_id) AS vendor_id,
  toInt16(passenger_count) AS passenger_count,
  trip_distance AS trip_distance,
  toInt32(pickup_location_id) AS pickup_location_id,
  toInt32(dropoff_location_id) AS dropoff_location_id,
  toInt16(payment_type) AS payment_type,
  fare_amount AS fare_amount,
  tip_amount AS tip_amount,
  total_amount AS total_amount,
  'realtime_cdc' AS filename
FROM $db.$tbl
WHERE _peerdb_is_deleted = 0"
  echo ">> MV created."
}

# --------------------------------------------------------------------------
# CDC (demo ClickPipe: shared PG demo slot -> instructor demo service)
# --------------------------------------------------------------------------
cmd_create_pipe() {
  need clickhousectl; admin_env_required
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID}"
  local row db user pass pub
  row="$(csv_row "$DEMO_SLOT")"
  [ -n "$row" ] || { echo "ERROR: demo slot not provisioned — run: $0 provision-demo" >&2; exit 1; }
  IFS=, read -r _ db user pass pub <<< "$row"
  echo ">> Creating Postgres CDC ClickPipe '$PIPE_NAME': $db -> service $CH_SERVICE_ID"
  clickhousectl cloud clickpipe create postgres "$CH_SERVICE_ID" \
    --name "$PIPE_NAME" \
    --host "$ADMIN_PGHOST" --port "$ADMIN_PGPORT" \
    --pg-database "$db" --username "$user" --password "$pass" \
    --publication-name "$pub" \
    --replication-mode cdc \
    --table-mapping "public.realtime_trips:realtime_trips"
  echo ""
  echo ">> Note the pipe id above, export PIPE_ID=<id>, then: $0 wait-pipe"
  echo ">> VERIFIED LIVE: CLI-created pipes land the destination table in the"
  echo "   'default' database (--table-mapping does not accept a database"
  echo "   qualifier; only the console wizard offers destination-database choice)."
  echo "   That is fine: '$0 create-mv' wires the MV to the detected location."
  echo ">> Also verified: 'clickpipe delete' can print 'Internal error' yet still"
  echo "   succeed — confirm with 'clickpipe list'. Deleting a pipe DOES drop its"
  echo "   replication slot on the source."
}

cmd_wait_pipe() {
  need clickhousectl; need python3
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID}"
  : "${PIPE_ID:?set PIPE_ID (printed by create-pipe, or: clickhousectl cloud clickpipe list $CH_SERVICE_ID --json)}"
  echo ">> Waiting for pipe $PIPE_ID (snapshot + streaming), up to 20 minutes"
  local i state
  for i in $(seq 1 80); do
    state="$(clickhousectl cloud clickpipe get "$CH_SERVICE_ID" "$PIPE_ID" --json 2>/dev/null | json_field state || true)"
    echo "   [$i] state: ${state:-unknown}"
    case "$state" in
      Running|running|RUNNING|Completed|completed) break ;;
      Failed|failed|Error|error) echo "ERROR: pipe entered state $state" >&2; exit 1 ;;
    esac
    sleep 15
  done
  echo ">> Destination table location (CLI pipes land in 'default'; engine observed"
  echo "   live is plain (Shared)MergeTree with _peerdb_synced_at/_peerdb_is_deleted/"
  echo "   _peerdb_version columns):"
  chq "SELECT database, name, engine FROM system.tables WHERE name = 'realtime_trips'" || true
  echo ">> Next: $0 create-mv"
}

cmd_verify_sync() {
  need psql; need clickhousectl; admin_env_required
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID}"
  local db="${DB_PREFIX}${DEMO_SLOT}" marker rows
  marker=$(( $(date +%s) % 100000 ))
  echo ">> Inserting 5 marker rows (vendor_id tag $marker is not used; matching on count) into $db.realtime_trips"
  psqla "$db" -c "
    INSERT INTO public.realtime_trips
      (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,
       passenger_count, trip_distance, fare_amount, tip_amount, total_amount,
       payment_type, vendor_id, car_type)
    SELECT now() - interval '30 seconds', now(), 140 + i, 150 + i,
           1, 2.5, 12.5, 2.0, 14.5, 1, 1, 'yellow'
    FROM generate_series(1, 5) AS i" >/dev/null
  local pg_count
  pg_count="$(psqla "$db" -c "SELECT count(*) FROM public.realtime_trips")"
  echo "   Postgres rows now: $pg_count"

  echo ">> Polling ClickHouse for CDC arrival (sync interval defaults to 60s)"
  # Locate the pipe's destination table (CLI pipes: default.realtime_trips).
  # No FINAL: observed live, the destination engine is plain (Shared)MergeTree,
  # where FINAL is not supported; the loadgen is append-only so a plain count
  # with the _peerdb_is_deleted filter is correct either way.
  local loc cdb ctbl
  loc="$(chq "SELECT database || ' ' || name FROM system.tables WHERE name = 'realtime_trips' AND database != 'nyc_tlc_data' LIMIT 1" 2>/dev/null || true)"
  [ -n "$loc" ] || loc="default realtime_trips"
  read -r cdb ctbl <<< "$loc"
  local i
  for i in $(seq 1 20); do
    rows="$(chq "SELECT count() FROM $cdb.$ctbl WHERE _peerdb_is_deleted = 0" 2>/dev/null || echo 0)"
    echo "   [$i] $cdb.$ctbl: ${rows:-0} rows (target $pg_count)"
    if [ "${rows:-0}" -ge "$pg_count" ] 2>/dev/null; then
      echo ">> CDC SYNC VERIFIED: Postgres -> ClickPipes -> ClickHouse is flowing."
      echo ">> MV fan-out into taxi_trips (requires '$0 create-mv' after wait-pipe):"
      chq "SELECT count() FROM nyc_tlc_data.taxi_trips WHERE filename = 'realtime_cdc'" || true
      return 0
    fi
    sleep 15
  done
  echo "ERROR: rows did not arrive within 5 minutes. Check pipe state/logs:" >&2
  echo "  clickhousectl cloud clickpipe get $CH_SERVICE_ID ${PIPE_ID:-<pipe-id>}" >&2
  exit 1
}

# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------
cmd_e2e() {
  need clickhousectl; need psql; need openssl; need python3
  echo "=== E2E: PG -> ClickHouse -> CDC ==="
  echo ""
  echo "Step 1/8 create-pg (skipped if ADMIN_PGHOST already set: ${ADMIN_PGHOST:-not set})"
  if [ -z "${ADMIN_PGHOST:-}" ]; then
    cmd_create_pg
    echo ""
    echo "E2E PAUSED: the managed Postgres is provisioning. Export PG_SERVICE_ID,"
    echo "ADMIN_PGHOST and ADMIN_PGPASSWORD (password was printed above), then re-run:"
    echo "  $0 e2e"
    exit 0
  fi
  echo "Step 1/8 wait-pg";        cmd_wait_pg
  echo "Step 2/8 configure-pg";   { [ -n "${PG_SERVICE_ID:-}" ] && cmd_configure_pg; } || echo "   (skipped: PG_SERVICE_ID not set — external provider assumed)"
  echo "Step 3/8 verify-pg";      cmd_verify_pg || echo "   (verify-pg flagged targets not met — OK for a demo/e2e run; see configure-pg output)"
  echo "Step 4/8 provision-demo"; cmd_provision_demo
  if [ -z "${CH_SERVICE_ID:-}" ]; then
    echo "Step 5/8 create-ch"; cmd_create_ch
    echo ""
    echo "E2E PAUSED: export CH_SERVICE_ID (clickhousectl cloud service list --json), then re-run: $0 e2e"
    exit 0
  fi
  echo "Step 5/8 create-ch (already have CH_SERVICE_ID=$CH_SERVICE_ID)"
  echo "Step 6/8 schema (base)";  cmd_schema
  if [ -z "${PIPE_ID:-}" ]; then
    echo "Step 7/8 create-pipe";  cmd_create_pipe
    echo ""
    echo "E2E PAUSED: export PIPE_ID=<id printed above>, then re-run: $0 e2e"
    exit 0
  fi
  echo "Step 7/8 wait-pipe";      cmd_wait_pipe
  echo "Step 7/8 create-mv";      cmd_create_mv
  echo "Step 8/8 verify-sync";    cmd_verify_sync
  echo ""
  echo "=== E2E COMPLETE: shared PG configured, demo service live, CDC verified ==="
  echo "Now provision the participant slots: PARTICIPANTS=$PARTICIPANTS $0 provision"
}

# --------------------------------------------------------------------------
# Teardown
# --------------------------------------------------------------------------
cmd_teardown() {
  need psql; admin_env_required
  echo ">> Tearing down participant slots (participants should delete their ClickPipes first;"
  echo "   pipe deletion is not documented to drop source slots, so leftovers are dropped here)"
  local i n db role
  for i in $(seq 0 "$PARTICIPANTS"); do
    n=$(pnum "$i"); db="${DB_PREFIX}${n}"; role="${DB_PREFIX}${n}"
    if [ "$(psqla postgres -c "SELECT 1 FROM pg_database WHERE datname = '$db'")" = "1" ]; then
      psqla postgres -c \
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE database = '$db' AND NOT active" >/dev/null || true
      psqla postgres -c "DROP DATABASE $db WITH (FORCE)" >/dev/null
      echo "   dropped $db"
    fi
    psqla postgres -c "DROP ROLE IF EXISTS $role" >/dev/null
  done
  echo ">> Remaining slots: $(psqla postgres -c "SELECT count(*) FROM pg_replication_slots")"
  echo ">> Shred $CSV and any printed slips — the credentials are invalid but sensitive."
}

cmd_delete_pipe() {
  need clickhousectl
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID}"; : "${PIPE_ID:?set PIPE_ID}"
  clickhousectl cloud clickpipe delete "$CH_SERVICE_ID" "$PIPE_ID"
}

cmd_delete_ch() {
  need clickhousectl; need python3
  : "${CH_SERVICE_ID:?set CH_SERVICE_ID}"
  # VERIFIED LIVE: a running service cannot be deleted (CONFLICT) — stop first.
  echo ">> Stopping service $CH_SERVICE_ID before delete"
  clickhousectl cloud service stop "$CH_SERVICE_ID" >/dev/null 2>&1 || true
  local i state
  for i in $(seq 1 30); do
    state="$(clickhousectl cloud service get "$CH_SERVICE_ID" --json 2>/dev/null | json_field state || true)"
    echo "   [$i] state: ${state:-unknown}"
    [ "$state" = "stopped" ] && break
    sleep 10
  done
  clickhousectl cloud service delete "$CH_SERVICE_ID"
}

cmd_delete_pg() {
  need clickhousectl
  : "${PG_SERVICE_ID:?set PG_SERVICE_ID}"
  if ! clickhousectl cloud postgres delete "$PG_SERVICE_ID"; then
    echo ">> WARNING: postgres delete failed (VERIFIED LIVE: returns FORBIDDEN on"
    echo "   orgs without the managed-Postgres management entitlement). Delete the"
    echo "   service '$PG_SERVICE_NAME' from the ClickHouse Cloud console instead —"
    echo "   it bills hourly until removed."
    exit 1
  fi
}

case "${1:-}" in
  create-pg)       cmd_create_pg ;;
  wait-pg)         cmd_wait_pg ;;
  configure-pg)    cmd_configure_pg ;;
  verify-pg)       cmd_verify_pg ;;
  provision)       cmd_provision ;;
  provision-demo)  cmd_provision_demo ;;
  slips)           cmd_slips ;;
  create-ch)       cmd_create_ch ;;
  schema)          cmd_schema ;;
  create-pipe)     cmd_create_pipe ;;
  wait-pipe)       cmd_wait_pipe ;;
  create-mv)       cmd_create_mv ;;
  verify-sync)     cmd_verify_sync ;;
  e2e)             cmd_e2e ;;
  teardown)        cmd_teardown ;;
  delete-pipe)     cmd_delete_pipe ;;
  delete-ch)       cmd_delete_ch ;;
  delete-pg)       cmd_delete_pg ;;
  *) usage ;;
esac
