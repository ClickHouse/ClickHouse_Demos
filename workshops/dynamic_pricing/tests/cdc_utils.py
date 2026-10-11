"""Insert recorded connector-shaped rows into real ClickHouse, not a real CDC test."""
from datetime import UTC, datetime
from psycopg import sql

TABLES = ("sales_transactions", "inventory_history", "price_history", "cost_history")
KEYS = {"sales_transactions":"owner_id,scenario_id,generation,event_id",
        "inventory_history":"revision_id", "price_history":"revision_id", "cost_history":"revision_id"}
TYPES = {"text":"String", "integer":"Int32", "bigint":"Int64", "uuid":"UUID",
         "timestamp with time zone":"DateTime64(3, 'UTC')"}


def create_replay_tables(analytics, ledger):
    for table in TABLES:
        with ledger.connect() as conn:
            columns = conn.execute("""SELECT column_name,data_type,is_nullable FROM information_schema.columns
              WHERE table_schema='pricing' AND table_name=%s ORDER BY ordinal_position""", (table,)).fetchall()
        fields = []
        for col in columns:
            kind = TYPES[col["data_type"]]
            if col["is_nullable"] == "YES":
                kind = f"Nullable({kind})"
            fields.append(f"{col['column_name']} {kind}")
        fields += ["_peerdb_version UInt64", "_peerdb_is_deleted UInt8", "_peerdb_synced_at DateTime64(3, 'UTC')"]
        analytics.command(f"CREATE TABLE {analytics.database}.{table}_cdc ({','.join(fields)}) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY ({KEYS[table]})")


def replay_table(analytics, ledger, table, version=1, deleted=False, predicate=lambda row: True):
    assert table in TABLES
    with ledger.connect() as conn:
        rows = conn.execute(sql.SQL("SELECT * FROM pricing.{}").format(sql.Identifier(table))).fetchall()
    rows = [r for r in rows if predicate(r)]
    if rows:
        columns = list(rows[0]) + ["_peerdb_version","_peerdb_is_deleted","_peerdb_synced_at"]
        analytics.client.insert(f"{analytics.database}.{table}_cdc",
                                [list(r.values()) + [version,int(deleted),datetime.now(UTC)] for r in rows], column_names=columns)


def replay_all(analytics, ledger, version=1):
    for table in TABLES:
        replay_table(analytics, ledger, table, version=version)
