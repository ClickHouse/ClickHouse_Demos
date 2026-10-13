"""Isolated import sets: verification precedes promotion; ambiguous loads start fresh."""
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import re
import time
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, urljoin, unquote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from http.client import IncompleteRead, RemoteDisconnected
from uuid import UUID, uuid4

import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from .analytics import identifier
from .contracts import Scope, WorkshopError
from .settings import LAB_ROOT
from generator.manifest import read_manifest


class ImportInterrupted(RuntimeError):
    pass


CONSOLE_SCHEMAS = {
    "hist_raw_console": [
        ("batch_id", "UUID"), ("dataset_version", "LowCardinality(String)"),
        ("event_id", "UUID"), ("original_event_id", "UUID"), ("sku", "UInt32"), ("store_id", "UInt16"),
        ("channel", "LowCardinality(String)"), ("event_type", "LowCardinality(String)"), ("quantity", "Int32"),
        ("offered_price_minor", "Int64"), ("receipts_minor", "Int64"), ("sold_cost_minor", "Int64"),
        ("event_time", "DateTime64(3, 'UTC')"), ("calibration", "Bool"), ("currency", "LowCardinality(String)"),
    ],
    "hist_hour_console": [
        ("store_id", "UInt16"), ("sku", "UInt32"), ("hour", "DateTime('UTC')"),
        ("net_units", "Int64"), ("receipts_minor", "Int64"), ("sold_cost_minor", "Int64"), ("row_count", "UInt64"),
    ],
}


def public_url(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        return f"https://{parsed.netloc}.s3.amazonaws.com{parsed.path}"
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Use a public HTTPS/S3 dataset URL without credentials or query parameters.")
    return uri


def public_read(request,consume):
    """Retry only transient transport/server failures, including partial bodies."""
    for attempt in range(4):
        try:
            with urlopen(request,timeout=30) as response:
                return consume(response)
        except HTTPError as error:
            if error.code not in (408,429,500,502,503,504) or attempt==3: raise
        except (URLError,OSError,TimeoutError,IncompleteRead,RemoteDisconnected):
            if attempt==3: raise
        time.sleep(.25*2**attempt)


def resolve_manifest(uri: str) -> tuple[dict, Path | str]:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path))
        return read_manifest(path), path.parent
    url = public_url(uri)
    manifest = public_read(url,json.load)
    if manifest.get("dataset_version") != "retail-v1" or manifest.get("source_namespace") != "history-v1" or manifest.get("currency") != "SGD":
        raise ValueError("Unsupported historical manifest.")
    files = manifest["files"] + manifest["companion_files"]
    keys = set()
    for file in files:
        key = file["key"]
        if PurePosixPath(key).is_absolute() or ".." in PurePosixPath(key).parts or key in keys or urlparse(key).scheme:
            raise ValueError("Manifest path must stay inside the dataset prefix.")
        keys.add(key)
    def verify_file(file):
        source = urljoin(url, file["key"])
        # S3 can return a service-validated SHA-256; otherwise hash a bounded stream.
        headers=public_read(Request(source,method='HEAD',headers={'x-amz-checksum-mode':'ENABLED'}),lambda response:response.headers)
        expected=b64encode(bytes.fromhex(file['sha256'])).decode()
        checksum=headers.get('x-amz-checksum-sha256')
        if int(headers['Content-Length'])!=file['bytes']:
            raise ValueError('Dataset size mismatch.')
        if checksum is not None:
            if checksum != expected:
                raise ValueError("Dataset checksum mismatch.")
        else:
            def digest_response(response):
                digest=hashlib.sha256()
                for chunk in iter(lambda:response.read(1024*1024),b''):
                    digest.update(chunk)
                return digest.hexdigest()
            if public_read(source,digest_response)!=file['sha256']:
                raise ValueError('Dataset checksum mismatch.')
    with ThreadPoolExecutor(max_workers=8) as pool:
        # Consume every result; one failed check prevents any import.
        list(pool.map(verify_file,files))
    if sum(f["rows"] for f in manifest["files"]) != manifest["rows"]:
        raise ValueError("Manifest row counts do not reconcile.")
    return manifest, url


class HistoryLoader:
    def __init__(self, analytics, ledger):
        self.analytics, self.ledger = analytics, ledger

    def load(self, manifest_uri: str, scope: Scope) -> dict:
        # Session lock spans external IO without an idle Postgres transaction.
        with self.ledger.connect() as guard:
            guard.autocommit = True
            locked = guard.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked", (scope.sandbox_id,)).fetchone()["locked"]
            if not locked:
                raise WorkshopError("import_running", "Wait for the active import to finish before starting another.")
            run_id = uuid4()
            data, views_changed = None, False
            previous = guard.execute("""SELECT r.data FROM pricing.history_active a JOIN pricing.import_runs r USING(run_id)
              WHERE a.owner_id=%s AND a.scenario_id=%s""", (scope.owner_id, scope.scenario_id)).fetchone()
            try:
                manifest, source = resolve_manifest(manifest_uri)
                data = {"manifest": manifest,
                        "raw_table": f"hist_raw_{run_id.hex}", "rollup_table": f"hist_hour_{run_id.hex}",
                        "mv_table": f"hist_mv_{run_id.hex}"}
                guard.execute("UPDATE pricing.import_runs SET status='abandoned' WHERE owner_id=%s AND scenario_id=%s AND status='loading'",
                              (scope.owner_id, scope.scenario_id))
                guard.execute("INSERT INTO pricing.import_runs(run_id,owner_id,scenario_id,status,data) VALUES (%s,%s,%s,'loading',%s)",
                              (run_id, scope.owner_id, scope.scenario_id, Jsonb(data)))
                load_started=time.monotonic()
                self._create_set(data)
                for file in manifest["files"]:
                    self._insert_file(source, file, data, run_id)
                load_finished=time.monotonic()
                result = self.verify(run_id)
                verified=time.monotonic()
                data["measurement"]={"rows":manifest["rows"],"load_seconds":load_finished-load_started,"verification_seconds":verified-load_finished,"clock":"monotonic"}
                views_changed = True
                self._promote_views(data)
                with guard.transaction():
                    guard.execute("UPDATE pricing.import_runs SET status='verified',data=%s WHERE run_id=%s", (Jsonb(data),run_id))
                    guard.execute("""INSERT INTO pricing.history_active VALUES (%s,%s,%s)
                                  ON CONFLICT(owner_id,scenario_id) DO UPDATE SET run_id=excluded.run_id""",
                                  (scope.owner_id, scope.scenario_id, run_id))
                return {**result,"measurement":data["measurement"]}
            except Exception:
                guard.execute("UPDATE pricing.import_runs SET status='abandoned' WHERE run_id=%s", (run_id,))
                if data is not None:
                    if views_changed:
                        if previous:
                            self._promote_views(previous["data"])
                        else:
                            for name in ("sales_history", "sales_history_hourly"):
                                self.analytics.command(f"DROP TABLE IF EXISTS {self.analytics.database}.{name} SYNC")
                    for key in ("mv_table", "raw_table", "rollup_table"):
                        self.analytics.command(f"DROP TABLE IF EXISTS {self.analytics.database}.{identifier(data[key])} SYNC")
                raise
            finally:
                guard.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (scope.sandbox_id,))

    def register_console(self, manifest_uri: str, scope: Scope) -> dict:
        """Verify and activate console-loaded tables; never INSERT or DROP their facts."""
        with self.ledger.connect() as guard:
            guard.autocommit = True
            locked = guard.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked",
                (scope.sandbox_id,),
            ).fetchone()["locked"]
            if not locked:
                raise WorkshopError("import_running", "Wait for the active import to finish before registering another.")
            run_id = None
            created = False
            views_changed = False
            previous = None
            try:
                manifest, _ = resolve_manifest(manifest_uri)
                data = {"manifest": manifest,
                        "raw_table": "hist_raw_console", "rollup_table": "hist_hour_console",
                        "mv_table": "hist_mv_console", "source": "sql_console"}
                db = self.analytics.database
                tables = self.analytics.query(
                    """SELECT name,engine,sorting_key,create_table_query FROM system.tables
                       WHERE database={db:String} AND name IN {names:Array(String)} LIMIT 3""",
                    {"db": db, "names": [data[key] for key in ("raw_table", "rollup_table", "mv_table")]},
                )
                meta = {row["name"]: row for row in tables}
                valid_engines = (
                    len(meta) == 3
                    and meta["hist_raw_console"]["engine"] in ("MergeTree", "SharedMergeTree")
                    and meta["hist_hour_console"]["engine"] in ("SummingMergeTree", "SharedSummingMergeTree")
                    and meta["hist_mv_console"]["engine"] == "MaterializedView"
                    and meta["hist_raw_console"]["sorting_key"] == "store_id, sku, event_time, event_id"
                    and meta["hist_hour_console"]["sorting_key"] == "store_id, sku, hour"
                )
                if not valid_engines:
                    raise WorkshopError(
                        "console_history_schema",
                        "Create the three tables and materialized view shown in the SQL console tab before registration.",
                    )
                columns = self.analytics.query(
                    """SELECT table,name,type FROM system.columns
                       WHERE database={db:String} AND table IN {tables:Array(String)} ORDER BY table,position""",
                    {"db": db, "tables": list(CONSOLE_SCHEMAS)},
                )
                actual_schemas = {
                    table: [(row["name"], row["type"]) for row in columns if row["table"] == table]
                    for table in CONSOLE_SCHEMAS
                }
                if actual_schemas != CONSOLE_SCHEMAS:
                    raise WorkshopError(
                        "console_history_schema",
                        "Use the column names and types shown in the SQL console tab.",
                    )
                definition = meta["hist_mv_console"]["create_table_query"].replace("`", "")
                if (not re.search(r"\bFROM\s+" + re.escape(db) + r"\.hist_raw_console\b", definition, re.I)
                        or not re.search(r"\bTO\s+" + re.escape(db) + r"\.hist_hour_console\b", definition, re.I)):
                    raise WorkshopError(
                        "console_history_schema",
                        "Use the materialized view linking hist_raw_console to hist_hour_console.",
                    )
                groups = self.analytics.query(
                    f"""SELECT dataset_version,uniqExact(batch_id) AS batches
                        FROM {db}.hist_raw_console GROUP BY dataset_version LIMIT 2"""
                )
                expected_group = {"dataset_version": manifest["dataset_version"], "batches": 1}
                if len(groups) != 1 or groups[0] != expected_group:
                    raise WorkshopError(
                        "console_history_dataset",
                        "Use the supplied dataset version and one batch ID in the console INSERT statement.",
                    )
                previous = guard.execute(
                    """SELECT r.data FROM pricing.history_active a JOIN pricing.import_runs r USING(run_id)
                       WHERE a.owner_id=%s AND a.scenario_id=%s""",
                    (scope.owner_id, scope.scenario_id),
                ).fetchone()
                existing = guard.execute(
                    """SELECT run_id,data FROM pricing.import_runs
                       WHERE owner_id=%s AND scenario_id=%s AND status='verified'
                         AND data->>'raw_table'='hist_raw_console'
                       ORDER BY created_at DESC LIMIT 1""",
                    (scope.owner_id, scope.scenario_id),
                ).fetchone()
                if existing and existing["data"] == data:
                    run_id = existing["run_id"]
                else:
                    run_id = uuid4()
                    created = True
                    guard.execute(
                        """INSERT INTO pricing.import_runs(run_id,owner_id,scenario_id,status,data)
                           VALUES (%s,%s,%s,'loading',%s)""",
                        (run_id, scope.owner_id, scope.scenario_id, Jsonb(data)),
                    )
                result = self.verify(run_id)
                views_changed = True
                self._promote_views(data)
                with guard.transaction():
                    guard.execute("UPDATE pricing.import_runs SET status='verified' WHERE run_id=%s", (run_id,))
                    guard.execute(
                        """INSERT INTO pricing.history_active VALUES (%s,%s,%s)
                           ON CONFLICT(owner_id,scenario_id) DO UPDATE SET run_id=excluded.run_id""",
                        (scope.owner_id, scope.scenario_id, run_id),
                    )
                return {**result, "source": "sql_console", "rows_loaded": 0}
            except Exception:
                if created:
                    guard.execute("UPDATE pricing.import_runs SET status='abandoned' WHERE run_id=%s", (run_id,))
                if views_changed:
                    if previous:
                        self._promote_views(previous["data"])
                    else:
                        for name in ("sales_history", "sales_history_hourly"):
                            self.analytics.command(f"DROP VIEW IF EXISTS {self.analytics.database}.{name} SYNC")
                raise
            finally:
                guard.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (scope.sandbox_id,))

    def _promote_views(self, data):
        db = self.analytics.database
        self.analytics.command(f"CREATE OR REPLACE VIEW {db}.sales_history AS SELECT * FROM {db}.{identifier(data['raw_table'])}")
        self.analytics.command(f"CREATE OR REPLACE VIEW {db}.sales_history_hourly AS SELECT * FROM {db}.{identifier(data['rollup_table'])}")

    def _create_set(self, data):
        substitutions = {"__DATABASE__": self.analytics.database, "__RAW__": data["raw_table"],
                         "__ROLLUP__": data["rollup_table"], "__MV__": data["mv_table"]}
        for name in ("001_history.sql", "002_history_rollup.sql"):
            sql = (LAB_ROOT / "sql/clickhouse" / name).read_text()
            sql = "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
            for token, value in substitutions.items():
                sql = sql.replace(token, identifier(value))
            for statement in sql.split(";"):
                if statement.strip():
                    self.analytics.command(statement)

    def _insert_file(self, source, file, data, run_id):
        table = f"{self.analytics.database}.{data['raw_table']}"
        if isinstance(source, Path):
            # Explicit local/developer path. This validates SQL semantics, not S3 connectivity.
            columns = ["batch_id","dataset_version","event_id","original_event_id","sku","store_id",
                       "channel","event_type","quantity","offered_price_minor","receipts_minor","sold_cost_minor",
                       "event_time","calibration","currency"]
            for batch in pq.ParquetFile(source / file["key"]).iter_batches(batch_size=10000):
                records = []
                for row in batch.to_pylist():
                    row.update(batch_id=run_id,dataset_version=data["manifest"]["dataset_version"])
                    row["event_id"] = UUID(row["event_id"])
                    row["original_event_id"] = UUID(row["original_event_id"]) if row["original_event_id"] else UUID(int=0)
                    records.append([row[c] for c in columns])
                self.analytics.client.insert(table, records, column_names=columns)
        else:
            self.analytics.command(f"""INSERT INTO {table} SELECT
              {{batch:UUID}}, {{version:String}}, toUUID(event_id), toUUIDOrZero(original_event_id),
              sku,store_id,channel,event_type,quantity,offered_price_minor,receipts_minor,sold_cost_minor,
              event_time,calibration,currency FROM s3({{url:String}}, NOSIGN, 'Parquet')""",
              {"batch":run_id,"version":data["manifest"]["dataset_version"],"url":urljoin(source,file["key"])})

    def active(self, scope: Scope) -> dict:
        with self.ledger.connect() as conn:
            row = conn.execute("""SELECT r.run_id,r.data FROM pricing.history_active a
              JOIN pricing.import_runs r USING(run_id) WHERE a.owner_id=%s AND a.scenario_id=%s""",
              (scope.owner_id,scope.scenario_id)).fetchone()
        if not row:
            raise WorkshopError("history_missing", "Load and verify the teaching dataset first.")
        return {**row["data"], "run_id": row["run_id"]}

    def verify(self, run_id: UUID) -> dict:
        with self.ledger.connect() as conn:
            row = conn.execute("SELECT data FROM pricing.import_runs WHERE run_id=%s", (run_id,)).fetchone()
        if not row:
            raise ValueError("Unknown import run.")
        data, db = row["data"], self.analytics.database
        raw = self.analytics.query(f"""SELECT count() AS canonical_rows, sum(quantity) AS net_units,
          sum(receipts_minor) AS receipts_minor, sum(sold_cost_minor) AS sold_cost_minor
          FROM {db}.{identifier(data['raw_table'])}""")[0]
        rollup = self.analytics.query(f"""SELECT sum(net_units) AS net_units, sum(receipts_minor) AS receipts_minor,
          sum(sold_cost_minor) AS sold_cost_minor FROM {db}.{identifier(data['rollup_table'])}""")[0]
        manifest = data["manifest"]
        expected = {"canonical_rows": manifest["rows"],
                    "net_units":sum(f["quantity"] for f in manifest["files"]),
                    "receipts_minor":sum(f["receipts_minor"] for f in manifest["files"]),
                    "sold_cost_minor":sum(f["sold_cost_minor"] for f in manifest["files"])}
        if raw != expected or any(raw[k] != rollup[k] for k in rollup):
            raise ValueError("History counts/revenue/rollup totals do not reconcile.")
        grouped = self.analytics.query(f"""SELECT count() AS mismatches FROM (
          SELECT store_id,sku,hour,
                 sum(net_units_delta) AS net_units_delta,
                 sum(receipts_delta) AS receipts_delta,
                 sum(cost_delta) AS cost_delta,
                 sum(rows_delta) AS rows_delta
          FROM (
            SELECT store_id,sku,toStartOfHour(event_time) AS hour,
                   sum(quantity) AS net_units_delta,sum(receipts_minor) AS receipts_delta,
                   sum(sold_cost_minor) AS cost_delta,toInt64(count()) AS rows_delta
            FROM {db}.{identifier(data['raw_table'])}
            GROUP BY store_id,sku,hour
            UNION ALL
            SELECT store_id,sku,hour,
                   -sum(net_units),-sum(receipts_minor),-sum(sold_cost_minor),-toInt64(sum(row_count))
            FROM {db}.{identifier(data['rollup_table'])}
            GROUP BY store_id,sku,hour
          ) GROUP BY store_id,sku,hour
          HAVING net_units_delta!=0 OR receipts_delta!=0 OR cost_delta!=0 OR rows_delta!=0
        )""")[0]["mismatches"]
        if grouped:
            raise ValueError("History grouped facts do not reconcile.")
        return {**raw, "run_id":run_id,"raw_table":data["raw_table"],"rollup_table":data["rollup_table"],"rollup":rollup}
