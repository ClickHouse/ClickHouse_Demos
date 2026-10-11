"""Capture effective inputs only after a bounded, writer-serialized CDC barrier."""
from .observability import traced, propagation_headers
from datetime import datetime, timedelta
import json
import logging
import time
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from .contracts import Scope, Subject, Snapshot, WorkshopError
from .history import HistoryLoader
from .ledger import SCOPE_WHERE, SUBJECT_WHERE, lock_scope, scope_key, subject_key
from .settings import LAB_ROOT

logger = logging.getLogger(__name__)


def json_value(value):
    return json.loads(json.dumps(value, default=lambda v: v.isoformat() if isinstance(v, datetime) else str(v)))


class SnapshotHeld(WorkshopError):
    def __init__(self, reason_ids: list[str]):
        self.reason_ids = reason_ids
        super().__init__("snapshot_held", "Inspect the source hold reasons. Restore the checkpoint for stale data, or wait for pending CDC revisions, then create a fresh snapshot.")


def verify_cdc_schema(analytics):
    db = analytics.database
    for table in ("sales_transactions_cdc","inventory_history_cdc","price_history_cdc","cost_history_cdc"):
        columns = analytics.query("SELECT name FROM system.columns WHERE database={db:String} AND table={table:String}", {"db":db,"table":table})
        names = {c["name"] for c in columns}
        engines = analytics.query("SELECT engine FROM system.tables WHERE database={db:String} AND name={table:String}", {"db":db,"table":table})
        if not {"_peerdb_version","_peerdb_is_deleted","_peerdb_synced_at","owner_id","scenario_id","generation"} <= names or not engines or "ReplacingMergeTree" not in engines[0]["engine"]:
            raise WorkshopError("cdc_schema_missing", "Inspect the ClickPipe destination mapping and version/delete columns.")


class FeatureService:
    def __init__(self, analytics, ledger, clock):
        self.analytics, self.ledger, self.clock = analytics, ledger, clock
        self.timeout_seconds = analytics.settings.convergence_timeout_seconds

    def configure_views(self):
        db = self.analytics.database
        verify_cdc_schema(self.analytics)
        for file in ("003_cdc_views.sql","004_evidence.sql"):
            text = (LAB_ROOT / "sql/clickhouse" / file).read_text().replace("__DATABASE__", db)
            text = "\n".join(line for line in text.splitlines() if not line.startswith("--"))
            for statement in text.split(";"):
                if statement.strip():
                    self.analytics.command(statement)

    @traced('pricing.cdc.poll')
    def await_revisions(self, scope: Scope, revisions: dict[str, str], timeout_seconds: float) -> dict:
        start = time.monotonic()
        targets = {"sale":("sales_live_current","event_id"), "inventory":("inventory_live_current","revision_id")}
        while True:
            missing = []
            for kind, revision in revisions.items():
                if kind not in targets:
                    continue
                table, key = targets[kind]
                rows = self.analytics.query(f"""SELECT count() AS count FROM {self.analytics.database}.{table}
                  WHERE owner_id={{owner:String}} AND scenario_id={{scenario:String}} AND generation={{generation:Int32}}
                  AND {key}={{revision:UUID}}""", {**self._params(scope),"revision":UUID(revision)})
                if rows[0]["count"] != 1:
                    missing.append(f"{kind}_not_converged")
            if not missing:
                return {"revisions":revisions,"visible_at":self.clock.wall_now(),"wait_seconds":time.monotonic()-start}
            remaining = timeout_seconds - (time.monotonic() - start)
            if remaining <= 0:
                raise SnapshotHeld(missing)
            time.sleep(min(.05, remaining))

    @staticmethod
    def _params(scope, subject=None, cutoff=None):
        params = {"owner":scope.owner_id,"scenario":scope.scenario_id,"generation":scope.generation,"sandbox":scope.sandbox_id}
        if subject:
            params.update(sku=subject.sku,store=subject.store_id)
        if cutoff:
            params["cutoff"] = cutoff.isoformat()
        return params

    def _effective(self, view, scope, subject, cutoff):
        # Source versions establish authority; effective_at bounds eligibility and age.
        # A later revision can report an older observation and must still be aged.
        rows = self.analytics.query(f"""SELECT * FROM {self.analytics.database}.{view}
          WHERE owner_id={{owner:String}} AND scenario_id={{scenario:String}} AND generation={{generation:Int32}}
          AND sku={{sku:Int32}} AND store_id={{store:Int32}} AND channel='store'
          AND effective_at <= parseDateTime64BestEffort({{cutoff:String}},3,'UTC')
          ORDER BY source_version DESC,effective_at DESC,revision_id DESC LIMIT 1""", self._params(scope,subject,cutoff))
        return rows[0] if rows else None

    @traced('pricing.snapshot')
    def capture(self, scope: Scope, subject: Subject) -> Snapshot:
        with self.ledger.connect() as conn:
            scenario = lock_scope(conn, scope)
            # Writers and reset serialize against these locks while the barrier is captured.
            state = conn.execute(f"SELECT * FROM pricing.subject_state WHERE {SUBJECT_WHERE} FOR UPDATE", subject_key(scope,subject)).fetchone()
            if not state:
                raise SnapshotHeld(["subject_missing"])
            cutoff = scenario["scenario_cutoff"]
            expected_sales = conn.execute(f"""SELECT * FROM pricing.sales_transactions WHERE {SUBJECT_WHERE}
              AND event_time<=%s ORDER BY event_id LIMIT 10001 FOR SHARE""", (*subject_key(scope,subject),cutoff)).fetchall()
            if len(expected_sales)>10000:
                raise SnapshotHeld(["live_window_too_large"])
            barrier_started = time.monotonic()
            revisions = {"inventory":str(state["inventory_revision_id"])}
            sale = conn.execute(f"SELECT event_id FROM pricing.sales_transactions WHERE {SCOPE_WHERE} AND inventory_revision_id=%s",
                                (*scope_key(scope),state["inventory_revision_id"])).fetchone()
            if sale:
                revisions["sale"] = str(sale["event_id"])
            convergence = self.await_revisions(scope,revisions,self.timeout_seconds)
            live = self._await_sales(scope,subject,cutoff,expected_sales,
                                     max(0.,self.timeout_seconds-(time.monotonic()-barrier_started)))
            selected = {kind:self._effective(view,scope,subject,cutoff) for kind,view in
                        [("inventory","inventory_live_current"),("price","price_live_current"),("cost","cost_live_current")]}
            reasons = [f"{kind}_missing" for kind,row in selected.items() if row is None]
            if reasons:
                raise SnapshotHeld(reasons)
            inventory, price, cost = selected["inventory"],selected["price"],selected["cost"]
            if inventory["revision_id"] != state["inventory_revision_id"]:
                reasons.append("inventory_not_effective")
            if price["source_version"] != state["applied_price_version"]:
                reasons.append("price_not_converged")
            if cost["source_version"] != state["cost_version"]:
                reasons.append("cost_not_converged")
            ages = {kind:max(0.,(cutoff-row["effective_at"]).total_seconds()) for kind,row in selected.items()}
            reasons += [f"{kind}_stale" for kind,age in ages.items() if age > state["rules"]["max_source_age_seconds"]]
            if reasons:
                raise SnapshotHeld(reasons)
            captured = self.clock.wall_now()
            snapshot = Snapshot(snapshot_id=uuid4(),scope=scope,subject=subject,scenario_cutoff=cutoff,captured_at=captured,
              source_versions={"inventory":str(inventory["revision_id"]),"price":str(price["source_version"]),"cost":str(cost["source_version"])},
              source_ages_seconds=ages,stock_units=inventory["available_units"],inventory_expires_at=inventory["inventory_expires_at"],
              expires_at=captured+timedelta(minutes=self.analytics.settings.lease_minutes),current_price_minor=price["price_minor"],
              reference_price_minor=state["reference_price_minor"],unit_cost_minor=cost["unit_cost_minor"],
              selling_cost_minor=cost["selling_cost_minor"],disposal_cost_minor=cost["disposal_cost_minor"],
              rule_version=state["rule_version"],applied_price_version=price["source_version"])
            evidence = json_value({"selected":selected,"live_sales":live,"convergence":convergence,
                                   "source_sales":expected_sales,
                                   "history":HistoryLoader(self.analytics,self.ledger).active(scope),"reconciliation":self.reconcile(scope,subject,cutoff)})
            payload = {"snapshot":snapshot.model_dump(mode="json"),"evidence":evidence}
            conn.execute("INSERT INTO pricing.snapshots VALUES(%s,%s,%s,%s,%s,%s)",
                         (snapshot.snapshot_id,*scope_key(scope),Jsonb(payload["snapshot"]),Jsonb(evidence)))
            conn.execute("INSERT INTO pricing.outbox(event_id,kind,data) VALUES(%s,'snapshot',%s)", (snapshot.snapshot_id,Jsonb({**payload,"trace_context":propagation_headers()})))
        try:
            self.deliver_snapshot(snapshot,evidence)
        except Exception:
            logger.warning("Snapshot analytical delivery is pending.",extra={"snapshot_id":str(snapshot.snapshot_id)})
        return snapshot

    def _await_sales(self,scope,subject,cutoff,expected,timeout_seconds):
        # ID visibility alone does not prove a correction or delete has converged.
        fields = ("event_id","sku","store_id","channel","event_type","quantity","unit_price_minor",
                  "receipts_minor","sold_cost_minor","original_event_id","applied_price_version",
                  "inventory_revision_id","inventory_version","event_time")
        def fingerprint(rows):
            def value(v):
                if isinstance(v,datetime):
                    return v.replace(microsecond=(v.microsecond//1000)*1000).isoformat()
                return str(v) if isinstance(v,UUID) else v
            return {str(row["event_id"]):tuple(value(row[field]) for field in fields) for row in rows}
        want,start = fingerprint(expected),time.monotonic()
        while True:
            rows = [r for r in self.analytics.live_current(scope,subject) if r["event_time"]<=cutoff]
            if fingerprint(rows)==want:
                return rows
            remaining = timeout_seconds-(time.monotonic()-start)
            if remaining<=0:
                raise SnapshotHeld(["sale_not_converged"])
            time.sleep(min(.05,remaining))

    def get_snapshot(self, scope: Scope, snapshot_id: UUID) -> tuple[Snapshot,dict]:
        with self.ledger.connect() as conn:
            row = conn.execute(f"SELECT data,evidence FROM pricing.snapshots WHERE {SCOPE_WHERE} AND snapshot_id=%s", (*scope_key(scope),snapshot_id)).fetchone()
        if not row:
            raise WorkshopError("snapshot_missing", "Choose a snapshot from this checkpoint.")
        return Snapshot.model_validate(row["data"]),row["evidence"]

    def deliver_snapshot(self,snapshot:Snapshot,evidence:dict):
        self.analytics.client.insert(f"{self.analytics.database}.input_snapshots",
          [[snapshot.snapshot_id,snapshot.scope.sandbox_id,snapshot.scope.generation,snapshot.subject.sku,snapshot.subject.store_id,
            snapshot.scenario_cutoff,snapshot.captured_at,snapshot.stock_units,snapshot.current_price_minor,
            snapshot.model_dump_json(),json.dumps(evidence,sort_keys=True),1]])
        with self.ledger.connect() as conn:
            conn.execute("UPDATE pricing.outbox SET delivered_at=%s,attempts=attempts+1 WHERE event_id=%s", (self.clock.wall_now(),snapshot.snapshot_id))

    @traced('pricing.reconcile')
    def reconcile(self,scope:Scope,subject:Subject,cutoff:datetime|None=None,*,include_live:bool=True)->dict:
        cutoff = cutoff or self.clock.scenario_now()
        active = HistoryLoader(self.analytics,self.ledger).active(scope)
        db,params = self.analytics.database,self._params(scope,subject,cutoff)
        live = f"""SELECT sum(quantity) AS net_units,sum(receipts_minor) AS receipts_minor,sum(sold_cost_minor) AS sold_cost_minor
          FROM {db}.sales_live_current WHERE owner_id={{owner:String}} AND scenario_id={{scenario:String}} AND generation={{generation:Int32}}
          AND sku={{sku:Int32}} AND store_id={{store:Int32}} AND event_time<toStartOfHour(parseDateTime64BestEffort({{cutoff:String}},3,'UTC'))"""
        condition = "sandbox_id={sandbox:String} AND sku={sku:Int32} AND store_id={store:Int32}"
        raw = self.analytics.query(f"""SELECT sum(quantity) AS net_units,sum(receipts_minor) AS receipts_minor,sum(sold_cost_minor) AS sold_cost_minor
          FROM {db}.{active['raw_table']} WHERE {condition} AND event_time<toStartOfHour(parseDateTime64BestEffort({{cutoff:String}},3,'UTC'))""",params)[0]
        rollup = self.analytics.query(f"""SELECT sum(net_units) AS net_units,sum(receipts_minor) AS receipts_minor,sum(sold_cost_minor) AS sold_cost_minor
          FROM {db}.{active['rollup_table']} WHERE {condition} AND hour<toStartOfHour(parseDateTime64BestEffort({{cutoff:String}},3,'UTC'))""",params)[0]
        current = self.analytics.query(live,params)[0] if include_live else {k:0 for k in raw}
        raw,rollup = ({k:(v or 0)+(current[k] or 0) for k,v in rows.items()} for rows in (raw,rollup))
        if raw != rollup:
            raise SnapshotHeld(["raw_rollup_mismatch"])
        return {"raw":raw,"rollup":rollup,"history_run_id":str(active["run_id"]),"complete_hour_cutoff":cutoff.isoformat()}
