"""Authoritative Postgres state. Analytics delivery never replaces these records."""
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .contracts import Receipt, Candidate, Scope, Subject, StaleGeneration, WorkshopError
from .settings import LAB_ROOT

SUBJECT_WHERE = "owner_id=%s AND scenario_id=%s AND generation=%s AND sku=%s AND store_id=%s AND channel=%s"
SCOPE_WHERE = "owner_id=%s AND scenario_id=%s AND generation=%s"


def scope_key(scope: Scope) -> tuple:
    return scope.owner_id, scope.scenario_id, scope.generation


def subject_key(scope: Scope, subject: Subject) -> tuple:
    return (*scope_key(scope), subject.sku, subject.store_id, subject.channel)


def lock_scope(conn, scope: Scope) -> dict:
    row = conn.execute("SELECT * FROM pricing.scenarios WHERE owner_id=%s AND scenario_id=%s FOR SHARE",
                       (scope.owner_id, scope.scenario_id)).fetchone()
    if not row or row["generation"] != scope.generation:
        raise StaleGeneration()
    return row


class Ledger:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def connect(self):
        if not self._dsn:
            raise WorkshopError("source_not_configured", "Set the managed Postgres connection in app/.env.workshop.")
        return psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=10)

    def migrate(self) -> None:
        with self.connect() as conn:
            conn.execute((LAB_ROOT / "sql/postgres/001_schema.sql").read_text())
            conn.execute((LAB_ROOT / "sql/postgres/003_retail.sql").read_text())

    def initialize(self, scope: Scope, subject: Subject, state: dict, rules: dict, cutoff: datetime) -> None:
        with self.connect() as conn:
            self.initialize_in_transaction(conn,scope,subject,state,rules,cutoff)

    def initialize_in_transaction(self,conn,scope,subject,state,rules,cutoff):
        conn.execute("INSERT INTO pricing.scenarios VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                     (*scope_key(scope), cutoff))
        lock_scope(conn, scope)
        revision_id = uuid4()
        fields = ("stock_units", "inventory_expires_at", "current_price_minor", "reference_price_minor",
                  "applied_price_version", "unit_cost_minor", "selling_cost_minor", "disposal_cost_minor", "rule_version")
        row = conn.execute("""INSERT INTO pricing.subject_state
          (owner_id,scenario_id,generation,sku,store_id,channel,inventory_revision_id,
           stock_units,inventory_expires_at,current_price_minor,reference_price_minor,
           applied_price_version,unit_cost_minor,selling_cost_minor,disposal_cost_minor,rule_version,rules)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT DO NOTHING RETURNING *""",
          (*subject_key(scope, subject), revision_id, *(state[f] for f in fields), Jsonb(rules))).fetchone()
        if row:
            append_inventory(conn, scope, subject, row, cutoff)
            conn.execute("""INSERT INTO pricing.price_history
              (revision_id,owner_id,scenario_id,generation,sku,store_id,channel,source_version,
               price_minor,effective_at,acknowledged_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (uuid4(), *subject_key(scope, subject), row["applied_price_version"],
               row["current_price_minor"], cutoff, datetime.now(UTC)))
            conn.execute("""INSERT INTO pricing.cost_history
              (revision_id,owner_id,scenario_id,generation,sku,store_id,channel,source_version,
               unit_cost_minor,selling_cost_minor,disposal_cost_minor,effective_at)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (uuid4(), *subject_key(scope, subject), row["cost_version"], row["unit_cost_minor"],
               row["selling_cost_minor"], row["disposal_cost_minor"], cutoff))

    def current_state(self, scope: Scope, subject: Subject) -> dict:
        with self.connect() as conn:
            scenario = lock_scope(conn, scope)
            row = conn.execute(f"SELECT * FROM pricing.subject_state WHERE {SUBJECT_WHERE}", subject_key(scope, subject)).fetchone()
            if not row:
                raise WorkshopError("subject_missing", "Choose a subject included in this checkpoint.")
            return {**row, "scope": scope, "scenario_cutoff": scenario["scenario_cutoff"],
                    "source_versions": {"inventory": str(row["inventory_revision_id"]),
                                        "price": str(row["applied_price_version"]), "cost": str(row["cost_version"])}}

    def last_acknowledged_price(self, scope, subject):
        return self.current_state(scope, subject)["current_price_minor"]

    def store_candidates(self, candidates: list[Candidate]) -> None:
        with self.connect() as conn:
            for candidate in candidates:
                lock_scope(conn, candidate.snapshot.scope)
                conn.execute("INSERT INTO pricing.candidates (candidate_id,owner_id,scenario_id,generation,data) VALUES (%s,%s,%s,%s,%s)",
                             (candidate.candidate_id,*scope_key(candidate.snapshot.scope),Jsonb(candidate.model_dump(mode="json"))))

    def get_candidate(self, candidate_id, scope=None) -> Candidate:
        with self.connect() as conn:
            row=conn.execute("SELECT * FROM pricing.candidates WHERE candidate_id=%s",(candidate_id,)).fetchone()
            if not row or (scope and tuple(row[k] for k in ("owner_id","scenario_id","generation"))!=scope_key(scope)):
                raise WorkshopError("candidate_missing","Create a fresh proposal in the current scenario.")
            candidate=Candidate.model_validate(row["data"])
            if scope: lock_scope(conn,scope)
            return candidate

    def receipt_count(self, candidate_id) -> int:
        with self.connect() as conn:
            return conn.execute("SELECT count(*) AS n FROM pricing.receipts WHERE candidate_id=%s",(candidate_id,)).fetchone()["n"]

    def get_receipt(self, scope, decision_id):
        with self.connect() as conn:
            lock_scope(conn,scope)
            row=conn.execute(f"SELECT data FROM pricing.receipts WHERE {SCOPE_WHERE} AND decision_id=%s",(*scope_key(scope),decision_id)).fetchone()
            return Receipt.model_validate(row["data"]) if row else None

    def current_scope(self, owner_id, scenario_id):
        with self.connect() as conn:
            row=conn.execute("SELECT generation,scenario_cutoff FROM pricing.scenarios WHERE owner_id=%s AND scenario_id=%s",(owner_id,scenario_id)).fetchone()
        if not row: raise WorkshopError("scenario_missing","Initialize this learner instance before running the lab.")
        return Scope(owner_id=owner_id,scenario_id=scenario_id,generation=row["generation"]),row["scenario_cutoff"]

    def list_candidates(self, scope, subject):
        with self.connect() as conn:
            lock_scope(conn,scope)
            rows=conn.execute(f"SELECT data FROM pricing.candidates WHERE {SCOPE_WHERE} AND (data->'snapshot'->'subject'->>'sku')::int=%s AND (data->'snapshot'->'subject'->>'store_id')::int=%s ORDER BY (data->>'created_at')::timestamptz DESC LIMIT 20",(*scope_key(scope),subject.sku,subject.store_id)).fetchall()
        return [Candidate.model_validate(r["data"]) for r in rows]

    def get_decision(self, scope, decision_id):
        with self.connect() as conn:
            lock_scope(conn,scope)
            row=conn.execute(f"SELECT * FROM pricing.decisions WHERE {SCOPE_WHERE} AND decision_id=%s",(*scope_key(scope),decision_id)).fetchone()
        if not row: raise WorkshopError("decision_missing","Choose a reviewed decision from this generation.")
        return {"decision_id":row["decision_id"],"candidate_id":row["candidate_id"],"reviewer_id":row["reviewer_id"],"reviewed_at":row["reviewed_at"],"status":row["status"],"review":row["data"]}

    def reset_request_origin(self, owner_id, scenario_id, request_key):
        with self.connect() as conn:
            return conn.execute("SELECT from_generation,to_generation,checkpoint FROM pricing.resets WHERE owner_id=%s AND scenario_id=%s AND idempotency_key=%s",(owner_id,scenario_id,request_key)).fetchone()

    def timeline(self, scope, subject):
        with self.connect() as conn:
            lock_scope(conn,scope)
            prices=conn.execute(f"SELECT * FROM pricing.price_history WHERE {SUBJECT_WHERE} ORDER BY acknowledged_at DESC LIMIT 50",subject_key(scope,subject)).fetchall()
            sales=conn.execute(f"SELECT event_id,unit_price_minor,applied_price_version,event_time,emitted_at,commit_acknowledged_at FROM pricing.sales_transactions WHERE {SUBJECT_WHERE} ORDER BY emitted_at DESC LIMIT 50",subject_key(scope,subject)).fetchall()
            candidates=conn.execute(f"SELECT data FROM pricing.candidates WHERE {SCOPE_WHERE} AND (data->'snapshot'->'subject'->>'sku')::int=%s AND (data->'snapshot'->'subject'->>'store_id')::int=%s ORDER BY (data->>'created_at')::timestamptz DESC LIMIT 50",(*scope_key(scope),subject.sku,subject.store_id)).fetchall()
            receipts=conn.execute("SELECT r.data FROM pricing.receipts r JOIN pricing.candidates c ON r.candidate_id=c.candidate_id WHERE r.owner_id=%s AND r.scenario_id=%s AND r.generation=%s AND (c.data->'snapshot'->'subject'->>'sku')::int=%s AND (c.data->'snapshot'->'subject'->>'store_id')::int=%s LIMIT 50",(*scope_key(scope),subject.sku,subject.store_id)).fetchall()
            reviews=conn.execute(f"SELECT * FROM pricing.decisions WHERE {SCOPE_WHERE} ORDER BY reviewed_at DESC LIMIT 50",scope_key(scope)).fetchall()
        versions={int(r['data']['applied_price_version']):r['data'] for r in receipts}
        events=[]
        for p in prices:
            receipt=versions.get(p['source_version'])
            events.append({'kind':'applied','evidence_id':receipt['receipt_id'] if receipt else str(p['revision_id']),
                'at':p['acknowledged_at'],'price_minor':p['price_minor'],'applied_price_version':p['source_version'],
                'scenario_time':p['effective_at'],'receipt_id':receipt['receipt_id'] if receipt else None})
        by_id={}
        for row in candidates:
            c=Candidate.model_validate(row['data']);by_id[c.candidate_id]=c
            events.append({'kind':'proposed','evidence_id':str(c.candidate_id),'at':c.created_at,'price_minor':c.price_minor,'snapshot_id':str(c.snapshot.snapshot_id)})
        for r in reviews:
            c=by_id.get(r['candidate_id'])
            if c: events.append({'kind':'approval' if r['status']=='approved' else 'rejection','evidence_id':str(r['decision_id']),
                'at':r['reviewed_at'],'price_minor':c.price_minor,'candidate_id':str(c.candidate_id)})
        for r in sales:
            events.append({'kind':'sale','evidence_id':str(r['event_id']),'at':r['commit_acknowledged_at'] or r['emitted_at'],
                'price_minor':r['unit_price_minor'],'applied_price_version':r['applied_price_version'],'scenario_time':r['event_time']})
        return events


def append_inventory(conn, scope: Scope, subject: Subject, state: dict, effective_at: datetime) -> None:
    conn.execute("""INSERT INTO pricing.inventory_history
      (revision_id,owner_id,scenario_id,generation,sku,store_id,channel,source_version,
       available_units,inventory_expires_at,effective_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
      (state["inventory_revision_id"], *subject_key(scope, subject), state["inventory_version"],
       state["stock_units"], state["inventory_expires_at"], effective_at))
