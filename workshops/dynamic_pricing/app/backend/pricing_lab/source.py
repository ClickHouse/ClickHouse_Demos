"""Sales and stock commit together; repeated event IDs return their first result."""
from .observability import traced
from datetime import UTC, datetime
from contextlib import contextmanager
import hashlib
import json
from uuid import UUID, uuid4

import psycopg

from .contracts import Scope, Subject, SaleRequest, WorkshopError
from .ledger import Ledger, SCOPE_WHERE, SUBJECT_WHERE, append_inventory, lock_scope, scope_key, subject_key


def _hash(content: dict) -> str:
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class SourceStore:
    def __init__(self, dsn: str):
        self.ledger = Ledger(dsn)

    def stock(self, scope: Scope, subject: Subject) -> int:
        return self.ledger.current_state(scope, subject)["stock_units"]

    @traced('pricing.source.sale')
    def create_sale(self, scope: Scope, request: SaleRequest) -> dict:
        request = SaleRequest.model_validate(request.model_dump())
        content_hash = _hash(request.model_dump(mode="json"))
        emitted_at = datetime.now(UTC)
        with self._event_transaction() as conn:
            scenario = lock_scope(conn, scope)
            state = self._lock_subject(conn, scope, request.subject)
            existing = self._existing(conn, scope, request.event_id, content_hash)
            if not existing:
                if request.expected_price_version != state["applied_price_version"]:
                    raise WorkshopError("price_version_changed", "Refresh the applied price before creating a sale.")
                if request.quantity > state["stock_units"]:
                    raise WorkshopError("insufficient_stock", "Reduce the sale quantity to the available stock.")
                self._record(conn, scope, request.subject, state, request.event_id, request.quantity,
                             state["current_price_minor"], state["unit_cost_minor"], state["applied_price_version"],
                             request.occurred_at, max(scenario["scenario_cutoff"], request.occurred_at),
                             emitted_at, content_hash)
        return self._acknowledge(scope, request.event_id)

    @traced('pricing.source.return')
    def create_return(self, scope: Scope, original_event_id: UUID, quantity: int, event_id: UUID) -> dict:
        if type(quantity) is not int or quantity <= 0:
            raise WorkshopError("invalid_return", "Return a positive quantity from an existing sale.")
        content_hash = _hash({"original_event_id": str(original_event_id), "quantity": quantity, "event_id": str(event_id)})
        emitted_at = datetime.now(UTC)
        with self._event_transaction() as conn:
            scenario = lock_scope(conn, scope)
            original = conn.execute(f"SELECT * FROM pricing.sales_transactions WHERE {SCOPE_WHERE} AND event_id=%s",
                                    (*scope_key(scope), original_event_id)).fetchone()
            if not original or original["event_type"] != "sale":
                raise WorkshopError("invalid_return", "Choose a sale from the current checkpoint.")
            subject = Subject(sku=original["sku"], store_id=original["store_id"], channel=original["channel"])
            state = self._lock_subject(conn, scope, subject)
            if not self._existing(conn, scope, event_id, content_hash):
                returned = conn.execute(f"SELECT coalesce(-sum(quantity),0) AS units FROM pricing.sales_transactions WHERE {SCOPE_WHERE} AND original_event_id=%s",
                                        (*scope_key(scope), original_event_id)).fetchone()["units"]
                if quantity > original["quantity"] - returned:
                    raise WorkshopError("invalid_return", "Reduce the return quantity to the unreturned sale quantity.")
                self._record(conn, scope, subject, state, event_id, -quantity, original["unit_price_minor"],
                             original["sold_cost_minor"] // original["quantity"], original["applied_price_version"],
                             scenario["scenario_cutoff"], scenario["scenario_cutoff"], emitted_at, content_hash, original_event_id)
        return self._acknowledge(scope, event_id)

    @contextmanager
    def _event_transaction(self):
        try:
            with self.ledger.connect() as conn:
                yield conn
        except psycopg.errors.UniqueViolation as error:
            raise WorkshopError("event_conflict", "Reuse the original event contents or create a new event ID.") from error

    @staticmethod
    def _lock_subject(conn, scope, subject):
        state = conn.execute(f"SELECT * FROM pricing.subject_state WHERE {SUBJECT_WHERE} FOR UPDATE", subject_key(scope, subject)).fetchone()
        if not state:
            raise WorkshopError("subject_missing", "Choose a subject included in this checkpoint.")
        return state

    @staticmethod
    def _existing(conn, scope, event_id, content_hash):
        row = conn.execute(f"SELECT * FROM pricing.sales_transactions WHERE {SCOPE_WHERE} AND event_id=%s",
                           (*scope_key(scope), event_id)).fetchone()
        if row and row["content_hash"] != content_hash:
            raise WorkshopError("event_conflict", "Reuse the original event contents or create a new event ID.")
        return row

    @staticmethod
    def _record(conn, scope, subject, state, event_id, quantity, price, cost, price_version,
                occurred_at, effective_at, emitted_at, content_hash, original=None):
        revision = uuid4()
        updated = conn.execute(f"""UPDATE pricing.subject_state SET stock_units=stock_units-%s,
          inventory_version=inventory_version+1,inventory_revision_id=%s WHERE {SUBJECT_WHERE} RETURNING *""",
          (quantity, revision, *subject_key(scope, subject))).fetchone()
        append_inventory(conn, scope, subject, updated, effective_at)
        conn.execute("""INSERT INTO pricing.sales_transactions
          (owner_id,scenario_id,generation,event_id,sku,store_id,channel,event_type,quantity,
           unit_price_minor,receipts_minor,sold_cost_minor,original_event_id,applied_price_version,
           inventory_revision_id,inventory_version,event_time,emitted_at,content_hash)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
          (*scope_key(scope), event_id, subject.sku, subject.store_id, subject.channel,
           "sale" if quantity > 0 else "return", quantity, price, quantity * price, quantity * cost,
           original, price_version, revision, updated["inventory_version"], occurred_at, emitted_at, content_hash))

    def _acknowledge(self, scope, event_id):
        # This time is recorded only after the sale/stock transaction has committed.
        # A separate idempotent update may produce another CDC version of the same sale.
        with self.ledger.connect() as conn:
            return conn.execute(f"""UPDATE pricing.sales_transactions SET
              commit_acknowledged_at=coalesce(commit_acknowledged_at,%s)
              WHERE {SCOPE_WHERE} AND event_id=%s RETURNING *""",
              (datetime.now(UTC), *scope_key(scope), event_id)).fetchone()
