from datetime import timedelta
from uuid import uuid4
import psycopg
import pytest

from pricing_lab.features import SnapshotHeld
from pricing_lab.contracts import StaleGeneration
from cdc_utils import replay_table

pytestmark = pytest.mark.integration


def test_snapshot_waits_for_stock_as_well_as_sale(features, source, ledger, analytics, scope, subject, sale_request):
    source.create_sale(scope, sale_request)
    replay_table(analytics, ledger, "sales_transactions")
    features.timeout_seconds = .05
    with pytest.raises(SnapshotHeld) as held:
        features.capture(scope, subject)
    assert "inventory_not_converged" in held.value.reason_ids
    replay_table(analytics, ledger, "inventory_history")
    snapshot = features.capture(scope, subject)
    assert snapshot.stock_units == 28


def test_snapshot_evidence_survives_later_cdc_replacement(features, source, ledger, analytics, scope, subject, sale_request, postgres_dsn):
    source.create_sale(scope, sale_request)
    replay_table(analytics, ledger, "sales_transactions")
    replay_table(analytics, ledger, "inventory_history")
    snapshot = features.capture(scope, subject)
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.sales_transactions SET unit_price_minor=700,receipts_minor=quantity*700")
    replay_table(analytics, ledger, "sales_transactions", version=2)
    restored, evidence = features.get_snapshot(scope, snapshot.snapshot_id)
    assert restored == snapshot
    assert evidence["live_sales"][0]["receipts_minor"] == 1600


def test_future_cost_and_equal_time_versions_choose_effective_state(features, ledger, analytics, scope, subject, clock, postgres_dsn):
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("""INSERT INTO pricing.cost_history SELECT %s,owner_id,scenario_id,generation,sku,store_id,
          channel,2,999,selling_cost_minor,disposal_cost_minor,%s,emitted_at FROM pricing.cost_history""",
          (uuid4(), clock.scenario_now()+timedelta(hours=1)))
    replay_table(analytics, ledger, "cost_history")
    assert features.capture(scope, subject).unit_cost_minor == 400
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.subject_state SET cost_version=3,unit_cost_minor=450")
        conn.execute("""INSERT INTO pricing.cost_history SELECT %s,owner_id,scenario_id,generation,sku,store_id,
          channel,3,450,selling_cost_minor,disposal_cost_minor,%s,emitted_at FROM pricing.cost_history WHERE source_version=1""",
          (uuid4(), clock.scenario_now()))
    replay_table(analytics, ledger, "cost_history", version=2)
    assert features.capture(scope, subject).unit_cost_minor == 450


def test_missing_acknowledged_price_holds(features, analytics, scope, subject):
    analytics.command(f"TRUNCATE TABLE {analytics.database}.price_history_cdc")
    with pytest.raises(SnapshotHeld) as held:
        features.capture(scope, subject)
    assert "price_missing" in held.value.reason_ids


def test_same_sale_id_with_unreplicated_correction_holds(features, source, ledger, analytics, scope, subject, sale_request, postgres_dsn):
    source.create_sale(scope,sale_request)
    replay_table(analytics,ledger,"sales_transactions")
    replay_table(analytics,ledger,"inventory_history")
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.sales_transactions SET unit_price_minor=700,receipts_minor=quantity*700")
    features.timeout_seconds=.05
    with pytest.raises(SnapshotHeld) as held:
        features.capture(scope,subject)
    assert "sale_not_converged" in held.value.reason_ids
    replay_table(analytics,ledger,"sales_transactions",version=2)
    snapshot=features.capture(scope,subject)
    _,evidence=features.get_snapshot(scope,snapshot.snapshot_id)
    assert evidence["live_sales"][0]["receipts_minor"] == 1400


def test_snapshot_delivery_retry_preserves_authoritative_evidence(features, ledger, analytics, scope, subject, monkeypatch):
    actual=analytics.client.insert
    def lost_ack(table,*args,**kwargs):
        result=actual(table,*args,**kwargs)
        if table.endswith(".input_snapshots"):
            raise RuntimeError("Lost analytical acknowledgement")
        return result
    with monkeypatch.context() as patch:
        patch.setattr(analytics.client,"insert",lost_ack)
        snapshot=features.capture(scope,subject)
    restored,evidence=features.get_snapshot(scope,snapshot.snapshot_id)
    assert restored==snapshot
    with ledger.connect() as conn:
        assert conn.execute("SELECT delivered_at FROM pricing.outbox WHERE event_id=%s",(snapshot.snapshot_id,)).fetchone()["delivered_at"] is None
    features.deliver_snapshot(snapshot,evidence)
    rows=analytics.query(f"SELECT count() AS count FROM {analytics.database}.input_snapshots FINAL")
    assert rows[0]["count"] == 1


def test_old_generation_cannot_capture_new_state(features, source, ledger, analytics, scope, subject, sale_request, postgres_dsn):
    source.create_sale(scope,sale_request)
    replay_table(analytics,ledger,"sales_transactions")
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.scenarios SET generation=2")
    with pytest.raises(StaleGeneration):
        features.capture(scope,subject)


def test_newer_stale_inventory_holds_by_age(features,ledger,source,analytics,scope,subject,clock):
    from pricing_lab.scenarios import ScenarioService
    ScenarioService(source,ledger,features,clock).trigger(scope,'stale_inventory',uuid4())
    replay_table(analytics,ledger,'inventory_history')
    with pytest.raises(SnapshotHeld) as held:
        features.capture(scope,subject)
    assert held.value.reason_ids==['inventory_stale']
    with ledger.connect() as conn:
        assert conn.execute('SELECT count(*) AS n FROM pricing.snapshots').fetchone()['n']==0
