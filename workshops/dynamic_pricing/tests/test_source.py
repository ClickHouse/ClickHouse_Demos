from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest

from pricing_lab.contracts import Subject, WorkshopError
from pricing_lab.source import SourceStore

pytestmark = pytest.mark.integration


def test_duplicate_sale_does_not_repeat_stock_decrement(source, scope, sale_request):
    first = source.create_sale(scope, sale_request)
    second = source.create_sale(scope, sale_request)
    assert first["event_id"] == second["event_id"]
    assert first["inventory_revision_id"] == second["inventory_revision_id"]
    assert first["commit_acknowledged_at"] == second["commit_acknowledged_at"]
    assert source.stock(scope, sale_request.subject) == 28


def test_concurrent_same_id_only_decrements_once(source, scope, sale_request):
    with ThreadPoolExecutor(max_workers=4) as pool:
        result = list(pool.map(lambda _: source.create_sale(scope, sale_request), range(4)))
    assert len({r["inventory_revision_id"] for r in result}) == 1
    assert source.stock(scope, sale_request.subject) == 28


def test_same_id_different_contents_refuses_conflict(source, scope, sale_request):
    source.create_sale(scope, sale_request)
    with pytest.raises(WorkshopError) as error:
        source.create_sale(scope, sale_request.model_copy(update={"quantity": 3}))
    assert error.value.code == "event_conflict"
    assert source.stock(scope, sale_request.subject) == 28


def test_only_one_sale_wins_last_unit(source, scope, sale_request, postgres_dsn):
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.subject_state SET stock_units=1")
    def attempt(_):
        try:
            source.create_sale(scope, sale_request.model_copy(update={"event_id": uuid4(), "quantity": 1}))
            return "accepted"
        except WorkshopError as error:
            return error.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, range(2)))
    assert sorted(results) == ["accepted", "insufficient_stock"]
    assert source.stock(scope, sale_request.subject) == 0


def test_wrong_price_version_rolls_back(source, scope, sale_request):
    with pytest.raises(WorkshopError) as error:
        source.create_sale(scope, sale_request.model_copy(update={"expected_price_version": 2}))
    assert error.value.code == "price_version_changed"
    assert source.stock(scope, sale_request.subject) == 30


def test_returns_reference_original_receipt_and_cannot_over_return(source, scope, sale_request):
    source.create_sale(scope, sale_request)
    key = uuid4()
    returned = source.create_return(scope, sale_request.event_id, 1, key)
    retried = source.create_return(scope, sale_request.event_id, 1, key)
    assert returned["receipts_minor"] == -800
    assert returned["sold_cost_minor"] == -400
    assert returned["inventory_revision_id"] == retried["inventory_revision_id"]
    assert source.stock(scope, sale_request.subject) == 29
    with pytest.raises(WorkshopError) as error:
        source.create_return(scope, sale_request.event_id, 2, uuid4())
    assert error.value.code == "invalid_return"
    assert source.stock(scope, sale_request.subject) == 29


def test_inventory_insert_failure_rolls_back_sale(source, scope, sale_request, postgres_dsn):
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("""CREATE FUNCTION pricing.fail_inventory() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected inventory failure'; END $$;
                        CREATE TRIGGER fail_inventory BEFORE INSERT ON pricing.inventory_history
                        FOR EACH ROW EXECUTE FUNCTION pricing.fail_inventory();""")
    with pytest.raises(psycopg.Error, match="injected inventory failure"):
        source.create_sale(scope, sale_request)
    assert source.stock(scope, sale_request.subject) == 30
    with psycopg.connect(postgres_dsn) as conn:
        assert conn.execute("SELECT count(*) FROM pricing.sales_transactions").fetchone()[0] == 0


def test_same_event_id_on_two_subjects_returns_conflict_not_database_error(source, ledger, scope, sale_request, golden_data, clock, monkeypatch):
    other = Subject(sku=102, store_id=1)
    ledger.initialize(scope, other, golden_data["state"], golden_data["rules"], clock.scenario_now())
    barrier = Barrier(2)
    actual = source._existing
    def concurrent_lookup(conn, scope, event_id, content_hash):
        result = actual(conn, scope, event_id, content_hash)
        barrier.wait(timeout=5)
        return result
    monkeypatch.setattr(source, "_existing", concurrent_lookup)
    def attempt(subject):
        try:
            source.create_sale(scope, sale_request.model_copy(update={"subject": subject}))
            return "accepted"
        except WorkshopError as error:
            return error.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        result = list(pool.map(attempt, [sale_request.subject, other]))
    assert sorted(result) == ["accepted", "event_conflict"]
    assert source.stock(scope, sale_request.subject) + source.stock(scope, other) == 58
