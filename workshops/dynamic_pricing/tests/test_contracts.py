from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from pricing_lab.contracts import SaleRequest, Scope, Subject, Snapshot
from pricing_lab.settings import Settings


@pytest.mark.parametrize("quantity", [0, -1, True, 1.5])
def test_invalid_sale_cannot_enter_source(subject, quantity):
    with pytest.raises(ValidationError):
        SaleRequest(event_id=uuid4(), subject=subject, quantity=quantity,
                    occurred_at="2026-01-30T12:00:00Z", expected_price_version=1)


def test_sale_rejects_client_price_and_naive_time(subject):
    values = dict(event_id=uuid4(), subject=subject, quantity=1,
                  occurred_at="2026-01-30T12:00:00Z", expected_price_version=1)
    with pytest.raises(ValidationError):
        SaleRequest(**values, price_minor=1)
    with pytest.raises(ValidationError):
        SaleRequest(**{**values, "occurred_at": datetime(2026, 1, 30)})


def test_invalid_subject_scope_cannot_cross_boundary():
    with pytest.raises(ValidationError):
        Subject(sku=101, store_id=1, channel="online")
    with pytest.raises(ValidationError):
        Scope(owner_id="learner-1", scenario_id="grocery", generation=0)
    with pytest.raises(ValidationError):
        Scope(owner_id="../another", scenario_id="grocery", generation=1)


@pytest.mark.parametrize("field,value", [("stock_units", -1), ("current_price_minor", 0),
                                         ("source_ages_seconds", {"stock": float("nan")})])
def test_invalid_snapshot_holds_instead_of_zero_filling(golden_snapshot, field, value):
    with pytest.raises(ValidationError):
        Snapshot.model_validate({**golden_snapshot.model_dump(), field: value})


def test_golden_facts_reconcile_without_model(golden_data):
    rows = golden_data["history"]
    assert sum(r["quantity"] for r in rows) == 4
    assert sum(r["receipts_minor"] for r in rows) == 2900
    assert sum(r["sold_cost_minor"] for r in rows) == 1600
    assert sum(r["receipts_minor"] - r["sold_cost_minor"] for r in rows) == 1300


def test_secret_settings_do_not_print_credentials():
    settings = Settings(postgres_dsn="postgresql://alice:private-password@example.test/pricing",
                        clickhouse_password="private-password")
    assert "private-password" not in repr(settings)
    assert "private-password" not in settings.model_dump_json()


@pytest.mark.parametrize("host", ["https://example.test", "host/path", "host:8443"])
def test_cloud_host_requires_separate_port_and_tls(host):
    with pytest.raises(ValidationError):
        Settings(clickhouse_host=host)
