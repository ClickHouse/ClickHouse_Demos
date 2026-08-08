from datetime import date
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pytest

import scripts.promote_to_golden as promotion
from scripts.promote_to_golden import build_dataset_item, production_provenance


def test_build_dataset_item_shape_and_serialization():
    item = build_dataset_item(
        qid="q101", question="How many orders in Q1?",
        golden_sql="SELECT count() FROM v_orders",
        tier=2, ordered=False,
        rows=[[date(2026, 1, 1), 5]], cols=["d", "n"], float_dp=4)
    assert item["id"] == "q101"
    assert item["question"] == "How many orders in Q1?"
    eo = item["expected_output"]
    assert eo["golden_sql"] == "SELECT count() FROM v_orders"
    assert eo["columns"] == ["d", "n"]
    assert eo["rows"] == [["2026-01-01", 5]]   # date serialized, int kept
    assert eo["ordered"] is False
    meta = item["metadata"]
    assert meta["golden_sql"] == "SELECT count() FROM v_orders"
    assert meta["tier"] == 2 and meta["ordered"] is False
    assert meta["float_dp"] == 4 and meta["question"] == "How many orders in Q1?"


def test_production_item_preserves_review_provenance():
    item = build_dataset_item(
        qid="prod-active-001", question="How many active customers do we have?",
        golden_sql="SELECT uniqExact(customer_id) FROM v_orders",
        tier=2, ordered=False, rows=[[10]], cols=["count"], float_dp=4,
        provenance={
            "source": "production-feedback", "source_trace_id": "trace-1",
            "failure_category": "stale-business-policy",
            "source_policy_version": "policy-v1", "annotation_id": "queue-item-1",
        },
    )

    assert item["metadata"] == {
        "ordered": False,
        "float_dp": 4,
        "golden_sql": "SELECT uniqExact(customer_id) FROM v_orders",
        "tier": 2,
        "question": "How many active customers do we have?",
        "source": "production-feedback",
        "source_trace_id": "trace-1",
        "failure_category": "stale-business-policy",
        "source_policy_version": "policy-v1",
        "annotation_id": "queue-item-1",
    }


def test_provenance_cannot_overwrite_grading_metadata_or_question():
    item = build_dataset_item(
        qid="q101", question="How many orders in Q1?",
        golden_sql="SELECT count() FROM v_orders",
        tier=2, ordered=False, rows=[[10]], cols=["count"], float_dp=4,
        provenance={
            "source": "production-feedback", "source_trace_id": "trace-1",
            "failure_category": "stale-business-policy",
            "source_policy_version": "policy-v1", "golden_sql": "SELECT 0",
            "tier": 0, "ordered": True, "float_dp": 0, "question": "Wrong",
        },
    )

    assert item["metadata"]["golden_sql"] == "SELECT count() FROM v_orders"
    assert item["metadata"]["tier"] == 2
    assert item["metadata"]["ordered"] is False
    assert item["metadata"]["float_dp"] == 4
    assert item["metadata"]["question"] == "How many orders in Q1?"


def test_production_provenance_rejects_missing_trace():
    with pytest.raises(ValueError, match="source_trace_id"):
        production_provenance({"source": "production-feedback"})


def test_tracked_production_review_fixture_is_complete_and_safe():
    fixture = Path(__file__).parent / "fixtures" / "reviewed.production-example.json"
    records = json.loads(fixture.read_text())

    assert [record["id"] for record in records] == [
        "prod-active-001", "prod-active-002", "prod-active-003",
    ]
    assert [record["question"] for record in records] == [
        "How many active customers do we have?",
        "What is our active customer count right now?",
        "How many customers qualify as active under our business definition?",
    ]
    for record in records:
        assert production_provenance(record) == {
            "source": "production-feedback",
            "source_trace_id": "seeded-incident-active-customer-trace",
            "failure_category": "stale-business-policy",
            "source_policy_version": "policy-v1",
            "annotation_id": "seeded-incident-active-customer-annotation",
        }
        assert "SELECT uniqExact(customer_id) FROM v_orders" in record["golden_sql"]
        assert "order_ts >= now() - INTERVAL 30 DAY" in record["golden_sql"]
        assert "status NOT IN ('cancelled','returned')" in record["golden_sql"]
        assert not any(re.search(r"key|token|secret|password|url|project", key, re.I)
                           for key in record)
        assert "http" not in json.dumps(record).lower()
        assert "sk-" not in json.dumps(record).lower()


def test_invalid_production_provenance_stops_before_query(monkeypatch, tmp_path):
    reviewed = tmp_path / "invalid-reviewed.json"
    reviewed.write_text(json.dumps([{
        "id": "prod-active-001",
        "question": "How many active customers do we have?",
        "golden_sql": "SELECT should_not_run()",
        "source": "production-feedback",
    }]))

    query_calls = []

    class FakeRO:
        def __init__(self, _config):
            pass

        def query(self, sql):
            query_calls.append(sql)

    class FakeTracer:
        def __init__(self, _config):
            pass

    monkeypatch.setattr(promotion, "load_config", lambda: SimpleNamespace(
        clickhouse=object(), langfuse=object(), eval=SimpleNamespace(float_dp=4)))
    monkeypatch.setattr(promotion, "ROClickHouseClient", FakeRO)
    monkeypatch.setattr(promotion, "LangfuseTracer", FakeTracer)
    monkeypatch.setattr(promotion.sys, "argv", ["promote_to_golden.py", str(reviewed)])

    with pytest.raises(ValueError, match="source_trace_id"):
        promotion.main()

    assert query_calls == []


def test_promotion_completion_message_uses_venv_module_command(
    monkeypatch, tmp_path, capsys
):
    reviewed = tmp_path / "reviewed.json"
    reviewed.write_text(json.dumps([{
        "id": "reviewed-1",
        "question": "Reviewed question?",
        "golden_sql": "SELECT 1",
    }]))

    class FakeRO:
        def __init__(self, _config):
            pass

        def query(self, _sql):
            return SimpleNamespace(rows=[[1]], cols=["result"])

    class FakeTracer:
        def __init__(self, _config):
            pass

        def ensure_dataset(self, _items):
            pass

        def flush(self):
            pass

    monkeypatch.setattr(promotion, "load_config", lambda: SimpleNamespace(
        clickhouse=object(), langfuse=object(), eval=SimpleNamespace(float_dp=4)))
    monkeypatch.setattr(promotion, "ROClickHouseClient", FakeRO)
    monkeypatch.setattr(promotion, "LangfuseTracer", FakeTracer)
    monkeypatch.setattr(promotion.sys, "argv", ["promote_to_golden.py", str(reviewed)])

    promotion.main()

    assert "re-run `.venv/bin/python -m eval.harness`" in capsys.readouterr().out
