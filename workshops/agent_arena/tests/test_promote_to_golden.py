from datetime import date

import pytest

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
