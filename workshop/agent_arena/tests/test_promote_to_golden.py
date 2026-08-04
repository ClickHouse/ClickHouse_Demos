from datetime import date
from scripts.promote_to_golden import build_dataset_item


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
