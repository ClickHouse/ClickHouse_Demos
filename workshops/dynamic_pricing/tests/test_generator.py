import json

import pyarrow.parquet as pq
import pytest

from generator.generate import generate_dataset
from generator.manifest import iter_manifest_rows, read_manifest


def test_manifest_reconciles_parquet(tmp_path):
    path = generate_dataset(tmp_path, rows=1001, seed=17, tier="test")
    manifest = read_manifest(path)
    assert sum(f["rows"] for f in manifest["files"]) == 1001
    events = list(iter_manifest_rows(path))
    assert len({r.event_id for r in events}) == 1001
    tables = [pq.read_table(tmp_path / f["key"]) for f in manifest["files"]]
    assert sum(sum(t["receipts_minor"].to_pylist()) for t in tables) == sum(f["receipts_minor"] for f in manifest["files"])
    assert all(r.event_time.isoformat() < "2026-01-30" for r in events)
    assert any(r.quantity < 0 for r in events)


def test_same_seed_generates_identical_bytes(tmp_path):
    a = read_manifest(generate_dataset(tmp_path / "a", 1001, 17, "test"))
    b = read_manifest(generate_dataset(tmp_path / "b", 1001, 17, "test"))
    assert a == b


def test_zero_sales_intervals_remain_available_to_training(tmp_path):
    manifest = read_manifest(generate_dataset(tmp_path, 1001, 17, "test"))
    intervals = pq.read_table(tmp_path / "availability.parquet").to_pylist()
    events = list(iter_manifest_rows(tmp_path / "manifest.json"))
    counts = {(r.subject.sku, r.subject.store_id, r.event_time.replace(minute=0, second=0, microsecond=0))
              for r in events if r.calibration}
    assert len(intervals) == 24
    assert any((r["sku"], r["store_id"], r["hour"]) not in counts for r in intervals)
    assert all(r["available_units"] == 10000 for r in intervals)


def test_calibration_identical_across_full_tiers(tmp_path):
    # Small complete core cohorts retain all 90 days; count is independent of tier.
    a = read_manifest(generate_dataset(tmp_path / "a", 40000, 17, "core"))
    b = read_manifest(generate_dataset(tmp_path / "b", 41000, 17, "showcase"))
    assert [f for f in a["files"] if f["key"].startswith("calibration")] == [f for f in b["files"] if f["key"].startswith("calibration")]
    assert a["companion_files"] == b["companion_files"]


def test_calibration_covers_keep_price_and_supported_step(tmp_path):
    generate_dataset(tmp_path, 40000, 17, "core")
    intervals = pq.read_table(tmp_path / "availability.parquet").to_pylist()
    assert all(r["offered_price_minor"] % 50 == 0 for r in intervals)
    for sku, reference in [(101, 800), (102, 1000)]:
        assert any(r["sku"] == sku and r["offered_price_minor"] == reference for r in intervals)


def test_manifest_refuses_corrupt_file_and_path_escape(tmp_path):
    path = generate_dataset(tmp_path, 1001, 17, "test")
    manifest = json.loads(path.read_text())
    file = tmp_path / manifest["files"][0]["key"]
    file.write_bytes(file.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        read_manifest(path)
    manifest["files"][0]["key"] = "../outside.parquet"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="path"):
        read_manifest(path)


def test_too_small_requested_count_never_truncates_calibration(tmp_path):
    with pytest.raises(ValueError, match="calibration"):
        generate_dataset(tmp_path, 1, 17, "core")


def test_manifest_declared_economics_match_actual_bytes(tmp_path):
    path = generate_dataset(tmp_path, 1001, 17, "test")
    manifest = json.loads(path.read_text())
    manifest["files"][0]["receipts_minor"] += 1
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="totals"):
        read_manifest(path)
