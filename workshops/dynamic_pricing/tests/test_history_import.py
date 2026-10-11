from concurrent.futures import ThreadPoolExecutor
from threading import Event
import shutil

import pytest

from pricing_lab.history import HistoryLoader, ImportInterrupted
from pricing_lab.contracts import WorkshopError
from conftest import LAB_ROOT

pytestmark = pytest.mark.integration


def test_ambiguous_import_restarts_isolated_stage(loader, scope, manifest_uri, monkeypatch):
    actual = loader._insert_file
    def lost_ack(*args):
        actual(*args)
        raise ImportInterrupted("The import acknowledgement was lost.")
    with monkeypatch.context() as patch:
        patch.setattr(loader, "_insert_file", lost_ack)
        with pytest.raises(ImportInterrupted):
            loader.load(manifest_uri, scope)
    result = loader.load(manifest_uri, scope)
    assert result["canonical_rows"] == 3
    assert result["receipts_minor"] == 2900
    assert loader.verify(result["run_id"])["net_units"] == 4


def test_invalid_checksum_never_replaces_active_history(loader, scope, manifest_uri, tmp_path):
    first = loader.load(manifest_uri, scope)
    shutil.copytree(LAB_ROOT / "fixtures/manifests", tmp_path / "bad")
    file = tmp_path / "bad/golden.parquet"
    file.write_bytes(file.read_bytes() + b"bad")
    with pytest.raises(ValueError, match="checksum"):
        loader.load((tmp_path / "bad/golden.json").as_uri(), scope)
    assert loader.active(scope)["run_id"] == first["run_id"]


def test_overlapping_imports_refuse_instead_of_mix_targets(loader, ledger, analytics, scope, manifest_uri, monkeypatch):
    entered, release = Event(), Event()
    actual = loader._insert_file
    def blocked(*args):
        entered.set()
        assert release.wait(timeout=10)
        actual(*args)
    monkeypatch.setattr(loader, "_insert_file", blocked)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(loader.load, manifest_uri, scope)
        assert entered.wait(timeout=10)
        try:
            with pytest.raises(WorkshopError) as error:
                HistoryLoader(analytics, ledger).load(manifest_uri, scope)
            assert error.value.code == "import_running"
        finally:
            release.set()
        assert pending.result()["canonical_rows"] == 3


def test_failed_stage_removes_its_tables_without_losing_active_set(loader, ledger, analytics, scope, manifest_uri, monkeypatch):
    initial = loader.load(manifest_uri, scope)
    actual = loader._insert_file
    def lost_ack(*args):
        actual(*args)
        raise ImportInterrupted("Lost acknowledgement")
    monkeypatch.setattr(loader, "_insert_file", lost_ack)
    with pytest.raises(ImportInterrupted):
        loader.load(manifest_uri, scope)
    with ledger.connect() as conn:
        abandoned = conn.execute("SELECT data FROM pricing.import_runs WHERE status='abandoned'").fetchone()["data"]
    for key in ("raw_table", "rollup_table", "mv_table"):
        rows = analytics.query("SELECT count() AS count FROM system.tables WHERE database={db:String} AND name={name:String}", {"db":analytics.database,"name":abandoned[key]})
        assert rows[0]["count"] == 0
    assert loader.active(scope)["run_id"] == initial["run_id"]
    assert analytics.query(f"SELECT count() AS count FROM {analytics.database}.sales_history")[0]["count"] == 3


def test_failed_view_promotion_restores_previous_pair(loader, analytics, scope, manifest_uri, monkeypatch):
    initial = loader.load(manifest_uri, scope)
    actual, failed = analytics.command, False
    def fail_second_view(sql, parameters=None):
        nonlocal failed
        if "CREATE OR REPLACE VIEW" in sql and ".sales_history_hourly " in sql and not failed:
            failed = True
            raise ImportInterrupted("Promotion interrupted")
        return actual(sql, parameters)
    monkeypatch.setattr(analytics, "command", fail_second_view)
    with pytest.raises(ImportInterrupted):
        loader.load(manifest_uri, scope)
    views = analytics.query("SELECT create_table_query FROM system.tables WHERE database={db:String} AND name='sales_history'", {"db":analytics.database})
    assert initial["raw_table"] in views[0]["create_table_query"]
