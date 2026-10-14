import pytest

pytestmark = pytest.mark.integration


def test_canonical_rollup_matches_raw_before_and_after_merges(loader, scope, manifest_uri, analytics):
    result = loader.load(manifest_uri, scope)
    columns = analytics.query(
        "SELECT table,name FROM system.columns WHERE database={db:String} AND table IN {tables:Array(String)}",
        {"db": analytics.database, "tables": [result["raw_table"], result["rollup_table"]]},
    )
    assert all(row["name"] != "sandbox_id" for row in columns)
    for _ in range(2):
        verified = loader.verify(result["run_id"])
        assert verified["canonical_rows"] == 3
        assert verified["net_units"] == 4
        assert verified["receipts_minor"] == 2900
        assert verified["sold_cost_minor"] == 1600
        assert verified["rollup"] == {"net_units": 4, "receipts_minor": 2900, "sold_cost_minor": 1600}
        # A characterization of merge-independent reads, never a learner tuning command.
        analytics.command(f"OPTIMIZE TABLE {analytics.database}.{result['rollup_table']} FINAL")
    explain = analytics.query(f"EXPLAIN indexes = 1 SELECT sum(receipts_minor) FROM {analytics.database}.{result['raw_table']} WHERE store_id=1 AND sku=101")
    assert any("PrimaryKey" in str(row) for row in explain)

@pytest.mark.integration
def test_historical_baseline_before_cdc(analytics,ledger,scope,subject,clock,manifest_uri):
    from pricing_lab.history import HistoryLoader
    from pricing_lab.features import FeatureService
    from clickhouse_connect.driver.exceptions import DatabaseError
    HistoryLoader(analytics,ledger).load(manifest_uri,scope)
    service=FeatureService(analytics,ledger,clock)
    result=service.reconcile(scope,subject,include_live=False)
    assert result['raw']==result['rollup'] and result['raw']['net_units']>0
    from fastapi.testclient import TestClient
    from pricing_lab.api import create_app
    from pricing_lab.settings import Settings
    settings=Settings(session_secret='baseline-session-secret-at-least-32-characters')
    with TestClient(create_app(settings,{'ledger':ledger,'features':service,'mode':'live'}),base_url=settings.allowed_origin) as client:
        client.post('/api/session',json={},headers={'Origin':settings.allowed_origin})
        response=client.get('/api/baseline')
        assert response.status_code==200
        assert response.json()['data']['reconciliation']['raw']==result['raw']
    with pytest.raises(DatabaseError,match='inventory_live_current'): service.capture(scope,subject)
