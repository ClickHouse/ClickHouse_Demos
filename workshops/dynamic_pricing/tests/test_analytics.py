from datetime import UTC, datetime
import pytest

pytestmark = pytest.mark.integration


def test_utc_clickhouse_dates_retain_zone_and_milliseconds(analytics):
    row = analytics.query("SELECT toDateTime64('2026-01-30 12:00:00.123',3,'UTC') AS effective_at")[0]
    assert row["effective_at"] == datetime(2026,1,30,12,0,0,123000,tzinfo=UTC)


def test_initialize_can_bootstrap_a_missing_database(analytics):
    from uuid import uuid4
    from pricing_lab.analytics import AnalyticsClient
    name='fresh_'+uuid4().hex
    fresh=AnalyticsClient(analytics.settings.model_copy(update={'clickhouse_database':name}))
    try:
        fresh.bootstrap_database()
        fresh.bootstrap_database()
        assert fresh.query('SELECT currentDatabase() AS name')[0]['name']==name
    finally:
        fresh.close()
        analytics.command(f'DROP DATABASE IF EXISTS {name} SYNC')
