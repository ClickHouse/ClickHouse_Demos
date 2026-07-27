from datetime import datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

import app as dashboard_app


client = TestClient(dashboard_app.app)


def test_dashboard_rejects_reversed_date_range(monkeypatch):
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("ClickHouse must not be queried for an invalid range")

    monkeypatch.setattr(dashboard_app, "_run", fail_if_called)

    response = client.get(
        "/api/dashboard",
        params={
            "pair": "EUR/USD",
            "start": "2020-01-10",
            "end": "2020-01-01",
            "bucket": "day",
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "start must be on or before end"}


def test_dashboard_binds_filters_in_both_queries(monkeypatch):
    calls = []

    def fake_run(sql, params=None):
        calls.append((sql, params))
        if "GROUP BY t" in sql:
            result = SimpleNamespace(
                result_rows=[
                    (datetime(2020, 1, 1), 1.1, 1.2, 1.0, 1.15, 42, 0.0001)
                ]
            )
            return result, 2.0, 42, 1.5

        result = SimpleNamespace(
            result_rows=[(42, 0.0001, 0.0002, 1.15, 1.0, 1.2)]
        )
        return result, 1.0, 42, 0.5

    monkeypatch.setattr(dashboard_app, "_run", fake_run)

    response = client.get(
        "/api/dashboard",
        params={
            "pair": "EUR/USD",
            "start": "2020-01-01",
            "end": "2020-01-02",
            "bucket": "hour",
        },
    )

    assert response.status_code == 200
    assert response.json()["timing"] == {
        "server_ms": 2.0,
        "wall_ms": 3.0,
        "rows_read": 84,
        "queries": 2,
    }
    assert len(calls) == 2
    for sql, params in calls:
        assert "base = {base:String}" in sql
        assert "quote = {quote:String}" in sql
        assert params == {
            "base": "EUR",
            "quote": "USD",
            "start": "2020-01-01 00:00:00",
            "end": "2020-01-03 00:00:00",
        }
