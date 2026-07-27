from __future__ import annotations

import math

import httpx


def test_health(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(f"{api_base_url}/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["clickhouse"]["ok"] is True
    assert isinstance(body["clickhouse"].get("version"), str)


def test_filters_zones(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(f"{api_base_url}/api/filters/zones")
    assert r.status_code == 200
    zones = r.json()["zones"]
    assert len(zones) >= 10
    assert any(z["zone_id"] == 161 and z["zone"] for z in zones)


def test_metrics_timeseries(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/timeseries",
        params={"start": start, "end": end, "interval": "15m"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["series"])
    assert len(body["series"]) > 0
    # We expect at least some trips in the selected time window.
    total_trips = sum(int(p["trips"]) for p in body["series"])
    assert total_trips > 0

    for p in body["series"]:
        assert p["p95_duration_s"] >= p["p50_duration_s"]
        assert p["fare"] >= 0
        assert p["tip"] >= 0


def test_metrics_top_zones_pickup_trips(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/top_zones",
        params={"start": start, "end": end, "metric": "trips", "direction": "pickup", "limit": 10},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    assert len(body["rows"]) > 0

    top = body["rows"][0]
    assert "zone_id" in top and "value" in top


def test_metrics_top_zones_dropoff_trips(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/top_zones",
        params={"start": start, "end": end, "metric": "trips", "direction": "dropoff", "limit": 10},
    )
    assert r.status_code == 200
    top = r.json()["rows"][0]
    assert "zone_id" in top and "value" in top


def test_metrics_zone_stats_pickup(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/zone_stats",
        params={"start": start, "end": end, "group_by": "pickup_zone"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    # Ensure at least one row exists and quantiles are sane.
    assert len(body["rows"]) > 0
    row = body["rows"][0]
    assert row["p95_duration_s"] >= row["p50_duration_s"]


def test_metrics_worst_pairs(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/worst_pairs",
        params={"start": start, "end": end, "metric": "p95_duration_s", "limit": 20},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    assert len(body["rows"]) > 0
    # Sanity: returned rows have expected fields and plausible values.
    row = body["rows"][0]
    assert "pickup_zone" in row and "dropoff_zone" in row
    assert int(row["trips"]) >= 1
    assert row["p95_duration_s"] is None or row["p95_duration_s"] >= 0


def test_compare_period_delta_pct_null_when_b_zero(api_base_url: str, http: httpx.Client) -> None:
    # Period B has no trips; delta_pct should be null.
    r = http.get(
        f"{api_base_url}/api/compare/period",
        params={
            "a_start": "2026-01-02T20:00:00Z",
            "a_end": "2026-01-02T21:00:00Z",
            "b_start": "2026-01-02T19:00:00Z",
            "b_end": "2026-01-02T20:00:00Z",
            "group_by": "pickup_zone",
            "metric": "trips",
            "limit": 20,
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) > 0
    assert all(x["b_value"] == 0 for x in rows)
    assert all(x["delta_pct"] is None for x in rows)


def test_compare_period_with_zone_filter(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    # Regression: a pickup/dropoff zone filter must be applied against the real
    # taxi_trips columns (pickup_location_id / dropoff_location_id), not the
    # output aliases pickup_zone_id / dropoff_zone_id, which do not exist in the
    # CTE scope and used to raise ClickHouse UNKNOWN_IDENTIFIER (code 47).
    start, end = sample_window
    for filter_key in ("pickup_zone_id", "dropoff_zone_id"):
        r = http.get(
            f"{api_base_url}/api/compare/period",
            params={
                "a_start": start,
                "a_end": end,
                "b_start": start,
                "b_end": end,
                "group_by": "pickup_zone",
                "metric": "trips",
                "limit": 20,
                filter_key: [161],
            },
        )
        assert r.status_code == 200, f"{filter_key}: {r.text}"


def test_anomalies_tip_ratio_with_threshold(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/anomalies/fare_outliers",
        params={
            "start": start,
            "end": end,
            "rule": "tip_ratio",
            "min_threshold": 0.15,
            "limit": 200,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    assert len(body["rows"]) > 0
    # tip_ratio score is tip/fare, so should be >= threshold.
    for row in body["rows"]:
        assert float(row["score"]) >= 0.15
        assert row["duration_s"] >= 0


def test_trips_pagination_and_filtering(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    start, end = sample_window
    r1 = http.get(
        f"{api_base_url}/api/trips",
        params={
            "start": start,
            "end": end,
            "sort": "pickup_datetime",
            "order": "asc",
            "limit": 5,
            "offset": 0,
        },
    )
    assert r1.status_code == 200
    body1 = r1.json()
    assert len(body1["rows"]) == 5
    first_pickup = body1["rows"][0]["pickup_datetime"]

    r2 = http.get(
        f"{api_base_url}/api/trips",
        params={
            "start": start,
            "end": end,
            "sort": "pickup_datetime",
            "order": "asc",
            "limit": 5,
            "offset": 5,
        },
    )
    assert r2.status_code == 200
    body2 = r2.json()
    assert len(body2["rows"]) == 5
    assert body2["rows"][0]["pickup_datetime"] != first_pickup

    # Filter to pickup_zone_id=161 should return only that pickup zone (if present in the selected window).
    r3 = http.get(
        f"{api_base_url}/api/trips",
        params={
            "start": start,
            "end": end,
            "limit": 1000,
            "pickup_zone_id": [161],
        },
    )
    assert r3.status_code == 200
    rows = r3.json()["rows"]
    for row in rows:
        assert row["pickup_zone"]


def test_meta_includes_inlined_sql(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    # Every analytics panel gets the executed SQL back in meta.sql so the UI can show it.
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/top_zones",
        params={"start": start, "end": end, "metric": "trips", "direction": "pickup", "limit": 10},
    )
    assert r.status_code == 200
    sql = r.json()["meta"]["sql"]
    assert isinstance(sql, str) and sql
    # Runnable: no leftover {name:Type} bind placeholders, real table, inlined window + limit.
    assert "{" not in sql and "}" not in sql
    assert "FROM taxi_trips" in sql
    assert "2022-07-02 20:00:00" in sql
    assert "LIMIT 10" in sql


def test_meta_sql_inlines_filters(api_base_url: str, http: httpx.Client, sample_window: tuple[str, str]) -> None:
    # A bound scalar filter (vendor_id) is inlined as a literal, not left as a placeholder.
    start, end = sample_window
    r = http.get(
        f"{api_base_url}/api/metrics/timeseries",
        params={"start": start, "end": end, "interval": "15m", "vendor_id": 1},
    )
    assert r.status_code == 200
    sql = r.json()["meta"]["sql"]
    assert "{" not in sql
    assert "vendor_id = 1" in sql

