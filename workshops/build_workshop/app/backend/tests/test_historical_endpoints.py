from __future__ import annotations

import httpx


def test_historical_timeseries_day(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(
        f"{api_base_url}/api/historical/timeseries",
        params={
            "start": "2022-07-01T00:00:00Z",
            "end": "2022-07-08T00:00:00Z",
            "bucket": "day",
            "car_type": "yellow",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "meta" in body and "series" in body
    assert body["meta"]["rows_returned"] == len(body["series"])
    # bounded result size (7 days => <= 7 points)
    assert len(body["series"]) <= 31


def test_historical_seasonality_dow_hour(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(
        f"{api_base_url}/api/historical/seasonality",
        params={
            "start": "2022-07-01T00:00:00Z",
            "end": "2022-07-08T00:00:00Z",
            "metric": "trips",
            "mode": "dow_hour",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["x_labels"] and body["y_labels"]
    assert len(body["x_labels"]) == 24
    assert len(body["y_labels"]) == 7
    # cells are sparse but bounded
    assert len(body["cells"]) <= 24 * 7


def test_historical_movers_pickup_zone(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(
        f"{api_base_url}/api/historical/movers",
        params={
            "a_start": "2022-07-02T20:00:00Z",
            "a_end": "2022-07-02T22:00:00Z",
            "b_start": "2022-06-25T20:00:00Z",
            "b_end": "2022-06-25T22:00:00Z",
            "group_by": "pickup_zone",
            "metric": "trips",
            "limit": 20,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    assert len(body["rows"]) <= 20
    # basic shape
    row = body["rows"][0] if body["rows"] else None
    if row:
        assert "key" in row and "label" in row
        assert "a_value" in row and "b_value" in row and "delta" in row


def test_historical_map_trips(api_base_url: str, http: httpx.Client) -> None:
    r = http.get(
        f"{api_base_url}/api/historical/map",
        params={
            "start": "2022-07-02T20:00:00Z",
            "end": "2022-07-02T22:00:00Z",
            "metric": "trips",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["meta"]["rows_returned"] == len(body["rows"])
    # ensure zone_id/value exist
    if body["rows"]:
        assert "zone_id" in body["rows"][0]
        assert "value" in body["rows"][0]

