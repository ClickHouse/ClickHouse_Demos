from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from app.chat import router as chat_router
from app.chat_service import shutdown_tracing
from app.db import get_client, run_query
from app.observability import configure_logging
from app.query_builders import (
    anomalies_sql,
    compare_period_sql,
    historical_map_sql,
    historical_movers_sql,
    historical_seasonality_sql,
    historical_timeseries_sql,
    timeseries_sql,
    top_zones_sql,
    trips_sql,
    zone_stats_sql,
    worst_pairs_sql,
)
from app.schemas import (
    AnomaliesResponse,
    AnomalyRule,
    CompareResponse,
    Direction,
    HistoricalBucket,
    HistoricalGroupBy,
    HistoricalMetric,
    HealthClickHouse,
    HealthResponse,
    Interval,
    MapResponse,
    MetricCompare,
    MetricTopZones,
    MetricWorstPairs,
    MoversResponse,
    Meta,
    Order,
    SeasonalityMode,
    SeasonalityResponse,
    TimeseriesResponse,
    TopZonesResponse,
    TripsResponse,
    TripSort,
    ZoneGroupBy,
    ZonesResponse,
    ZoneStatsResponse,
    WorstPairsResponse,
    HistoricalTimeseriesResponse,
)
from app.settings import settings
# Structured stdout logging, configured at import so it is in place before the
# app is built. Traces are wired separately via opentelemetry-instrument (see
# backend/entrypoint.sh and OBSERVABILITY.md).
configure_logging()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    # Flush any buffered Langfuse events on shutdown (no-op when tracing is disabled).
    shutdown_tracing()


app = FastAPI(title="NYC Taxi Ops War Room API", version="0.1.0", lifespan=lifespan)

origins = [o.strip() for o in settings.api_cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    client = get_client()
    try:
        version = client.command("SELECT version()")
        return HealthResponse(ok=True, clickhouse=HealthClickHouse(ok=True, version=str(version)))
    except Exception:
        return HealthResponse(ok=True, clickhouse=HealthClickHouse(ok=False, version=None))


@app.get("/api/filters/zones", response_model=ZonesResponse)
def zones() -> ZonesResponse:
    client = get_client()
    rows, _ = run_query(
        client,
        """
SELECT
  location_id AS zone_id,
  borough,
  zone,
  subregion AS service_zone,
  NULL AS centroid_lat,
  NULL AS centroid_lon
FROM taxi_zones
ORDER BY borough, zone
""",
    )
    return ZonesResponse(zones=rows)


@app.get("/api/metrics/timeseries", response_model=TimeseriesResponse)
def metrics_timeseries(
    start: datetime,
    end: datetime,
    interval: Interval,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> TimeseriesResponse:
    client = get_client()
    sql, params = timeseries_sql(
        start=start,
        end=end,
        interval=interval,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    series, meta = run_query(client, sql, params)
    return TimeseriesResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), series=series)


@app.get("/api/metrics/top_zones", response_model=TopZonesResponse)
def metrics_top_zones(
    start: datetime,
    end: datetime,
    metric: MetricTopZones,
    direction: Direction,
    limit: int = 10,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> TopZonesResponse:
    client = get_client()
    limit = max(1, min(int(limit), 100))
    sql, params = top_zones_sql(
        start=start,
        end=end,
        metric=metric,
        direction=direction,
        limit=limit,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return TopZonesResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/metrics/zone_stats", response_model=ZoneStatsResponse)
def metrics_zone_stats(
    start: datetime,
    end: datetime,
    group_by: ZoneGroupBy,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> ZoneStatsResponse:
    client = get_client()
    sql, params = zone_stats_sql(
        start=start,
        end=end,
        group_by=group_by,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return ZoneStatsResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/metrics/worst_pairs", response_model=WorstPairsResponse)
def metrics_worst_pairs(
    start: datetime,
    end: datetime,
    metric: MetricWorstPairs,
    limit: int = 20,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> WorstPairsResponse:
    client = get_client()
    limit = max(1, min(int(limit), 200))
    sql, params = worst_pairs_sql(
        start=start,
        end=end,
        metric=metric,
        limit=limit,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return WorstPairsResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/compare/period", response_model=CompareResponse)
def compare_period(
    a_start: datetime,
    a_end: datetime,
    b_start: datetime,
    b_end: datetime,
    group_by: ZoneGroupBy,
    metric: MetricCompare,
    limit: int = 20,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> CompareResponse:
    client = get_client()
    limit = max(1, min(int(limit), 500))
    sql, params = compare_period_sql(
        a_start=a_start,
        a_end=a_end,
        b_start=b_start,
        b_end=b_end,
        group_by=group_by,
        metric=metric,
        limit=limit,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return CompareResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/anomalies/fare_outliers", response_model=AnomaliesResponse)
def anomalies_fare_outliers(
    start: datetime,
    end: datetime,
    rule: AnomalyRule,
    min_threshold: float | None = None,
    limit: int = 200,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> AnomaliesResponse:
    client = get_client()
    limit = max(1, min(int(limit), 1000))
    sql, params = anomalies_sql(
        start=start,
        end=end,
        rule=rule,
        min_threshold=min_threshold,
        limit=limit,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return AnomaliesResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/trips", response_model=TripsResponse)
def trips(
    start: datetime,
    end: datetime,
    sort: TripSort = TripSort.pickup_datetime,
    order: Order = Order.desc,
    limit: int = 200,
    offset: int = 0,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> TripsResponse:
    client = get_client()
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))
    sql, params = trips_sql(
        start=start,
        end=end,
        sort=sort,
        order=order,
        limit=limit,
        offset=offset,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    rows, meta = run_query(client, sql, params)
    return TripsResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/historical/timeseries", response_model=HistoricalTimeseriesResponse)
def historical_timeseries(
    start: datetime,
    end: datetime,
    bucket: HistoricalBucket,
    car_type: str | None = None,
    reasonable_only: bool = False,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> HistoricalTimeseriesResponse:
    client = get_client()
    sql, params = historical_timeseries_sql(
        start=start,
        end=end,
        bucket=bucket,
        car_type=car_type,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
        reasonable_only=bool(reasonable_only),
    )
    rows, meta = run_query(client, sql, params)
    return HistoricalTimeseriesResponse(
        meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached),
        series=rows,
    )


@app.get("/api/historical/seasonality", response_model=SeasonalityResponse)
def historical_seasonality(
    start: datetime,
    end: datetime,
    metric: HistoricalMetric,
    mode: SeasonalityMode,
    car_type: str | None = None,
    reasonable_only: bool = False,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> SeasonalityResponse:
    client = get_client()
    sql, params, x_labels, y_labels = historical_seasonality_sql(
        start=start,
        end=end,
        metric=metric,
        mode=mode,
        car_type=car_type,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
        reasonable_only=bool(reasonable_only),
    )
    rows, meta = run_query(client, sql, params)
    return SeasonalityResponse(
        meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached),
        x_labels=x_labels,
        y_labels=y_labels,
        cells=rows,
    )


@app.get("/api/historical/movers", response_model=MoversResponse)
def historical_movers(
    a_start: datetime,
    a_end: datetime,
    b_start: datetime,
    b_end: datetime,
    group_by: HistoricalGroupBy,
    metric: HistoricalMetric,
    limit: int = 50,
    car_type: str | None = None,
    reasonable_only: bool = False,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> MoversResponse:
    client = get_client()
    limit = max(1, min(int(limit), 500))
    sql, params = historical_movers_sql(
        a_start=a_start,
        a_end=a_end,
        b_start=b_start,
        b_end=b_end,
        group_by=group_by,
        metric=metric,
        limit=limit,
        car_type=car_type,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
        reasonable_only=bool(reasonable_only),
    )
    rows, meta = run_query(client, sql, params)
    return MoversResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)


@app.get("/api/historical/map", response_model=MapResponse)
def historical_map(
    start: datetime,
    end: datetime,
    metric: HistoricalMetric,
    car_type: str | None = None,
    reasonable_only: bool = False,
    vendor_id: int | None = None,
    payment_type: int | None = None,
    pickup_zone_id: Annotated[list[int] | None, Query()] = None,
    dropoff_zone_id: Annotated[list[int] | None, Query()] = None,
) -> MapResponse:
    client = get_client()
    sql, params = historical_map_sql(
        start=start,
        end=end,
        metric=metric,
        car_type=car_type,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
        reasonable_only=bool(reasonable_only),
    )
    rows, meta = run_query(client, sql, params)
    return MapResponse(meta=Meta(elapsed_ms=meta.elapsed_ms, rows_returned=meta.rows_returned, cached=meta.cached), rows=rows)

