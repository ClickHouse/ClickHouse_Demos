from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class Meta(BaseModel):
    elapsed_ms: int
    rows_returned: int
    cached: bool = False


class Interval(str, Enum):
    m1 = "1m"
    m5 = "5m"
    m15 = "15m"
    h1 = "1h"


class HistoricalBucket(str, Enum):
    day = "day"
    week = "week"
    month = "month"


class HistoricalMetric(str, Enum):
    trips = "trips"
    revenue = "revenue"
    tip = "tip"
    p50_duration_s = "p50_duration_s"
    p95_duration_s = "p95_duration_s"


class SeasonalityMode(str, Enum):
    dow_hour = "dow_hour"
    month_dow = "month_dow"


class HistoricalGroupBy(str, Enum):
    pickup_zone = "pickup_zone"
    dropoff_zone = "dropoff_zone"
    borough = "borough"


class Direction(str, Enum):
    pickup = "pickup"
    dropoff = "dropoff"


class MetricTopZones(str, Enum):
    trips = "trips"
    fare = "fare"
    tip = "tip"
    p95_duration_s = "p95_duration_s"


class MetricWorstPairs(str, Enum):
    p95_duration_s = "p95_duration_s"
    avg_fare = "avg_fare"
    trips = "trips"


class ZoneGroupBy(str, Enum):
    pickup_zone = "pickup_zone"
    dropoff_zone = "dropoff_zone"


class MetricCompare(str, Enum):
    trips = "trips"
    fare = "fare"
    p50_duration_s = "p50_duration_s"
    p95_duration_s = "p95_duration_s"


class AnomalyRule(str, Enum):
    fare_per_mile = "fare_per_mile"
    fare_per_minute = "fare_per_minute"
    tip_ratio = "tip_ratio"


class TripSort(str, Enum):
    pickup_datetime = "pickup_datetime"
    fare_amount = "fare_amount"
    duration_s = "duration_s"


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class Zone(BaseModel):
    zone_id: int
    borough: str
    zone: str
    service_zone: str
    centroid_lat: float | None = None
    centroid_lon: float | None = None


class ZonesResponse(BaseModel):
    zones: list[Zone]


class HealthClickHouse(BaseModel):
    ok: bool
    version: str | None = None


class HealthResponse(BaseModel):
    ok: bool
    clickhouse: HealthClickHouse


class TimeseriesPoint(BaseModel):
    ts: datetime
    trips: int
    fare: float
    tip: float
    p50_duration_s: float = Field(..., description="Median trip duration in seconds")
    p95_duration_s: float = Field(..., description="95th percentile trip duration in seconds")


class TimeseriesResponse(BaseModel):
    meta: Meta
    series: list[TimeseriesPoint]


class TopZoneRow(BaseModel):
    zone_id: int
    zone: str
    borough: str
    value: float


class TopZonesResponse(BaseModel):
    meta: Meta
    rows: list[TopZoneRow]


class ZoneStatsRow(BaseModel):
    zone_id: int
    zone: str
    borough: str
    trips: int
    p50_duration_s: float
    p95_duration_s: float
    avg_fare: float


class ZoneStatsResponse(BaseModel):
    meta: Meta
    rows: list[ZoneStatsRow]


class WorstPairRow(BaseModel):
    pickup_zone_id: int
    pickup_zone: str
    dropoff_zone_id: int
    dropoff_zone: str
    trips: int
    p95_duration_s: float | None = None
    avg_fare: float | None = None


class WorstPairsResponse(BaseModel):
    meta: Meta
    rows: list[WorstPairRow]


class CompareRow(BaseModel):
    zone_id: int
    zone: str
    borough: str
    a_value: float
    b_value: float
    delta: float
    delta_pct: float | None


class CompareResponse(BaseModel):
    meta: Meta
    rows: list[CompareRow]


class AnomalyRow(BaseModel):
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pickup_zone: str
    dropoff_zone: str
    trip_distance: float
    fare_amount: float
    tip_amount: float
    duration_s: int
    score: float


class AnomaliesResponse(BaseModel):
    meta: Meta
    rows: list[AnomalyRow]


class TripRow(BaseModel):
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pickup_zone: str
    dropoff_zone: str
    passenger_count: int
    trip_distance: float
    fare_amount: float
    tip_amount: float
    payment_type: int
    vendor_id: int
    duration_s: int


class TripsResponse(BaseModel):
    meta: Meta
    rows: list[TripRow]


class HistoricalTimeseriesPoint(BaseModel):
    ts: datetime
    trips: int
    revenue: float
    tip: float
    p50_duration_s: float
    p95_duration_s: float


class HistoricalTimeseriesResponse(BaseModel):
    meta: Meta
    series: list[HistoricalTimeseriesPoint]


class HeatmapCell(BaseModel):
    x: int
    y: int
    value: float


class SeasonalityResponse(BaseModel):
    meta: Meta
    x_labels: list[str]
    y_labels: list[str]
    cells: list[HeatmapCell]


class MoversRow(BaseModel):
    key: str
    label: str
    a_value: float
    b_value: float
    delta: float
    delta_pct: float | None


class MoversResponse(BaseModel):
    meta: Meta
    rows: list[MoversRow]


class MapRow(BaseModel):
    zone_id: int
    value: float
    delta: float | None = None
    delta_pct: float | None = None


class MapResponse(BaseModel):
    meta: Meta
    rows: list[MapRow]


class ChatChartSpec(BaseModel):
    # Minimal chart hint the frontend uses to render an ECharts plot.
    # x / y reference column aliases in the generated SQL's SELECT list.
    type: Literal["line", "bar", "none"] = "none"
    x: str | None = None
    y: str | list[str] | None = None


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    rows: list[dict[str, Any]] | None = None
    chart: ChatChartSpec | None = None

