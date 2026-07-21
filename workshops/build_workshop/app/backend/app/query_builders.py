from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.schemas import (
    AnomalyRule,
    Direction,
    HistoricalBucket,
    HistoricalGroupBy,
    HistoricalMetric,
    Interval,
    MetricCompare,
    MetricTopZones,
    MetricWorstPairs,
    Order,
    SeasonalityMode,
    TripSort,
    ZoneGroupBy,
)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _filters_sql(
    *,
    start: datetime,
    end: datetime,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = [
        "pickup_datetime >= {start:DateTime}",
        "pickup_datetime < {end:DateTime}",
    ]
    params: dict[str, Any] = {"start": ensure_utc(start), "end": ensure_utc(end)}

    if vendor_id is not None:
        clauses.append("vendor_id = {vendor_id:UInt16}")
        params["vendor_id"] = int(vendor_id)
    if payment_type is not None:
        clauses.append("payment_type = {payment_type:UInt16}")
        params["payment_type"] = int(payment_type)
    if pickup_zone_id:
        clauses.append("pickup_location_id IN {pickup_zone_ids:Array(UInt16)}")
        params["pickup_zone_ids"] = [int(x) for x in pickup_zone_id]
    if dropoff_zone_id:
        clauses.append("dropoff_location_id IN {dropoff_zone_ids:Array(UInt16)}")
        params["dropoff_zone_ids"] = [int(x) for x in dropoff_zone_id]

    return " AND ".join(clauses), params


def timeseries_sql(
    *,
    start: datetime,
    end: datetime,
    interval: Interval,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    bucket = {
        Interval.m1: "toStartOfInterval(pickup_datetime, INTERVAL 1 MINUTE)",
        Interval.m5: "toStartOfInterval(pickup_datetime, INTERVAL 5 MINUTE)",
        Interval.m15: "toStartOfInterval(pickup_datetime, INTERVAL 15 MINUTE)",
        Interval.h1: "toStartOfInterval(pickup_datetime, INTERVAL 1 HOUR)",
    }[interval]

    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )

    sql = f"""
SELECT
  {bucket} AS ts,
  count() AS trips,
  sum(fare_amount) AS fare,
  sum(tip_amount) AS tip,
  quantileTDigest(0.50)(dateDiff('second', pickup_datetime, dropoff_datetime)) AS p50_duration_s,
  quantileTDigest(0.95)(dateDiff('second', pickup_datetime, dropoff_datetime)) AS p95_duration_s
FROM taxi_trips
WHERE {where_sql}
GROUP BY ts
ORDER BY ts
"""
    return sql, params


def top_zones_sql(
    *,
    start: datetime,
    end: datetime,
    metric: MetricTopZones,
    direction: Direction,
    limit: int,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    zone_col = "pickup_location_id" if direction == Direction.pickup else "dropoff_location_id"
    metric_expr = {
        MetricTopZones.trips: "count()",
        MetricTopZones.fare: "sum(fare_amount)",
        MetricTopZones.tip: "sum(tip_amount)",
        MetricTopZones.p95_duration_s: "quantileTDigest(0.95)(dateDiff('second', pickup_datetime, dropoff_datetime))",
    }[metric]

    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    params["limit"] = int(limit)

    sql = f"""
SELECT
  z.location_id AS zone_id,
  z.zone AS zone,
  z.borough AS borough,
  value
FROM
(
  SELECT
    {zone_col} AS zone_id,
    {metric_expr} AS value
  FROM taxi_trips
  WHERE {where_sql}
  GROUP BY zone_id
  ORDER BY value DESC
  LIMIT {{limit:UInt16}}
) t
INNER JOIN taxi_zones z ON z.location_id = t.zone_id
ORDER BY value DESC
"""
    return sql, params


def zone_stats_sql(
    *,
    start: datetime,
    end: datetime,
    group_by: ZoneGroupBy,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    zone_col = "pickup_location_id" if group_by == ZoneGroupBy.pickup_zone else "dropoff_location_id"
    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )

    sql = f"""
SELECT
  z.location_id AS zone_id,
  z.zone AS zone,
  z.borough AS borough,
  trips,
  p50_duration_s,
  p95_duration_s,
  avg_fare
FROM
(
  SELECT
    {zone_col} AS zone_id,
    count() AS trips,
    quantileTDigest(0.50)(dateDiff('second', pickup_datetime, dropoff_datetime)) AS p50_duration_s,
    quantileTDigest(0.95)(dateDiff('second', pickup_datetime, dropoff_datetime)) AS p95_duration_s,
    avg(fare_amount) AS avg_fare
  FROM taxi_trips
  WHERE {where_sql}
  GROUP BY zone_id
) s
INNER JOIN taxi_zones z ON z.location_id = s.zone_id
ORDER BY trips DESC
"""
    return sql, params


def worst_pairs_sql(
    *,
    start: datetime,
    end: datetime,
    metric: MetricWorstPairs,
    limit: int,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    metric_expr = {
        MetricWorstPairs.p95_duration_s: "quantileTDigest(0.95)(dateDiff('second', pickup_datetime, dropoff_datetime))",
        MetricWorstPairs.avg_fare: "avg(fare_amount)",
        MetricWorstPairs.trips: "count()",
    }[metric]

    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    params["limit"] = int(limit)

    sql = f"""
SELECT
  p.pickup_zone_id,
  p.pickup_zone,
  p.dropoff_zone_id,
  p.dropoff_zone,
  p.trips,
  p.p95_duration_s,
  p.avg_fare
FROM
(
  SELECT
    t.pickup_location_id AS pickup_zone_id,
    zp.zone AS pickup_zone,
    t.dropoff_location_id AS dropoff_zone_id,
    zd.zone AS dropoff_zone,
    count() AS trips,
    quantileTDigest(0.95)(dateDiff('second', t.pickup_datetime, t.dropoff_datetime)) AS p95_duration_s,
    avg(t.fare_amount) AS avg_fare,
    {metric_expr} AS sort_value
  FROM taxi_trips t
  INNER JOIN taxi_zones zp ON zp.location_id = t.pickup_location_id
  INNER JOIN taxi_zones zd ON zd.location_id = t.dropoff_location_id
  WHERE {where_sql}
  GROUP BY pickup_zone_id, pickup_zone, dropoff_zone_id, dropoff_zone
  ORDER BY sort_value DESC
  LIMIT {{limit:UInt16}}
) p
ORDER BY
  CASE WHEN isFinite(p.p95_duration_s) THEN p.p95_duration_s ELSE 0 END DESC,
  p.trips DESC
"""
    return sql, params


def compare_period_sql(
    *,
    a_start: datetime,
    a_end: datetime,
    b_start: datetime,
    b_end: datetime,
    group_by: ZoneGroupBy,
    metric: MetricCompare,
    limit: int,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    zone_col = "pickup_location_id" if group_by == ZoneGroupBy.pickup_zone else "dropoff_location_id"
    metric_expr = {
        MetricCompare.trips: "count()",
        MetricCompare.fare: "sum(fare_amount)",
        MetricCompare.p50_duration_s: "quantileTDigest(0.50)(dateDiff('second', pickup_datetime, dropoff_datetime))",
        MetricCompare.p95_duration_s: "quantileTDigest(0.95)(dateDiff('second', pickup_datetime, dropoff_datetime))",
    }[metric]

    # Build "shared" filters (excluding time window) safely.
    shared_clauses: list[str] = []
    params: dict[str, Any] = {
        "a_start": ensure_utc(a_start),
        "a_end": ensure_utc(a_end),
        "b_start": ensure_utc(b_start),
        "b_end": ensure_utc(b_end),
        "limit": int(limit),
    }
    if vendor_id is not None:
        shared_clauses.append("vendor_id = {vendor_id:UInt8}")
        params["vendor_id"] = int(vendor_id)
    if payment_type is not None:
        shared_clauses.append("payment_type = {payment_type:UInt8}")
        params["payment_type"] = int(payment_type)
    if pickup_zone_id:
        shared_clauses.append("pickup_location_id IN {pickup_zone_ids:Array(UInt16)}")
        params["pickup_zone_ids"] = [int(x) for x in pickup_zone_id]
    if dropoff_zone_id:
        shared_clauses.append("dropoff_location_id IN {dropoff_zone_ids:Array(UInt16)}")
        params["dropoff_zone_ids"] = [int(x) for x in dropoff_zone_id]
    shared_sql = (" AND " + " AND ".join(shared_clauses)) if shared_clauses else ""

    sql = f"""
WITH
  a AS (
    SELECT
      {zone_col} AS zone_id,
      {metric_expr} AS a_value
    FROM taxi_trips
    WHERE pickup_datetime >= {{a_start:DateTime}}
      AND pickup_datetime < {{a_end:DateTime}}
      {shared_sql}
    GROUP BY zone_id
  ),
  b AS (
    SELECT
      {zone_col} AS zone_id,
      {metric_expr} AS b_value
    FROM taxi_trips
    WHERE pickup_datetime >= {{b_start:DateTime}}
      AND pickup_datetime < {{b_end:DateTime}}
      {shared_sql}
    GROUP BY zone_id
  )
SELECT
  z.location_id AS zone_id,
  z.zone AS zone,
  z.borough AS borough,
  ifNull(a.a_value, 0) AS a_value,
  ifNull(b.b_value, 0) AS b_value,
  (ifNull(a.a_value, 0) - ifNull(b.b_value, 0)) AS delta,
  if(ifNull(b.b_value, 0) = 0, NULL, (ifNull(a.a_value, 0) - ifNull(b.b_value, 0)) / b.b_value) AS delta_pct
FROM taxi_zones z
LEFT JOIN a ON a.zone_id = z.location_id
LEFT JOIN b ON b.zone_id = z.location_id
ORDER BY abs(delta) DESC
LIMIT {{limit:UInt16}}
"""
    return sql, params


def anomalies_sql(
    *,
    start: datetime,
    end: datetime,
    rule: AnomalyRule,
    min_threshold: float | None,
    limit: int,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    params["limit"] = int(limit)

    score_expr = {
        AnomalyRule.fare_per_mile: "fare_amount / nullIf(trip_distance, 0)",
        AnomalyRule.fare_per_minute: "fare_amount / nullIf(dateDiff('minute', pickup_datetime, dropoff_datetime), 0)",
        AnomalyRule.tip_ratio: "tip_amount / nullIf(fare_amount, 0)",
    }[rule]

    threshold_sql = ""
    if min_threshold is not None:
        params["min_threshold"] = float(min_threshold)
        threshold_sql = " AND score >= {min_threshold:Float64}"

    sql = f"""
SELECT
  t.pickup_datetime AS pickup_datetime,
  t.dropoff_datetime AS dropoff_datetime,
  zp.zone AS pickup_zone,
  zd.zone AS dropoff_zone,
  t.trip_distance AS trip_distance,
  t.fare_amount AS fare_amount,
  t.tip_amount AS tip_amount,
  dateDiff('second', t.pickup_datetime, t.dropoff_datetime) AS duration_s,
  score
FROM
(
  SELECT
    *,
    {score_expr} AS score
  FROM taxi_trips
  WHERE {where_sql}
) t
INNER JOIN taxi_zones zp ON zp.location_id = t.pickup_location_id
INNER JOIN taxi_zones zd ON zd.location_id = t.dropoff_location_id
WHERE isFinite(score) {threshold_sql}
ORDER BY score DESC
LIMIT {{limit:UInt16}}
"""
    return sql, params


def trips_sql(
    *,
    start: datetime,
    end: datetime,
    sort: TripSort,
    order: Order,
    limit: int,
    offset: int,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
) -> tuple[str, dict[str, Any]]:
    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    params["limit"] = int(limit)
    params["offset"] = int(offset)

    sort_expr = {
        TripSort.pickup_datetime: "t.pickup_datetime",
        TripSort.fare_amount: "t.fare_amount",
        TripSort.duration_s: "duration_s",
    }[sort]
    order_sql = "ASC" if order == Order.asc else "DESC"

    sql = f"""
SELECT
  t.pickup_datetime AS pickup_datetime,
  t.dropoff_datetime AS dropoff_datetime,
  zp.zone AS pickup_zone,
  zd.zone AS dropoff_zone,
  ifNull(t.passenger_count, 0) AS passenger_count,
  ifNull(t.trip_distance, 0) AS trip_distance,
  ifNull(t.fare_amount, 0) AS fare_amount,
  ifNull(t.tip_amount, 0) AS tip_amount,
  ifNull(t.payment_type, 0) AS payment_type,
  ifNull(t.vendor_id, 0) AS vendor_id,
  dateDiff('second', t.pickup_datetime, t.dropoff_datetime) AS duration_s
FROM taxi_trips t
INNER JOIN taxi_zones zp ON zp.location_id = t.pickup_location_id
INNER JOIN taxi_zones zd ON zd.location_id = t.dropoff_location_id
WHERE {where_sql}
ORDER BY {sort_expr} {order_sql}
LIMIT {{limit:UInt16}}
OFFSET {{offset:UInt32}}
"""
    return sql, params


def _revenue_expr() -> str:
    # total_amount may be nullable; fallback to fare+tip.
    return "sum(ifNull(total_amount, ifNull(fare_amount, 0) + ifNull(tip_amount, 0)))"


def _duration_expr() -> str:
    return "dateDiff('second', pickup_datetime, dropoff_datetime)"


def _metric_expr(metric: HistoricalMetric) -> str:
    return {
        HistoricalMetric.trips: "count()",
        HistoricalMetric.revenue: _revenue_expr(),
        HistoricalMetric.tip: "sum(ifNull(tip_amount, 0))",
        HistoricalMetric.p50_duration_s: f"quantileTDigest(0.50)({_duration_expr()})",
        HistoricalMetric.p95_duration_s: f"quantileTDigest(0.95)({_duration_expr()})",
    }[metric]


def historical_timeseries_sql(
    *,
    start: datetime,
    end: datetime,
    bucket: HistoricalBucket,
    car_type: str | None,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
    reasonable_only: bool,
) -> tuple[str, dict[str, Any]]:
    bucket_expr = {
        HistoricalBucket.day: "toStartOfDay(pickup_datetime)",
        HistoricalBucket.week: "toStartOfWeek(pickup_datetime)",
        HistoricalBucket.month: "toStartOfMonth(pickup_datetime)",
    }[bucket]

    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    if car_type:
        where_sql += " AND car_type = {car_type:String}"
        params["car_type"] = car_type

    table = "taxi_trips_expanded" if reasonable_only else "taxi_trips"
    if reasonable_only:
        where_sql += " AND reasonable_time_distance_fare = true"

    sql = f"""
SELECT
  {bucket_expr} AS ts,
  count() AS trips,
  {_revenue_expr()} AS revenue,
  sum(ifNull(tip_amount, 0)) AS tip,
  quantileTDigest(0.50)({_duration_expr()}) AS p50_duration_s,
  quantileTDigest(0.95)({_duration_expr()}) AS p95_duration_s
FROM {table}
WHERE {where_sql}
GROUP BY ts
ORDER BY ts
"""
    return sql, params


def historical_seasonality_sql(
    *,
    start: datetime,
    end: datetime,
    metric: HistoricalMetric,
    mode: SeasonalityMode,
    car_type: str | None,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
    reasonable_only: bool,
) -> tuple[str, dict[str, Any], list[str], list[str]]:
    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    if car_type:
        where_sql += " AND car_type = {car_type:String}"
        params["car_type"] = car_type

    table = "taxi_trips_expanded" if reasonable_only else "taxi_trips"
    if reasonable_only:
        where_sql += " AND reasonable_time_distance_fare = true"

    metric_expr = _metric_expr(metric)

    if mode == SeasonalityMode.dow_hour:
        # x: hour (0-23), y: day-of-week (Mon=1..Sun=7 => 0..6)
        x_labels = [f"{h:02d}" for h in range(24)]
        y_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        sql = f"""
SELECT
  toHour(pickup_datetime) AS x,
  toDayOfWeek(pickup_datetime) - 1 AS y,
  {metric_expr} AS value
FROM {table}
WHERE {where_sql}
GROUP BY x, y
ORDER BY y, x
"""
        return sql, params, x_labels, y_labels

    # month_dow: x: month (1-12 mapped to 0-11), y: dow (0-6)
    x_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    y_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    sql = f"""
SELECT
  toMonth(pickup_datetime) - 1 AS x,
  toDayOfWeek(pickup_datetime) - 1 AS y,
  {metric_expr} AS value
FROM {table}
WHERE {where_sql}
GROUP BY x, y
ORDER BY y, x
"""
    return sql, params, x_labels, y_labels


def historical_movers_sql(
    *,
    a_start: datetime,
    a_end: datetime,
    b_start: datetime,
    b_end: datetime,
    group_by: HistoricalGroupBy,
    metric: HistoricalMetric,
    limit: int,
    car_type: str | None,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
    reasonable_only: bool,
) -> tuple[str, dict[str, Any]]:
    metric_expr = _metric_expr(metric)
    params: dict[str, Any] = {
        "a_start": ensure_utc(a_start),
        "a_end": ensure_utc(a_end),
        "b_start": ensure_utc(b_start),
        "b_end": ensure_utc(b_end),
        "limit": int(limit),
    }

    shared_clauses: list[str] = []
    if car_type:
        shared_clauses.append("car_type = {car_type:String}")
        params["car_type"] = car_type
    if vendor_id is not None:
        shared_clauses.append("vendor_id = {vendor_id:UInt16}")
        params["vendor_id"] = int(vendor_id)
    if payment_type is not None:
        shared_clauses.append("payment_type = {payment_type:UInt16}")
        params["payment_type"] = int(payment_type)
    if pickup_zone_id:
        shared_clauses.append("pickup_location_id IN {pickup_zone_ids:Array(UInt16)}")
        params["pickup_zone_ids"] = [int(x) for x in pickup_zone_id]
    if dropoff_zone_id:
        shared_clauses.append("dropoff_location_id IN {dropoff_zone_ids:Array(UInt16)}")
        params["dropoff_zone_ids"] = [int(x) for x in dropoff_zone_id]

    table = "taxi_trips_expanded" if reasonable_only else "taxi_trips"
    if reasonable_only:
        shared_clauses.append("reasonable_time_distance_fare = true")

    shared_sql = (" AND " + " AND ".join(shared_clauses)) if shared_clauses else ""

    if group_by == HistoricalGroupBy.pickup_zone:
        dim_select = "pickup_location_id AS key"
        dim_join = "LEFT JOIN taxi_zones z ON z.location_id = key"
        label_expr = "coalesce(z.zone, toString(key))"
        where_a = f"pickup_datetime >= {{a_start:DateTime}} AND pickup_datetime < {{a_end:DateTime}}{shared_sql}"
        where_b = f"pickup_datetime >= {{b_start:DateTime}} AND pickup_datetime < {{b_end:DateTime}}{shared_sql}"
    elif group_by == HistoricalGroupBy.dropoff_zone:
        dim_select = "dropoff_location_id AS key"
        dim_join = "LEFT JOIN taxi_zones z ON z.location_id = key"
        label_expr = "coalesce(z.zone, toString(key))"
        where_a = f"pickup_datetime >= {{a_start:DateTime}} AND pickup_datetime < {{a_end:DateTime}}{shared_sql}"
        where_b = f"pickup_datetime >= {{b_start:DateTime}} AND pickup_datetime < {{b_end:DateTime}}{shared_sql}"
    else:
        # Borough based on pickup_location_id lookup.
        dim_select = "z.borough AS key"
        dim_join = "INNER JOIN taxi_zones z ON z.location_id = t.pickup_location_id"
        label_expr = "key"
        where_a = f"t.pickup_datetime >= {{a_start:DateTime}} AND t.pickup_datetime < {{a_end:DateTime}}{shared_sql}"
        where_b = f"t.pickup_datetime >= {{b_start:DateTime}} AND t.pickup_datetime < {{b_end:DateTime}}{shared_sql}"

    # Note: for borough case, we introduce alias t in subqueries.
    table_ref = f"{table} t" if group_by == HistoricalGroupBy.borough else table

    sql = f"""
WITH
  a AS (
    SELECT {dim_select},
           {metric_expr} AS a_value
    FROM {table_ref}
    {dim_join if group_by == HistoricalGroupBy.borough else ""}
    WHERE {where_a}
    GROUP BY key
  ),
  b AS (
    SELECT {dim_select},
           {metric_expr} AS b_value
    FROM {table_ref}
    {dim_join if group_by == HistoricalGroupBy.borough else ""}
    WHERE {where_b}
    GROUP BY key
  )
SELECT
  toString(coalesce(a.key, b.key)) AS key,
  {label_expr} AS label,
  ifNull(a.a_value, 0) AS a_value,
  ifNull(b.b_value, 0) AS b_value,
  (ifNull(a.a_value, 0) - ifNull(b.b_value, 0)) AS delta,
  if(ifNull(b.b_value, 0) = 0, NULL, (ifNull(a.a_value, 0) - ifNull(b.b_value, 0)) / b.b_value) AS delta_pct
FROM a
FULL OUTER JOIN b ON a.key = b.key
{("LEFT JOIN taxi_zones z ON z.location_id = toUInt16OrZero(key)" if group_by != HistoricalGroupBy.borough else "")}
ORDER BY abs(delta) DESC
LIMIT {{limit:UInt16}}
"""
    return sql, params


def historical_map_sql(
    *,
    start: datetime,
    end: datetime,
    metric: HistoricalMetric,
    car_type: str | None,
    vendor_id: int | None,
    payment_type: int | None,
    pickup_zone_id: list[int] | None,
    dropoff_zone_id: list[int] | None,
    reasonable_only: bool,
) -> tuple[str, dict[str, Any]]:
    # Map values by pickup zone id.
    metric_expr = _metric_expr(metric)
    where_sql, params = _filters_sql(
        start=start,
        end=end,
        vendor_id=vendor_id,
        payment_type=payment_type,
        pickup_zone_id=pickup_zone_id,
        dropoff_zone_id=dropoff_zone_id,
    )
    if car_type:
        where_sql += " AND car_type = {car_type:String}"
        params["car_type"] = car_type
    table = "taxi_trips_expanded" if reasonable_only else "taxi_trips"
    if reasonable_only:
        where_sql += " AND reasonable_time_distance_fare = true"

    sql = f"""
SELECT
  pickup_location_id AS zone_id,
  {metric_expr} AS value
FROM {table}
WHERE {where_sql}
GROUP BY zone_id
ORDER BY value DESC
"""
    return sql, params

