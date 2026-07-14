export type Meta = {
  elapsed_ms: number;
  rows_returned: number;
  cached: boolean;
};

export type Zone = {
  zone_id: number;
  borough: string;
  zone: string;
  service_zone: string;
  centroid_lat?: number | null;
  centroid_lon?: number | null;
};

export type ZonesResponse = { zones: Zone[] };

export type TimeseriesPoint = {
  ts: string;
  trips: number;
  fare: number;
  tip: number;
  p50_duration_s: number;
  p95_duration_s: number;
};

export type TimeseriesResponse = { meta: Meta; series: TimeseriesPoint[] };

export type TopZoneRow = { zone_id: number; zone: string; borough: string; value: number };
export type TopZonesResponse = { meta: Meta; rows: TopZoneRow[] };

export type ZoneStatsRow = {
  zone_id: number;
  zone: string;
  borough: string;
  trips: number;
  p50_duration_s: number;
  p95_duration_s: number;
  avg_fare: number;
};
export type ZoneStatsResponse = { meta: Meta; rows: ZoneStatsRow[] };

export type WorstPairRow = {
  pickup_zone_id: number;
  pickup_zone: string;
  dropoff_zone_id: number;
  dropoff_zone: string;
  trips: number;
  p95_duration_s?: number | null;
  avg_fare?: number | null;
};
export type WorstPairsResponse = { meta: Meta; rows: WorstPairRow[] };

export type CompareRow = {
  zone_id: number;
  zone: string;
  borough: string;
  a_value: number;
  b_value: number;
  delta: number;
  delta_pct?: number | null;
};
export type CompareResponse = { meta: Meta; rows: CompareRow[] };

export type AnomalyRow = {
  pickup_datetime: string;
  dropoff_datetime: string;
  pickup_zone: string;
  dropoff_zone: string;
  trip_distance: number;
  fare_amount: number;
  tip_amount: number;
  duration_s: number;
  score: number;
};
export type AnomaliesResponse = { meta: Meta; rows: AnomalyRow[] };

export type TripRow = {
  pickup_datetime: string;
  dropoff_datetime: string;
  pickup_zone: string;
  dropoff_zone: string;
  passenger_count: number;
  trip_distance: number;
  fare_amount: number;
  tip_amount: number;
  payment_type: number;
  vendor_id: number;
  duration_s: number;
};
export type TripsResponse = { meta: Meta; rows: TripRow[] };

export type HistoricalBucket = "day" | "week" | "month";
export type HistoricalMetric = "trips" | "revenue" | "tip" | "p50_duration_s" | "p95_duration_s";
export type SeasonalityMode = "dow_hour" | "month_dow";
export type HistoricalGroupBy = "pickup_zone" | "dropoff_zone" | "borough";

export type HistoricalTimeseriesPoint = {
  ts: string;
  trips: number;
  revenue: number;
  tip: number;
  p50_duration_s: number;
  p95_duration_s: number;
};
export type HistoricalTimeseriesResponse = { meta: Meta; series: HistoricalTimeseriesPoint[] };

export type HeatmapCell = { x: number; y: number; value: number };
export type SeasonalityResponse = { meta: Meta; x_labels: string[]; y_labels: string[]; cells: HeatmapCell[] };

export type MoversRow = {
  key: string;
  label: string;
  a_value: number;
  b_value: number;
  delta: number;
  delta_pct?: number | null;
};
export type MoversResponse = { meta: Meta; rows: MoversRow[] };

export type MapRow = { zone_id: number; value: number; delta?: number | null; delta_pct?: number | null };
export type MapResponse = { meta: Meta; rows: MapRow[] };

export type ChatChartSpec = {
  type: "line" | "bar" | "none";
  x?: string | null;
  y?: string | string[] | null;
};

export type ChatRequest = { message: string; conversation_id?: string };

export type ChatResponse = {
  answer: string;
  sql?: string | null;
  rows?: Record<string, unknown>[] | null;
  chart?: ChatChartSpec | null;
};
