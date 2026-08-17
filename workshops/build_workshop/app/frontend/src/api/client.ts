import type {
  AnomaliesResponse,
  ChatRequest,
  ChatResponse,
  CompareResponse,
  HistoricalGroupBy,
  HistoricalMetric,
  HistoricalTimeseriesResponse,
  MapResponse,
  MoversResponse,
  SeasonalityMode,
  SeasonalityResponse,
  TimeseriesResponse,
  TopZonesResponse,
  TripsResponse,
  ZoneStatsResponse,
  ZonesResponse,
  WorstPairsResponse
} from "./types";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api";

function buildUrl(path: string, params?: Record<string, string | number | (string | number)[] | undefined>) {
  const url = new URL(`${API_BASE_URL}${path}`, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined) continue;
      if (Array.isArray(v)) {
        for (const item of v) url.searchParams.append(k, String(item));
      } else {
        url.searchParams.set(k, String(v));
      }
    }
  }
  return url.toString();
}

async function getJson<T>(path: string, params?: Record<string, any>): Promise<T> {
  const r = await fetch(buildUrl(path, params));
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(`GET ${path} failed: ${r.status} ${r.statusText} ${text}`);
  }
  return (await r.json()) as T;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(buildUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!r.ok) {
    // FastAPI errors carry a human-readable "detail" (e.g. the 503 setup hint).
    const payload = await r.json().catch(() => null);
    const detail = payload && typeof payload.detail === "string" ? payload.detail : `${r.status} ${r.statusText}`;
    throw new Error(detail);
  }
  return (await r.json()) as T;
}

export const api = {
  health: () => getJson<{ ok: boolean; clickhouse: { ok: boolean; version?: string | null } }>("/health"),
  zones: () => getJson<ZonesResponse>("/filters/zones"),

  timeseries: (params: {
    start: string;
    end: string;
    interval: "1m" | "5m" | "15m" | "1h";
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<TimeseriesResponse>("/metrics/timeseries", params),

  topZones: (params: {
    start: string;
    end: string;
    metric: "trips" | "fare" | "tip" | "p95_duration_s";
    direction: "pickup" | "dropoff";
    limit?: number;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<TopZonesResponse>("/metrics/top_zones", params),

  zoneStats: (params: {
    start: string;
    end: string;
    group_by: "pickup_zone" | "dropoff_zone";
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<ZoneStatsResponse>("/metrics/zone_stats", params),

  worstPairs: (params: {
    start: string;
    end: string;
    metric: "p95_duration_s" | "avg_fare" | "trips";
    limit?: number;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<WorstPairsResponse>("/metrics/worst_pairs", params),

  comparePeriod: (params: {
    a_start: string;
    a_end: string;
    b_start: string;
    b_end: string;
    group_by: "pickup_zone" | "dropoff_zone";
    metric: "trips" | "fare" | "p50_duration_s" | "p95_duration_s";
    limit?: number;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<CompareResponse>("/compare/period", params),

  anomalies: (params: {
    start: string;
    end: string;
    rule: "fare_per_mile" | "fare_per_minute" | "tip_ratio";
    min_threshold?: number;
    limit?: number;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<AnomaliesResponse>("/anomalies/fare_outliers", params),

  trips: (params: {
    start: string;
    end: string;
    sort?: "pickup_datetime" | "fare_amount" | "duration_s";
    order?: "asc" | "desc";
    limit?: number;
    offset?: number;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) => getJson<TripsResponse>("/trips", params),

  historicalTimeseries: (params: {
    start: string;
    end: string;
    bucket: "day" | "week" | "month";
    car_type?: "yellow" | "green" | "all";
    reasonable_only?: boolean;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) =>
    getJson<HistoricalTimeseriesResponse>("/historical/timeseries", {
      ...params,
      car_type: params.car_type === "all" ? undefined : params.car_type
    }),

  historicalSeasonality: (params: {
    start: string;
    end: string;
    metric: HistoricalMetric;
    mode: SeasonalityMode;
    car_type?: "yellow" | "green" | "all";
    reasonable_only?: boolean;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) =>
    getJson<SeasonalityResponse>("/historical/seasonality", {
      ...params,
      car_type: params.car_type === "all" ? undefined : params.car_type
    }),

  historicalMovers: (params: {
    a_start: string;
    a_end: string;
    b_start: string;
    b_end: string;
    group_by: HistoricalGroupBy;
    metric: HistoricalMetric;
    limit?: number;
    car_type?: "yellow" | "green" | "all";
    reasonable_only?: boolean;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) =>
    getJson<MoversResponse>("/historical/movers", {
      ...params,
      car_type: params.car_type === "all" ? undefined : params.car_type
    }),

  historicalMap: (params: {
    start: string;
    end: string;
    metric: HistoricalMetric;
    car_type?: "yellow" | "green" | "all";
    reasonable_only?: boolean;
    vendor_id?: number;
    payment_type?: number;
    pickup_zone_id?: number[];
    dropoff_zone_id?: number[];
  }) =>
    getJson<MapResponse>("/historical/map", {
      ...params,
      car_type: params.car_type === "all" ? undefined : params.car_type
    }),

  chat: (body: ChatRequest) => postJson<ChatResponse>("/chat", body)
};

