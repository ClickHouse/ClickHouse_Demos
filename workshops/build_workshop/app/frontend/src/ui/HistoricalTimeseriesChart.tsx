import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { HistoricalTimeseriesPoint } from "../api/types";
import type { HistoricalFilters } from "./HistoricalFilterBar";
import { EChart } from "./EChart";

type Props = { filters: HistoricalFilters };

function metricLabel(m: HistoricalFilters["metric"]) {
  return {
    trips: "Trips",
    revenue: "Revenue",
    tip: "Tip",
    p50_duration_s: "p50 duration (s)",
    p95_duration_s: "p95 duration (s)"
  }[m];
}

export function HistoricalTimeseriesChart({ filters }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["historicalTimeseries", filters],
    queryFn: () =>
      api.historicalTimeseries({
        start: filters.start,
        end: filters.end,
        bucket: filters.bucket,
        car_type: filters.car_type,
        reasonable_only: filters.reasonable_only,
        vendor_id: filters.vendor_id,
        payment_type: filters.payment_type,
        pickup_zone_id: filters.pickup_zone_id.length ? filters.pickup_zone_id : undefined,
        dropoff_zone_id: filters.dropoff_zone_id.length ? filters.dropoff_zone_id : undefined
      }),
    refetchInterval,
    refetchIntervalInBackground: true
  });

  const series = (q.data?.series ?? []) as HistoricalTimeseriesPoint[];
  const m = filters.metric;

  const data = useMemo(() => series.map((p) => [p.ts, (p as any)[m] as number]), [series, m]);

  const option = useMemo(() => {
    return {
      tooltip: { trigger: "axis" as const },
      // Prevent y-axis tick labels from being clipped (e.g. showing only "00,000").
      grid: { left: 70, right: 10, top: 10, bottom: 25, containLabel: true },
      xAxis: { type: "time" as const },
      yAxis: {
        type: "value" as const,
        scale: true,
        axisLabel: {
          formatter: (v: unknown) => {
            const n = Number(v);
            return Number.isFinite(n) ? n.toLocaleString() : String(v);
          }
        }
      },
      series: [
        {
          type: "line" as const,
          name: metricLabel(m),
          showSymbol: false,
          lineStyle: { width: 2 },
          data
        }
      ]
    };
  }, [data, m]);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">{(q.error as Error).message}</div>;

  return (
    <div>
      <EChart option={option} height={280} />
      <div className="text-secondary small mt-2">
        Query {q.data?.meta.elapsed_ms}ms • rows {q.data?.meta.rows_returned}
      </div>
    </div>
  );
}

