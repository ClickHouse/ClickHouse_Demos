import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { TimeseriesPoint } from "../api/types";
import type { DashboardFilters } from "./FilterBar";
import { EChart } from "./EChart";
import { useReportSql } from "./SqlRegistry";

type Props = { filters: DashboardFilters; sqlKey: string };

function toSeries(points: TimeseriesPoint[], key: keyof TimeseriesPoint) {
  return points.map((p) => [p.ts, p[key]] as [string, any]);
}

export function TimeseriesChart({ filters, sqlKey }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["timeseries", filters],
    queryFn: () =>
      api.timeseries({
        start: filters.start,
        end: filters.end,
        interval: filters.interval,
        vendor_id: filters.vendor_id,
        payment_type: filters.payment_type,
        pickup_zone_id: filters.pickup_zone_id.length ? filters.pickup_zone_id : undefined,
        dropoff_zone_id: filters.dropoff_zone_id.length ? filters.dropoff_zone_id : undefined
      }),
    refetchInterval,
    refetchIntervalInBackground: true
  });

  useReportSql(sqlKey, q.data?.meta?.sql);

  const option = useMemo(() => {
    const pts = q.data?.series ?? [];
    return {
      tooltip: { trigger: "axis" as const },
      legend: { top: 0 },
      grid: { left: 50, right: 25, top: 35, bottom: 35 },
      xAxis: { type: "time" as const },
      yAxis: [
        { type: "value" as const, name: "Trips" },
        { type: "value" as const, name: "USD / seconds" }
      ],
      series: [
        { name: "Trips", type: "line" as const, showSymbol: false, yAxisIndex: 0, data: toSeries(pts, "trips") },
        { name: "Fare", type: "line" as const, showSymbol: false, yAxisIndex: 1, data: toSeries(pts, "fare") },
        { name: "Tip", type: "line" as const, showSymbol: false, yAxisIndex: 1, data: toSeries(pts, "tip") },
        { name: "p50 duration (s)", type: "line" as const, showSymbol: false, yAxisIndex: 1, data: toSeries(pts, "p50_duration_s") },
        { name: "p95 duration (s)", type: "line" as const, showSymbol: false, yAxisIndex: 1, data: toSeries(pts, "p95_duration_s") }
      ]
    };
  }, [q.data?.series]);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">Failed to load: {(q.error as Error).message}</div>;

  return (
    <div>
      <div className="text-secondary small mb-2">
        Query: {q.data?.meta.elapsed_ms}ms • rows {q.data?.meta.rows_returned}
      </div>
      <EChart option={option} height={360} />
    </div>
  );
}

