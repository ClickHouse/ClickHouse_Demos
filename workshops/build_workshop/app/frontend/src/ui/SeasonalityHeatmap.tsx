import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { HeatmapCell } from "../api/types";
import type { HistoricalFilters } from "./HistoricalFilterBar";
import { EChart } from "./EChart";

type Props = { filters: HistoricalFilters };

export function SeasonalityHeatmap({ filters }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["historicalSeasonality", filters],
    queryFn: () =>
      api.historicalSeasonality({
        start: filters.start,
        end: filters.end,
        metric: filters.metric,
        mode: filters.seasonality_mode,
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

  const option = useMemo(() => {
    const xLabels = q.data?.x_labels ?? [];
    const yLabels = q.data?.y_labels ?? [];
    const cells: HeatmapCell[] = q.data?.cells ?? [];

    const data: Array<[number, number, number]> = cells.map((c: HeatmapCell) => [c.x, c.y, c.value]);
    const max = data.reduce((m: number, d: [number, number, number]) => Math.max(m, Number(d[2]) || 0), 0);

    return {
      tooltip: { position: "top" as const },
      grid: { height: 240, top: 10, left: 45, right: 10, bottom: 25 },
      xAxis: { type: "category" as const, data: xLabels, splitArea: { show: true } },
      yAxis: { type: "category" as const, data: yLabels, splitArea: { show: true } },
      visualMap: {
        min: 0,
        max,
        calculable: true,
        orient: "horizontal" as const,
        left: "center",
        bottom: 0,
        inRange: { color: ["#454722", "#7e8a2f", "#b9c53f", "#e4ec55", "#faff69"] }
      },
      series: [
        {
          name: "value",
          type: "heatmap" as const,
          data,
          emphasis: { itemStyle: { borderColor: "#ffffff", borderWidth: 1 } },
          progressive: 1000
        }
      ]
    };
  }, [q.data]);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">{(q.error as Error).message}</div>;

  return (
    <div>
      <EChart option={option} height={300} />
      <div className="text-secondary small mt-2">
        Query {q.data?.meta.elapsed_ms}ms • cells {q.data?.meta.rows_returned}
      </div>
    </div>
  );
}

