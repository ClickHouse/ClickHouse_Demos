import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { DashboardFilters } from "./FilterBar";
import { EChart } from "./EChart";
import { useReportSql } from "./SqlRegistry";

type Props = {
  filters: DashboardFilters;
  direction: "pickup" | "dropoff";
  sqlKey: string;
};

export function TopZonesBar({ filters, direction, sqlKey }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["topZones", direction, filters],
    queryFn: () =>
      api.topZones({
        start: filters.start,
        end: filters.end,
        metric: "trips",
        direction,
        limit: 10,
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
    const rows = q.data?.rows ?? [];
    const labels = rows.map((r) => `${r.zone}`);
    const values = rows.map((r) => r.value);
    return {
      tooltip: { trigger: "axis" as const },
      grid: { left: 10, right: 10, top: 10, bottom: 10, containLabel: true },
      xAxis: { type: "value" as const },
      yAxis: { type: "category" as const, data: labels, inverse: true },
      series: [{ type: "bar" as const, data: values }]
    };
  }, [q.data?.rows]);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">Failed to load: {(q.error as Error).message}</div>;
  return <EChart option={option} height={260} />;
}

