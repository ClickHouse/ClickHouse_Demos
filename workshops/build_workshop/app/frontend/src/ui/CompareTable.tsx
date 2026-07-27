import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { DashboardFilters } from "./FilterBar";
import { useReportSql } from "./SqlRegistry";

type Props = {
  filters: DashboardFilters;
  aStart: string;
  aEnd: string;
  bStart: string;
  bEnd: string;
  sqlKey: string;
};

export function CompareTable({ filters, aStart, aEnd, bStart, bEnd, sqlKey }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["comparePeriod", filters, aStart, aEnd, bStart, bEnd],
    queryFn: () =>
      api.comparePeriod({
        a_start: aStart,
        a_end: aEnd,
        b_start: bStart,
        b_end: bEnd,
        group_by: "pickup_zone",
        metric: "trips",
        limit: 20,
        vendor_id: filters.vendor_id,
        payment_type: filters.payment_type,
        pickup_zone_id: filters.pickup_zone_id.length ? filters.pickup_zone_id : undefined,
        dropoff_zone_id: filters.dropoff_zone_id.length ? filters.dropoff_zone_id : undefined
      }),
    refetchInterval,
    refetchIntervalInBackground: true
  });

  useReportSql(sqlKey, q.data?.meta?.sql);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">Failed to load: {(q.error as Error).message}</div>;

  const rows = q.data?.rows ?? [];

  return (
    <div className="table-responsive" style={{ maxHeight: 320 }}>
      <table className="table table-sm table-striped align-middle">
        <thead>
          <tr>
            <th>Zone</th>
            <th className="text-end">A</th>
            <th className="text-end">B</th>
            <th className="text-end">Δ</th>
            <th className="text-end">Δ%</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.zone_id}>
              <td>
                <div className="fw-semibold">{r.zone}</div>
                <div className="text-secondary small">{r.borough}</div>
              </td>
              <td className="text-end">{r.a_value}</td>
              <td className="text-end">{r.b_value}</td>
              <td className={`text-end ${r.delta >= 0 ? "text-success" : "text-danger"}`}>{r.delta}</td>
              <td className="text-end">{r.delta_pct == null ? "—" : `${(r.delta_pct * 100).toFixed(1)}%`}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

