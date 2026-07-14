import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { MoversRow } from "../api/types";
import type { HistoricalFilters } from "./HistoricalFilterBar";

type Props = {
  filters: HistoricalFilters;
  aStart: string;
  aEnd: string;
  bStart: string;
  bEnd: string;
};

function fmt(n: number) {
  if (!Number.isFinite(n)) return "-";
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function MoversTable({ filters, aStart, aEnd, bStart, bEnd }: Props) {
  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["historicalMovers", filters, aStart, aEnd, bStart, bEnd],
    queryFn: () =>
      api.historicalMovers({
        a_start: aStart,
        a_end: aEnd,
        b_start: bStart,
        b_end: bEnd,
        group_by: "pickup_zone",
        metric: filters.metric,
        limit: 20,
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

  const rows: MoversRow[] = q.data?.rows ?? [];

  const rendered = useMemo(() => {
    return rows.map((r: MoversRow) => ({
      label: r.label,
      a: r.a_value,
      b: r.b_value,
      delta: r.delta,
      pct: r.delta_pct
    }));
  }, [rows]);

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">{(q.error as Error).message}</div>;

  return (
    <div>
      <table className="table table-sm table-hover mb-0">
        <thead>
          <tr className="text-secondary">
            <th>Zone</th>
            <th className="text-end">A</th>
            <th className="text-end">B</th>
            <th className="text-end">Δ</th>
            <th className="text-end">Δ%</th>
          </tr>
        </thead>
        <tbody>
          {rendered.map((r: { label: string; a: number; b: number; delta: number; pct?: number | null }) => (
            <tr key={r.label}>
              <td style={{ maxWidth: 240 }} className="text-truncate">
                {r.label}
              </td>
              <td className="text-end">{fmt(r.a)}</td>
              <td className="text-end">{fmt(r.b)}</td>
              <td className={`text-end ${r.delta < 0 ? "text-danger" : "text-success"}`}>{fmt(r.delta)}</td>
              <td className="text-end">{r.pct == null ? "-" : `${(r.pct * 100).toFixed(1)}%`}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="text-secondary small mt-2">Query {q.data?.meta.elapsed_ms}ms</div>
    </div>
  );
}

