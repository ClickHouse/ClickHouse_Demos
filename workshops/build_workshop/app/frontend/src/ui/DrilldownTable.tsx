import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { Zone } from "../api/types";
import type { DashboardFilters } from "./FilterBar";

type Props = {
  filters: DashboardFilters;
  zonesById: Map<number, Zone>;
};

export function DrilldownTable({ filters }: Props) {
  const [page, setPage] = useState(0);
  const limit = 10;
  const offset = page * limit;

  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["trips", filters, limit, offset],
    queryFn: () =>
      api.trips({
        start: filters.start,
        end: filters.end,
        sort: "pickup_datetime",
        order: "asc",
        limit,
        offset,
        vendor_id: filters.vendor_id,
        payment_type: filters.payment_type,
        pickup_zone_id: filters.pickup_zone_id.length ? filters.pickup_zone_id : undefined,
        dropoff_zone_id: filters.dropoff_zone_id.length ? filters.dropoff_zone_id : undefined
      }),
    refetchInterval,
    refetchIntervalInBackground: true
  });

  if (q.isLoading) return <div className="text-secondary">Loading…</div>;
  if (q.isError) return <div className="text-danger">Failed to load: {(q.error as Error).message}</div>;

  const rows = q.data?.rows ?? [];

  return (
    <div>
      <div className="d-flex justify-content-between align-items-center mb-2">
        <div className="text-secondary small">
          Query: {q.data?.meta.elapsed_ms}ms • rows {q.data?.meta.rows_returned}
        </div>
        <div className="btn-group btn-group-sm">
          <button className="btn btn-outline-secondary" disabled={page === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>
            Prev
          </button>
          <button className="btn btn-outline-secondary" disabled={rows.length < limit} onClick={() => setPage((p) => p + 1)}>
            Next
          </button>
        </div>
      </div>

      <div className="table-responsive" style={{ maxHeight: 360 }}>
        <table className="table table-sm table-hover align-middle">
          <thead>
            <tr>
              <th>Pickup</th>
              <th>Dropoff</th>
              <th className="text-end">Distance</th>
              <th className="text-end">Fare</th>
              <th className="text-end">Tip</th>
              <th className="text-end">Duration</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, idx) => (
              <tr key={`${r.pickup_datetime}-${idx}`}>
                <td>
                  <div className="fw-semibold">{r.pickup_zone}</div>
                  <div className="text-secondary small">{new Date(r.pickup_datetime).toISOString()}</div>
                </td>
                <td>
                  <div className="fw-semibold">{r.dropoff_zone}</div>
                  <div className="text-secondary small">{new Date(r.dropoff_datetime).toISOString()}</div>
                </td>
                <td className="text-end">{r.trip_distance.toFixed(1)}</td>
                <td className="text-end">${r.fare_amount.toFixed(2)}</td>
                <td className="text-end">${r.tip_amount.toFixed(2)}</td>
                <td className="text-end">{Math.round(r.duration_s / 60)}m</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

