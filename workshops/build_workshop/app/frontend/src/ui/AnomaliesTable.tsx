import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { DashboardFilters } from "./FilterBar";

type Props = { filters: DashboardFilters };

export function AnomaliesTable({ filters }: Props) {
  const [rule, setRule] = useState<"fare_per_mile" | "fare_per_minute" | "tip_ratio">("tip_ratio");
  const [minThreshold, setMinThreshold] = useState<number>(0.15);

  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const q = useQuery({
    queryKey: ["anomalies", filters, rule, minThreshold],
    queryFn: () =>
      api.anomalies({
        start: filters.start,
        end: filters.end,
        rule,
        min_threshold: minThreshold,
        limit: 50,
        vendor_id: filters.vendor_id,
        payment_type: filters.payment_type,
        pickup_zone_id: filters.pickup_zone_id.length ? filters.pickup_zone_id : undefined,
        dropoff_zone_id: filters.dropoff_zone_id.length ? filters.dropoff_zone_id : undefined
      }),
    refetchInterval,
    refetchIntervalInBackground: true
  });

  return (
    <div>
      <div className="row g-2 mb-2">
        <div className="col-7">
          <label className="form-label">Rule</label>
          <select className="form-select form-select-sm" value={rule} onChange={(e) => setRule(e.target.value as any)}>
            <option value="tip_ratio">tip_ratio</option>
            <option value="fare_per_mile">fare_per_mile</option>
            <option value="fare_per_minute">fare_per_minute</option>
          </select>
        </div>
        <div className="col-5">
          <label className="form-label">Min</label>
          <input
            className="form-control form-control-sm"
            type="number"
            step="0.01"
            value={minThreshold}
            onChange={(e) => setMinThreshold(Number(e.target.value))}
          />
        </div>
      </div>

      {q.isLoading ? <div className="text-secondary">Loading…</div> : null}
      {q.isError ? <div className="text-danger">Failed to load: {(q.error as Error).message}</div> : null}

      <div className="table-responsive" style={{ maxHeight: 360 }}>
        <table className="table table-sm table-striped align-middle">
          <thead>
            <tr>
              <th>Pickup → Dropoff</th>
              <th className="text-end">Score</th>
              <th className="text-end">Fare</th>
              <th className="text-end">Tip</th>
              <th className="text-end">Dur</th>
            </tr>
          </thead>
          <tbody>
            {(q.data?.rows ?? []).map((r, idx) => (
              <tr key={`${r.pickup_datetime}-${idx}`}>
                <td>
                  <div className="fw-semibold">{r.pickup_zone}</div>
                  <div className="text-secondary small">{r.dropoff_zone}</div>
                </td>
                <td className="text-end">{r.score.toFixed(2)}</td>
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

