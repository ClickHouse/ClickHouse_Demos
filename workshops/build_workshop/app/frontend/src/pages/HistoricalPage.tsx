import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { Zone } from "../api/types";
import { HistoricalFilterBar, type HistoricalFilters } from "../ui/HistoricalFilterBar";
import { HistoricalTimeseriesChart } from "../ui/HistoricalTimeseriesChart";
import { SeasonalityHeatmap } from "../ui/SeasonalityHeatmap";
import { MoversTable } from "../ui/MoversTable";
import { HistoricalZoneMap } from "../ui/HistoricalZoneMap";

function utcStartOfYearIso() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), 0, 1, 0, 0, 0, 0)).toISOString();
}

function utcEndOfYearIso() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), 11, 31, 23, 59, 59, 999)).toISOString();
}

export function HistoricalPage() {
  const resetFilters = (): HistoricalFilters => ({
    start: utcStartOfYearIso(),
    end: utcEndOfYearIso(),
    bucket: "month",
    car_type: "all",
    metric: "trips",
    seasonality_mode: "dow_hour",
    reasonable_only: false,
    auto_refresh_s: 0,
    vendor_id: undefined,
    payment_type: undefined,
    pickup_zone_id: [],
    dropoff_zone_id: []
  });

  const [filters, setFilters] = useState<HistoricalFilters>({
    start: utcStartOfYearIso(),
    end: utcEndOfYearIso(),
    bucket: "month",
    car_type: "all",
    metric: "trips",
    seasonality_mode: "dow_hour",
    reasonable_only: false,
    auto_refresh_s: 0,
    vendor_id: undefined,
    payment_type: undefined,
    pickup_zone_id: [],
    dropoff_zone_id: []
  });

  const zonesQ = useQuery({ queryKey: ["zones"], queryFn: api.zones });

  const compare = useMemo(() => {
    // Default compare: previous period of same length
    const aStart = filters.start;
    const aEnd = filters.end;
    const lenMs = new Date(aEnd).getTime() - new Date(aStart).getTime();
    const bEnd = new Date(aStart).toISOString();
    const bStart = new Date(new Date(aStart).getTime() - lenMs).toISOString();
    return { aStart, aEnd, bStart, bEnd };
  }, [filters.start, filters.end]);

  const zoneOptions: Zone[] = zonesQ.data?.zones ?? [];

  return (
    <div>
      <div className="mb-2">
        <div className="h4 mb-0">Historical Metrics</div>
        <div className="text-secondary">Trends, seasonality, and movers over long ranges</div>
      </div>

      <HistoricalFilterBar zones={zoneOptions} value={filters} onChange={setFilters} onReset={() => setFilters(resetFilters())} />

      <div className="row g-3 mt-1">
        <div className="col-12 col-xl-8">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">KPI Timeseries</div>
                <div className="text-secondary small">Bucket: {filters.bucket}</div>
              </div>
              <HistoricalTimeseriesChart filters={filters} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-4">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Seasonality</div>
                <div className="text-secondary small">{filters.seasonality_mode}</div>
              </div>
              <SeasonalityHeatmap filters={filters} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-6">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Movers</div>
                <div className="text-secondary small">A vs previous period</div>
              </div>
              <MoversTable filters={filters} aStart={compare.aStart} aEnd={compare.aEnd} bStart={compare.bStart} bEnd={compare.bEnd} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-6">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Map</div>
                <div className="text-secondary small">Metric: {filters.metric}</div>
              </div>
              <HistoricalZoneMap filters={filters} />
            </div>
          </div>
        </div>
      </div>

      <div className="text-secondary small mt-3">
        Tip: for large ranges, use bucket=month and metric=trips/revenue for fastest interaction.
      </div>
    </div>
  );
}

