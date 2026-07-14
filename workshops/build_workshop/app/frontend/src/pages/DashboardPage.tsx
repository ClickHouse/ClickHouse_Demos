import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type { Zone } from "../api/types";
import { FilterBar, type DashboardFilters } from "../ui/FilterBar";
import { TimeseriesChart } from "../ui/TimeseriesChart";
import { TopZonesBar } from "../ui/TopZonesBar";
import { CompareTable } from "../ui/CompareTable";
import { ZoneMap } from "../ui/ZoneMap";
import { DrilldownTable } from "../ui/DrilldownTable";
import { AnomaliesTable } from "../ui/AnomaliesTable";
import { ChatPanel } from "../ui/ChatPanel";

// Defaults that exist in the TLC datasets commonly used in demos.
const SAMPLE_START = "2022-07-02T20:00:00Z";
const SAMPLE_END = "2022-07-02T22:00:00Z";

function utcStartOfTodayIso() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0)).toISOString();
}

function utcEndOfTodayIso() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 23, 59, 59, 999)).toISOString();
}

function subtractDays(iso: string, days: number) {
  const d = new Date(iso);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}

export function DashboardPage() {
  const resetFilters = (): DashboardFilters => ({
    start: utcStartOfTodayIso(),
    end: utcEndOfTodayIso(),
    interval: "15m",
    auto_refresh_s: 0,
    vendor_id: undefined,
    payment_type: undefined,
    pickup_zone_id: [],
    dropoff_zone_id: []
  });

  const [filters, setFilters] = useState<DashboardFilters>({
    start: utcStartOfTodayIso(),
    end: utcEndOfTodayIso(),
    interval: "15m",
    auto_refresh_s: 0,
    vendor_id: undefined,
    payment_type: undefined,
    pickup_zone_id: [],
    dropoff_zone_id: []
  });

  const zonesQ = useQuery({
    queryKey: ["zones"],
    queryFn: api.zones
  });

  const zonesById = useMemo(() => {
    const map = new Map<number, Zone>();
    for (const z of zonesQ.data?.zones ?? []) map.set(z.zone_id, z);
    return map;
  }, [zonesQ.data]);

  const compareParams = useMemo(() => {
    const a_start = filters.start;
    const a_end = filters.end;
    const b_start = subtractDays(filters.start, 7);
    const b_end = subtractDays(filters.end, 7);
    return { a_start, a_end, b_start, b_end };
  }, [filters.start, filters.end]);

  return (
    <div>
      <FilterBar
        zones={zonesQ.data?.zones ?? []}
        value={filters}
        onChange={setFilters}
        onReset={() => setFilters(resetFilters())}
        onUseSample={() =>
          setFilters((v) => ({
            ...v,
            start: SAMPLE_START,
            end: SAMPLE_END
          }))
        }
      />

      <div className="row g-3 mt-1">
        <div className="col-12 col-xl-8">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Act 1 — What’s happening now?</div>
                <div className="text-secondary small">Trips, revenue, p50/p95 duration</div>
              </div>
              <TimeseriesChart filters={filters} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-4">
          <div className="card mb-3">
            <div className="card-body">
              <div className="h5 mb-2">Top pickup zones</div>
              <TopZonesBar filters={filters} direction="pickup" />
            </div>
          </div>
          <div className="card">
            <div className="card-body">
              <div className="h5 mb-2">Top dropoff zones</div>
              <TopZonesBar filters={filters} direction="dropoff" />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-6">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Act 3 — Compare vs last Friday</div>
                <div className="text-secondary small">Auto: B = A - 7 days</div>
              </div>
              <CompareTable filters={filters} aStart={compareParams.a_start} aEnd={compareParams.a_end} bStart={compareParams.b_start} bEnd={compareParams.b_end} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-6">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Map</div>
                <div className="text-secondary small">Taxi zones choropleth</div>
              </div>
              <ZoneMap zones={zonesQ.data?.zones ?? []} filters={filters} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-7">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Drilldown</div>
                <div className="text-secondary small">Raw trips (paginated)</div>
              </div>
              <DrilldownTable filters={filters} zonesById={zonesById} />
            </div>
          </div>
        </div>

        <div className="col-12 col-xl-5">
          <div className="card">
            <div className="card-body">
              <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="h5 mb-0">Act 4 — Suspicious trips</div>
                <div className="text-secondary small">Outliers (rule-based)</div>
              </div>
              <AnomaliesTable filters={filters} />
            </div>
          </div>
        </div>
      </div>

      <ChatPanel />
    </div>
  );
}

