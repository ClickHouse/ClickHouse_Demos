import type { Zone } from "../api/types";

export type HistoricalFilters = {
  start: string;
  end: string;
  bucket: "day" | "week" | "month";
  car_type: "all" | "yellow" | "green";
  metric: "trips" | "revenue" | "tip" | "p50_duration_s" | "p95_duration_s";
  seasonality_mode: "dow_hour" | "month_dow";
  reasonable_only: boolean;
  auto_refresh_s: 0 | 5 | 15 | 30 | 60;
  vendor_id?: number;
  payment_type?: number;
  pickup_zone_id: number[];
  dropoff_zone_id: number[];
};

type Props = {
  zones: Zone[];
  value: HistoricalFilters;
  onChange: (v: HistoricalFilters) => void;
  onReset: () => void;
};

function toLocalInputValue(iso: string) {
  // datetime-local expects "YYYY-MM-DDTHH:mm"
  const d = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

function fromLocalInputValue(v: string) {
  // Treat input as UTC
  return new Date(v + ":00Z").toISOString();
}

export function HistoricalFilterBar({ zones, value, onChange, onReset }: Props) {
  const groupedZones = zones.reduce<Record<string, Zone[]>>((acc, z) => {
    (acc[z.borough] ||= []).push(z);
    return acc;
  }, {});

  return (
    <div className="card">
      <div className="card-body">
        <div className="d-flex align-items-center justify-content-between mb-2">
          <div className="h6 mb-0">Filters</div>
          <div className="d-flex gap-2">
            <button className="btn btn-outline-secondary btn-sm" onClick={onReset}>
              Reset filters
            </button>
            <button
              className="btn btn-outline-secondary btn-sm"
              onClick={() =>
                onChange({
                  ...value,
                  start: "2022-01-01T00:00:00Z",
                  end: "2023-01-01T00:00:00Z",
                  bucket: "month"
                })
              }
            >
              Use 2022 window
            </button>
          </div>
        </div>

        {/* Row 1: date range */}
        <div className="row g-2">
          <div className="col-12 col-md-6">
            <div className="form-label mb-1">Start (UTC)</div>
            <input
              className="form-control form-control-sm"
              type="datetime-local"
              value={toLocalInputValue(value.start)}
              onChange={(e) => onChange({ ...value, start: fromLocalInputValue(e.target.value) })}
            />
          </div>

          <div className="col-12 col-md-6">
            <div className="form-label mb-1">End (UTC)</div>
            <input
              className="form-control form-control-sm"
              type="datetime-local"
              value={toLocalInputValue(value.end)}
              onChange={(e) => onChange({ ...value, end: fromLocalInputValue(e.target.value) })}
            />
          </div>
        </div>

        {/* Row 2: dropdown filters (single row; scrolls instead of wrapping) */}
        <div className="d-flex gap-2 align-items-end flex-nowrap overflow-auto mt-2 pb-1">
          <div style={{ minWidth: 140 }}>
            <div className="form-label mb-1">Bucket</div>
            <select className="form-select form-select-sm" value={value.bucket} onChange={(e) => onChange({ ...value, bucket: e.target.value as any })}>
              <option value="day">day</option>
              <option value="week">week</option>
              <option value="month">month</option>
            </select>
          </div>

          <div style={{ minWidth: 140 }}>
            <div className="form-label mb-1">Car type</div>
            <select className="form-select form-select-sm" value={value.car_type} onChange={(e) => onChange({ ...value, car_type: e.target.value as any })}>
              <option value="all">All</option>
              <option value="yellow">Yellow</option>
              <option value="green">Green</option>
            </select>
          </div>

          <div style={{ minWidth: 160 }}>
            <div className="form-label mb-1">Metric</div>
            <select className="form-select form-select-sm" value={value.metric} onChange={(e) => onChange({ ...value, metric: e.target.value as any })}>
              <option value="trips">Trips</option>
              <option value="revenue">Revenue</option>
              <option value="tip">Tip</option>
              <option value="p50_duration_s">p50 duration</option>
              <option value="p95_duration_s">p95 duration</option>
            </select>
          </div>

          <div style={{ minWidth: 160 }}>
            <div className="form-label mb-1">Auto-refresh</div>
            <select
              className="form-select form-select-sm"
              value={value.auto_refresh_s}
              onChange={(e) => onChange({ ...value, auto_refresh_s: Number(e.target.value) as any })}
            >
              <option value={0}>Off</option>
              <option value={5}>5s</option>
              <option value={15}>15s</option>
              <option value={30}>30s</option>
              <option value={60}>1m</option>
            </select>
          </div>

          <div style={{ minWidth: 170 }}>
            <div className="form-label mb-1">Seasonality</div>
            <select
              className="form-select form-select-sm"
              value={value.seasonality_mode}
              onChange={(e) => onChange({ ...value, seasonality_mode: e.target.value as any })}
            >
              <option value="dow_hour">dow_hour</option>
              <option value="month_dow">month_dow</option>
            </select>
          </div>

          <div style={{ minWidth: 140 }}>
            <div className="form-label mb-1">Vendor</div>
            <select
              className="form-select form-select-sm"
              value={value.vendor_id ?? ""}
              onChange={(e) => onChange({ ...value, vendor_id: e.target.value ? Number(e.target.value) : undefined })}
            >
              <option value="">All</option>
              <option value="1">1</option>
              <option value="2">2</option>
            </select>
          </div>

          <div style={{ minWidth: 140 }}>
            <div className="form-label mb-1">Payment</div>
            <select
              className="form-select form-select-sm"
              value={value.payment_type ?? ""}
              onChange={(e) => onChange({ ...value, payment_type: e.target.value ? Number(e.target.value) : undefined })}
            >
              <option value="">All</option>
              <option value="1">Card</option>
              <option value="2">Cash</option>
            </select>
          </div>

          <div className="d-flex align-items-end" style={{ minWidth: 190 }}>
            <div className="form-check">
              <input
                className="form-check-input"
                type="checkbox"
                checked={value.reasonable_only}
                onChange={(e) => onChange({ ...value, reasonable_only: e.target.checked })}
                id="reasonableOnly"
              />
              <label className="form-check-label" htmlFor="reasonableOnly">
                Reasonable trips only
              </label>
            </div>
          </div>
        </div>

        {/* Row 3: zone selections */}
        <div className="row g-2 mt-2">
          <div className="col-12 col-md-6">
            <div className="form-label mb-1">Pickup zones</div>
            <select
              className="form-select form-select-sm"
              multiple
              value={value.pickup_zone_id.map(String)}
              onChange={(e) =>
                onChange({
                  ...value,
                  pickup_zone_id: Array.from(e.target.selectedOptions).map((o) => Number(o.value))
                })
              }
              style={{ height: 120 }}
            >
              {Object.entries(groupedZones).map(([borough, zs]) => (
                <optgroup key={borough} label={borough}>
                  {zs.map((z) => (
                    <option key={z.zone_id} value={z.zone_id}>
                      {z.zone}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>

          <div className="col-12 col-md-6">
            <div className="form-label mb-1">Dropoff zones</div>
            <select
              className="form-select form-select-sm"
              multiple
              value={value.dropoff_zone_id.map(String)}
              onChange={(e) =>
                onChange({
                  ...value,
                  dropoff_zone_id: Array.from(e.target.selectedOptions).map((o) => Number(o.value))
                })
              }
              style={{ height: 120 }}
            >
              {Object.entries(groupedZones).map(([borough, zs]) => (
                <optgroup key={borough} label={borough}>
                  {zs.map((z) => (
                    <option key={z.zone_id} value={z.zone_id}>
                      {z.zone}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>
        </div>
      </div>
    </div>
  );
}

