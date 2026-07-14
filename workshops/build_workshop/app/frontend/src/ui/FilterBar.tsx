import type { Zone } from "../api/types";

export type DashboardFilters = {
  start: string;
  end: string;
  interval: "1m" | "5m" | "15m" | "1h";
  auto_refresh_s: 0 | 5 | 15 | 30 | 60;
  vendor_id?: number;
  payment_type?: number;
  pickup_zone_id: number[];
  dropoff_zone_id: number[];
};

type Props = {
  zones: Zone[];
  value: DashboardFilters;
  onChange: (next: DashboardFilters) => void;
  onUseSample: () => void;
  onReset: () => void;
};

function toLocalInputValue(iso: string) {
  const d = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, "0");
  // datetime-local doesn't support Z; we keep UTC-ish by using UTC fields.
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

function fromLocalInputValue(v: string) {
  // interpret as UTC
  return new Date(v + ":00Z").toISOString();
}

export function FilterBar({ zones, value, onChange, onUseSample, onReset }: Props) {
  return (
    <div className="card">
      <div className="card-body">
        <div className="d-flex justify-content-between align-items-center mb-2">
          <div className="h5 mb-0">Filters</div>
          <div className="d-flex gap-2">
            <button className="btn btn-sm btn-outline-secondary" onClick={onReset}>
              Reset filters
            </button>
            <button className="btn btn-sm btn-outline-primary" onClick={onUseSample}>
              Use sample window
            </button>
          </div>
        </div>

        {/* Row 1: date range */}
        <div className="row g-2 align-items-end">
          <div className="col-12 col-md-6">
            <label className="form-label">Start (UTC)</label>
            <input
              className="form-control"
              type="datetime-local"
              value={toLocalInputValue(value.start)}
              onChange={(e) => onChange({ ...value, start: fromLocalInputValue(e.target.value) })}
            />
          </div>
          <div className="col-12 col-md-6">
            <label className="form-label">End (UTC)</label>
            <input
              className="form-control"
              type="datetime-local"
              value={toLocalInputValue(value.end)}
              onChange={(e) => onChange({ ...value, end: fromLocalInputValue(e.target.value) })}
            />
          </div>
        </div>

        {/* Row 2: dropdown filters (single row on md+) */}
        <div className="row g-2 align-items-end mt-0">
          <div className="col-6 col-md-3">
            <label className="form-label">Interval</label>
            <select className="form-select" value={value.interval} onChange={(e) => onChange({ ...value, interval: e.target.value as any })}>
              <option value="1m">1m</option>
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="1h">1h</option>
            </select>
          </div>
          <div className="col-6 col-md-3">
            <label className="form-label">Auto-refresh</label>
            <select
              className="form-select"
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
          <div className="col-6 col-md-3">
            <label className="form-label">Vendor</label>
            <select
              className="form-select"
              value={value.vendor_id ?? ""}
              onChange={(e) => onChange({ ...value, vendor_id: e.target.value ? Number(e.target.value) : undefined })}
            >
              <option value="">All</option>
              <option value="1">1</option>
              <option value="2">2</option>
            </select>
          </div>
          <div className="col-6 col-md-3">
            <label className="form-label">Payment</label>
            <select
              className="form-select"
              value={value.payment_type ?? ""}
              onChange={(e) => onChange({ ...value, payment_type: e.target.value ? Number(e.target.value) : undefined })}
            >
              <option value="">All</option>
              <option value="1">Card</option>
              <option value="2">Cash</option>
            </select>
          </div>
        </div>

        {/* Row 3: zone selections */}
        <div className="row g-2 align-items-end mt-0">
          <div className="col-12 col-md-6">
            <label className="form-label">Pickup zones</label>
            <select
              className="form-select"
              multiple
              size={4}
              value={value.pickup_zone_id.map(String)}
              onChange={(e) => {
                const selected = Array.from(e.target.selectedOptions).map((o) => Number(o.value));
                onChange({ ...value, pickup_zone_id: selected });
              }}
            >
              {zones.map((z) => (
                <option key={z.zone_id} value={z.zone_id}>
                  {z.borough} — {z.zone}
                </option>
              ))}
            </select>
          </div>

          <div className="col-12 col-md-6">
            <label className="form-label">Dropoff zones</label>
            <select
              className="form-select"
              multiple
              size={4}
              value={value.dropoff_zone_id.map(String)}
              onChange={(e) => {
                const selected = Array.from(e.target.selectedOptions).map((o) => Number(o.value));
                onChange({ ...value, dropoff_zone_id: selected });
              }}
            >
              {zones.map((z) => (
                <option key={z.zone_id} value={z.zone_id}>
                  {z.borough} — {z.zone}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>
    </div>
  );
}

