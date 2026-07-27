import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import maplibregl from "maplibre-gl";

import { api } from "../api/client";
import type { HistoricalFilters } from "./HistoricalFilterBar";
import { applyDarkBasemap, choroplethFill, ZONE_OUTLINE_COLOR } from "./mapTheme";
import { useReportSql } from "./SqlRegistry";

type Props = { filters: HistoricalFilters; sqlKey: string };

export function HistoricalZoneMap({ filters, sqlKey }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [geojson, setGeojson] = useState<any | null>(null);
  const [geojsonError, setGeojsonError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch("/taxi_zones.geojson")
      .then(async (r) => {
        if (!r.ok) throw new Error(`Failed to load /taxi_zones.geojson: ${r.status} ${r.statusText}`);
        return await r.json();
      })
      .then((j) => {
        if (!cancelled) setGeojson(j);
      })
      .catch((e) => {
        if (!cancelled) {
          setGeojson(null);
          setGeojsonError((e as Error).message);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const refetchInterval = filters.auto_refresh_s ? filters.auto_refresh_s * 1000 : false;
  const mapQ = useQuery({
    queryKey: ["historicalMap", filters],
    queryFn: () =>
      api.historicalMap({
        start: filters.start,
        end: filters.end,
        metric: filters.metric,
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

  useReportSql(sqlKey, mapQ.data?.meta?.sql);

  const valueByZoneId = useMemo(() => {
    const m = new Map<number, number>();
    for (const r of mapQ.data?.rows ?? []) m.set(r.zone_id, r.value);
    return m;
  }, [mapQ.data?.rows]);

  const choroplethGeojson = useMemo(() => {
    if (!geojson) return null;
    return {
      type: "FeatureCollection" as const,
      features: (geojson.features ?? []).map((f: any) => {
        const zoneId = Number(f?.properties?.zone_id ?? f?.id);
        const value = valueByZoneId.get(zoneId) ?? 0;
        return { ...f, id: zoneId, properties: { ...(f.properties ?? {}), zone_id: zoneId, value } };
      })
    };
  }, [geojson, valueByZoneId]);

  const breaks = useMemo(() => {
    const vals = Array.from(valueByZoneId.values()).filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
    if (vals.length === 0) return [1, 10, 100, 1000];
    const q = (p: number) => vals[Math.min(vals.length - 1, Math.floor(p * (vals.length - 1)))];
    return [q(0.5), q(0.75), q(0.9), q(0.97)].map((v) => Math.max(1, Math.floor(v)));
  }, [valueByZoneId]);

  useEffect(() => {
    if (!ref.current || mapRef.current) return;
    const map = new maplibregl.Map({
      container: ref.current,
      style: "https://demotiles.maplibre.org/style.json",
      center: [-73.9855, 40.758],
      zoom: 9.8
    });
    mapRef.current = map;
    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");

    map.on("load", () => {
      applyDarkBasemap(map);

      map.addSource("zones", {
        type: "geojson",
        data: choroplethGeojson ?? { type: "FeatureCollection", features: [] }
      });

      map.addLayer({
        id: "zones-fill",
        type: "fill",
        source: "zones",
        paint: {
          "fill-color": choroplethFill(breaks),
          "fill-opacity": 0.8
        }
      });

      map.addLayer({
        id: "zones-outline",
        type: "line",
        source: "zones",
        paint: { "line-color": ZONE_OUTLINE_COLOR, "line-width": 0.5, "line-opacity": 0.25 }
      });

      map.on("mousemove", "zones-fill", (e) => {
        const f = e.features?.[0];
        if (!f) return;
        const props = f.properties as any;
        map.getCanvas().style.cursor = "pointer";
        const value = props?.value ?? 0;
        (map as any).__popup ||= new maplibregl.Popup({ closeButton: false, closeOnClick: false });
        (map as any).__popup
          .setLngLat(e.lngLat)
          .setHTML(
            `<div style="font-weight:600;color:#fff">${props.zone ?? "Zone"}</div>
             <div style="color:#9a9ea7">${props.borough ?? ""}</div>
             <div style="margin-top:4px"><span style="color:#9a9ea7">Value:</span> <span style="color:#faff69">${Number(value).toLocaleString()}</span></div>`
          )
          .addTo(map);
      });

      map.on("mouseleave", "zones-fill", () => {
        map.getCanvas().style.cursor = "";
        const p = (map as any).__popup as maplibregl.Popup | undefined;
        if (p) p.remove();
      });
    });

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [choroplethGeojson, breaks]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const src = map.getSource("zones") as maplibregl.GeoJSONSource | undefined;
    if (!src || !choroplethGeojson) return;
    src.setData(choroplethGeojson as any);
    if (map.getLayer("zones-fill")) {
      map.setPaintProperty("zones-fill", "fill-color", choroplethFill(breaks));
    }
  }, [choroplethGeojson, breaks]);

  return (
    <div>
      <div ref={ref} style={{ width: "100%", height: 320, borderRadius: 8, overflow: "hidden" }} />
      <div className="text-secondary small mt-2">
        {mapQ.data ? `Query ${mapQ.data.meta.elapsed_ms}ms` : mapQ.isLoading ? "Loading…" : ""}
        {geojsonError ? <span className="text-danger"> • {geojsonError}</span> : null}
      </div>
    </div>
  );
}

