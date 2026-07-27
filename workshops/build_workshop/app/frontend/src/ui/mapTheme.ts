// Shared ClickHouse map theming for the MapLibre choropleths (ZoneMap +
// HistoricalZoneMap). Darkens the demo basemap and provides the brand
// choropleth ramp so both maps stay in sync.

import type maplibregl from "maplibre-gl";

// Dim → bright ClickHouse yellow: higher value = brighter (reads well on dark).
export const CHOROPLETH_COLORS = ["#454722", "#7e8a2f", "#b9c53f", "#e4ec55", "#faff69"];

// Build a MapLibre "step" fill-color expression from four break points.
export function choroplethFill(breaks: number[]): any {
  return [
    "step",
    ["get", "value"],
    CHOROPLETH_COLORS[0],
    breaks[0],
    CHOROPLETH_COLORS[1],
    breaks[1],
    CHOROPLETH_COLORS[2],
    breaks[2],
    CHOROPLETH_COLORS[3],
    breaks[3],
    CHOROPLETH_COLORS[4]
  ];
}

export const ZONE_OUTLINE_COLOR = "#e6e7e9";

// Recolor the demo basemap's own layers to a dark ClickHouse surface. Call this
// at the start of the map's "load" handler, before adding the choropleth layers
// (so those are left untouched). Every setter is guarded so an unexpected demo
// style can never break the map.
export function applyDarkBasemap(map: maplibregl.Map): void {
  try {
    const layers = map.getStyle()?.layers ?? [];
    for (const layer of layers) {
      const id = layer.id;
      try {
        if (layer.type === "background") {
          map.setPaintProperty(id, "background-color", "#131312");
        } else if (layer.type === "fill") {
          map.setPaintProperty(id, "fill-color", "#20201d");
          map.setPaintProperty(id, "fill-outline-color", "#2b2b28");
        } else if (layer.type === "line") {
          map.setPaintProperty(id, "line-color", "#2f2f2c");
        } else if (layer.type === "symbol") {
          try { map.setPaintProperty(id, "text-color", "#6b6f77"); } catch { /* layer has no text */ }
          try { map.setPaintProperty(id, "text-halo-color", "#131312"); } catch { /* no halo */ }
        }
      } catch { /* skip any layer that rejects the paint override */ }
    }
  } catch { /* getStyle unavailable — leave the basemap as-is */ }
}
