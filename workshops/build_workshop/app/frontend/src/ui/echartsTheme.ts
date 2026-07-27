// ClickHouse-styled ECharts theme (Click UI dark palette).
// Registered once in EChart.tsx and applied to every chart, so individual
// components don't need to set colors, axis, or tooltip styling themselves.

const FONT =
  'Inter, "SF Pro Display", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", system-ui, sans-serif';

// Categorical series palette — the Click UI accent colors, brightest first.
export const CH_CHART_PALETTE = [
  "#faff69", // ClickHouse yellow
  "#437eef", // blue
  "#00cbeb", // cyan
  "#fb64d6", // magenta
  "#33ff44", // green
  "#ff7729", // orange
  "#bb33ff", // purple
  "#6df8e1"  // teal
];

const MUTED = "#9a9ea7";
const LEGEND = "#b3b6bd";
const LINE = "#414141";
const SPLIT = "#282828";

const axis = {
  axisLine: { show: true, lineStyle: { color: LINE } },
  axisTick: { show: false, lineStyle: { color: LINE } },
  axisLabel: { color: MUTED },
  splitLine: { show: true, lineStyle: { color: SPLIT } },
  splitArea: { show: false }
};

export const clickhouseEChartsTheme = {
  color: CH_CHART_PALETTE,
  backgroundColor: "transparent",
  textStyle: { fontFamily: FONT, color: LEGEND },

  title: {
    textStyle: { color: "#ffffff", fontFamily: FONT, fontWeight: 600 },
    subtextStyle: { color: MUTED, fontFamily: FONT }
  },

  legend: { textStyle: { color: LEGEND, fontFamily: FONT }, inactiveColor: "#53575f" },

  tooltip: {
    backgroundColor: "#1f1f1c",
    borderColor: "#414141",
    borderWidth: 1,
    textStyle: { color: "#e6e7e9", fontFamily: FONT },
    axisPointer: {
      lineStyle: { color: "#53575f" },
      crossStyle: { color: "#53575f" },
      shadowStyle: { color: "rgba(255,255,255,0.04)" }
    }
  },

  grid: { borderColor: LINE, containLabel: true },

  categoryAxis: { ...axis, splitLine: { show: false, lineStyle: { color: SPLIT } } },
  valueAxis: { ...axis, axisLine: { show: false, lineStyle: { color: LINE } } },
  logAxis: { ...axis, axisLine: { show: false, lineStyle: { color: LINE } } },
  timeAxis: { ...axis },

  line: { symbol: "circle", symbolSize: 4, lineStyle: { width: 2 } },
  bar: { itemStyle: { borderRadius: [3, 3, 0, 0] } },

  visualMap: {
    textStyle: { color: LEGEND, fontFamily: FONT },
    inRange: { color: ["#454722", "#7e8a2f", "#b9c53f", "#e4ec55", "#faff69"] }
  },

  dataZoom: {
    textStyle: { color: MUTED, fontFamily: FONT },
    borderColor: "#323232",
    handleStyle: { color: "#faff69", borderColor: "#faff69" },
    moveHandleStyle: { color: "#53575f" },
    fillerColor: "rgba(250,255,105,0.12)",
    dataBackground: { lineStyle: { color: "#414141" }, areaStyle: { color: "#282828" } }
  }
};
