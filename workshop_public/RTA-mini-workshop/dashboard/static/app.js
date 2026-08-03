/* ClickHouse Forex Live Dashboard — front end.
   Fetches /api/dashboard on every filter change and renders an ECharts
   candlestick + volume view, plus a prominent query-latency badge. */

const UP = "#2F7D4F";
const DOWN = "#C0392B";
const ACCENT = "#FFE000";

const el = (id) => document.getElementById(id);
const chart = echarts.init(el("chart"));
window.addEventListener("resize", () => chart.resize());

let meta = { pairs: [], date_min: null, date_max: null };
let bucket = "day";

function showBanner(msg) {
  const b = el("banner");
  b.textContent = msg;
  b.classList.add("show");
}
function hideBanner() {
  el("banner").classList.remove("show");
}

function addDays(iso, n) {
  const d = new Date(iso + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function fmtPrice(v) {
  if (v == null) return "—";
  const abs = Math.abs(v);
  const digits = abs >= 100 ? 2 : abs >= 1 ? 4 : 6;
  return Number(v).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
function fmtInt(v) {
  if (v == null) return "—";
  return Number(v).toLocaleString();
}

async function init() {
  try {
    const res = await fetch("/api/meta");
    if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
    meta = await res.json();
  } catch (e) {
    showBanner(
      "Could not reach ClickHouse. Check your .env connection details and that the forex table is loaded. Details: " +
        e.message
    );
    return;
  }

  const pairSel = el("pair");
  meta.pairs.forEach((p) => {
    const o = document.createElement("option");
    o.value = p;
    o.textContent = p;
    pairSel.appendChild(o);
  });
  // Default to gold if present — it's the liveliest series in the dataset.
  pairSel.value = meta.pairs.includes("XAU/USD") ? "XAU/USD" : meta.pairs[0];

  el("start").min = el("end").min = meta.date_min;
  el("start").max = el("end").max = meta.date_max;
  el("start").value = meta.date_min;
  el("end").value = meta.date_max;

  // Wire up controls.
  pairSel.addEventListener("change", load);
  el("start").addEventListener("change", load);
  el("end").addEventListener("change", load);

  el("bucket").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", () => {
      bucket = btn.dataset.bucket;
      el("bucket").querySelectorAll("button").forEach((b) => b.classList.toggle("on", b === btn));
      load();
    });
  });

  el("presets").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", () => applyPreset(btn.dataset.preset));
  });

  load();
}

function applyPreset(kind) {
  if (kind === "month") {
    el("start").value = meta.date_min;
    el("end").value = meta.date_max;
    setBucket("day");
  } else if (kind === "week") {
    el("start").value = meta.date_min;
    el("end").value = addDays(meta.date_min, 6);
    setBucket("hour");
  } else if (kind === "day") {
    // A day mid-dataset so there's plenty of data on either side.
    const d = addDays(meta.date_min, 6);
    el("start").value = d;
    el("end").value = d;
    setBucket("hour");
  }
  load();
}

function setBucket(b) {
  bucket = b;
  el("bucket").querySelectorAll("button").forEach((btn) => btn.classList.toggle("on", btn.dataset.bucket === b));
}

async function load() {
  const pair = el("pair").value;
  const start = el("start").value;
  const end = el("end").value;
  if (!pair || !start || !end) return;
  if (start > end) {
    showBanner("The From date is after the To date.");
    return;
  }
  hideBanner();
  chart.showLoading("default", { text: "", color: ACCENT, maskColor: "rgba(251,251,249,0.6)" });

  const qs = new URLSearchParams({ pair, start, end, bucket });
  let data;
  try {
    const res = await fetch("/api/dashboard?" + qs.toString());
    if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
    data = await res.json();
  } catch (e) {
    chart.hideLoading();
    showBanner("Query failed: " + e.message);
    return;
  }
  chart.hideLoading();

  updateLatency(data.timing);
  updateKpis(data.kpis);
  renderChart(data);
}

function updateLatency(t) {
  const ms = t.server_ms != null ? t.server_ms : t.wall_ms;
  el("lat-ms").textContent = ms != null ? ms.toFixed(ms < 10 ? 1 : 0) : "—";
  el("lat-label").textContent = t.server_ms != null ? "ClickHouse query time" : "round-trip time";
  el("lat-rows").textContent = fmtInt(t.rows_read);
  const badge = el("latency");
  badge.classList.remove("flash");
  void badge.offsetWidth; // restart the animation
  badge.classList.add("flash");
}

function updateKpis(k) {
  el("kpi-ticks").textContent = fmtInt(k.ticks);
  el("kpi-last").textContent = fmtPrice(k.last_bid);
  el("kpi-range").textContent =
    k.low == null ? "—" : fmtPrice(k.low) + " / " + fmtPrice(k.high);
  el("kpi-median").textContent = fmtPrice(k.median_spread);
  el("kpi-p99").textContent = fmtPrice(k.p99_spread);
}

function renderChart(data) {
  el("chart-title").textContent = data.pair + " — price & volume (" + data.bucket + ")";
  const rows = data.ohlc;
  const cats = rows.map((d) => d.t);
  const candle = rows.map((d) => [d.open, d.close, d.low, d.high]);
  const volume = rows.map((d) => ({
    value: d.volume,
    itemStyle: { color: d.close >= d.open ? UP : DOWN },
  }));

  chart.setOption(
    {
      animationDuration: 250,
      textStyle: { fontFamily: "Inter, sans-serif" },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        backgroundColor: "#1A1A17",
        borderColor: "#1A1A17",
        textStyle: { color: "#F2F1EA", fontFamily: "JetBrains Mono, monospace", fontSize: 12 },
      },
      axisPointer: { link: [{ xAxisIndex: "all" }] },
      grid: [
        { left: 62, right: 24, top: 24, height: "58%" },
        { left: 62, right: 24, top: "72%", height: "16%" },
      ],
      xAxis: [
        {
          type: "category",
          data: cats,
          boundaryGap: true,
          axisLine: { lineStyle: { color: "#B9B8AC" } },
          axisLabel: { color: "#6B6B63", fontSize: 11 },
          splitLine: { show: false },
        },
        {
          type: "category",
          gridIndex: 1,
          data: cats,
          axisLine: { lineStyle: { color: "#B9B8AC" } },
          axisLabel: { show: false },
          axisTick: { show: false },
        },
      ],
      yAxis: [
        {
          scale: true,
          splitLine: { lineStyle: { color: "#EEEDE4" } },
          axisLabel: { color: "#6B6B63", fontSize: 11 },
        },
        {
          gridIndex: 1,
          splitNumber: 2,
          axisLabel: { color: "#6B6B63", fontSize: 10 },
          splitLine: { show: false },
        },
      ],
      dataZoom: [
        { type: "inside", xAxisIndex: [0, 1] },
        { type: "slider", xAxisIndex: [0, 1], height: 18, bottom: 6, borderColor: "#E6E5DD" },
      ],
      series: [
        {
          name: "OHLC",
          type: "candlestick",
          data: candle,
          itemStyle: {
            color: UP, color0: DOWN,
            borderColor: UP, borderColor0: DOWN,
          },
        },
        {
          name: "Volume",
          type: "bar",
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: volume,
        },
      ],
    },
    true
  );
}

init();
