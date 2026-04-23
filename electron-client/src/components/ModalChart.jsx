/**
 * ModalChart — Highcharts Stock candlestick + volume chart for the
 * Trade Ideas modal.  Two panes: price (top 65%) and volume (bottom 35%).
 *
 * Timestamps: backend returns milliseconds → use b.t directly.
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from "react";
import Highcharts from "highcharts/highstock";
import HighchartsReact from "highcharts-react-official";
import { stockApi, alpacaApi, preferencesApi } from "../api/client";
import { Loader2, AlertCircle, Target, X } from "lucide-react";
import { etStringToUtcMs } from "../utils/timeUtils";

Highcharts.setOptions({ lang: { rangeSelectorZoom: "" } });

// Standalone ET time formatter — used in all axis/tooltip formatters so we never
// depend on `this.chart`, which is unavailable in some Highcharts Stock contexts.
const etTime = new Highcharts.Time({ timezone: "America/New_York" });

// ── Constants ─────────────────────────────────────────────────────────────────
const FUCHSIA      = "#e879f9";
const GREEN_CANDLE = "#22c55e";
const RED_CANDLE   = "#ef4444";
const GREEN_VOL    = "rgba(34,197,94,0.55)";
const RED_VOL      = "rgba(239,68,68,0.55)";

// ── Build Highcharts options ──────────────────────────────────────────────────
function buildOptions(ticker, ohlcv, barTimeMs, threshold) {
  // Append 30 empty 15-min slots after the last bar so ordinal axis shows
  // whitespace to the right (needed for R/R drawing extensions to be visible).
  const BAR_MS    = 15 * 60 * 1000;
  const PAD_BARS  = 30;
  const lastT     = ohlcv.length ? ohlcv[ohlcv.length - 1].t : 0;
  const padPoints = Array.from({ length: PAD_BARS }, (_, i) => lastT + BAR_MS * (i + 1));

  // Split OHLC and volume into separate arrays (fastest for large datasets)
  const ohlcData = [
    ...ohlcv.map(b => [b.t, b.o, b.h, b.l, b.c]),
    ...padPoints.map(t => [t, null, null, null, null]),
  ];
  const volData  = ohlcv.map(b => ({
    x:     b.t,
    y:     b.v,
    color: b.c >= b.o ? GREEN_VOL : RED_VOL,
  }));

  return {
    chart: {
      backgroundColor: "#0f172a",
      style: { fontFamily: "ui-sans-serif, system-ui, sans-serif" },
      animation: false,
    },

    title: { text: null },

    rangeSelector: { enabled: false },
    scrollbar:     { enabled: false },

    navigator: {
      enabled: true,
      height:  36,
      outlineColor:  "#334155",
      outlineWidth:  1,
      maskFill:      "rgba(250,204,21,0.15)",
      handles: { backgroundColor: "#475569", borderColor: "#94a3b8" },
      series:  { color: "#3b82f6", lineWidth: 1 },
      xAxis:   {
        labels: {
          style: { color: "#94a3b8", fontSize: "10px", textOutline: "none" },
          formatter() {
            return etTime.dateFormat("%b %e", this.value);
          },
        },
      },
    },

    xAxis: {
      type: "datetime",
      ordinal: true,
      lineColor:  "#334155",
      tickColor:  "#334155",
      labels: {
        style: { color: "#94a3b8", fontSize: "10px", textOutline: "none" },
        formatter() {
          return etTime.dateFormat("%b %e %H:%M", this.value);
        },
      },
      // Signal vertical line
      plotLines: barTimeMs ? [{
        value:     barTimeMs,
        color:     FUCHSIA,
        width:     2,
        dashStyle: "Solid",
        zIndex:    5,
        label: {
          text:  "⚑ Signal",
          style: { color: FUCHSIA, fontSize: "10px", fontWeight: "bold" },
          rotation: 0,
          y: 14,
        },
      }] : [],
      // Faint highlight band around signal
      plotBands: barTimeMs ? [{
        from:  barTimeMs - 15 * 60 * 1000,
        to:    barTimeMs + 15 * 60 * 1000,
        color: "rgba(232,121,249,0.08)",
        zIndex: 4,
      }] : [],
    },

    yAxis: [
      {
        // ── Price panel (top 80%) ──
        height:    "80%",
        offset:    0,
        lineWidth: 1,
        lineColor: "#334155",
        gridLineColor: "#1e293b",
        labels: {
          align: "right",
          x: -4,
          style: { color: "#94a3b8", fontSize: "10px" },
        },
        resize: { enabled: true, lineColor: "#334155" },
        // Resistance / threshold horizontal line (clipped at signal bar in render callback)
        plotLines: threshold != null ? [{
          id:        "rr-threshold",
          value:     Number(threshold),
          color:     FUCHSIA,
          width:     1,
          dashStyle: "Dash",
          zIndex:    5,
          label: {
            text:  `$${Number(threshold).toFixed(2)}`,
            align: "right",
            x: -4,
            style: { color: FUCHSIA, fontSize: "10px" },
          },
        }] : [],
      },
      {
        // ── Volume panel (bottom 18%) ──
        top:       "82%",
        height:    "18%",
        offset:    0,
        lineWidth: 1,
        lineColor: "#334155",
        gridLineColor: "#1e293b",
        labels: {
          align: "right",
          x: -4,
          style: { color: "#94a3b8", fontSize: "10px" },
          formatter() {
            const v = this.value;
            if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
            if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
            return String(v);
          },
        },
      },
    ],

    tooltip: {
      split:           false,
      shared:          true,
      useHTML:         true,
      backgroundColor: "#0f172a",
      borderColor:     "#334155",
      borderRadius:    8,
      style:           { color: "#e2e8f0", fontSize: "11px" },
      positioner()     { return { x: this.chart.plotLeft + 8, y: this.chart.plotTop + 8 }; },
      formatter() {
        const candle = this.points?.find(p => p.series.type === "candlestick");
        const vol    = this.points?.find(p => p.series.name === "Volume");
        if (!candle) return "";

        const dt  = etTime.dateFormat("%a %b %e %H:%M ET", this.x);
        const chg = candle.point.close - candle.point.open;
        const pct = ((chg / candle.point.open) * 100).toFixed(2);
        const col = chg >= 0 ? GREEN_CANDLE : RED_CANDLE;

        const fmtVol = v => v >= 1e6 ? `${(v / 1e6).toFixed(2)}M`
                          : v >= 1e3 ? `${(v / 1e3).toFixed(0)}K`
                          : String(v);

        return `<span style="color:#94a3b8;font-size:10px">${dt}</span><br/>
          O <b>${candle.point.open.toFixed(2)}</b> &nbsp;
          H <b>${candle.point.high.toFixed(2)}</b> &nbsp;
          L <b>${candle.point.low.toFixed(2)}</b> &nbsp;
          C <b style="color:${col}">${candle.point.close.toFixed(2)}</b>
          <span style="color:${col}">&nbsp;(${chg >= 0 ? "+" : ""}${pct}%)</span>
          ${vol ? `<br/>Vol <b>${fmtVol(vol.y)}</b>` : ""}`;
      },
    },

    plotOptions: {
      candlestick: {
        color:     RED_CANDLE,
        lineColor: RED_CANDLE,
        upColor:     GREEN_CANDLE,
        upLineColor: GREEN_CANDLE,
        dataGrouping: { enabled: false },
      },
      column: {
        dataGrouping: { enabled: false },
        borderWidth:  0,
        pointPadding: 0.05,
        groupPadding: 0,
      },
    },

    series: [
      {
        type:  "candlestick",
        name:  ticker,
        id:    "main",
        data:  ohlcData,
        yAxis: 0,
      },
      {
        type:     "column",
        name:     "Volume",
        id:       "volume",
        data:     volData,
        yAxis:    1,
        linkedTo: "main",
      },
    ],

    legend:  { enabled: false },
    credits: { enabled: false },
  };
}

// ── ATR-based default stop distance ──────────────────────────────────────────
// Returns 1 ATR (average true range) calculated over `period` bars ending at `idx`.
function calcATR(bars, idx, period = 14) {
  const end   = Math.min(idx + 1, bars.length);
  const start = Math.max(0, end - period - 1);
  const slice = bars.slice(start, end);
  if (slice.length < 2) return null;
  let trSum = 0;
  for (let i = 1; i < slice.length; i++) {
    const { h, l } = slice[i];
    const prevC    = slice[i - 1].c;
    trSum += Math.max(h - l, Math.abs(h - prevC), Math.abs(l - prevC));
  }
  return trSum / (slice.length - 1);
}

// ── Derive share qty from risk management settings ────────────────────────────
// qty = floor(riskDollars / entryPrice)
// riskDollars = the "Equals approximately" value shown in the Risk Management section
function deriveQty(entry, riskPrefs, portfolioValue) {
  if (!riskPrefs || entry <= 0) return null;

  let riskDollars = 0;
  if (riskPrefs.risk_mode === "dollar") {
    riskDollars = parseFloat(riskPrefs.risk_value) || 0;
  } else if (riskPrefs.risk_mode === "percent") {
    const pv = parseFloat(portfolioValue) || 0;
    riskDollars = ((parseFloat(riskPrefs.risk_value) || 0) / 100) * pv;
  }

  if (riskDollars <= 0) return null;
  return Math.max(1, Math.floor(riskDollars / entry));
}

// ── R/R drawing helpers ───────────────────────────────────────────────────────
const RR_BANDS     = ["rr-profit", "rr-loss"];
const RR_LINES     = ["rr-entry"];
const RR_CLIP_IDS  = [...RR_BANDS, ...RR_LINES].map(id => `${id}-clip`);

// Grey renderer paths drawn to the LEFT of the entry bar (module-level so drag can clear them).
let rrGreyElems = [];
function clearGreyElems() {
  rrGreyElems.forEach(el => { try { el.destroy(); } catch (_) {} });
  rrGreyElems = [];
}

// Clip a list of plotLine/plotBand SVG elements to only show RIGHT of entryTime.
function applyClipsAtEntry(chart, entryTime) {
  if (!entryTime) return;
  const svg = chart.container.querySelector("svg");
  if (!svg) return;

  let defs = svg.querySelector("defs");
  if (!defs) {
    defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
    svg.insertBefore(defs, svg.firstChild);
  }

  const xPx   = chart.xAxis[0].toPixels(entryTime, false);
  const clipX = Math.max(xPx, chart.plotLeft);
  const clipW = chart.plotLeft + chart.plotWidth - clipX;

  [...RR_BANDS, ...RR_LINES].forEach(id => {
    const plb = chart.yAxis[0].plotLinesAndBands.find(p => p.id === id);
    if (!plb?.svgElem?.element) return;

    const clipId = `${id}-clip`;
    defs.querySelector(`#${clipId}`)?.remove();
    if (clipW <= 0) return;

    const clipPath = document.createElementNS("http://www.w3.org/2000/svg", "clipPath");
    clipPath.id = clipId;
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x",      String(clipX));
    rect.setAttribute("y",      String(chart.plotTop));
    rect.setAttribute("width",  String(clipW));
    rect.setAttribute("height", String(chart.plotHeight));
    clipPath.appendChild(rect);
    defs.appendChild(clipPath);
    plb.svgElem.element.setAttribute("clip-path", `url(#${clipId})`);
  });
}

// Draw muted grey lines to the LEFT of the entry bar for stop, target and entry.
function drawGreyLeftLines(chart, rr) {
  clearGreyElems();
  if (!rr?.entryTime) return;

  const xPx  = chart.xAxis[0].toPixels(rr.entryTime, false);
  const leftX = chart.plotLeft;
  const leftW = Math.min(xPx, chart.plotLeft + chart.plotWidth) - leftX;
  if (leftW <= 0) return;

  const isLong  = rr.target > rr.entry;
  const risk    = Math.abs(rr.entry - rr.stop);
  const rrNum   = Number(rr.rrRatio); // use the stored value, not a price-derived one
  const rLevelLines = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].reduce((acc, r) => {
    if (r >= rrNum) return acc; // skip levels at or beyond the target R
    const rPrice = isLong ? rr.entry + r * risk : rr.entry - r * risk;
    acc.push({ price: rPrice, strokeWidth: 1, dash: "2,3" });
    return acc;
  }, []);

  const YELLOW = "#facc15";
  [
    { price: rr.target, strokeWidth: 1, dash: "4,3" },
    { price: rr.stop,   strokeWidth: 1, dash: "4,3" },
    { price: rr.entry,  strokeWidth: 1, dash: "4,3" },
    ...rLevelLines,
  ].forEach(({ price, strokeWidth, dash }) => {
    const yPx   = chart.yAxis[0].toPixels(price, false);
    const attrs = {
      d:              `M ${leftX} ${yPx} L ${leftX + leftW} ${yPx}`,
      stroke:         YELLOW,
      "stroke-width": strokeWidth,
      zIndex:         4,
    };
    if (dash) attrs["stroke-dasharray"] = dash;
    rrGreyElems.push(chart.renderer.path().attr(attrs).add());
  });
}

function removeClipPaths(chart) {
  const defs = chart?.container?.querySelector("svg defs");
  if (!defs) return;
  RR_CLIP_IDS.forEach(clipId => defs.querySelector(`#${clipId}`)?.remove());
}

// Draw the green/red zones and R-level dashed lines as SVG renderer elements
// starting at the entry bar — no clip paths needed since we control the x origin.
function drawColoredZones(chart, rr) {
  if (!rr?.entry || !rr?.stop || !rr?.target || !rr?.entryTime) return;

  const entryXPx  = chart.xAxis[0].toPixels(rr.entryTime, false);
  const rightEdge = chart.plotLeft + chart.plotWidth;
  const zoneX     = Math.max(entryXPx, chart.plotLeft);
  const zoneW     = rightEdge - zoneX;
  if (zoneW <= 0) return;

  const { entry, stop, target, qty } = rr;
  const isLong  = target > entry;
  const risk    = Math.abs(entry - stop);
  const reward  = Math.abs(target - entry);
  const rrNum   = Number(rr.rrRatio);
  const toY     = price => chart.yAxis[0].toPixels(price, false);

  const entryY  = toY(entry);
  const targetY = toY(target);
  const stopY   = toY(stop);

  // Green profit zone
  const greenTop    = Math.min(entryY, targetY);
  const greenBottom = Math.max(entryY, targetY);
  rrGreyElems.push(
    chart.renderer.rect(zoneX, greenTop, zoneW, greenBottom - greenTop)
      .attr({ fill: "rgba(34,197,94,0.10)", stroke: "rgba(34,197,94,0.40)", "stroke-width": 1, zIndex: 2 })
      .add()
  );
  const tgtPct    = ((reward / entry) * 100).toFixed(2);
  const rewardAmt = (reward * qty).toFixed(2);
  const tgtLabel  = isLong
    ? `▲  $${target.toFixed(2)}  (+${tgtPct}%)  ×${qty}  =  $${rewardAmt}`
    : `▼  $${target.toFixed(2)}  (−${tgtPct}%)  ×${qty}  =  $${rewardAmt}`;
  rrGreyElems.push(
    chart.renderer.text(tgtLabel, zoneX + zoneW / 2, (greenTop + greenBottom) / 2 + 4)
      .attr({ align: "center", zIndex: 5 })
      .css({ color: "#22c55e", fontSize: "11px", fontWeight: "bold",
             backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" })
      .add()
  );

  // Red loss zone
  const redTop    = Math.min(entryY, stopY);
  const redBottom = Math.max(entryY, stopY);
  rrGreyElems.push(
    chart.renderer.rect(zoneX, redTop, zoneW, redBottom - redTop)
      .attr({ fill: "rgba(239,68,68,0.10)", stroke: "rgba(239,68,68,0.40)", "stroke-width": 1, zIndex: 2 })
      .add()
  );
  const stpPct   = ((risk / entry) * 100).toFixed(2);
  const riskAmt  = (risk * qty).toFixed(2);
  const stpLabel = isLong
    ? `▼  $${stop.toFixed(2)}  (−${stpPct}%)  ×${qty}  =  $${riskAmt}`
    : `▲  $${stop.toFixed(2)}  (+${stpPct}%)  ×${qty}  =  $${riskAmt}`;
  rrGreyElems.push(
    chart.renderer.text(stpLabel, zoneX + zoneW / 2, (redTop + redBottom) / 2 + 4)
      .attr({ align: "center", zIndex: 5 })
      .css({ color: "#ef4444", fontSize: "11px", fontWeight: "bold",
             backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" })
      .add()
  );

  // Target line (solid green)
  rrGreyElems.push(
    chart.renderer.path()
      .attr({ d: `M ${zoneX} ${targetY} L ${rightEdge} ${targetY}`,
              stroke: "#22c55e", "stroke-width": 2, zIndex: 5 })
      .add()
  );

  // Stop line (solid red)
  rrGreyElems.push(
    chart.renderer.path()
      .attr({ d: `M ${zoneX} ${stopY} L ${rightEdge} ${stopY}`,
              stroke: "#ef4444", "stroke-width": 2, zIndex: 5 })
      .add()
  );

  // R-level dashed lines and labels (right side of entry only)
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].forEach(r => {
    if (r >= rrNum) return;
    const rPrice = isLong ? entry + r * risk : entry - r * risk;
    const rY     = toY(rPrice);
    rrGreyElems.push(
      chart.renderer.path()
        .attr({ d: `M ${zoneX} ${rY} L ${rightEdge} ${rY}`,
                stroke: "rgba(34,197,94,0.45)", "stroke-width": 1,
                "stroke-dasharray": "4,3", zIndex: 4 })
        .add()
    );
    rrGreyElems.push(
      chart.renderer.text(`${r}R`, zoneX + 8, rY - 2)
        .attr({ zIndex: 5 })
        .css({ color: "#22c55e", fontSize: "10px", fontWeight: "700",
               backgroundColor: "rgba(15,23,42,0.75)", padding: "1px 5px", borderRadius: "2px" })
        .add()
    );
  });
}

function applyRR(chart, rr) {
  const yAxis = chart?.yAxis?.[0];
  if (!yAxis) return;

  RR_BANDS.forEach(id => yAxis.removePlotBand(id));
  RR_LINES.forEach(id => yAxis.removePlotLine(id));
  removeClipPaths(chart);
  clearGreyElems();

  if (!rr?.entry || !rr?.stop || !rr?.target) return;

  const { entry, stop, target } = rr;
  const risk    = Math.abs(entry - stop);
  const reward  = Math.abs(target - entry);
  const rrRatio = risk > 0 ? (reward / risk).toFixed(2) : "∞";

  // Green/red zones and R-level lines are drawn as renderer elements in drawColoredZones()
  // (called below inside the entryTime block) so they are naturally bounded to the right of entry.

  // Entry dashed line
  yAxis.addPlotLine({
    id: "rr-entry", value: entry,
    color: "#94a3b8", width: 1, dashStyle: "Dash", zIndex: 5,
    label: {
      text:  `Entry $${entry.toFixed(2)}  ·  R/R ${rrRatio}`,
      align: "right", x: -6,
      style: { color: "#94a3b8", fontSize: "10px", fontWeight: "600" },
    },
  });

  // Target and stop lines are drawn as renderer paths in drawColoredZones()
  // so they never appear to the left of the entry bar.

  // Clip the entry plotLine and draw everything else as renderer elements
  if (rr.entryTime) {
    applyClipsAtEntry(chart, rr.entryTime);
    drawGreyLeftLines(chart, rr);  // clears rrGreyElems, then draws left stubs
    drawColoredZones(chart, rr);   // draws zones + target/stop lines + R-level lines
  }
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function ModalChart({ ticker, barTime, threshold, height, bias, onClose }) {
  const [bars,    setBars]    = useState([]);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState(null);
  const [rrMode,     setRrMode]     = useState(false);    // waiting for entry click
  const [rr,         setRr]         = useState(null);     // { entry, stop, target, qty, rrRatio }
  const [direction,  setDirection]  = useState(bias === "short" ? "short" : "long");   // "long" | "short"
  const [orderType,      setOrderType]      = useState(null);   // null | "market" | "limit"
  const [orderSubmitting, setOrderSubmitting] = useState(false);
  const [orderResult,     setOrderResult]     = useState(null);  // { ok, message, orderId } | null
  const [liveQuote,       setLiveQuote]       = useState(null);  // null | { bid, ask, last, spread, updatedAt, fetching }
  const [activeZoom, setActiveZoom] = useState("3D");
  const [riskPrefs,      setRiskPrefs]      = useState(null);   // { risk_mode, risk_value }
  const [portfolioValue, setPortfolioValue] = useState(null);   // numeric string from Alpaca
  const [qtyDerived,     setQtyDerived]     = useState(false);  // true when qty was auto-set
  const chartRef = useRef(null);
  const rrRef    = useRef(null);                          // live mirror of rr (used in drag handlers)

  const ZOOM_PRESETS = [
    { label: "1D", days: 1   },
    { label: "3D", days: 3   },
    { label: "1W", days: 7   },
    { label: "2W", days: 14  },
    { label: "All", days: null },
  ];

  // Last timestamp including the 30 null padding bars appended in buildOptions
  const paddedLastT = useCallback(() => {
    if (!bars.length) return 0;
    return bars[bars.length - 1].t + 30 * 15 * 60 * 1000;
  }, [bars]);

  const applyZoom = useCallback((preset) => {
    const chart = chartRef.current?.chart;
    if (!chart || !bars.length) return;
    setActiveZoom(preset.label);
    const padT = paddedLastT();
    if (!preset.days) {
      chart.xAxis[0].setExtremes(bars[0].t, padT, true, false);
    } else {
      const lastT   = bars[bars.length - 1].t;
      const fromT   = lastT - preset.days * 24 * 60 * 60 * 1000;
      const fromIdx = bars.findIndex(b => b.t >= fromT);
      const startT  = bars[Math.max(0, fromIdx)].t;
      chart.xAxis[0].setExtremes(startT, padT, true, false);
    }
  }, [bars, paddedLastT]);

  // ── Data fetch ──────────────────────────────────────────────────────────────
  const fetchBars = useCallback(() => {
    if (!ticker) return;
    setLoading(true);
    setError(null);

    const today = new Date();
    const from  = new Date(today);
    from.setDate(from.getDate() - 28);
    const fmt = d => d.toISOString().slice(0, 10);

    stockApi.history(ticker, {
      multiplier: 15, timespan: "minute",
      from: fmt(from), to: fmt(today), limit: 3000,
    })
      .then(r => {
        const raw = (r.data.bars || []).sort((a, b) => a.t - b.t);
        setBars(raw.map(b => ({ t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v })));
      })
      .catch(() => setError("Failed to load chart data"))
      .finally(() => setLoading(false));
  }, [ticker]);

  useEffect(() => { fetchBars(); }, [fetchBars]);

  // ── Load risk prefs + portfolio value once on mount ──────────────────────────
  useEffect(() => {
    preferencesApi.get()
      .then(r => {
        const p = r.data.preferences ?? {};
        if (p.risk_mode && p.risk_value) setRiskPrefs(p);
      })
      .catch(() => {});

    alpacaApi.test()
      .then(r => { if (r.data.ok) setPortfolioValue(r.data.portfolio_value ?? null); })
      .catch(() => {});
  }, []);

  // ── Zoom to signal bar after data loads, within the active preset window ────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !bars.length) return;
    chart.reflow();

    const signalMs  = etStringToUtcMs(barTime);
    const lastT     = bars[bars.length - 1].t;
    const preset    = ZOOM_PRESETS.find(p => p.label === activeZoom);
    const windowMs  = preset?.days ? preset.days * 24 * 60 * 60 * 1000 : null;
    const windowStart = windowMs ? lastT - windowMs : bars[0].t;

    if (signalMs) {
      const signalIdx = bars.findIndex(b => b.t >= signalMs);
      if (signalIdx >= 0) {
        // Centre on signal but clamp to the active zoom window
        const half    = Math.round(78 * 1.5);
        const fromIdx = Math.max(0, signalIdx - half);
        const toIdx   = Math.min(bars.length - 1, signalIdx + half);
        const from = Math.max(bars[fromIdx].t, windowStart);
        chart.xAxis[0].setExtremes(from, paddedLastT(), true, false);
        return;
      }
    }
    // No signal — just apply the preset window
    chart.xAxis[0].setExtremes(windowStart, paddedLastT(), true, false);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bars, barTime]);

  // ── Live bid/ask polling — active only while a drawing is placed ─────────────
  const hasDrawing = rr !== null;
  useEffect(() => {
    if (!hasDrawing) {
      setLiveQuote(null);
      return;
    }

    let cancelled = false;

    const fetchQuote = async () => {
      setLiveQuote(prev => prev ? { ...prev, fetching: true } : { fetching: true });
      try {
        const res = await alpacaApi.quote(ticker);
        if (cancelled) return;
        const d = res.data;
        // Alpaca returns: bid, ask, bid_size, ask_size, spread, last, timestamp
        setLiveQuote({
          bid:      d.bid  > 0 ? d.bid  : null,
          ask:      d.ask  > 0 ? d.ask  : null,
          bidSize:  d.bid_size || null,
          askSize:  d.ask_size || null,
          spread:   d.spread > 0 ? d.spread : null,
          last:     d.last  > 0 ? d.last  : null,
          updatedAt: Date.now(),
          fetching:  false,
        });
      } catch {
        if (cancelled) return;
        setLiveQuote(prev => prev ? { ...prev, fetching: false } : null);
      }
    };

    fetchQuote();
    const id = setInterval(fetchQuote, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [hasDrawing, ticker]);

  // ── Click-to-place entry (rrMode) ───────────────────────────────────────────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !rrMode) return;

    chart.container.style.cursor = "crosshair";

    const handleClick = (e) => {
      const norm = chart.pointer.normalize(e);

      // Ignore clicks outside the price pane (top 80%)
      if (norm.chartY < chart.plotTop ||
          norm.chartY > chart.plotTop + chart.plotHeight * 0.80) return;

      // Snap to closest bar's close price
      const xVal      = chart.xAxis[0].toValue(norm.chartX);
      const nearestIdx = bars.reduce((bestIdx, b, i) =>
        Math.abs(b.t - xVal) < Math.abs(bars[bestIdx].t - xVal) ? i : bestIdx, 0
      );
      const nearest = bars[nearestIdx];
      const entry   = parseFloat(nearest.c.toFixed(2));

      // ATR-based stop: 1.5× ATR so the stop hugs recent price action
      const atr      = calcATR(bars, nearestIdx) ?? entry * 0.02;
      const stopDist = parseFloat((atr * 1.5).toFixed(2));
      // direction is captured from the outer scope via closure
      const stop     = direction === "long"
        ? parseFloat((entry - stopDist).toFixed(2))   // long:  stop below entry
        : parseFloat((entry + stopDist).toFixed(2));  // short: stop above entry

      setRr(prev => {
        const rrRatio    = prev?.rrRatio ?? 2;
        const target     = parseFloat((entry + (entry - stop) * rrRatio).toFixed(2));
        const autoQty    = deriveQty(entry, riskPrefs, portfolioValue);
        const qty        = autoQty ?? prev?.qty ?? 10;
        if (autoQty != null) setQtyDerived(true);
        return { entry, stop, target, qty, rrRatio, entryTime: nearest.t };
      });
      setRrMode(false);
    };

    chart.container.addEventListener("click", handleClick);
    return () => {
      chart.container.removeEventListener("click", handleClick);
      chart.container.style.cursor = "";
    };
  }, [rrMode, bars, direction]);

  // Derived values needed by effects below — must be declared before any useEffect that uses them
  const barTimeMs = etStringToUtcMs(barTime);
  const ready     = !loading && !error && bars.length > 0 && height != null;

  // ── Keep rrRef in sync with React state ─────────────────────────────────────
  useEffect(() => { rrRef.current = rr; }, [rr]);

  // ── Imperative R/R drawing (re-runs when rr or bars change) ─────────────────
  useEffect(() => {
    applyRR(chartRef.current?.chart, rr);
  }, [rr, bars]);

  // ── Re-clip + redraw on every Highcharts render (zoom / scroll) ────────────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart) return;
    const onRender = () => {
      // Clip the threshold plotLine to start at the signal bar
      if (barTimeMs) {
        const plb = chart.yAxis[0].plotLinesAndBands.find(p => p.id === "rr-threshold");
        if (plb?.svgElem?.element) {
          const svg = chart.container.querySelector("svg");
          let defs = svg.querySelector("defs");
          if (!defs) {
            defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
            svg.insertBefore(defs, svg.firstChild);
          }
          const xPx  = chart.xAxis[0].toPixels(barTimeMs, false);
          const clipX = Math.max(xPx, chart.plotLeft);
          const clipW = chart.plotLeft + chart.plotWidth - clipX;
          defs.querySelector("#rr-threshold-clip")?.remove();
          if (clipW > 0) {
            const clipPath = document.createElementNS("http://www.w3.org/2000/svg", "clipPath");
            clipPath.id = "rr-threshold-clip";
            const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
            rect.setAttribute("x",      String(clipX));
            rect.setAttribute("y",      String(chart.plotTop));
            rect.setAttribute("width",  String(clipW));
            rect.setAttribute("height", String(chart.plotHeight));
            clipPath.appendChild(rect);
            defs.appendChild(clipPath);
            plb.svgElem.element.setAttribute("clip-path", "url(#rr-threshold-clip)");
          }
        }
      }
      // Redraw entry clip, yellow left stubs, and colored zones
      const cur = rrRef.current;
      if (!cur?.entryTime) return;
      applyClipsAtEntry(chart, cur.entryTime);
      drawGreyLeftLines(chart, cur);
      drawColoredZones(chart, cur);
    };
    Highcharts.addEvent(chart, "render", onRender);
    return () => Highcharts.removeEvent(chart, "render", onRender);
  }, [bars, barTimeMs]); // re-attach if chart recreated or barTime changes

  // ── Drag stop / target lines ─────────────────────────────────────────────────
  // Uses rrRef (not rr) so no re-renders mid-drag; commits to state on mouseup.
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !ready) return;

    const container = chart.container;
    let drag  = null;   // { type: "stop" | "target" }
    let rafId = null;

    const pricePaneBottom = () => chart.plotTop + chart.plotHeight * 0.80;

    const onMouseMove = (e) => {
      const norm   = chart.pointer.normalize(e);
      const chartY = norm.chartY;

      if (drag) {
        // ── Active drag: update imperatively via rAF ──────────────────────────
        if (rafId) cancelAnimationFrame(rafId);
        rafId = requestAnimationFrame(() => {
          const cur = rrRef.current;
          if (!cur) return;
          // Clamp to price pane
          const clampedY = Math.max(chart.plotTop, Math.min(chartY, pricePaneBottom()));
          const price    = parseFloat(chart.yAxis[0].toValue(clampedY).toFixed(2));

          let newRr;
          if (drag.type === "stop") {
            const stop   = price;
            const target = parseFloat((cur.entry + (cur.entry - stop) * cur.rrRatio).toFixed(2));
            newRr = { ...cur, stop, target };
          } else {
            const target = price;
            const stop   = parseFloat((cur.entry - (target - cur.entry) / cur.rrRatio).toFixed(2));
            newRr = { ...cur, target, stop };
          }
          rrRef.current = newRr;
          applyRR(chart, newRr);
        });
        return;
      }

      // ── Hover: proximity check → cursor hint ─────────────────────────────
      if (rrMode || !rrRef.current) return;
      const inPane = chartY >= chart.plotTop && chartY <= pricePaneBottom();
      if (!inPane) { container.style.cursor = ""; return; }

      const { stop, target } = rrRef.current;
      const stopPx   = chart.yAxis[0].toPixels(stop,   false);
      const targetPx = chart.yAxis[0].toPixels(target, false);
      container.style.cursor =
        (Math.abs(chartY - stopPx) <= 8 || Math.abs(chartY - targetPx) <= 8)
          ? "ns-resize" : "";
    };

    const onMouseDown = (e) => {
      if (rrMode || !rrRef.current) return;
      const norm   = chart.pointer.normalize(e);
      const chartY = norm.chartY;
      const { stop, target } = rrRef.current;
      const stopPx   = chart.yAxis[0].toPixels(stop,   false);
      const targetPx = chart.yAxis[0].toPixels(target, false);

      if (Math.abs(chartY - stopPx) <= 8) {
        drag = { type: "stop" };
        e.stopPropagation();
      } else if (Math.abs(chartY - targetPx) <= 8) {
        drag = { type: "target" };
        e.stopPropagation();
      }
    };

    const onMouseUp = () => {
      if (!drag) return;
      drag = null;
      if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
      if (rrRef.current) setRr({ ...rrRef.current });
    };

    container.addEventListener("mousemove", onMouseMove);
    container.addEventListener("mousedown", onMouseDown);
    document.addEventListener("mouseup",    onMouseUp);

    return () => {
      container.removeEventListener("mousemove", onMouseMove);
      container.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("mouseup",    onMouseUp);
      if (rafId) cancelAnimationFrame(rafId);
    };
  }, [ready, rrMode]); // rrRef used intentionally — avoids re-attaching on every rr change

  // ── Memoised chart options (does NOT depend on rr — managed imperatively) ───
  const chartOptions = useMemo(
    () => buildOptions(ticker, bars, barTimeMs, threshold),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [ticker, bars, barTimeMs, threshold]
  );

  // ── Derived R/R metrics for the input panel ──────────────────────────────────
  const rrMetrics = rr?.entry && rr?.stop && rr?.target ? (() => {
    const reward  = Math.abs(rr.target - rr.entry);
    const risk    = Math.abs(rr.entry  - rr.stop);
    return {
      ratio:     risk > 0 ? (reward / risk).toFixed(2) : "∞",
      rewardAmt: (reward * rr.qty).toFixed(2),
      riskAmt:   (risk   * rr.qty).toFixed(2),
    };
  })() : null;

  // Flip stop & target around entry when switching long ↔ short
  const handleDirectionChange = useCallback((newDir) => {
    if (newDir === direction) return;
    setDirection(newDir);
    setRr(r => r ? {
      ...r,
      stop:   parseFloat((2 * r.entry - r.stop).toFixed(2)),
      target: parseFloat((2 * r.entry - r.target).toFixed(2)),
    } : null);
  }, [direction]);

  const inputCls = "w-24 bg-slate-900 rounded px-2 py-1 font-mono text-xs text-slate-200 focus:outline-none focus:ring-1 transition";

  return (
    <div className="relative flex flex-col h-full w-full">

      {/* ── Loading / error states ── */}
      {(loading || height == null) && !error && (
        <div className="flex-1 flex items-center justify-center gap-2 text-slate-400">
          <Loader2 className="w-5 h-5 animate-spin" />
          <span className="text-sm">Loading chart…</span>
        </div>
      )}
      {error && (
        <div className="flex-1 flex items-center justify-center gap-2 text-red-400">
          <AlertCircle className="w-4 h-4" />
          <span className="text-sm">{error}</span>
        </div>
      )}

      {ready && (
        <>
          {/* ── R/R toolbar ── */}
          <div className="flex items-center gap-2 px-3 py-1.5 border-b border-slate-800 bg-slate-900/60 shrink-0">
            {/* Long / Short direction — single badge when bias is set, toggle when unset */}
            {bias ? (
              <span className={`px-3 py-1 text-xs font-bold rounded border shrink-0 ${
                bias === "long"
                  ? "bg-emerald-900/60 text-emerald-300 border-emerald-800"
                  : "bg-red-900/60 text-red-300 border-red-800"
              }`}>
                {bias === "long" ? "▲ Long" : "▼ Short"}
              </span>
            ) : (
              <div className="flex items-center rounded overflow-hidden border border-slate-700 shrink-0">
                <button
                  onClick={() => handleDirectionChange("long")}
                  className={`px-3 py-1 text-xs font-bold transition ${
                    direction === "long"
                      ? "bg-emerald-900/60 text-emerald-300"
                      : "bg-slate-800 text-slate-500 hover:text-slate-300"
                  }`}
                >
                  ▲ Long
                </button>
                <button
                  onClick={() => handleDirectionChange("short")}
                  className={`px-3 py-1 text-xs font-bold border-l border-slate-700 transition ${
                    direction === "short"
                      ? "bg-red-900/60 text-red-300"
                      : "bg-slate-800 text-slate-500 hover:text-slate-300"
                  }`}
                >
                  ▼ Short
                </button>
              </div>
            )}

            <div className="w-px h-4 bg-slate-700 shrink-0" />

            <button
              onClick={() => { setRrMode(m => !m); if (rrMode) setRr(null); }}
              title="Click a bar on the chart to set the entry price"
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium border transition ${
                rrMode
                  ? "bg-brand-600/20 border-brand-500 text-brand-300 animate-pulse"
                  : "bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500"
              }`}
            >
              <Target className="w-3.5 h-3.5" />
              {rrMode ? "Click a bar to set entry…" : "R/R Draw"}
            </button>

            {rr && (
              <button
                onClick={() => { setRr(null); setQtyDerived(false); }}
                className="flex items-center gap-1 px-2 py-1 rounded text-xs text-slate-500 hover:text-red-400 border border-transparent hover:border-red-900/50 transition"
              >
                <X className="w-3 h-3" /> Clear
              </button>
            )}

            {/* Zoom presets */}
            <div className="flex items-center rounded overflow-hidden border border-slate-700 shrink-0 ml-2">
              {ZOOM_PRESETS.map((preset, i) => (
                <button
                  key={preset.label}
                  onClick={() => applyZoom(preset)}
                  className={`px-2.5 py-1 text-xs font-semibold transition ${
                    i > 0 ? "border-l border-slate-700" : ""
                  } ${
                    activeZoom === preset.label
                      ? "bg-brand-700/40 text-brand-300"
                      : "bg-slate-800 text-slate-500 hover:text-slate-200"
                  }`}
                >
                  {preset.label}
                </button>
              ))}
            </div>

            {/* Order buttons — only when a drawing is placed */}
            {rr && (
              <div className="ml-auto flex items-center gap-2 shrink-0">
                <button
                  onClick={() => { setOrderType("limit"); setOrderResult(null); }}
                  className="px-3 py-1.5 rounded text-xs font-semibold bg-yellow-500/20 hover:bg-yellow-500/30 text-yellow-300 border border-yellow-500/50 transition shadow-sm animate-pulse"
                >
                  Open Limit Order
                </button>
              </div>
            )}
          </div>

          {/* ── Live quote strip — visible while a drawing is active ── */}
          {rr && (
            <div className="flex items-center gap-x-5 px-4 py-1.5 border-b border-slate-700/60 bg-slate-950/50 shrink-0 text-xs">
              {liveQuote && !liveQuote.fetching || (liveQuote && liveQuote.updatedAt) ? (
                <>
                  {liveQuote.bid != null && (
                    <span className="flex items-center gap-1">
                      <span className="text-slate-500">Bid</span>
                      <span className="font-mono font-semibold text-emerald-400">
                        ${liveQuote.bid.toFixed(2)}
                      </span>
                      {liveQuote.bidSize != null && (
                        <span className="text-slate-600 text-[10px]">×{liveQuote.bidSize}</span>
                      )}
                    </span>
                  )}
                  {liveQuote.ask != null && (
                    <span className="flex items-center gap-1">
                      <span className="text-slate-500">Ask</span>
                      <span className="font-mono font-semibold text-red-400">
                        ${liveQuote.ask.toFixed(2)}
                      </span>
                      {liveQuote.askSize != null && (
                        <span className="text-slate-600 text-[10px]">×{liveQuote.askSize}</span>
                      )}
                    </span>
                  )}
                  {liveQuote.spread != null && (
                    <span className="flex items-center gap-1">
                      <span className="text-slate-500">Spread</span>
                      <span className="font-mono text-yellow-400">
                        ${liveQuote.spread.toFixed(2)}
                      </span>
                    </span>
                  )}
                  {liveQuote.last != null && (
                    <span className="flex items-center gap-1">
                      <span className="text-slate-500">Last</span>
                      <span className="font-mono text-slate-300">
                        ${liveQuote.last.toFixed(2)}
                      </span>
                    </span>
                  )}
                  {liveQuote.bid == null && liveQuote.ask == null && liveQuote.last == null && (
                    <span className="text-slate-600 italic">No quote data</span>
                  )}
                  <span className="ml-auto flex items-center gap-1.5 text-slate-600">
                    {liveQuote.fetching && (
                      <span className="w-1.5 h-1.5 rounded-full bg-brand-400 animate-pulse" />
                    )}
                    {liveQuote.updatedAt && (
                      <span title="Last refreshed">
                        {new Date(liveQuote.updatedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                      </span>
                    )}
                    <span className="text-slate-700">· 30s refresh</span>
                  </span>
                </>
              ) : (
                <span className="flex items-center gap-1.5 text-slate-600">
                  <span className="w-1.5 h-1.5 rounded-full bg-slate-500 animate-pulse" />
                  Fetching quote…
                </span>
              )}
            </div>
          )}

          {/* ── R/R input panel ── */}
          {rr && (
            <div className="flex items-center flex-wrap gap-x-5 gap-y-1.5 px-4 py-2 border-b border-slate-700/60 bg-slate-800/40 shrink-0 text-xs">
              {/* Entry (readonly) */}
              <div className="flex items-center gap-1.5">
                <span className="text-slate-500">Entry</span>
                <span className="font-mono font-bold text-slate-200">${rr.entry.toFixed(2)}</span>
              </div>

              {/* Stop — changing stop re-derives target from R/R ratio */}
              <label className="flex items-center gap-1.5">
                <span className="text-red-400 font-medium">{direction === "long" ? "▼ Stop" : "▲ Stop"}</span>
                <input
                  type="number" step="0.01" value={rr.stop}
                  onChange={e => {
                    const val = e.target.value;
                    setRr(r => {
                      const stop   = parseFloat(val) || r.stop;
                      const target = parseFloat((r.entry + (r.entry - stop) * r.rrRatio).toFixed(2));
                      return { ...r, stop, target };
                    });
                  }}
                  className={`${inputCls} border border-red-900/60 focus:ring-red-500/40`}
                />
              </label>

              {/* R/R ratio — changing it keeps stop fixed and re-derives target */}
              <label className="flex items-center gap-1.5">
                <span className="text-slate-400 font-medium">R/R</span>
                <input
                  type="number" step="1" min="1" value={rr.rrRatio}
                  onChange={e => {
                    const val = e.target.value;
                    setRr(r => {
                      const rrRatio = Math.max(0.1, parseFloat(val) || r.rrRatio);
                      const target  = parseFloat((r.entry + (r.entry - r.stop) * rrRatio).toFixed(2));
                      return { ...r, rrRatio, target };
                    });
                  }}
                  className={`${inputCls} w-16 border border-slate-600 focus:ring-slate-400/40`}
                />
              </label>

              {/* Target — manual override */}
              <label className="flex items-center gap-1.5">
                <span className="text-emerald-400 font-medium">{direction === "long" ? "▲ Target" : "▼ Target"}</span>
                <input
                  type="number" step="0.01" value={rr.target}
                  onChange={e => {
                    const val = e.target.value;
                    setRr(r => ({ ...r, target: parseFloat(val) || r.target }));
                  }}
                  className={`${inputCls} border border-emerald-900/60 focus:ring-emerald-500/40`}
                />
              </label>

              {/* Qty */}
              <label className="flex items-center gap-1.5">
                <span className="text-slate-500 flex items-center gap-1">
                  Qty
                  {qtyDerived && (
                    <span
                      className="text-[9px] px-1 py-0.5 rounded bg-brand-900/60 text-brand-400 border border-brand-700/50 leading-none"
                      title="Auto-derived from your risk management settings"
                    >
                      auto
                    </span>
                  )}
                </span>
                <input
                  type="number" step="1" min="1" value={rr.qty}
                  onChange={e => {
                    setQtyDerived(false);
                    setRr(r => ({ ...r, qty: Math.max(1, parseInt(e.target.value) || r.qty) }));
                  }}
                  className={`${inputCls} w-16 border ${qtyDerived ? "border-brand-700/60 focus:ring-brand-500/40" : "border-slate-700 focus:ring-slate-500/40"}`}
                />
              </label>

              {/* Effective metrics (updates live; reflects manual target overrides) */}
              {rrMetrics && (
                <div className="flex items-center gap-4 pl-4 border-l border-slate-700">
                  <span className="text-slate-500 text-[11px]">
                    effective&nbsp;R/R&nbsp;
                    <span className="text-white font-bold font-mono">{rrMetrics.ratio}</span>
                  </span>
                  <span className="text-slate-500 text-[11px]">
                    Profit&nbsp;
                    <span className="text-emerald-400 font-bold font-mono">+${rrMetrics.rewardAmt}</span>
                  </span>
                  <span className="text-slate-500 text-[11px]">
                    Loss&nbsp;
                    <span className="text-red-400 font-bold font-mono">−${rrMetrics.riskAmt}</span>
                  </span>
                  <span className="text-slate-500 text-[11px]">
                    Trade Cost&nbsp;
                    <span className="text-slate-200 font-bold font-mono">${(rr.entry * rr.qty).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                  </span>
                </div>
              )}
            </div>
          )}

          {/* ── Order confirmation modal ── */}
          {orderType && rr && (() => {
            const isMarket  = orderType === "market";
            const isLong    = direction === "long";
            const dirColor  = isLong ? "text-emerald-400" : "text-red-400";
            const dirLabel  = isLong ? "▲ Long" : "▼ Short";

            // Smart entry: use live Alpaca quote for a slightly-better fill price.
            // Long  → ask + $0.01   (join the ask side, improves fill odds)
            // Short → bid − $0.01   (join the bid side, improves fill odds)
            const liveAsk  = liveQuote?.ask  > 0 ? liveQuote.ask  : null;
            const liveBid  = liveQuote?.bid  > 0 ? liveQuote.bid  : null;
            const liveLast = liveQuote?.last > 0 ? liveQuote.last : null;

            const smartEntry = isMarket ? null : (() => {
              if (isLong) {
                const base = liveAsk ?? liveLast ?? rr.entry;
                return parseFloat((base + 0.01).toFixed(2));
              }
              const base = liveBid ?? liveLast ?? rr.entry;
              return parseFloat((base - 0.01).toFixed(2));
            })();
            const entrySource = isMarket ? null : (() => {
              if (isLong) {
                if (liveAsk  != null) return "ask + $0.01";
                if (liveLast != null) return "last trade";
                return "chart level";
              }
              if (liveBid  != null) return "bid − $0.01";
              if (liveLast != null) return "last trade";
              return "chart level";
            })();

            // Effective entry price for this submission
            const entryPrice = smartEntry ?? rr.entry;

            // TP and SL are exactly as drawn — the smartEntry only shifts the
            // limit entry price, never the exit levels the user set.
            const risk      = Math.abs(entryPrice - rr.stop);
            const reward    = Math.abs(rr.target  - entryPrice);
            const rrRatio   = risk > 0 ? (reward / risk).toFixed(2) : "∞";
            const riskAmt   = (risk   * rr.qty).toFixed(2);
            const rewardAmt = (reward * rr.qty).toFixed(2);

            // Stop is the only level that can still be invalid (can't be auto-fixed)
            const stopAboveEntry = isLong  && rr.stop >= entryPrice;
            const stopBelowEntry = !isLong && rr.stop <= entryPrice;
            const hasLevelError  = stopAboveEntry || stopBelowEntry;

            const Row = ({ label, value, valueClass = "text-slate-200" }) => (
              <div className="flex justify-between items-center py-1.5 border-b border-slate-700/50 last:border-0">
                <span className="text-slate-500 text-xs">{label}</span>
                <span className={`font-mono text-xs font-semibold ${valueClass}`}>{value}</span>
              </div>
            );

            return (
              <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/75 backdrop-blur-[2px]">
                <div className="bg-slate-800 border border-slate-600 rounded-xl shadow-2xl w-72 overflow-hidden">
                  {/* Header */}
                  <div className={`flex items-center justify-between px-4 py-3 border-b border-slate-700 ${
                    isMarket ? "bg-blue-900/40" : "bg-yellow-900/20"
                  }`}>
                    <div>
                      <p className="text-white font-bold text-sm">
                        {isMarket ? "Market Order" : "Limit Order"}
                      </p>
                      <p className="text-slate-400 text-[11px] mt-0.5">
                        {ticker} &nbsp;·&nbsp;
                        <span className={dirColor}>{dirLabel}</span>
                      </p>
                    </div>
                    <button
                      onClick={() => setOrderType(null)}
                      className="text-slate-500 hover:text-slate-300 transition"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>

                  {/* Order details */}
                  <div className="px-4 py-2">
                    <Row label="Quantity" value={`${rr.qty} shares`} />
                    {isMarket ? (
                      <Row label="Entry" value="at market" valueClass="text-blue-300" />
                    ) : (
                      <div className="flex justify-between items-center py-1.5 border-b border-slate-700/50">
                        <span className="text-slate-500 text-xs">Entry</span>
                        <span className="flex flex-col items-end gap-0.5">
                          <span className="font-mono text-xs font-semibold text-slate-200">
                            ${smartEntry.toFixed(2)}
                          </span>
                          <span className="text-[10px] text-slate-600">{entrySource}</span>
                        </span>
                      </div>
                    )}
                    <Row label="Stop loss"
                         value={`$${rr.stop.toFixed(2)}`}
                         valueClass="text-red-400" />
                    <div className="flex justify-between items-center py-1.5 border-b border-slate-700/50">
                      <span className="text-slate-500 text-xs">Take profit</span>
                      <span className="flex flex-col items-end gap-0.5">
                        <span className="font-mono text-xs font-semibold text-emerald-400">
                          ${rr.target.toFixed(2)}
                        </span>
                      </span>
                    </div>
                  </div>

                  {/* R/R summary */}
                  <div className="mx-4 mb-3 rounded-lg bg-slate-900/60 border border-slate-700/50 px-3 py-2 flex items-center justify-between text-xs">
                    <span className="text-slate-500">R/R ratio</span>
                    <span className="text-white font-bold font-mono">{rrRatio}</span>
                    <span className="text-red-400 font-mono">−${riskAmt}</span>
                    <span className="text-emerald-400 font-mono">+${rewardAmt}</span>
                  </div>

                  {/* Limit entries are always good-till-cancelled */}
                  {!isMarket && (
                    <div className="mx-4 mb-3">
                      <p className="text-slate-500 text-[10px] mb-1.5 uppercase tracking-wide">
                        Entry valid for
                      </p>
                      <div className="rounded-lg border border-slate-700 bg-slate-600 px-3 py-1.5 text-center text-xs font-semibold text-white">
                        Good till cancelled
                      </div>
                      <p className="text-slate-600 text-[10px] mt-1">
                        Entry stays open across sessions. Cancel manually if the trade is no longer valid.
                      </p>
                    </div>
                  )}

                  {/* Level validation warning */}
                  {hasLevelError && !orderResult && (
                    <div className="mx-4 mb-2 rounded-lg px-3 py-2 text-xs font-medium bg-red-900/40 border border-red-500/40 text-red-300">
                      <p className="font-bold mb-0.5">Invalid stop for {isLong ? "Long" : "Short"}</p>
                      <p>· Stop loss must be {isLong ? "below" : "above"} the entry price.</p>
                      <p className="mt-1 text-red-400/70">Drag the stop line on the chart to fix.</p>
                    </div>
                  )}

                  {/* Order result banner */}
                  {orderResult && (
                    <div className={`mx-4 mb-2 rounded-lg px-3 py-2 text-xs font-medium ${
                      orderResult.ok
                        ? "bg-emerald-900/40 border border-emerald-500/40 text-emerald-300"
                        : "bg-red-900/40 border border-red-500/40 text-red-300"
                    }`}>
                      {orderResult.ok ? (
                        <>
                          <span className="font-bold">Order placed!</span>
                          {orderResult.orderId && (
                            <span className="block text-[11px] text-emerald-400/70 font-mono mt-0.5 truncate">
                              ID: {orderResult.orderId}
                            </span>
                          )}
                        </>
                      ) : (
                        <span>{orderResult.message}</span>
                      )}
                    </div>
                  )}

                  {/* Actions */}
                  <div className="flex gap-2 px-4 pb-4">
                    <button
                      disabled={orderSubmitting}
                      onClick={() => {
                        if (orderResult?.ok) {
                          setOrderType(null);
                          setOrderResult(null);
                          onClose?.();
                        } else {
                          setOrderType(null);
                          setOrderResult(null);
                        }
                      }}
                      className="flex-1 py-2 rounded-lg text-xs font-medium bg-slate-700 hover:bg-slate-600 text-slate-300 border border-slate-600 transition disabled:opacity-40"
                    >
                      {orderResult?.ok ? "Close" : "Cancel"}
                    </button>
                    {!orderResult?.ok && (
                      <button
                        disabled={orderSubmitting || hasLevelError}
                        onClick={async () => {
                          setOrderResult(null);

                          // Stop is the only remaining guard (target is always derived correctly)
                          if (isLong && rr.stop >= entryPrice) {
                            setOrderResult({ ok: false, message: "Stop loss must be below the entry price for a Long trade. Drag the stop line to fix." });
                            return;
                          }
                          if (!isLong && rr.stop <= entryPrice) {
                            setOrderResult({ ok: false, message: "Stop loss must be above the entry price for a Short trade. Drag the stop line to fix." });
                            return;
                          }

                          setOrderSubmitting(true);
                          const risk   = Math.abs(entryPrice - rr.stop);
                          const reward = Math.abs(rr.target  - entryPrice);
                          try {
                            const res = await alpacaApi.placeOrder({
                              ticker,
                              direction,
                              order_type:   orderType,
                              entry_tif:    isMarket ? "day" : "gtc",
                              qty:                rr.qty,
                              entry_price:        entryPrice,
                              stop_price:         rr.stop,
                              target_price:       rr.target,
                              rr_ratio:           rr.rrRatio ?? null,
                              rr_ratio_effective: risk > 0 ? parseFloat((reward / risk).toFixed(4)) : null,
                              risk_amt:           parseFloat((risk   * rr.qty).toFixed(4)),
                              reward_amt:         parseFloat((reward * rr.qty).toFixed(4)),
                              // chart reconstruction metadata
                              bias,
                              bar_time:   barTime  ?? null,
                              threshold:  threshold != null ? parseFloat(threshold) : null,
                              entry_time: rr.entryTime ?? null,
                            });
                            window.dispatchEvent(new CustomEvent("tf:trade-opened"));
                            setOrderResult({
                              ok:      true,
                              message: res.data.message,
                              orderId: res.data.order?.alpaca_order_id,
                            });
                          } catch (err) {
                            const msg =
                              err.response?.data?.error ||
                              err.response?.data?.message ||
                              err.message ||
                              "Failed to place order.";
                            setOrderResult({ ok: false, message: msg });
                          } finally {
                            setOrderSubmitting(false);
                          }
                        }}
                        className={`flex-1 py-2 rounded-lg text-xs font-bold text-white border transition disabled:opacity-50 disabled:cursor-not-allowed ${
                          isMarket
                            ? "bg-blue-600 hover:bg-blue-500 border-blue-500"
                            : "bg-yellow-500/30 hover:bg-yellow-500/50 text-yellow-200 border-yellow-500/60"
                        }`}
                      >
                        {orderSubmitting ? "Placing…" : `Confirm ${isMarket ? "Market" : "Limit"}`}
                      </button>
                    )}
                  </div>
                </div>
              </div>
            );
          })()}

          {/* ── Chart ── */}
          <HighchartsReact
            ref={chartRef}
            highcharts={Highcharts}
            constructorType="stockChart"
            options={chartOptions}
            containerProps={{ style: { height: `${height}px`, width: "100%" } }}
          />
        </>
      )}
    </div>
  );
}
