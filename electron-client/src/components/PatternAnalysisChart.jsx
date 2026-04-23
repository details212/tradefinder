/**
 * PatternAnalysisChart
 *
 * Full-featured daily chart for Pattern Analysis:
 *   • Price pane with SMA 50/150/200, EMA 10/20, 52W Donchian, Pivot Highs
 *   • Volume pane with heatmap colouring and Vol MA
 *   • ATR(10) pane with squeeze dots
 *   • RS vs SPY pane with SMA 50 fill and new-high markers
 *   • RMV 15 pane
 *   • RSI 14 pane with 50-cross markers and vertical plot lines
 *   • Toggle toolbar (overlay series + pane show/hide)
 *   • Sidebar: hover OHLCV panel, MA alignment arrows, Volatility, AS 1M
 *   • Replay mode (step through history bar-by-bar)
 *   • R/R drawing with drag handles + Alpaca order execution
 */
import React, {
  useCallback, useEffect, useMemo, useRef, useState,
} from "react";
import Highcharts from "highcharts/highstock";
import "highcharts/highcharts-more";
import HighchartsReact from "highcharts-react-official";
import { chartApi, alpacaApi, preferencesApi, boxApi, stockApi } from "../api/client";
import {
  Loader2, AlertCircle, Target, X, RefreshCw,
  ChevronLeft, ChevronRight, Square,
} from "lucide-react";

Highcharts.setOptions({ lang: { rangeSelectorZoom: "" } });

// ── Colours ───────────────────────────────────────────────────────────────────
const C = {
  bg:     "#0f172a", surface: "#1e293b", border: "#334155",
  text:   "#e2e8f0", muted:   "#94a3b8",
  green:  "#22c55e", red:    "#ef4444",
  blue:   "#3b82f6", yellow: "#facc15", purple: "#a78bfa",
  orange: "#f97316",
};

// ── Pane layout constants (% of chart height) ─────────────────────────────────
// Price pane is separate; volume + ATR + RS + RMV + RSI share one equal height each.
const GAP = 1.5, PRICE_H = 40;

function calcPaneLayout(show) {
  const k = 1 + (show.atr ? 1 : 0) + (show.rs ? 1 : 0) + (show.rmv ? 1 : 0) + (show.rsi ? 1 : 0);
  // k = stacked panes below price (always includes volume)
  const paneH = (100 - PRICE_H - k * GAP) / k;

  let t = PRICE_H + GAP;
  const volTop = t;
  t += paneH + GAP;
  const atrTop = t;
  if (show.atr) t += paneH + GAP;
  const rsTop = t;
  if (show.rs) t += paneH + GAP;
  const rmvTop = t;
  if (show.rmv) t += paneH + GAP;
  const rsiTop = t;
  if (show.rsi) t += paneH + GAP;

  return { volTop, atrTop, rsTop, rmvTop, rsiTop, bottom: 100, paneH };
}

/** yAxis % heights — every visible lower pane uses the same paneH (fills plot, no dead band). */
function subPaneHeightsPercent(show) {
  const layout = calcPaneLayout(show);
  const H = layout.paneH;
  return {
    layout,
    hVol: H,
    hAtr: show.atr ? H : 0,
    hRs: show.rs ? H : 0,
    hRmv: show.rmv ? H : 0,
    hRsi: show.rsi ? H : 0,
  };
}


/**
 * Shared tooltip often omits the candlestick when the pointer is over the volume column
 * or between bars — this.points then has no candlestick and the formatter returned ""
 * (tooltip blinked). Resolve OHLC from the candlestick series + x instead.
 */
function resolveCandlePoint(chart, x, points) {
  const fromShared = points?.find((p) => p.series?.type === "candlestick");
  if (fromShared?.point != null && fromShared.point.open != null) {
    return fromShared.point;
  }
  if (x == null || !chart?.series) return null;
  const s = chart.series.find((se) => se.type === "candlestick");
  if (!s) return null;
  const raw = s.options.data || [];
  let exact = null;
  let best = null;
  for (let i = 0; i < raw.length; i++) {
    const row = raw[i];
    if (!row || row[1] == null) continue;
    if (row[0] === x) {
      exact = row;
      break;
    }
    const d = Math.abs(row[0] - x);
    if (best == null || d < best.d) best = { row, d };
  }
  const row = exact || best?.row;
  if (!row) return null;
  return {
    x: row[0],
    open: row[1],
    high: row[2],
    low: row[3],
    close: row[4],
  };
}

// ── RS fill builder ───────────────────────────────────────────────────────────
function buildRsFill(rsLine, rsSma50) {
  const smaMap = {};
  (rsSma50 || []).forEach(([ts, v]) => { smaMap[ts] = v; });
  const above = [], below = [];
  (rsLine || []).forEach(([ts, rv]) => {
    const sv = smaMap[ts];
    if (sv == null) return;
    if (rv >= sv) { above.push([ts, sv, rv]); below.push([ts, null, null]); }
    else          { above.push([ts, null, null]); below.push([ts, rv, sv]); }
  });
  return { above, below };
}

// ── Build Highcharts series from studies ─────────────────────────────────────
function buildOptions(ticker, ohlcv, volume, studies, showPanes, rsiCrossUpTs) {
  const st = studies || {};
  const rsF = buildRsFill(st.rsLine, st.rsSma50);

  // Pivot line segments: each level extends from its timestamp to the last bar
  const lastTs   = ohlcv.length ? ohlcv[ohlcv.length - 1][0] : 0;
  const pivotPts = [];
  (st.pivotHighs || []).forEach(({ ts, high }) => {
    pivotPts.push([ts, high], [lastTs, high], [lastTs, null]);
  });

  // RS new-high scatter points
  const rsMap = {};
  (st.rsLine || []).forEach(([ts, v]) => { rsMap[ts] = v; });
  const rsNewHighData = (st.rsNewHighTs || [])
    .map(ts => rsMap[ts] != null ? [ts, rsMap[ts]] : null).filter(Boolean);

  // RSI scatter points (at value 50 for cross-up, and cross-down ts)
  const rsi50Up  = (st.rsiCrossUpTs   || []).map(ts => [ts, 50]);
  const rsi50Dn  = (st.rsiCrossDownTs || []).map(ts => [ts, 50]);

  const { layout, hVol, hAtr, hRs, hRmv, hRsi } = subPaneHeightsPercent(showPanes);

  return {
    chart: {
      backgroundColor: C.bg,
      style: { fontFamily: "ui-sans-serif, system-ui, sans-serif" },
      animation: false,
      cursor: "crosshair",
      margin: [8, 8, 8, 8],
      events: {
        redraw() {
          if (_ignoreNextBoxRedraw) { _ignoreNextBoxRedraw = false; return; }
          // Zoom / pan triggered redraw — reposition boxes using latest data
          renderBoxes(this, _boxesRef.current, _onDeleteBox.current, _onWLBox.current);
        },
      },
    },

    rangeSelector: {
      selected: 4,
      inputStyle:  { color: C.text,  background: C.surface, border: `1px solid ${C.border}` },
      labelStyle:  { color: C.muted },
      buttonTheme: {
        fill: C.surface, stroke: C.border,
        style: { color: C.muted, fontSize: "11px" },
        states: {
          hover:  { fill: C.surface, stroke: C.blue,   style: { color: C.blue } },
          select: { fill: C.blue,    stroke: C.blue,   style: { color: "#0f172a", fontWeight: "700" } },
        },
      },
      buttons: [
        { type: "month", count: 1,  text: "1M" },
        { type: "month", count: 3,  text: "3M" },
        { type: "month", count: 6,  text: "6M" },
        { type: "ytd",               text: "YTD" },
        { type: "year",  count: 1,  text: "1Y" },
        { type: "all",               text: "All" },
      ],
    },

    navigator: {
      enabled: true, height: 40,
      outlineColor: C.border, outlineWidth: 1,
      maskFill: "rgba(250,204,21,0.15)",
      handles: { backgroundColor: "#475569", borderColor: C.muted },
      series:  { color: C.blue, lineWidth: 1 },
      xAxis: { labels: { style: { color: C.muted, fontSize: "10px" }, formatter() { return Highcharts.dateFormat("%b '%y", this.value); } } },
    },

    scrollbar: { enabled: false },
    title:     { text: null },
    credits:   { enabled: false },
    legend:    { enabled: false },

    xAxis: {
      lineColor: C.border, tickColor: C.border,
      labels: { style: { color: C.muted, fontSize: "11px" } },
      crosshair: { color: C.border },
      plotLines: (rsiCrossUpTs || []).map(ts => ({
        value: ts, color: "rgba(167,139,250,0.18)", width: 1, dashStyle: "Solid", zIndex: 2,
      })),
    },

    yAxis: [
      // [0] Price
      {
        height: PRICE_H + "%", offset: 0,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "11px" } },
        crosshair: { dashStyle: "Dash", color: C.border },
        plotLines: [],
        resize: { enabled: true, lineColor: C.border },
      },
      // [1] Volume
      {
        top: layout.volTop + "%", height: hVol + "%", offset: 0,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "11px" },
          formatter() { const v = this.value; return v >= 1e6 ? `${(v/1e6).toFixed(1)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : String(v); } },
      },
      // [2] ATR
      {
        top: layout.atrTop + "%", height: hAtr + "%", offset: 0,
        visible: showPanes.atr,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "10px" }, format: "{value:.2f}" },
      },
      // [3] RS
      {
        top: layout.rsTop + "%", height: hRs + "%", offset: 0,
        visible: showPanes.rs,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "10px" }, format: "{value:.3f}" },
      },
      // [4] RMV
      {
        top: layout.rmvTop + "%", height: hRmv + "%", offset: 0,
        visible: showPanes.rmv,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "10px" }, format: "{value:.0f}" },
        plotLines: [
          { value: 100, width: 1, dashStyle: "Dash", color: "rgba(148,163,184,0.5)", zIndex: 3 },
          { value: 20,  width: 1, dashStyle: "Dash", color: "rgba(239,68,68,0.6)",   zIndex: 3 },
        ],
      },
      // [5] RSI
      {
        top: layout.rsiTop + "%", height: hRsi + "%", offset: 0,
        visible: showPanes.rsi,
        min: 0, max: 100,
        lineColor: C.border, gridLineColor: C.border,
        labels: { align: "right", x: -4, style: { color: C.muted, fontSize: "10px" } },
        plotLines: [{ value: 50, width: 1, dashStyle: "Dash", color: "rgba(148,163,184,0.5)", zIndex: 3 }],
        plotBands: [
          { from: 70, to: 100, color: "rgba(239,68,68,0.07)" },
          { from: 0,  to: 30,  color: "rgba(34,197,94,0.07)"  },
        ],
      },
    ],

    tooltip: {
      split: false, shared: true, useHTML: true,
      animation: false,
      backgroundColor: C.bg, borderColor: C.border, borderRadius: 8,
      style: { color: C.text, fontSize: "11px" },
      positioner() { return { x: this.chart.plotLeft + 8, y: this.chart.plotTop + 8 }; },
      formatter() {
        const x = this.x;
        const pt = resolveCandlePoint(this.chart, x, this.points);
        const vol = this.points?.find((p) => p.series.name === "Volume");
        if (!pt || pt.open == null) return "";
        const dt = Highcharts.dateFormat("%a %b %e, %Y", x);
        const chg = pt.close - pt.open;
        const pct = ((chg / pt.open) * 100).toFixed(2);
        const col = chg >= 0 ? C.green : C.red;
        const fv = (v) => (v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}K` : String(v));
        return `<span style="color:${C.muted};font-size:10px">${dt}</span><br/>
          O <b>${pt.open.toFixed(2)}</b> &nbsp;
          H <b>${pt.high.toFixed(2)}</b> &nbsp;
          L <b>${pt.low.toFixed(2)}</b> &nbsp;
          C <b style="color:${col}">${pt.close.toFixed(2)}</b>
          <span style="color:${col}">&nbsp;(${chg >= 0 ? "+" : ""}${pct}%)</span>
          ${vol ? `<br/>Vol <b>${fv(vol.y)}</b>` : ""}`;
      },
    },

    plotOptions: {
      candlestick: {
        color: C.red, lineColor: C.red, upColor: C.green, upLineColor: C.green,
        dataGrouping: { enabled: false },
        stickyTracking: true,
      },
      column: {
        dataGrouping: { enabled: false }, borderWidth: 0, pointPadding: 0.05, groupPadding: 0,
        stickyTracking: true,
      },
      line:   { dataGrouping: { enabled: false }, enableMouseTracking: false },
      scatter:{ dataGrouping: { enabled: false } },
      arearange: { dataGrouping: { enabled: false }, enableMouseTracking: false, lineWidth: 0 },
    },

    series: [
      // ── Price pane ──
      { type: "candlestick", name: ticker, id: "main",  data: ohlcv, yAxis: 0 },
      { type: "line", name: "SMA 50",  data: st.sma50  || [], yAxis: 0, color: C.blue,   lineWidth: 1, marker: { enabled: false } },
      { type: "line", name: "SMA 150", data: st.sma150 || [], yAxis: 0, color: C.yellow, lineWidth: 1, marker: { enabled: false } },
      { type: "line", name: "SMA 200", data: st.sma200 || [], yAxis: 0, color: C.red,    lineWidth: 1.5, marker: { enabled: false } },
      { type: "line", name: "EMA 10",  data: st.ema10  || [], yAxis: 0, color: C.green,  lineWidth: 1, marker: { enabled: false } },
      { type: "line", name: "EMA 20",  data: st.ema20  || [], yAxis: 0, color: C.purple, lineWidth: 1, marker: { enabled: false } },
      { type: "line", name: "52 Week High", data: st.dcUpper || [], yAxis: 0, color: "#2dd4bf", lineWidth: 1, dashStyle: "Dash", marker: { enabled: false } },
      { type: "line", name: "52 Week Low",  data: st.dcLower || [], yAxis: 0, color: "#2dd4bf", lineWidth: 1, dashStyle: "Dash", marker: { enabled: false } },
      { type: "line", name: "Pivots", data: pivotPts, yAxis: 0, color: "rgba(148,163,184,0.55)", lineWidth: 1, dashStyle: "Dash", marker: { enabled: false }, visible: false },
      // ── Volume pane ──
      { type: "column",  name: "Volume",   id: "volume", data: volume,      yAxis: 1, linkedTo: "main", animation: false },
      { type: "line",    name: "Vol MA",   data: st.volMa || [],             yAxis: 1, color: "rgba(255,255,255,0.5)", lineWidth: 1, marker: { enabled: false } },
      // ── ATR pane ──
      { type: "line",    name: "ATR (10)", data: st.atr10 || [], yAxis: 2, color: "#38bdf8", lineWidth: 1.5, marker: { enabled: false }, visible: showPanes.atr },
      { type: "scatter", name: "ATR Squeeze", data: st.atrSqueeze || [], yAxis: 2, color: C.green, marker: { symbol: "circle", radius: 3, fillColor: C.green }, visible: showPanes.atr, tooltip: { pointFormat: "" } },
      // ── RS pane ──
      { type: "line",      name: "RS Line",      data: st.rsLine   || [], yAxis: 3, color: "rgba(226,232,240,0.85)", lineWidth: 1.5, marker: { enabled: false }, visible: showPanes.rs },
      { type: "line",      name: "RS SMA 50",    data: st.rsSma50  || [], yAxis: 3, color: C.yellow,                lineWidth: 1,   marker: { enabled: false }, visible: showPanes.rs },
      { type: "arearange", name: "RS Fill Above", data: rsF.above,         yAxis: 3, color: C.blue,   fillOpacity: 0.18, visible: showPanes.rs },
      { type: "arearange", name: "RS Fill Below", data: rsF.below,         yAxis: 3, color: C.muted,  fillOpacity: 0.12, visible: showPanes.rs },
      { type: "scatter",   name: "RS New High",  data: rsNewHighData,      yAxis: 3,
        marker: { symbol: "triangle", radius: 4, fillColor: C.green }, color: C.green, visible: showPanes.rs,
        tooltip: { pointFormat: "" } },
      // ── RMV pane ──
      { type: "line",    name: "RMV 15", data: st.rmv15 || [], yAxis: 4, color: C.orange, lineWidth: 1.5, marker: { enabled: false }, visible: showPanes.rmv },
      // ── RSI pane ──
      { type: "line",    name: "RSI (14)",     data: st.rsi14     || [], yAxis: 5, color: C.purple, lineWidth: 1.5, marker: { enabled: false }, visible: showPanes.rsi },
      { type: "scatter", name: "RSI Cross Up",   data: rsi50Up,          yAxis: 5, color: C.green,  marker: { symbol: "triangle",      radius: 4, fillColor: C.green }, visible: showPanes.rsi, tooltip: { pointFormat: "" } },
      { type: "scatter", name: "RSI Cross Down", data: rsi50Dn,          yAxis: 5, color: C.red,    marker: { symbol: "triangle-down", radius: 4, fillColor: C.red   }, visible: showPanes.rsi, tooltip: { pointFormat: "" } },
    ],
  };
}

// ── Pane name labels ──────────────────────────────────────────────────────────
let _paneLabels = [];
function updatePaneLabels(chart, showPanes) {
  _paneLabels.forEach(el => { try { el.destroy(); } catch (_) {} });
  _paneLabels = [];
  if (!chart) return;
  const r  = chart.renderer;
  const px = chart.plotLeft + 6;
  const st = { color: "rgba(148,163,184,0.65)", fontSize: "10px", textTransform: "uppercase", letterSpacing: "0.8px", pointerEvents: "none" };
  [
    { idx: 1, text: "Volume",    show: true },
    { idx: 2, text: "ATR",       show: showPanes.atr },
    { idx: 3, text: "RS vs SPY", show: showPanes.rs  },
    { idx: 4, text: "RMV 15",    show: showPanes.rmv },
    { idx: 5, text: "RSI 14",    show: showPanes.rsi },
  ].forEach(({ idx, text, show }) => {
    if (!show) return;
    const ax = chart.yAxis[idx];
    if (!ax || ax.height <= 0) return;
    _paneLabels.push(r.text(text, px, ax.top + 15).attr({ zIndex: 6 }).css(st).add());
  });
}

// ── R/R drawing helpers (same as before) ─────────────────────────────────────
let rrGreyElems = [];
function clearGreyElems() {
  rrGreyElems.forEach(el => { try { el.destroy(); } catch (_) {} });
  rrGreyElems = [];
}

function applyClipsAtEntry(chart, entryTime) {
  if (!entryTime) return;
  const svg = chart.container.querySelector("svg");
  if (!svg) return;
  let defs = svg.querySelector("defs");
  if (!defs) { defs = document.createElementNS("http://www.w3.org/2000/svg", "defs"); svg.insertBefore(defs, svg.firstChild); }
  const xPx   = chart.xAxis[0].toPixels(entryTime, false);
  const clipX = Math.max(xPx, chart.plotLeft);
  const clipW = chart.plotLeft + chart.plotWidth - clipX;
  ["rr-profit", "rr-loss", "rr-entry"].forEach(id => {
    const plb = chart.yAxis[0].plotLinesAndBands.find(p => p.id === id);
    if (!plb?.svgElem?.element) return;
    const clipId = `${id}-clip`;
    defs.querySelector(`#${clipId}`)?.remove();
    if (clipW <= 0) return;
    const cp = document.createElementNS("http://www.w3.org/2000/svg", "clipPath");
    cp.id = clipId;
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x",      String(clipX));
    rect.setAttribute("y",      String(chart.plotTop));
    rect.setAttribute("width",  String(clipW));
    rect.setAttribute("height", String(chart.plotHeight));
    cp.appendChild(rect); defs.appendChild(cp);
    plb.svgElem.element.setAttribute("clip-path", `url(#${clipId})`);
  });
}

function drawColoredZones(chart, rr) {
  if (!rr?.entry || !rr?.stop || !rr?.target || !rr?.entryTime) return;
  const entryXPx  = chart.xAxis[0].toPixels(rr.entryTime, false);
  const rightEdge = chart.plotLeft + chart.plotWidth;
  const zoneX     = Math.max(entryXPx, chart.plotLeft);
  const zoneW     = rightEdge - zoneX;
  if (zoneW <= 0) return;
  const { entry, stop, target, qty } = rr;
  const risk   = Math.abs(entry - stop);
  const reward = Math.abs(target - entry);
  const rrNum  = Number(rr.rrRatio);
  const toY    = price => chart.yAxis[0].toPixels(price, false);
  const entryY = toY(entry); const targetY = toY(target); const stopY = toY(stop);
  const greenTop = Math.min(entryY, targetY); const greenBot = Math.max(entryY, targetY);
  rrGreyElems.push(chart.renderer.rect(zoneX, greenTop, zoneW, greenBot - greenTop)
    .attr({ fill: "rgba(34,197,94,0.10)", stroke: "rgba(34,197,94,0.40)", "stroke-width": 1, zIndex: 2 }).add());
  const tgtPct = ((reward / entry) * 100).toFixed(2);
  rrGreyElems.push(chart.renderer.text(`▲  $${target.toFixed(2)}  (+${tgtPct}%)  ×${qty}  =  $${(reward * qty).toFixed(2)}`,
    zoneX + zoneW / 2, (greenTop + greenBot) / 2 + 4).attr({ align: "center", zIndex: 5 })
    .css({ color: C.green, fontSize: "11px", fontWeight: "bold", backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" }).add());
  const redTop = Math.min(entryY, stopY); const redBot = Math.max(entryY, stopY);
  rrGreyElems.push(chart.renderer.rect(zoneX, redTop, zoneW, redBot - redTop)
    .attr({ fill: "rgba(239,68,68,0.10)", stroke: "rgba(239,68,68,0.40)", "stroke-width": 1, zIndex: 2 }).add());
  const stpPct = ((risk / entry) * 100).toFixed(2);
  rrGreyElems.push(chart.renderer.text(`▼  $${stop.toFixed(2)}  (−${stpPct}%)  ×${qty}  =  $${(risk * qty).toFixed(2)}`,
    zoneX + zoneW / 2, (redTop + redBot) / 2 + 4).attr({ align: "center", zIndex: 5 })
    .css({ color: C.red, fontSize: "11px", fontWeight: "bold", backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" }).add());
  rrGreyElems.push(chart.renderer.path().attr({ d: `M ${zoneX} ${targetY} L ${rightEdge} ${targetY}`, stroke: C.green, "stroke-width": 2, zIndex: 5 }).add());
  rrGreyElems.push(chart.renderer.path().attr({ d: `M ${zoneX} ${stopY} L ${rightEdge} ${stopY}`,   stroke: C.red,   "stroke-width": 2, zIndex: 5 }).add());
  [1,2,3,4,5,6,7,8,9,10].forEach(r => {
    if (r >= rrNum) return;
    const rPrice = entry + r * risk;
    const rY = toY(rPrice);
    rrGreyElems.push(chart.renderer.path().attr({ d: `M ${zoneX} ${rY} L ${rightEdge} ${rY}`, stroke: "rgba(34,197,94,0.45)", "stroke-width": 1, "stroke-dasharray": "4,3", zIndex: 4 }).add());
    rrGreyElems.push(chart.renderer.text(`${r}R`, zoneX + 8, rY - 2).attr({ zIndex: 5 })
      .css({ color: C.green, fontSize: "10px", fontWeight: "700", backgroundColor: "rgba(15,23,42,0.75)", padding: "1px 5px", borderRadius: "2px" }).add());
  });
}

function applyRR(chart, rr) {
  const yAxis = chart?.yAxis?.[0];
  if (!yAxis) return;
  ["rr-profit","rr-loss"].forEach(id => yAxis.removePlotBand(id));
  yAxis.removePlotLine("rr-entry");
  chart.container.querySelector("svg defs")?.querySelectorAll("[id$='-clip']")?.forEach(n => n.remove());
  clearGreyElems();
  if (!rr?.entry || !rr?.stop || !rr?.target) return;
  const risk   = Math.abs(rr.entry - rr.stop);
  const reward = Math.abs(rr.target - rr.entry);
  const rrRatio = risk > 0 ? (reward / risk).toFixed(2) : "∞";
  yAxis.addPlotLine({
    id: "rr-entry", value: rr.entry, color: C.muted, width: 1, dashStyle: "Dash", zIndex: 5,
    label: { text: `Entry $${rr.entry.toFixed(2)}  ·  R/R ${rrRatio}`, align: "right", x: -6, style: { color: C.muted, fontSize: "10px", fontWeight: "600" } },
  });
  if (rr.entryTime) { applyClipsAtEntry(chart, rr.entryTime); drawColoredZones(chart, rr); }
}

// ── Box drawing helpers ───────────────────────────────────────────────────────
let _boxElems = [];
let _ignoreNextBoxRedraw = false;
// Stable refs updated by the component so chart events can reach them
const _boxesRef   = { current: [] };
const _onDeleteBox = { current: () => {} };
const _onWLBox     = { current: () => {} };

function clearBoxElems() {
  _boxElems.forEach(el => { try { el.destroy(); } catch (_) {} });
  _boxElems = [];
}

function renderBoxes(chart, boxes, onDelete, onWatchlist) {
  clearBoxElems();
  if (!chart || !boxes?.length) return;

  boxes.forEach(box => {
    const x1px = chart.xAxis[0].toPixels(box.x1);
    const x2px = chart.xAxis[0].toPixels(box.x2);
    const y1px = chart.yAxis[0].toPixels(box.y1); // top  = higher price = smaller pixel
    const y2px = chart.yAxis[0].toPixels(box.y2); // btm  = lower  price = larger  pixel

    const left   = Math.min(x1px, x2px);
    const top    = Math.min(y1px, y2px);
    const width  = Math.abs(x2px - x1px);
    const height = Math.abs(y2px - y1px);

    if (width < 4 || height < 4) return;

    // Filled rect
    const rect = chart.renderer.rect(left, top, width, height, 2)
      .attr({ fill: "rgba(255,215,0,0.07)", stroke: box.color || "#ffd700", "stroke-width": 1.5, zIndex: 4 })
      .add();
    _boxElems.push(rect);

    // "✕" delete — top-right
    const xBtn = chart.renderer.text("✕", left + width - 3, top + 13)
      .attr({ zIndex: 6, cursor: "pointer", align: "right" })
      .css({ color: "#ef4444", fontSize: "13px", fontWeight: "bold" })
      .add();
    _boxElems.push(xBtn);
    xBtn.element.addEventListener("click", e => { e.stopPropagation(); onDelete(box.id); });

    // "+ WL" — top-left, watchlist shortcut
    const wlBtn = chart.renderer.text("+ WL", left + 4, top + 13)
      .attr({ zIndex: 6, cursor: "pointer" })
      .css({ color: "#facc15", fontSize: "10px", fontWeight: "bold" })
      .add();
    _boxElems.push(wlBtn);
    wlBtn.element.addEventListener("click", e => { e.stopPropagation(); onWatchlist(box); });

    // Price label bottom-left
    const priceLabel = chart.renderer
      .text(`$${box.y2.toFixed(2)} – $${box.y1.toFixed(2)}`, left + 4, top + height - 4)
      .attr({ zIndex: 6 })
      .css({ color: "rgba(250,204,21,0.65)", fontSize: "9px", fontWeight: "600" })
      .add();
    _boxElems.push(priceLabel);
  });
}

// ── ATR lookup ────────────────────────────────────────────────────────────────
function calcATRAtIdx(bars, idx) {
  const end   = Math.min(idx + 1, bars.length);
  const start = Math.max(0, end - 15);
  const sl    = bars.slice(start, end);
  if (sl.length < 2) return null;
  let s = 0;
  for (let i = 1; i < sl.length; i++) {
    const { h, l } = sl[i]; const pc = sl[i-1].c;
    s += Math.max(h-l, Math.abs(h-pc), Math.abs(l-pc));
  }
  return s / (sl.length - 1);
}

function deriveQty(entry, prefs, portVal) {
  if (!prefs || entry <= 0) return null;
  let dollars = 0;
  if (prefs.risk_mode === "dollar")   dollars = parseFloat(prefs.risk_value) || 0;
  if (prefs.risk_mode === "percent")  dollars = ((parseFloat(prefs.risk_value)||0)/100) * (parseFloat(portVal)||0);
  if (dollars <= 0) return null;
  return Math.max(1, Math.floor(dollars / entry));
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmtP = v => v != null ? `$${parseFloat(v).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}` : "—";
const fmtN = (v, d=2) => v != null ? parseFloat(v).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d}) : "—";
const fmtV = v => { if (v == null) return "—"; const n=parseInt(v); return n>=1e9?`${(n/1e9).toFixed(2)}B`:n>=1e6?`${(n/1e6).toFixed(1)}M`:n>=1e3?`${(n/1e3).toFixed(0)}K`:n.toLocaleString(); };
const fmtC = v => { if (v == null) return "—"; const n=parseInt(v); return n>=1e12?`$${(n/1e12).toFixed(2)}T`:n>=1e9?`$${(n/1e9).toFixed(2)}B`:n>=1e6?`$${(n/1e6).toFixed(1)}M`:`$${n.toLocaleString()}`; };
const fmtPct = v => v != null ? `${(parseFloat(v)*100)>=0?"+":""}${(parseFloat(v)*100).toFixed(1)}%` : "—";
const recText = m => { if (m==null) return null; const v=parseFloat(m); return v<=1.5?"Strong Buy":v<=2.5?"Buy":v<=3.5?"Hold":v<=4.5?"Sell":"Strong Sell"; };

// ── Replay data slice ─────────────────────────────────────────────────────────
function sliceStudies(studies, maxTs) {
  if (!studies) return null;
  const s = {};
  ["sma50","sma150","sma200","ema10","ema20","rsi14","volMa","dcUpper","dcLower","atr10","atrSqueeze","atrDeclining","rsLine","rsEma21","rsSma50","rmv15"].forEach(k => {
    s[k] = (studies[k]||[]).filter(d => d[0] <= maxTs);
  });
  s.rsiCrossUpTs   = (studies.rsiCrossUpTs   ||[]).filter(ts => ts <= maxTs);
  s.rsiCrossDownTs = (studies.rsiCrossDownTs ||[]).filter(ts => ts <= maxTs);
  s.rsNewHighTs    = (studies.rsNewHighTs    ||[]).filter(ts => ts <= maxTs);
  s.volBars        = (studies.volBars        ||[]).filter(b => b.x <= maxTs);
  s.pivotHighs     = (studies.pivotHighs     ||[]).filter(p => p.ts <= maxTs);
  return s;
}

// ─────────────────────────────────────────────────────────────────────────────
export default function PatternAnalysisChart({ ticker, height, onClose }) {
  // ── Data ──────────────────────────────────────────────────────────────────
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState(null);
  const [chartData, setChartData] = useState(null); // { ohlcv, volume, studies, info }

  // ── Panes / toggles ───────────────────────────────────────────────────────
  const [showPanes, setShowPanes] = useState({ atr: true, rs: true, rmv: true, rsi: false });
  const [showOverlay, setShowOverlay] = useState({
    sma50: true, sma150: true, sma200: true, emas: true, "52w": false, pivots: false,
  });

  // ── Replay ────────────────────────────────────────────────────────────────
  const [replayActive, setReplayActive] = useState(false);
  const [replayIdx,    setReplayIdx]    = useState(-1);
  const [replayDate,   setReplayDate]   = useState("");

  // ── Sidebar hover panel ───────────────────────────────────────────────────
  const [hoverInfo, setHoverInfo] = useState(null);

  // ── R/R drawing ───────────────────────────────────────────────────────────
  const [rrMode,          setRrMode]          = useState(false);
  const [rr,              setRr]              = useState(null);
  const [liveQuote,       setLiveQuote]       = useState(null);
  const [orderType,       setOrderType]       = useState(null);
  const [orderSubmitting, setOrderSubmitting] = useState(false);
  const [orderResult,     setOrderResult]     = useState(null);
  const [riskPrefs,       setRiskPrefs]       = useState(null);
  const [portfolioValue,  setPortfolioValue]  = useState(null);
  const [qtyDerived,      setQtyDerived]      = useState(false);

  // ── Box drawing ───────────────────────────────────────────────────────────
  const [boxMode, setBoxMode] = useState(false);
  const [boxes,   setBoxes]   = useState([]);

  const chartRef   = useRef(null);
  const rrRef      = useRef(null);
  const boxesRef   = useRef([]);
  const boxModeRef = useRef(false);

  // ── Fetch data ────────────────────────────────────────────────────────────
  const fetchData = useCallback(() => {
    if (!ticker) return;
    setLoading(true); setError(null);
    chartApi.get(ticker)
      .then(r => {
        const d = r.data;
        if (d?.error) throw new Error(d.error);
        setChartData(d);
        // set replay date picker default
        if (d.ohlcv?.length) {
          const lastTs = d.ohlcv[d.ohlcv.length - 1][0];
          const defTs  = lastTs - 90 * 24 * 60 * 60 * 1000;
          setReplayDate(new Date(defTs).toISOString().slice(0, 10));
        }
      })
      .catch(err => {
        const msg = err?.response?.data?.error || err?.message || "Failed to load chart data";
        console.error("[PatternAnalysisChart] fetch error:", msg, err);
        setError(msg);
      })
      .finally(() => setLoading(false));
  }, [ticker]);

  useEffect(() => { fetchData(); }, [fetchData]);

  // ── Risk prefs + portfolio ─────────────────────────────────────────────────
  useEffect(() => {
    preferencesApi.get().then(r => { const p=r.data.preferences??{}; if (p.risk_mode&&p.risk_value) setRiskPrefs(p); }).catch(()=>{});
    alpacaApi.test().then(r => { if (r.data.ok) setPortfolioValue(r.data.portfolio_value??null); }).catch(()=>{});
  }, []);

  // ── Sync box refs (local + module-level for chart events) ────────────────
  useEffect(() => { boxesRef.current = boxes; _boxesRef.current = boxes; }, [boxes]);
  useEffect(() => { boxModeRef.current = boxMode; }, [boxMode]);

  // ── Load saved boxes when ticker changes ──────────────────────────────────
  useEffect(() => {
    if (!ticker) return;
    setBoxes([]);
    boxApi.list(ticker)
      .then(r => { setBoxes(r.data); boxesRef.current = r.data; })
      .catch(() => {});
  }, [ticker]);

  // ── Delete a box ──────────────────────────────────────────────────────────
  const handleDeleteBox = useCallback((id) => {
    boxApi.remove(id).catch(() => {});
    setBoxes(prev => {
      const upd = prev.filter(b => b.id !== id);
      boxesRef.current = upd;
      return upd;
    });
  }, []);

  // ── Add box to watchlist ──────────────────────────────────────────────────
  const handleBoxToWatchlist = useCallback((box) => {
    const threshold = box.y1;                                   // top = breakout level
    const bar_time  = new Date(box.x2).toISOString().slice(0, 10);
    stockApi.addToWatchlist(ticker, { bias: "long", threshold, bar_time, source: "patternanalysis" })
      .then(() => {
        // Sidebar listens for this (same as axios interceptor); explicit here so WL from chart SVG always refreshes
        window.dispatchEvent(new CustomEvent("tf:watchlist-changed"));
      })
      .catch(err => console.error("[BoxWL] add failed", err));
  }, [ticker]);

  // Keep module-level refs current for the Highcharts redraw event
  _onDeleteBox.current = handleDeleteBox;
  _onWLBox.current     = handleBoxToWatchlist;

  // ── Live quote while drawing ──────────────────────────────────────────────
  const hasDrawing = rr !== null;
  useEffect(() => {
    if (!hasDrawing) { setLiveQuote(null); return; }
    let cancelled = false;
    const go = async () => {
      try {
        const res = await alpacaApi.quote(ticker);
        if (!cancelled) {
          const d = res.data;
          setLiveQuote({ bid: d.bid>0?d.bid:null, ask: d.ask>0?d.ask:null,
            bidSize: d.bid_size||null, askSize: d.ask_size||null,
            spread: d.spread>0?d.spread:null, last: d.last>0?d.last:null,
            updatedAt: Date.now() });
        }
      } catch {}
    };
    go(); const id = setInterval(go, 30_000);
    return () => { cancelled=true; clearInterval(id); };
  }, [hasDrawing, ticker]);

  // ── Determine active OHLCV/studies (replay vs full) ───────────────────────
  const activeOhlcv   = useMemo(() => {
    if (!chartData?.ohlcv) return [];
    if (!replayActive || replayIdx < 0) return chartData.ohlcv;
    return chartData.ohlcv.slice(0, replayIdx + 1);
  }, [chartData, replayActive, replayIdx]);

  const activeVolume  = useMemo(() => {
    if (!chartData?.volume) return [];
    if (!replayActive || replayIdx < 0) return chartData.volume;
    return chartData.volume.slice(0, replayIdx + 1);
  }, [chartData, replayActive, replayIdx]);

  const activeStudies = useMemo(() => {
    if (!chartData?.studies) return null;
    if (!replayActive || replayIdx < 0) return chartData.studies;
    const maxTs = chartData.ohlcv[replayIdx]?.[0] ?? Infinity;
    return sliceStudies(chartData.studies, maxTs);
  }, [chartData, replayActive, replayIdx]);

  const activeStudiesRef = useRef(activeStudies);
  activeStudiesRef.current = activeStudies;

  const setHoverInfoRef = useRef(setHoverInfo);
  setHoverInfoRef.current = setHoverInfo;

  // Keep a stable ref to showPanes for building initial chartOptions without it being a dep.
  const showPanesRef = useRef(showPanes);

  // ── Chart options ─────────────────────────────────────────────────────────
  // showPanes is intentionally NOT a dep — pane layout is applied imperatively via showPanes effect
  // below. This prevents HighchartsReact's full chart.update() from running on pane toggle, which
  // is what creates the dead-space gap in the stacked yAxis layout.
  const chartOptions = useMemo(() => {
    if (!activeOhlcv.length) return null;
    // Pad right for R/R drawing space
    const padded = [...activeOhlcv];
    if (activeOhlcv.length >= 2) {
      const step = (activeOhlcv[activeOhlcv.length-1][0] - activeOhlcv[activeOhlcv.length-2][0]);
      const last = activeOhlcv[activeOhlcv.length-1][0];
      for (let i=1; i<=10; i++) padded.push([last + step*i, null, null, null, null]);
    }
    const volBars = activeStudies?.volBars?.length
      ? activeStudies.volBars
      : activeVolume.map(([x, y]) => ({ x, y, color: "#a0d6dc" }));
    const opts = buildOptions(ticker, padded, volBars, activeStudies, showPanesRef.current,
      activeStudies?.rsiCrossUpTs || []);
    // Tooltip formatter lives in opts so HighchartsReact can't overwrite it on re-render.
    opts.tooltip.formatter = function paTooltipFormatter() {
      const pt = resolveCandlePoint(this.chart, this.x, this.points);
      if (!pt || pt.open == null) return "";
      const ts = pt.x;
      const st = activeStudiesRef.current || {};
      let volY = this.points?.find((p) => p.series.name === "Volume")?.y ?? null;
      if (volY == null) {
        const vs = this.chart.series.find((s) => s.type === "column" && s.name === "Volume");
        volY = vs?.points?.find((p) => p.x === ts)?.y ?? null;
      }
      const lookup = (arr) => {
        if (!arr) return null;
        const found = arr.find((d) => d[0] === ts);
        return found ? found[1] : null;
      };
      const hi = {
        ts,
        date: Highcharts.dateFormat("%a %b %e, %Y", ts),
        o: pt.open, h: pt.high, l: pt.low, c: pt.close,
        vol: volY,
        sma50: lookup(st.sma50), sma150: lookup(st.sma150), sma200: lookup(st.sma200),
        ema10: lookup(st.ema10), ema20: lookup(st.ema20),
        rsi: lookup(st.rsi14), atr: lookup(st.atr10),
        rs: lookup(st.rsLine), rsSma50: lookup(st.rsSma50),
        rmv15: lookup(st.rmv15),
        dcUpper: lookup(st.dcUpper), dcLower: lookup(st.dcLower),
        atrDecl: (() => { const a = (st.atrDeclining || []); const f = a.find((d) => d[0] === ts); return f ? f[1] : null; })(),
        as1m: (() => { const a = (st.as1m || []); if (!a.length) return null; const f = a.find((d) => d[0] === ts); return f ? f[1] : a[a.length - 1][1]; })(),
      };
      setHoverInfoRef.current(hi);
      const chg = pt.close - pt.open;
      const pct = ((chg / pt.open) * 100).toFixed(2);
      const col = chg >= 0 ? C.green : C.red;
      const fv = (v) => (v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}K` : String(v));
      return `<span style="color:${C.muted};font-size:10px">${hi.date}</span><br/>
        O <b>${pt.open.toFixed(2)}</b> &nbsp; H <b>${pt.high.toFixed(2)}</b> &nbsp;
        L <b>${pt.low.toFixed(2)}</b> &nbsp; C <b style="color:${col}">${pt.close.toFixed(2)}</b>
        <span style="color:${col}">&nbsp;(${chg >= 0 ? "+" : ""}${pct}%)</span>
        ${hi.vol != null ? `<br/>Vol <b>${fv(hi.vol)}</b>` : ""}`;
    };
    return opts;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeOhlcv, activeVolume, activeStudies, ticker]);

  // ── Imperative pane layout — runs when showPanes changes, NOT via chart.update(wholeOptions).
  // chart.update(wholeOptions) with new yAxis top/height percentages leaves a dead-space gap;
  // targeted yAxis.update() + redraw is the correct Highcharts API for this.
  const PANE_SERIES_MAP = {
    atr: ["ATR (10)", "ATR Squeeze"],
    rs:  ["RS Line", "RS SMA 50", "RS New High", "RS Fill Above", "RS Fill Below"],
    rmv: ["RMV 15"],
    rsi: ["RSI (14)", "RSI Cross Up", "RSI Cross Down"],
  };
  useEffect(() => {
    showPanesRef.current = showPanes;
    const chart = chartRef.current?.chart;
    if (!chart) return;
    const { layout, hVol, hAtr, hRs, hRmv, hRsi } = subPaneHeightsPercent(showPanes);
    chart.yAxis[0].update({ height: PRICE_H + "%" }, false);
    chart.yAxis[1].update({ top: layout.volTop + "%", height: hVol + "%", visible: true }, false);
    chart.yAxis[2].update({ top: layout.atrTop + "%", height: hAtr + "%", visible: showPanes.atr }, false);
    chart.yAxis[3].update({ top: layout.rsTop  + "%", height: hRs  + "%", visible: showPanes.rs  }, false);
    chart.yAxis[4].update({ top: layout.rmvTop + "%", height: hRmv + "%", visible: showPanes.rmv }, false);
    chart.yAxis[5].update({ top: layout.rsiTop + "%", height: hRsi + "%", visible: showPanes.rsi }, false);
    Object.entries(PANE_SERIES_MAP).forEach(([pane, names]) => {
      chart.series.forEach(s => { if (names.includes(s.name)) s.setVisible(showPanes[pane], false); });
    });
    chart.redraw(false);
    updatePaneLabels(chart, showPanes);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showPanes]);

  // Reflow chart when window resizes (handles modal stretch/shrink).
  useEffect(() => {
    const onResize = () => {
      const chart = chartRef.current?.chart;
      if (chart) chart.reflow();
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // ── After chart renders: apply overlay visibility, R/R, pane labels ───────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !chartOptions) return;
    // Overlay toggles
    const OVERLAY_MAP = {
      sma50: ["SMA 50"], sma150: ["SMA 150"], sma200: ["SMA 200"],
      emas: ["EMA 10", "EMA 20"],
      "52w": ["52 Week High", "52 Week Low"],
      pivots: ["Pivots"],
    };
    Object.entries(OVERLAY_MAP).forEach(([key, names]) => {
      chart.series.forEach(s => {
        if (names.includes(s.name)) s.setVisible(showOverlay[key], false);
      });
    });
    _ignoreNextBoxRedraw = true;
    chart.redraw(false);
    updatePaneLabels(chart, showPanes);
    applyRR(chart, rrRef.current);
    renderBoxes(chart, boxesRef.current, handleDeleteBox, handleBoxToWatchlist);
  });

  // ── R/R drawing logic ─────────────────────────────────────────────────────
  useEffect(() => { rrRef.current = rr; }, [rr]);

  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !rrMode) return;
    const c = chart.container;
    if (!c) return;
    c.style.cursor = "crosshair";
    const bars = activeOhlcv.filter(b => b[1] != null);
    const cap = { capture: true };
    const handleClick = (e) => {
      const norm = chart.pointer.normalize(e);
      if (norm.chartY < chart.plotTop || norm.chartY > chart.plotTop + chart.plotHeight * 0.80) return;
      e.preventDefault();
      e.stopPropagation();
      const xVal = chart.xAxis[0].toValue(norm.chartX);
      const ni   = bars.reduce((bi, b, i) => Math.abs(b[0]-xVal) < Math.abs(bars[bi][0]-xVal) ? i : bi, 0);
      const nb   = bars[ni];
      const entry = parseFloat(nb[4].toFixed(2));
      const fakeBars = bars.map(b => ({ t: b[0], o: b[1], h: b[2], l: b[3], c: b[4] }));
      const atr   = calcATRAtIdx(fakeBars, ni) ?? entry * 0.02;
      const stop  = parseFloat((entry - atr * 1.5).toFixed(2));
      setRr(prev => {
        const rrRatio = prev?.rrRatio ?? 2;
        const target  = parseFloat((entry + (entry - stop) * rrRatio).toFixed(2));
        const autoQty = deriveQty(entry, riskPrefs, portfolioValue);
        const qty = autoQty ?? prev?.qty ?? 10;
        if (autoQty != null) setQtyDerived(true);
        return { entry, stop, target, qty, rrRatio, entryTime: nb[0] };
      });
      setRrMode(false);
    };
    c.addEventListener("click", handleClick, cap);
    return () => {
      if (c?.removeEventListener) c.removeEventListener("click", handleClick, cap);
      try { if (c?.style) c.style.cursor = ""; } catch (_) {}
    };
  }, [rrMode, activeOhlcv, riskPrefs, portfolioValue]);

  // ── Disable pan/zoom while drawing (box or R/R) ───────────────────────────
  // Panning enabled by default so Highcharts intercepts mousedown as a pan
  // start; even a tiny mouse movement swallows the click. Disable whenever
  // the user is in an active drawing mode so clicks register cleanly.
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart) return;
    const drawing = boxMode || rrMode;
    try {
      chart.update({
        chart: {
          panning: { enabled: !drawing },
          zooming: { mouseWheel: { enabled: !drawing } },
        },
      }, false);
    } catch (_) {}
  }, [boxMode, rrMode, chartOptions]);

  // ── Box drawing (mousedown → drag → mouseup) ──────────────────────────────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !boxMode) return;
    const container = chart.container;
    if (!container) return;
    container.style.cursor = "crosshair";

    let startX = null, startY = null;
    let previewElems = [];

    const clearPreview = () => {
      previewElems.forEach(el => { try { el.destroy(); } catch (_) {} });
      previewElems = [];
    };

    const cap = { capture: true };

    const onDown = (e) => {
      const norm = chart.pointer.normalize(e);
      if (norm.chartX < chart.plotLeft || norm.chartX > chart.plotLeft + chart.plotWidth) return;
      if (norm.chartY < chart.plotTop  || norm.chartY > chart.plotTop  + chart.plotHeight) return;
      e.preventDefault();
      e.stopPropagation();
      startX = norm.chartX; startY = norm.chartY;
    };

    const onMove = (e) => {
      if (startX === null) return;
      e.preventDefault();
      e.stopPropagation();
      const norm = chart.pointer.normalize(e);
      clearPreview();
      const l = Math.min(startX, norm.chartX), t = Math.min(startY, norm.chartY);
      const w = Math.abs(norm.chartX - startX),  h = Math.abs(norm.chartY - startY);
      if (w < 3 || h < 3) return;
      previewElems.push(
        chart.renderer.rect(l, t, w, h, 2)
          .attr({ fill: "rgba(255,215,0,0.05)", stroke: "#ffd700", "stroke-width": 1,
                  "stroke-dasharray": "4,3", zIndex: 7 })
          .add()
      );
    };

    const onUp = (e) => {
      if (startX === null) return;
      e.preventDefault();
      e.stopPropagation();
      const norm = chart.pointer.normalize(e);
      clearPreview();
      const x1v = chart.xAxis[0].toValue(startX);
      const x2v = chart.xAxis[0].toValue(norm.chartX);
      const y1v = chart.yAxis[0].toValue(startY);
      const y2v = chart.yAxis[0].toValue(norm.chartY);
      startX = null; startY = null;

      // Need a minimum size
      if (Math.abs(x2v - x1v) < 86400000 || Math.abs(y2v - y1v) < 0.01) return;

      const newBox = {
        ticker,
        x1: Math.min(x1v, x2v),
        x2: Math.max(x1v, x2v),
        y1: Math.max(y1v, y2v), // top = higher price
        y2: Math.min(y1v, y2v), // bottom = lower price
        color: "#ffd700",
      };

      setBoxMode(false);

      // Optimistic add with temp id, replace once API responds
      const tempId = `t_${Date.now()}`;
      setBoxes(prev => { const upd = [...prev, { ...newBox, id: tempId }]; boxesRef.current = upd; return upd; });

      boxApi.create(newBox)
        .then(r => setBoxes(prev => { const upd = prev.map(b => b.id === tempId ? r.data : b); boxesRef.current = upd; return upd; }))
        .catch(() => setBoxes(prev => { const upd = prev.filter(b => b.id !== tempId); boxesRef.current = upd; return upd; }));
    };

    container.addEventListener("mousedown", onDown, cap);
    container.addEventListener("mousemove", onMove, cap);
    document.addEventListener("mouseup", onUp, cap);
    return () => {
      if (container?.removeEventListener) {
        container.removeEventListener("mousedown", onDown, cap);
        container.removeEventListener("mousemove", onMove, cap);
      }
      document.removeEventListener("mouseup", onUp, cap);
      clearPreview();
      try { if (container?.style) container.style.cursor = ""; } catch (_) {}
    };
  }, [boxMode, ticker]);

  // ── Drag stop / target ────────────────────────────────────────────────────
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart) return;
    const container = chart.container;
    if (!container) return;
    let drag = null, rafId = null;
    const paneBot = () => chart.plotTop + chart.plotHeight * 0.80;
    const onMove = (e) => {
      const norm = chart.pointer.normalize(e);
      const cy   = norm.chartY;
      if (drag) {
        if (rafId) cancelAnimationFrame(rafId);
        rafId = requestAnimationFrame(() => {
          const cur = rrRef.current; if (!cur) return;
          const clampedY = Math.max(chart.plotTop, Math.min(cy, paneBot()));
          const price    = parseFloat(chart.yAxis[0].toValue(clampedY).toFixed(2));
          const newRr    = drag.type === "stop"
            ? { ...cur, stop: price, target: parseFloat((cur.entry+(cur.entry-price)*cur.rrRatio).toFixed(2)) }
            : { ...cur, target: price, stop: parseFloat((cur.entry-(price-cur.entry)/cur.rrRatio).toFixed(2)) };
          rrRef.current = newRr; applyRR(chart, newRr);
        });
        return;
      }
      if (rrMode || boxModeRef.current || !rrRef.current) return;
      const inPane = cy >= chart.plotTop && cy <= paneBot();
      if (!inPane) { container.style.cursor = ""; return; }
      const { stop, target } = rrRef.current;
      const sp = chart.yAxis[0].toPixels(stop,  false);
      const tp = chart.yAxis[0].toPixels(target,false);
      container.style.cursor = (Math.abs(cy-sp)<=8 || Math.abs(cy-tp)<=8) ? "ns-resize" : "";
    };
    const onDown = (e) => {
      if (rrMode || boxModeRef.current || !rrRef.current) return;
      const norm = chart.pointer.normalize(e);
      const cy   = norm.chartY;
      const { stop, target } = rrRef.current;
      const sp = chart.yAxis[0].toPixels(stop,  false);
      const tp = chart.yAxis[0].toPixels(target,false);
      if      (Math.abs(cy-sp)<=8) { drag={type:"stop"};   e.stopPropagation(); }
      else if (Math.abs(cy-tp)<=8) { drag={type:"target"}; e.stopPropagation(); }
    };
    const onUp = () => {
      if (!drag) return; drag=null;
      if (rafId) { cancelAnimationFrame(rafId); rafId=null; }
      if (rrRef.current) setRr({ ...rrRef.current });
    };
    container.addEventListener("mousemove", onMove);
    container.addEventListener("mousedown", onDown);
    document.addEventListener("mouseup",    onUp);
    return () => {
      if (container?.removeEventListener) {
        container.removeEventListener("mousemove", onMove);
        container.removeEventListener("mousedown", onDown);
      }
      document.removeEventListener("mouseup",    onUp);
      if (rafId) cancelAnimationFrame(rafId);
    };
  }, [chartOptions, rrMode]);

  const togglePane = useCallback((pane) => {
    // Layout + series visibility come from `chartOptions` (buildOptions + showPanes).
    // Do not call a second manual yAxis/setSize pass — it raced HighchartsReact’s `options`
    // update and could blank the lower panes when toggling RMV (0%-height axis bug).
    setShowPanes((prev) => ({ ...prev, [pane]: !prev[pane] }));
  }, []);

  const toggleOverlay = useCallback((key) => {
    setShowOverlay(prev => {
      const next = { ...prev, [key]: !prev[key] };
      const OVERLAY_MAP = {
        sma50: ["SMA 50"], sma150: ["SMA 150"], sma200: ["SMA 200"],
        emas:  ["EMA 10","EMA 20"],
        "52w": ["52 Week High","52 Week Low"],
        pivots:["Pivots"],
      };
      const chart = chartRef.current?.chart;
      if (chart) {
        const names = OVERLAY_MAP[key] || [];
        chart.series.forEach(s => { if (names.includes(s.name)) s.setVisible(next[key], false); });
        chart.redraw(false);
      }
      return next;
    });
  }, []);

  // ── Replay ────────────────────────────────────────────────────────────────
  const startReplay = useCallback(() => {
    if (!chartData?.ohlcv?.length || !replayDate) return;
    const targetTs = new Date(replayDate + "T00:00:00Z").getTime();
    let idx = -1;
    for (let i = 0; i < chartData.ohlcv.length; i++) {
      if (chartData.ohlcv[i][0] <= targetTs) idx = i; else break;
    }
    if (idx < 0) return;
    setReplayActive(true); setReplayIdx(idx);
  }, [chartData, replayDate]);

  const replayNext = useCallback(() => {
    if (!chartData?.ohlcv) return;
    setReplayIdx(i => Math.min(i + 1, chartData.ohlcv.length - 1));
  }, [chartData]);

  const replayPrev = useCallback(() => { setReplayIdx(i => Math.max(i - 1, 0)); }, []);
  const replayExit = useCallback(() => { setReplayActive(false); setReplayIdx(-1); }, []);

  useEffect(() => {
    if (!replayActive) return;
    const onKey = (e) => {
      if (e.target?.tagName === "INPUT") return;
      if (e.key === "ArrowRight") { e.preventDefault(); replayNext(); }
      if (e.key === "ArrowLeft")  { e.preventDefault(); replayPrev(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [replayActive, replayNext, replayPrev]);

  // ── Derived R/R metrics ────────────────────────────────────────────────────
  const rrMetrics = rr?.entry && rr?.stop && rr?.target ? (() => {
    const reward = Math.abs(rr.target - rr.entry);
    const risk   = Math.abs(rr.entry  - rr.stop);
    return { ratio: risk>0?(reward/risk).toFixed(2):"∞", rewardAmt:(reward*rr.qty).toFixed(2), riskAmt:(risk*rr.qty).toFixed(2) };
  })() : null;

  const info = chartData?.info;
  const ready = !loading && !error && !!chartOptions;
  const inputCls = "w-24 bg-slate-900 rounded px-2 py-1 font-mono text-xs text-slate-200 focus:outline-none focus:ring-1 transition";

  // Two toolbar rows (overlay toggles + R/R & Box) each ~36px — subtract from
  // the available content height passed by the parent modal.
  const TOOLBAR_H = 76;
  const chartContainerHeightPx = height != null ? Math.max(200, height - TOOLBAR_H) : 400;

  // ── MA alignment arrows ───────────────────────────────────────────────────
  const maArrows = hoverInfo ? (() => {
    const c = hoverInfo.c;
    return [
      { lbl: "EMA 10", val: hoverInfo.ema10,  color: C.green  },
      { lbl: "EMA 20", val: hoverInfo.ema20,  color: C.purple },
      { lbl: "SMA 50", val: hoverInfo.sma50,  color: C.blue   },
      { lbl: "SMA 150",val: hoverInfo.sma150, color: C.yellow },
      { lbl: "SMA 200",val: hoverInfo.sma200, color: C.red    },
    ].filter(m => m.val != null).map(m => ({ ...m, above: c > m.val }));
  })() : null;

  const replayCurrentDate = replayActive && replayIdx >= 0 && chartData?.ohlcv?.[replayIdx]
    ? new Date(chartData.ohlcv[replayIdx][0]).toISOString().slice(0, 10) : "";

  // ── Toggle button helper ───────────────────────────────────────────────────
  const TB = ({ onClick, active, dot, dot2, children }) => (
    <button onClick={onClick} className={`flex items-center gap-1 px-2.5 py-1 rounded text-xs font-semibold border transition ${active ? "border-brand-500/60 text-brand-300 bg-brand-600/15" : "border-slate-700 text-slate-500 hover:text-slate-300 bg-slate-800"}`}>
      {dot  && <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: dot  }} />}
      {dot2 && <span className="w-1.5 h-1.5 rounded-full flex-shrink-0 -ml-0.5" style={{ background: dot2 }} />}
      {children}
    </button>
  );

  return (
    <div className="relative flex flex-col h-full min-h-0 w-full overflow-x-hidden overflow-y-visible">

      {/* Loading */}
      {(loading) && !error && (
        <div className="flex-1 flex items-center justify-center gap-2 text-slate-400">
          <Loader2 className="w-5 h-5 animate-spin" />
          <span className="text-sm">Loading chart…</span>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="flex-1 flex items-center justify-center gap-2 text-red-400">
          <AlertCircle className="w-4 h-4" />
          <span className="text-sm">{error}</span>
          <button onClick={fetchData} className="flex items-center gap-1 ml-2 px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs transition">
            <RefreshCw className="w-3 h-3" /> Retry
          </button>
        </div>
      )}

      {ready && (
        <div className="flex flex-1 min-h-0 min-w-0 overflow-x-hidden overflow-y-visible">

          {/* ── Chart area ── */}
          <div className="flex-1 flex flex-col min-w-0 min-h-0 overflow-x-hidden overflow-y-visible">

            {/* Toolbar row 1: overlay toggles */}
            <div className="flex items-center flex-wrap gap-1.5 px-3 py-1.5 border-b border-slate-800 bg-slate-900/60 shrink-0">
              <span className="text-[10px] text-slate-600 uppercase tracking-wider mr-0.5">Show</span>
              <TB onClick={() => toggleOverlay("sma50")}  active={showOverlay.sma50}  dot={C.blue}>SMA 50</TB>
              <TB onClick={() => toggleOverlay("sma150")} active={showOverlay.sma150} dot={C.yellow}>SMA 150</TB>
              <TB onClick={() => toggleOverlay("sma200")} active={showOverlay.sma200} dot={C.red}>SMA 200</TB>
              <TB onClick={() => toggleOverlay("emas")}   active={showOverlay.emas}   dot={C.green} dot2={C.purple}>EMA 10/20</TB>
              <TB onClick={() => toggleOverlay("52w")}    active={showOverlay["52w"]} dot="#2dd4bf">52W Range</TB>
              <TB onClick={() => toggleOverlay("pivots")} active={showOverlay.pivots} dot={C.muted}>Pivots</TB>
              <span className="w-px h-4 bg-slate-700 mx-1" />
              <TB onClick={() => togglePane("atr")} active={showPanes.atr} dot="#38bdf8">ATR</TB>
              <TB onClick={() => togglePane("rs")}  active={showPanes.rs}  dot={C.blue}>RS</TB>
              <TB onClick={() => togglePane("rmv")} active={showPanes.rmv} dot={C.orange}>RMV</TB>
              <TB onClick={() => togglePane("rsi")} active={showPanes.rsi} dot={C.purple}>RSI</TB>
              <button onClick={fetchData} className="ml-auto text-slate-600 hover:text-slate-300 transition" title="Refresh">
                <RefreshCw className="w-3.5 h-3.5" />
              </button>
            </div>

            {/* Toolbar row 2: R/R drawing + Box drawing */}
            <div className="flex items-center gap-2 px-3 py-1.5 border-b border-slate-800 bg-slate-900/40 shrink-0">
              <span className="flex items-center gap-1 text-xs font-bold text-emerald-400 shrink-0">
                <span className="w-2 h-2 rounded-full bg-emerald-500/70" />Long
              </span>
              <span className="w-px h-4 bg-slate-700" />
              <button onClick={() => { setRrMode(m => !m); if (rrMode) setRr(null); if (!rrMode) setBoxMode(false); }}
                className={`flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium border transition ${rrMode ? "bg-brand-600/20 border-brand-500 text-brand-300 animate-pulse" : "bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200"}`}>
                <Target className="w-3.5 h-3.5" />
                {rrMode ? "Click a bar…" : "R/R Draw"}
              </button>
              {rr && (
                <button onClick={() => { setRr(null); setQtyDerived(false); }}
                  className="flex items-center gap-1 px-2 py-1 rounded text-xs text-slate-500 hover:text-red-400 border border-transparent hover:border-red-900/50 transition">
                  <X className="w-3 h-3" /> Clear
                </button>
              )}
              <span className="w-px h-4 bg-slate-700 shrink-0" />
              {/* Box draw button */}
              <button onClick={() => { setBoxMode(m => !m); if (!boxMode) setRrMode(false); }}
                className={`flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium border transition ${boxMode ? "bg-yellow-600/20 border-yellow-500 text-yellow-300 animate-pulse" : "bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200"}`}>
                <Square className="w-3.5 h-3.5" />
                {boxMode ? "Drag to draw box…" : "Box"}
              </button>
              {boxes.length > 0 && !boxMode && (
                <span className="text-[10px] text-yellow-400/70 font-medium">{boxes.length} box{boxes.length > 1 ? "es" : ""}</span>
              )}
              {rr && (
                <button onClick={() => { setOrderType("limit"); setOrderResult(null); }}
                  className="ml-auto px-3 py-1.5 rounded text-xs font-semibold bg-yellow-500/20 hover:bg-yellow-500/30 text-yellow-300 border border-yellow-500/50 transition animate-pulse">
                  Open Limit Order
                </button>
              )}
            </div>

            {/* Live quote strip */}
            {rr && (
              <div className="flex items-center gap-x-5 px-4 py-1 border-b border-slate-700/60 bg-slate-950/50 shrink-0 text-xs">
                {liveQuote ? (
                  <>
                    {liveQuote.bid != null && <span className="flex items-center gap-1"><span className="text-slate-500">Bid</span><span className="font-mono font-semibold text-emerald-400">${liveQuote.bid.toFixed(2)}</span></span>}
                    {liveQuote.ask != null && <span className="flex items-center gap-1"><span className="text-slate-500">Ask</span><span className="font-mono font-semibold text-red-400">${liveQuote.ask.toFixed(2)}</span></span>}
                    {liveQuote.spread != null && <span className="flex items-center gap-1"><span className="text-slate-500">Spread</span><span className="font-mono text-yellow-400">${liveQuote.spread.toFixed(2)}</span></span>}
                    {liveQuote.last != null && <span className="flex items-center gap-1"><span className="text-slate-500">Last</span><span className="font-mono text-slate-300">${liveQuote.last.toFixed(2)}</span></span>}
                    <span className="ml-auto text-slate-600 text-[10px]">{liveQuote.updatedAt && new Date(liveQuote.updatedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })} · 30s</span>
                  </>
                ) : <span className="text-slate-600 flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-slate-500 animate-pulse"/>Fetching quote…</span>}
              </div>
            )}

            {/* R/R input panel */}
            {rr && (
              <div className="flex items-center flex-wrap gap-x-5 gap-y-1.5 px-4 py-2 border-b border-slate-700/60 bg-slate-800/40 shrink-0 text-xs">
                <span className="flex items-center gap-1.5"><span className="text-slate-500">Entry</span><span className="font-mono font-bold text-slate-200">${rr.entry.toFixed(2)}</span></span>
                <label className="flex items-center gap-1.5"><span className="text-red-400 font-medium">▼ Stop</span>
                  <input type="number" step="0.01" value={rr.stop}
                    onChange={e => setRr(r => { const stop=parseFloat(e.target.value)||r.stop; return {...r, stop, target:parseFloat((r.entry+(r.entry-stop)*r.rrRatio).toFixed(2))}; })}
                    className={`${inputCls} border border-red-900/60 focus:ring-red-500/40`} />
                </label>
                <label className="flex items-center gap-1.5"><span className="text-slate-400 font-medium">R/R</span>
                  <input type="number" step="1" min="1" value={rr.rrRatio}
                    onChange={e => setRr(r => { const rrRatio=Math.max(0.1,parseFloat(e.target.value)||r.rrRatio); return {...r,rrRatio,target:parseFloat((r.entry+(r.entry-r.stop)*rrRatio).toFixed(2))}; })}
                    className={`${inputCls} w-16 border border-slate-600 focus:ring-slate-400/40`} />
                </label>
                <label className="flex items-center gap-1.5"><span className="text-emerald-400 font-medium">▲ Target</span>
                  <input type="number" step="0.01" value={rr.target}
                    onChange={e => setRr(r => ({...r, target:parseFloat(e.target.value)||r.target}))}
                    className={`${inputCls} border border-emerald-900/60 focus:ring-emerald-500/40`} />
                </label>
                <label className="flex items-center gap-1.5">
                  <span className="text-slate-500 flex items-center gap-1">Qty
                    {qtyDerived && <span className="text-[9px] px-1 py-0.5 rounded bg-brand-900/60 text-brand-400 border border-brand-700/50 leading-none">auto</span>}
                  </span>
                  <input type="number" step="1" min="1" value={rr.qty}
                    onChange={e => { setQtyDerived(false); setRr(r => ({...r,qty:Math.max(1,parseInt(e.target.value)||r.qty)})); }}
                    className={`${inputCls} w-16 border ${qtyDerived?"border-brand-700/60":"border-slate-700"}`} />
                </label>
                {rrMetrics && (
                  <div className="flex items-center gap-4 pl-4 border-l border-slate-700">
                    <span className="text-slate-500">R/R <span className="text-white font-bold font-mono">{rrMetrics.ratio}</span></span>
                    <span className="text-slate-500">+<span className="text-emerald-400 font-bold font-mono">${rrMetrics.rewardAmt}</span></span>
                    <span className="text-slate-500">−<span className="text-red-400 font-bold font-mono">${rrMetrics.riskAmt}</span></span>
                    <span className="text-slate-500">Cost <span className="text-slate-200 font-bold font-mono">${(rr.entry*rr.qty).toLocaleString("en-US",{minimumFractionDigits:2})}</span></span>
                  </div>
                )}
              </div>
            )}

            {/* Chart — explicit pixel height mirrors the Trade Ideas ModalChart pattern */}
            <div className="overflow-hidden">
              <HighchartsReact
                ref={chartRef}
                highcharts={Highcharts}
                constructorType="stockChart"
                options={chartOptions}
                containerProps={{ style: { height: `${chartContainerHeightPx}px`, width: "100%" } }}
              />
            </div>
          </div>

          {/* ── Sidebar ── */}
          <div className="w-48 flex-shrink-0 border-l border-slate-800 bg-slate-950/60 flex flex-col overflow-y-auto text-xs">

            {/* Ticker info */}
            {info && (
              <div className="px-3 py-2.5 border-b border-slate-800">
                <div className="font-bold text-yellow-400 text-sm">{info.ticker}</div>
                <div className="text-slate-400 text-[11px] leading-tight truncate">{info.name}</div>
                {info.sector && <div className="text-[10px] text-blue-400/80 mt-0.5 truncate">{info.sector}</div>}
              </div>
            )}

            {/* Hover OHLCV panel */}
            <div className="px-3 py-2 border-b border-slate-800">
              {hoverInfo ? (
                <>
                  <div className="text-[10px] text-slate-500 mb-1">{hoverInfo.date}</div>
                  <div className={`text-lg font-bold ${hoverInfo.c >= hoverInfo.o ? "text-emerald-400" : "text-red-400"}`}>${hoverInfo.c?.toFixed(2)}</div>
                  {(() => { const chg=hoverInfo.c-hoverInfo.o; const pct=((chg/hoverInfo.o)*100).toFixed(2); const col=chg>=0?"text-emerald-400":"text-red-400"; return <div className={`text-[11px] ${col}`}>{chg>=0?"+":""}{chg.toFixed(2)} ({chg>=0?"+":""}{pct}%)</div>; })()}
                  <div className="mt-1.5 space-y-0.5 text-[11px]">
                    {[["O",hoverInfo.o],["H",hoverInfo.h],["L",hoverInfo.l]].map(([lbl,v])=>(
                      <div key={lbl} className="flex justify-between"><span className="text-slate-500">{lbl}</span><span className="font-mono text-slate-300">${v?.toFixed(2)}</span></div>
                    ))}
                    {hoverInfo.vol != null && <div className="flex justify-between"><span className="text-slate-500">Vol</span><span className="font-mono text-slate-300">{fmtV(hoverInfo.vol)}</span></div>}
                  </div>
                  {/* MA values */}
                  <div className="mt-2 pt-1.5 border-t border-slate-800 space-y-0.5 text-[11px]">
                    {[
                      { lbl: "SMA 50",  val: hoverInfo.sma50,  col: "#3b82f6" },
                      { lbl: "SMA 150", val: hoverInfo.sma150, col: "#facc15" },
                      { lbl: "SMA 200", val: hoverInfo.sma200, col: "#ef4444" },
                      { lbl: "EMA 10",  val: hoverInfo.ema10,  col: "#22c55e" },
                      { lbl: "EMA 20",  val: hoverInfo.ema20,  col: "#a78bfa" },
                    ].filter(m => m.val != null).map(m => (
                      <div key={m.lbl} className="flex justify-between items-center">
                        <span style={{ color: m.col }} className="font-medium">{m.lbl}</span>
                        <div className="flex items-center gap-1">
                          <span className="font-mono text-slate-300">${m.val.toFixed(2)}</span>
                          <span className={hoverInfo.c > m.val ? "text-emerald-400" : "text-red-400"}>{hoverInfo.c > m.val ? "▲" : "▼"}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                  {/* RSI */}
                  {hoverInfo.rsi != null && (
                    <div className="flex justify-between mt-1.5 pt-1.5 border-t border-slate-800 text-[11px]">
                      <span className="text-slate-500">RSI 14</span>
                      <span className={`font-mono font-semibold ${hoverInfo.rsi > 70 ? "text-red-400" : hoverInfo.rsi < 30 ? "text-emerald-400" : "text-slate-300"}`}>{hoverInfo.rsi.toFixed(1)}</span>
                    </div>
                  )}
                  {/* RS */}
                  {hoverInfo.rs != null && (
                    <div className="flex justify-between mt-0.5 text-[11px]">
                      <span className="text-slate-500">RS Line</span>
                      <div className="flex items-center gap-1">
                        <span className="font-mono text-slate-300">{hoverInfo.rs.toFixed(4)}</span>
                        {hoverInfo.rsSma50 != null && <span className={hoverInfo.rs > hoverInfo.rsSma50 ? "text-emerald-400" : "text-red-400"}>{hoverInfo.rs > hoverInfo.rsSma50 ? "▲" : "▼"}</span>}
                      </div>
                    </div>
                  )}
                  {/* 52W range bar */}
                  {hoverInfo.dcUpper != null && hoverInfo.dcLower != null && (
                    <div className="mt-1.5 pt-1.5 border-t border-slate-800 text-[10px]">
                      <div className="flex justify-between text-slate-500 mb-1">
                        <span>52W Low</span><span>52W High</span>
                      </div>
                      <div className="relative h-1.5 rounded-full bg-slate-700">
                        <div className="absolute left-0 top-0 h-full rounded-full bg-brand-500"
                          style={{ width: `${Math.max(0, Math.min(100, ((hoverInfo.c - hoverInfo.dcLower) / (hoverInfo.dcUpper - hoverInfo.dcLower)) * 100))}%` }} />
                      </div>
                      <div className="flex justify-between mt-0.5 text-slate-600">
                        <span>{fmtP(hoverInfo.dcLower)}</span><span>{fmtP(hoverInfo.dcUpper)}</span>
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <div className="text-slate-600 text-[11px] py-2 text-center">Hover over the chart</div>
              )}
            </div>

            {/* Volatility panel */}
            {(hoverInfo?.atr != null || hoverInfo?.atrDecl != null) && (
              <div className="px-3 py-2 border-b border-slate-800">
                <div className="text-[10px] text-slate-500 uppercase tracking-wide mb-1">Volatility</div>
                {hoverInfo.atr != null && <div className="flex justify-between text-[11px]"><span className="text-slate-500">ATR</span><span className="font-mono text-slate-300">${hoverInfo.atr.toFixed(2)}</span></div>}
                {hoverInfo.atrDecl != null && (
                  <div className="flex justify-between text-[11px]">
                    <span className="text-slate-500">Declining</span>
                    <span className={`font-semibold ${hoverInfo.atrDecl>=5?"text-emerald-400":hoverInfo.atrDecl>=2?"text-yellow-400":"text-slate-400"}`}>{hoverInfo.atrDecl} bars</span>
                  </div>
                )}
                {hoverInfo.rmv15 != null && <div className="flex justify-between text-[11px] mt-0.5"><span className="text-slate-500">RMV 15</span><span className="font-mono text-slate-300">{hoverInfo.rmv15.toFixed(1)}</span></div>}
              </div>
            )}

            {/* AS 1M panel */}
            {(hoverInfo?.as1m != null || (chartData?.studies?.as1m?.length)) && (() => {
              const score = hoverInfo?.as1m ?? (chartData?.studies?.as1m?.slice(-1)?.[0]?.[1] ?? null);
              if (score == null) return null;
              const col = score>=80?"text-emerald-400":score>=50?"text-yellow-400":"text-red-400";
              return (
                <div className="px-3 py-2 border-b border-slate-800">
                  <div className="text-[10px] text-slate-500 uppercase tracking-wide mb-1">Momentum</div>
                  <div className="flex justify-between text-[11px]">
                    <span className="text-slate-500">AS 1M</span>
                    <span className={`font-bold font-mono ${col}`}>{score}</span>
                  </div>
                </div>
              );
            })()}

            {/* Replay panel */}
            <div className="px-3 py-2 border-b border-slate-800">
              <div className="text-[10px] text-slate-500 uppercase tracking-wide mb-1.5">Replay</div>
              {!replayActive ? (
                <>
                  <input type="date" value={replayDate} onChange={e => setReplayDate(e.target.value)}
                    className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1 text-xs text-slate-300 mb-1.5 focus:outline-none focus:border-brand-500 color-scheme-dark" />
                  <button onClick={startReplay}
                    className="w-full py-1.5 rounded text-xs font-semibold text-center bg-brand-600/20 border border-brand-500/50 text-brand-300 hover:bg-brand-600/30 transition">
                    ▶ Start Replay
                  </button>
                </>
              ) : (
                <>
                  <div className="flex items-center justify-center mb-1.5">
                    <span className="px-1.5 py-0.5 rounded text-[9px] font-bold tracking-wider bg-red-900/40 border border-red-500/50 text-red-300 uppercase">● Live Replay</span>
                  </div>
                  <div className="text-center font-bold text-sm text-slate-200 mb-2">{replayCurrentDate}</div>
                  <div className="flex gap-1 mb-1">
                    <button onClick={replayPrev} disabled={replayIdx <= 0}
                      className="flex-1 flex items-center justify-center py-1.5 rounded text-xs bg-slate-800 border border-slate-700 text-slate-400 hover:text-slate-200 disabled:opacity-30 transition">
                      <ChevronLeft className="w-3.5 h-3.5" />
                    </button>
                    <button onClick={replayNext} disabled={replayIdx >= (chartData?.ohlcv?.length ?? 0) - 1}
                      className="flex-1 flex items-center justify-center py-1.5 rounded text-xs bg-brand-600/20 border border-brand-500/50 text-brand-300 hover:bg-brand-600/30 disabled:opacity-30 transition">
                      <ChevronRight className="w-3.5 h-3.5" />
                    </button>
                  </div>
                  <button onClick={replayExit}
                    className="w-full py-1.5 rounded text-xs font-semibold bg-red-900/30 border border-red-500/40 text-red-300 hover:bg-red-900/50 transition">
                    ✕ Exit Replay
                  </button>
                  <div className="text-center text-[10px] text-slate-600 mt-1">← → arrow keys</div>
                </>
              )}
            </div>

            {/* Price cards */}
            {info && (
              <div className="px-3 py-2 border-b border-slate-800 space-y-0.5 text-[11px]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wide mb-1">Last Day</div>
                {[
                  ["Close",  fmtP(info.last_day_close)],
                  ["Volume", fmtV(info.last_day_volume)],
                  ["Avg Vol",fmtV(info.average_volume)],
                  ["Mkt Cap",fmtC(info.market_cap)],
                ].map(([lbl, val]) => (
                  <div key={lbl} className="flex justify-between">
                    <span className="text-slate-500">{lbl}</span>
                    <span className="font-mono text-slate-300">{val}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Fundamentals */}
            {info && (info.trailing_pe || info.forward_pe || info.target_mean_price) && (
              <div className="px-3 py-2 space-y-0.5 text-[11px]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wide mb-1">Fundamentals</div>
                {info.trailing_pe    != null && <div className="flex justify-between"><span className="text-slate-500">P/E (TTM)</span><span className="font-mono text-slate-300">{fmtN(info.trailing_pe)}</span></div>}
                {info.forward_pe     != null && <div className="flex justify-between"><span className="text-slate-500">P/E (Fwd)</span><span className="font-mono text-slate-300">{fmtN(info.forward_pe)}</span></div>}
                {info.price_to_book  != null && <div className="flex justify-between"><span className="text-slate-500">P/B</span><span className="font-mono text-slate-300">{fmtN(info.price_to_book)}</span></div>}
                {info.earnings_growth!= null && <div className="flex justify-between"><span className="text-slate-500">EPS Growth</span><span className={`font-mono ${parseFloat(info.earnings_growth)>=0?"text-emerald-400":"text-red-400"}`}>{fmtPct(info.earnings_growth)}</span></div>}
                {info.target_mean_price!= null && <div className="flex justify-between"><span className="text-slate-500">Analyst Target</span><span className="font-mono text-emerald-400">{fmtP(info.target_mean_price)}</span></div>}
                {info.recommendation_mean != null && <div className="flex justify-between"><span className="text-slate-500">Analyst Rec.</span><span className="font-mono text-slate-300">{recText(info.recommendation_mean)}</span></div>}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Order confirmation modal ── */}
      {orderType && rr && (() => {
        const liveAsk  = liveQuote?.ask  > 0 ? liveQuote.ask  : null;
        const liveLast = liveQuote?.last > 0 ? liveQuote.last : null;
        const smartEntry = parseFloat(((liveAsk ?? liveLast ?? rr.entry) + 0.01).toFixed(2));
        const entrySource = liveAsk != null ? "ask + $0.01" : liveLast != null ? "last trade" : "chart level";
        const risk   = Math.abs(smartEntry - rr.stop);
        const reward = Math.abs(rr.target  - smartEntry);
        const rrRatio = risk > 0 ? (reward / risk).toFixed(2) : "∞";
        const hasErr  = rr.stop >= smartEntry;
        const Row = ({ label, value, valueClass = "text-slate-200" }) => (
          <div className="flex justify-between items-center py-1.5 border-b border-slate-700/50 last:border-0">
            <span className="text-slate-500 text-xs">{label}</span>
            <span className={`font-mono text-xs font-semibold ${valueClass}`}>{value}</span>
          </div>
        );
        return (
          <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/75 backdrop-blur-[2px]">
            <div className="bg-slate-800 border border-slate-600 rounded-xl shadow-2xl w-72 overflow-hidden">
              <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700 bg-yellow-900/20">
                <div>
                  <p className="text-white font-bold text-sm">Limit Order</p>
                  <p className="text-slate-400 text-[11px] mt-0.5">{ticker} · <span className="text-emerald-400">▲ Long</span></p>
                </div>
                <button onClick={() => setOrderType(null)} className="text-slate-500 hover:text-slate-300"><X className="w-4 h-4" /></button>
              </div>
              <div className="px-4 py-2">
                <Row label="Quantity" value={`${rr.qty} shares`} />
                <div className="flex justify-between items-center py-1.5 border-b border-slate-700/50">
                  <span className="text-slate-500 text-xs">Entry</span>
                  <div className="flex flex-col items-end"><span className="font-mono text-xs font-semibold">${smartEntry.toFixed(2)}</span><span className="text-[10px] text-slate-600">{entrySource}</span></div>
                </div>
                <Row label="Stop loss"   value={`$${rr.stop.toFixed(2)}`}         valueClass="text-red-400" />
                <Row label="Take profit" value={`$${rr.target.toFixed(2)}`}         valueClass="text-emerald-400" />
              </div>
              <div className="mx-4 mb-3 rounded-lg bg-slate-900/60 border border-slate-700/50 px-3 py-2 flex items-center justify-between text-xs">
                <span className="text-slate-500">R/R</span><span className="text-white font-bold font-mono">{rrRatio}</span>
                <span className="text-red-400 font-mono">−${(risk*rr.qty).toFixed(2)}</span>
                <span className="text-emerald-400 font-mono">+${(reward*rr.qty).toFixed(2)}</span>
              </div>
              <div className="mx-4 mb-3">
                <p className="text-slate-500 text-[10px] mb-1.5 uppercase tracking-wide">Entry valid for</p>
                <div className="rounded-lg border border-slate-700 bg-slate-600 px-3 py-1.5 text-center text-xs font-semibold text-white">
                  Good till cancelled
                </div>
                <p className="text-slate-600 text-[10px] mt-1">
                  Entry stays open across sessions. Cancel manually if the trade is no longer valid.
                </p>
              </div>
              {hasErr && !orderResult && <div className="mx-4 mb-2 rounded-lg px-3 py-2 text-xs font-medium bg-red-900/40 border border-red-500/40 text-red-300">Stop must be below entry price.</div>}
              {orderResult && <div className={`mx-4 mb-2 rounded-lg px-3 py-2 text-xs font-medium ${orderResult.ok?"bg-emerald-900/40 border border-emerald-500/40 text-emerald-300":"bg-red-900/40 border border-red-500/40 text-red-300"}`}>
                {orderResult.ok ? <><span className="font-bold">Order placed!</span>{orderResult.orderId&&<span className="block text-[11px] mt-0.5 truncate font-mono">ID: {orderResult.orderId}</span>}</> : orderResult.message}
              </div>}
              <div className="flex gap-2 px-4 pb-4">
                <button disabled={orderSubmitting} onClick={() => { setOrderType(null); setOrderResult(null); if (orderResult?.ok) onClose?.(); }}
                  className="flex-1 py-2 rounded-lg text-xs font-medium bg-slate-700 hover:bg-slate-600 text-slate-300 border border-slate-600 transition disabled:opacity-40">
                  {orderResult?.ok ? "Close" : "Cancel"}
                </button>
                {!orderResult?.ok && (
                  <button disabled={orderSubmitting || hasErr}
                    onClick={async () => {
                      setOrderResult(null); setOrderSubmitting(true);
                      const risk2   = Math.abs(smartEntry - rr.stop);
                      const reward2 = Math.abs(rr.target  - smartEntry);
                      try {
                        const res = await alpacaApi.placeOrder({
                          ticker, direction: "long", order_type: "limit", entry_tif: "gtc",
                          qty: rr.qty, entry_price: smartEntry, stop_price: rr.stop,
                          target_price: rr.target, rr_ratio: rr.rrRatio ?? null,
                          rr_ratio_effective: risk2>0?parseFloat((reward2/risk2).toFixed(4)):null,
                          risk_amt: parseFloat((risk2*rr.qty).toFixed(4)), reward_amt: parseFloat((reward2*rr.qty).toFixed(4)),
                          bias: null, bar_time: null, threshold: null, entry_time: rr.entryTime ?? null,
                        });
                        window.dispatchEvent(new CustomEvent("tf:trade-opened"));
                        setOrderResult({ ok: true, message: res.data.message, orderId: res.data.order?.alpaca_order_id });
                      } catch (err) {
                        setOrderResult({ ok: false, message: err.response?.data?.error || err.message || "Failed to place order." });
                      } finally { setOrderSubmitting(false); }
                    }}
                    className="flex-1 py-2 rounded-lg text-xs font-bold text-yellow-200 bg-yellow-500/30 hover:bg-yellow-500/50 border border-yellow-500/60 transition disabled:opacity-50 disabled:cursor-not-allowed">
                    {orderSubmitting ? "Placing…" : "Confirm Limit"}
                  </button>
                )}
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
