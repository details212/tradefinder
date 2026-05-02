/**
 * TradeReviewModal — read-only chart modal opened from My Trades.
 * Loads the candlestick chart for a saved order and reconstructs the
 * R/R drawing (entry, stop, target, R-level lines) from the stored data.
 * Shows a live P/L strip polled from Alpaca every 30 s.
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from "react";
import Highcharts from "highcharts/highstock";
import HighchartsReact from "highcharts-react-official";
import { stockApi, alpacaApi, aiApi } from "../api/client";
import { Loader2, AlertCircle, AlertTriangle, X, TrendingUp, TrendingDown, RefreshCw, LogOut, ShieldCheck, ShieldAlert, Pencil, Check, ClipboardList, Sparkles, Volume2, VolumeX, Square } from "lucide-react";
import { etStringToUtcMs } from "../utils/timeUtils";

Highcharts.setOptions({ lang: { rangeSelectorZoom: "" } });

const etTime = new Highcharts.Time({ timezone: "America/New_York" });

const FUCHSIA      = "#e879f9";
const GREEN_CANDLE = "#22c55e";
const RED_CANDLE   = "#ef4444";
const GREEN_VOL    = "rgba(34,197,94,0.55)";
const RED_VOL      = "rgba(239,68,68,0.55)";

// ── Build Highcharts options ──────────────────────────────────────────────────
// Threshold and fill-price are drawn as SVG renderer elements (clipped to entry
// bar) so they never distort the y-axis scale.
function buildOptions(ticker, ohlcv, barTimeMs) {
  const BAR_MS   = 5 * 60 * 1000;
  const PAD_BARS = 30;
  const lastT    = ohlcv.length ? ohlcv[ohlcv.length - 1].t : 0;
  const padPts   = Array.from({ length: PAD_BARS }, (_, i) => lastT + BAR_MS * (i + 1));

  const ohlcData = [
    ...ohlcv.map(b => [b.t, b.o, b.h, b.l, b.c]),
    ...padPts.map(t => [t, null, null, null, null]),
  ];
  const volData = ohlcv.map(b => ({
    x: b.t, y: b.v,
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
      enabled: true, height: 36,
      outlineColor: "#334155", outlineWidth: 1,
      maskFill: "rgba(250,204,21,0.15)",
      handles: { backgroundColor: "#475569", borderColor: "#94a3b8" },
      series:  { color: "#3b82f6", lineWidth: 1 },
      xAxis:   { labels: { style: { color: "#94a3b8", fontSize: "10px", textOutline: "none" }, formatter() { return etTime.dateFormat("%b %e", this.value); } } },
    },
    xAxis: {
      type: "datetime", ordinal: true,
      lineColor: "#334155", tickColor: "#334155",
      labels: { style: { color: "#94a3b8", fontSize: "10px", textOutline: "none" }, formatter() { return etTime.dateFormat("%b %e %H:%M", this.value); } },
      plotLines: [],
      plotBands: [],
    },
    yAxis: [
      {
        height: "80%", offset: 0, lineWidth: 1,
        lineColor: "#334155", gridLineColor: "#1e293b",
        labels: { align: "right", x: -4, style: { color: "#94a3b8", fontSize: "10px" } },
        resize: { enabled: true, lineColor: "#334155" },
      },
      {
        top: "82%", height: "18%", offset: 0, lineWidth: 1,
        lineColor: "#334155", gridLineColor: "#1e293b",
        labels: {
          align: "right", x: -4,
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
      split: false, shared: true, useHTML: true,
      backgroundColor: "#0f172a", borderColor: "#334155", borderRadius: 8,
      style: { color: "#e2e8f0", fontSize: "11px" },
      positioner() { return { x: this.chart.plotLeft + 8, y: this.chart.plotTop + 8 }; },
      formatter() {
        const candle = this.points?.find(p => p.series.type === "candlestick");
        const vol    = this.points?.find(p => p.series.name === "Volume");
        if (!candle) return "";
        const dt  = etTime.dateFormat("%a %b %e %H:%M ET", this.x);
        const chg = candle.point.close - candle.point.open;
        const pct = ((chg / candle.point.open) * 100).toFixed(2);
        const col = chg >= 0 ? GREEN_CANDLE : RED_CANDLE;
        const fmtVol = v => v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}K` : String(v);
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
        color: RED_CANDLE, lineColor: RED_CANDLE,
        upColor: GREEN_CANDLE, upLineColor: GREEN_CANDLE,
        dataGrouping: { enabled: false },
      },
      column: { dataGrouping: { enabled: false }, borderWidth: 0, pointPadding: 0.05, groupPadding: 0 },
    },
    series: [
      { type: "candlestick", name: ticker, id: "main", data: ohlcData, yAxis: 0 },
      { type: "column",      name: "Volume", id: "volume", data: volData, yAxis: 1, linkedTo: "main" },
    ],
    legend:  { enabled: false },
    credits: { enabled: false },
  };
}

// ── R/R drawing helpers (mirrors ModalChart) ─────────────────────────────────
const RR_BANDS    = ["rr-profit", "rr-loss"];
const RR_LINES    = ["rr-entry"];
const RR_CLIP_IDS = [...RR_BANDS, ...RR_LINES].map(id => `${id}-clip`);
let rrGreyElems   = [];

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
  const xPx  = chart.xAxis[0].toPixels(entryTime, false);
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
    rect.setAttribute("x", String(clipX)); rect.setAttribute("y", String(chart.plotTop));
    rect.setAttribute("width", String(clipW)); rect.setAttribute("height", String(chart.plotHeight));
    clipPath.appendChild(rect); defs.appendChild(clipPath);
    plb.svgElem.element.setAttribute("clip-path", `url(#${clipId})`);
  });
}

function drawGreyLeftLines(chart, rr) {
  clearGreyElems();
  if (!rr?.entryTime) return;
  const xPx  = chart.xAxis[0].toPixels(rr.entryTime, false);
  const leftX = chart.plotLeft;
  const leftW = Math.min(xPx, chart.plotLeft + chart.plotWidth) - leftX;
  if (leftW <= 0) return;
  const isLong = rr.target > rr.entry;
  const risk   = Math.abs(rr.entry - rr.stop);
  const YELLOW = "#facc15";

  // Intermediate R-levels — use stored rr_ratio so lines match the right side
  const maxR = rr.rrRatio != null
    ? Math.min(Math.floor(Number(rr.rrRatio)), 15)
    : Math.floor(Math.abs(rr.target - rr.entry) / risk);
  const rLevels = [];
  for (let r = 1; r < maxR; r++) {
    const rPrice = isLong ? rr.entry + r * risk : rr.entry - r * risk;
    rLevels.push({ price: rPrice, strokeWidth: 1, dash: "2,3" });
  }

  [
    { price: rr.target, strokeWidth: 1, dash: "4,3" },
    { price: rr.stop,   strokeWidth: 1, dash: "4,3" },
    { price: rr.entry,  strokeWidth: 1, dash: "4,3" },
    ...rLevels,
  ].forEach(({ price, strokeWidth, dash }) => {
    const yPx  = chart.yAxis[0].toPixels(price, false);
    const attrs = { d: `M ${leftX} ${yPx} L ${leftX + leftW} ${yPx}`, stroke: YELLOW, "stroke-width": strokeWidth, zIndex: 4 };
    if (dash) attrs["stroke-dasharray"] = dash;
    rrGreyElems.push(chart.renderer.path().attr(attrs).add());
  });
}

function drawColoredZones(chart, rr) {
  if (!rr?.entry || !rr?.stop || !rr?.target || !rr?.entryTime) return;
  const entryXPx   = chart.xAxis[0].toPixels(rr.entryTime, false);
  const chartRight = chart.plotLeft + chart.plotWidth;

  // If closeTime is set, clip zone to the close bar; otherwise extend to chart right
  let rightEdge = chartRight;
  let closeXPx  = null;
  if (rr.closeTime) {
    const raw = chart.xAxis[0].toPixels(rr.closeTime, false);
    // Snap to the nearest 5-min bar width forward
    const barW = chart.plotWidth / Math.max((chart.xAxis[0].max - chart.xAxis[0].min) / (5 * 60 * 1000), 1);
    closeXPx  = Math.min(raw + barW, chartRight);
    rightEdge = Math.max(Math.min(closeXPx, chartRight), entryXPx + barW);
  }

  const zoneX = Math.max(entryXPx, chart.plotLeft);
  const zoneW = rightEdge - zoneX;
  if (zoneW <= 0) return;

  const { entry, stop, target, qty, fillPrice: fp, exitPrice: ep } = rr;
  const isLong = target > entry;
  const risk   = Math.abs(entry - stop);
  const reward = Math.abs(target - entry);
  const toY    = price => chart.yAxis[0].toPixels(price, false);
  const entryY = toY(entry); const targetY = toY(target); const stopY = toY(stop);

  // Fill price line (only if it differs from limit entry)
  if (fp != null && Math.abs(Number(fp) - entry) > 0.005) {
    const fpY = toY(Number(fp));
    if (fpY >= chart.plotTop && fpY <= chart.plotTop + chart.plotHeight) {
      rrGreyElems.push(chart.renderer.path()
        .attr({ d: `M ${zoneX} ${fpY} L ${rightEdge} ${fpY}`, stroke: "#facc15", "stroke-width": 1, "stroke-dasharray": "3,3", zIndex: 5 }).add());
      rrGreyElems.push(chart.renderer.text(`Fill $${Number(fp).toFixed(2)}`, zoneX + 5, fpY - 3)
        .attr({ zIndex: 6 })
        .css({ color: "#facc15", fontSize: "10px", fontWeight: "bold" }).add());
    }
  }

  // Profit zone
  const greenTop = Math.min(entryY, targetY); const greenBot = Math.max(entryY, targetY);
  rrGreyElems.push(chart.renderer.rect(zoneX, greenTop, zoneW, greenBot - greenTop)
    .attr({ fill: "rgba(34,197,94,0.10)", stroke: "rgba(34,197,94,0.40)", "stroke-width": 1, zIndex: 2 }).add());
  const tgtPct    = ((reward / entry) * 100).toFixed(2);
  const rewardAmt = (reward * qty).toFixed(2);
  const effectiveRR = risk > 0 ? (reward / risk).toFixed(2) : "∞";
  const tgtLabel  = isLong
    ? `▲  $${target.toFixed(2)}  (+${tgtPct}%)  ×${qty}  =  $${rewardAmt}  [${effectiveRR}R]`
    : `▼  $${target.toFixed(2)}  (−${tgtPct}%)  ×${qty}  =  $${rewardAmt}  [${effectiveRR}R]`;
  rrGreyElems.push(chart.renderer.text(tgtLabel, zoneX + zoneW / 2, (greenTop + greenBot) / 2 + 4)
    .attr({ align: "center", zIndex: 5 })
    .css({ color: "#22c55e", fontSize: "11px", fontWeight: "bold", backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" }).add());

  // Loss zone
  const redTop = Math.min(entryY, stopY); const redBot = Math.max(entryY, stopY);
  rrGreyElems.push(chart.renderer.rect(zoneX, redTop, zoneW, redBot - redTop)
    .attr({ fill: "rgba(239,68,68,0.10)", stroke: "rgba(239,68,68,0.40)", "stroke-width": 1, zIndex: 2 }).add());
  const stpPct  = ((risk / entry) * 100).toFixed(2);
  const riskAmt = (risk * qty).toFixed(2);
  const stpLabel = isLong
    ? `▼  $${stop.toFixed(2)}  (−${stpPct}%)  ×${qty}  =  $${riskAmt}`
    : `▲  $${stop.toFixed(2)}  (+${stpPct}%)  ×${qty}  =  $${riskAmt}`;
  rrGreyElems.push(chart.renderer.text(stpLabel, zoneX + zoneW / 2, (redTop + redBot) / 2 + 4)
    .attr({ align: "center", zIndex: 5 })
    .css({ color: "#ef4444", fontSize: "11px", fontWeight: "bold", backgroundColor: "rgba(15,23,42,0.85)", padding: "2px 8px", borderRadius: "3px" }).add());

  // Target line
  rrGreyElems.push(chart.renderer.path().attr({ d: `M ${zoneX} ${targetY} L ${rightEdge} ${targetY}`, stroke: "#22c55e", "stroke-width": 2, zIndex: 5 }).add());
  // Stop line
  rrGreyElems.push(chart.renderer.path().attr({ d: `M ${zoneX} ${stopY} L ${rightEdge} ${stopY}`, stroke: "#ef4444", "stroke-width": 2, zIndex: 5 }).add());

  // R-level reference lines
  const { rrRatio } = rr;
  const maxR = rrRatio != null ? Math.min(Math.floor(Number(rrRatio)), 15) : Math.floor(reward / risk);
  for (let r = 1; r < maxR; r++) {
    const rPrice  = isLong ? entry + r * risk : entry - r * risk;
    const rY      = toY(rPrice);
    const inZone  = isLong ? rPrice < target : rPrice > target;
    const lineCol = inZone ? "rgba(34,197,94,0.65)" : "rgba(34,197,94,0.28)";
    const textCol = inZone ? "#22c55e"               : "#4ade80";
    rrGreyElems.push(chart.renderer.path()
      .attr({ d: `M ${zoneX} ${rY} L ${rightEdge} ${rY}`, stroke: lineCol, "stroke-width": 1, "stroke-dasharray": "4,3", zIndex: 4 }).add());
    rrGreyElems.push(chart.renderer.text(`${r}R`, zoneX + 8, rY - 2)
      .attr({ zIndex: 5 })
      .css({ color: textCol, fontSize: "10px", fontWeight: "700", backgroundColor: "rgba(15,23,42,0.75)", padding: "1px 5px", borderRadius: "2px" }).add());
  }

  // ── Close bar marker ────────────────────────────────────────────────────────
  if (closeXPx != null && closeXPx > chart.plotLeft && closeXPx <= chartRight) {
    // Vertical close line
    rrGreyElems.push(chart.renderer.path()
      .attr({
        d: `M ${closeXPx} ${chart.plotTop} L ${closeXPx} ${chart.plotTop + chart.plotHeight}`,
        stroke: "rgba(255,255,255,0.25)", "stroke-width": 1, "stroke-dasharray": "3,3", zIndex: 6,
      }).add());

    // Exit price horizontal line (from close bar extending right)
    if (ep != null) {
      const epY = toY(Number(ep));
      const isWin = isLong ? Number(ep) > entry : Number(ep) < entry;
      const epColor = isWin ? "#34d399" : "#f87171";
      if (epY >= chart.plotTop && epY <= chart.plotTop + chart.plotHeight) {
        // Dotted line from close marker to chart right
        rrGreyElems.push(chart.renderer.path()
          .attr({
            d: `M ${closeXPx} ${epY} L ${chartRight} ${epY}`,
            stroke: epColor, "stroke-width": 1, "stroke-dasharray": "4,3", zIndex: 6,
          }).add());
        // Exit label
        const pl = (isLong ? 1 : -1) * (Number(ep) - entry) * qty;
        const plStr = `${pl >= 0 ? "+" : ""}$${Math.abs(pl).toFixed(2)}`;
        rrGreyElems.push(chart.renderer.text(
          `Exit $${Number(ep).toFixed(2)}  ${plStr}`,
          closeXPx + 5, epY - 4,
        )
          .attr({ zIndex: 7 })
          .css({ color: epColor, fontSize: "10px", fontWeight: "bold",
            backgroundColor: "rgba(15,23,42,0.88)", padding: "2px 6px", borderRadius: "3px" }).add());
      }
    }

    // "Closed" label at the top of the vertical line
    rrGreyElems.push(chart.renderer.text("✕ Closed", closeXPx + 4, chart.plotTop + 12)
      .attr({ zIndex: 7 })
      .css({ color: "rgba(255,255,255,0.4)", fontSize: "9px", fontWeight: "600" }).add());
  }
}

function removeClipPaths(chart) {
  const defs = chart?.container?.querySelector("svg defs");
  if (!defs) return;
  RR_CLIP_IDS.forEach(id => defs.querySelector(`#${id}`)?.remove());
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
  const risk   = Math.abs(entry - stop);
  const reward = Math.abs(target - entry);
  const rrRatio = risk > 0 ? (reward / risk).toFixed(2) : "∞";
  yAxis.addPlotLine({
    id: "rr-entry", value: entry, color: "#94a3b8", width: 1, dashStyle: "Dash", zIndex: 5,
    label: { text: `Entry $${entry.toFixed(2)}  ·  R/R ${rrRatio}`, align: "right", x: -6, style: { color: "#94a3b8", fontSize: "10px", fontWeight: "600" } },
  });
  if (rr.entryTime) {
    applyClipsAtEntry(chart, rr.entryTime);
    drawGreyLeftLines(chart, rr);
    drawColoredZones(chart, rr);
  }
}

// ── Plain-English trade narrative ────────────────────────────────────────────
function fmtDate(isoStr) {
  if (!isoStr) return null;
  const d = new Date(isoStr.endsWith("Z") ? isoStr : isoStr + "Z");
  return d.toLocaleString("en-US", {
    month: "short", day: "numeric", year: "numeric",
    hour: "2-digit", minute: "2-digit", timeZone: "America/New_York",
    hour12: true,
  }) + " ET";
}

function fmtDuration(isoA, isoB) {
  if (!isoA || !isoB) return null;
  const ms  = Math.abs(new Date(isoB.endsWith("Z") ? isoB : isoB + "Z") -
                        new Date(isoA.endsWith("Z") ? isoA : isoA + "Z"));
  const min = Math.round(ms / 60000);
  if (min < 60) return `${min} minute${min !== 1 ? "s" : ""}`;
  const hrs = Math.floor(min / 60);
  const rem = min % 60;
  if (hrs < 24) return rem ? `${hrs}h ${rem}m` : `${hrs} hour${hrs !== 1 ? "s" : ""}`;
  const days = Math.floor(hrs / 24);
  return `${days} day${days !== 1 ? "s" : ""}`;
}

function buildTradeNarrative(order, rr, slippage, effectiveRR) {
  if (!rr) return null;

  const dir      = order.direction === "long" ? "long" : "short";
  const isLong   = dir === "long";
  const ticker   = (order.ticker || "").toUpperCase();
  const qty      = order.qty ?? 1;
  const paper    = order.paper_mode ? " (paper account)" : "";
  const fill     = rr.fillPrice ?? Number(order.filled_avg_price ?? 0);
  const entry    = rr.entry;
  const stop     = rr.stop;
  const target   = rr.target;
  const exitPx   = rr.exitPrice;
  const pl       = order.unrealized_pl != null ? Number(order.unrealized_pl) : null;
  const method   = order.exit_method ?? null;
  const openedAt = fmtDate(order.created_at);
  const closedAt = fmtDate(order.closed_at ?? order.synced_at);
  const duration = fmtDuration(order.created_at, order.closed_at ?? order.synced_at);
  const risk     = order.risk_amt   != null ? Number(order.risk_amt)   : null;
  const reward   = order.reward_amt != null ? Number(order.reward_amt) : null;
  const rrPlan   = order.rr_ratio   != null ? Number(order.rr_ratio)   : null;

  // ── Para 1: Entry ────────────────────────────────────────────────────────────
  let entry_para = openedAt
    ? `On ${openedAt}, you opened a ${dir} position${paper} in ${ticker}, ${isLong ? "buying" : "selling short"} ${qty} share${qty !== 1 ? "s" : ""}.`
    : `You opened a ${dir} position${paper} in ${ticker}, ${isLong ? "buying" : "selling short"} ${qty} share${qty !== 1 ? "s" : ""}.`;
  if (fill && Math.abs(fill - entry) > 0.005) {
    entry_para += ` Your limit entry was set at $${entry.toFixed(2)} but the order filled at $${fill.toFixed(2)} — a ${fill > entry ? "slightly higher" : "slightly lower"} price due to market movement at the moment of execution.`;
  } else if (fill) {
    entry_para += ` The order filled at $${fill.toFixed(2)}.`;
  }

  // ── Para 2: The plan ─────────────────────────────────────────────────────────
  let plan_para = `Your take-profit target was set at $${target.toFixed(2)} and your stop-loss at $${stop.toFixed(2)}.`;
  if (risk != null && reward != null && rrPlan != null) {
    plan_para += ` This meant risking $${risk.toFixed(2)} to potentially gain $${reward.toFixed(2)} — a planned ${rrPlan.toFixed(1)}R trade.`;
  } else if (rrPlan != null) {
    plan_para += ` The intended reward-to-risk ratio was ${rrPlan.toFixed(1)}R.`;
  }
  if (isLong) {
    plan_para += ` As a long trade you profit when the price rises above your entry, and lose if it falls below your stop.`;
  } else {
    plan_para += ` As a short trade you profit when the price falls below your entry, and lose if it rises above your stop.`;
  }

  // ── Para 3: What happened ────────────────────────────────────────────────────
  let exit_para;
  switch (method) {
    case "bracket_tp":
      exit_para = `The stock reached your $${target.toFixed(2)} take-profit level and the exchange filled your limit order, closing the position at your planned target.${
        exitPx && Math.abs(exitPx - target) > 0.02
          ? ` The actual exit price was $${exitPx.toFixed(2)}.`
          : ""
      }`;
      break;
    case "bracket_sl":
      exit_para = `The price moved against you and hit your $${stop.toFixed(2)} stop-loss level. The exchange triggered your stop order and closed the position to prevent further losses.${
        exitPx && Math.abs(exitPx - stop) > 0.02
          ? ` In fast-moving markets stop orders sometimes fill slightly beyond the stop level — the actual exit was $${exitPx.toFixed(2)}.`
          : ""
      }`;
      break;
    case "auto_close_tp":
      exit_para = `The price stayed beyond your $${target.toFixed(2)} take-profit target for three consecutive 60-second checks. ` +
        `Because the broker's native bracket order had not yet been filled, the auto-close system stepped in and sent a market order to close the position.` +
        (exitPx ? ` The position closed at approximately $${exitPx.toFixed(2)}.` : "");
      break;
    case "auto_close_sl":
      exit_para = `The price stayed beyond your $${stop.toFixed(2)} stop-loss for three consecutive 60-second checks. ` +
        `The auto-close system sent a market order to limit further loss.` +
        (exitPx ? ` The position closed at approximately $${exitPx.toFixed(2)}.` : "");
      break;
    case "manual":
      exit_para = `You chose to close the position manually` +
        (exitPx ? ` at approximately $${exitPx.toFixed(2)}` : "") +
        `, before it reached either your target or stop.`;
      break;
    default:
      exit_para = `The position was closed` +
        (exitPx ? ` at approximately $${exitPx.toFixed(2)}` : "") +
        `. The exact exit method is not recorded for this trade.`;
  }
  if (duration) exit_para += ` The trade was held for ${duration}.`;

  // ── Para 4: Outcome ──────────────────────────────────────────────────────────
  let outcome_para = null;
  if (pl != null) {
    const plSign  = pl >= 0 ? "+" : "";
    const plAmt   = `${plSign}$${Math.abs(pl).toFixed(2)}`;
    const fillBase = fill && qty ? fill * qty : null;
    const pctStr  = fillBase ? ` (${plSign}${((pl / fillBase) * 100).toFixed(2)}%)` : "";
    outcome_para  = `The trade closed with a ${pl >= 0 ? "gain" : "loss"} of ${plAmt}${pctStr}.`;

    if (effectiveRR != null && rrPlan != null) {
      const eRR = Number(effectiveRR);
      if (pl >= 0) {
        outcome_para += ` You achieved ${eRR.toFixed(2)}R of your planned ${rrPlan.toFixed(1)}R reward.`;
      } else {
        const lossVsRisk = risk != null ? (Math.abs(pl) / risk) * 100 : null;
        outcome_para += lossVsRisk != null
          ? ` The loss was ${lossVsRisk.toFixed(0)}% of your planned risk amount.`
          : ` The actual reward-to-risk achieved was ${eRR.toFixed(2)}R.`;
      }
    }

    // Beginner tip based on outcome
    if (method === "bracket_sl" || method === "auto_close_sl") {
      outcome_para += ` Stop-losses are a core part of risk management — they ensure one bad trade can never wipe out many good ones.`;
    } else if (method === "bracket_tp" || method === "auto_close_tp") {
      outcome_para += ` Taking profit at a pre-planned level removes emotion from the exit decision.`;
    } else if (method === "manual" && pl < 0) {
      outcome_para += ` Exiting early can be valid when market conditions change, but consider whether your original stop would have been a better plan.`;
    }
  }

  return { entry: entry_para, plan: plan_para, exit: exit_para, outcome: outcome_para };
}

function TradeNarrative({ order, rr, slippage, effectiveRR }) {
  const [open, setOpen] = useState(true);
  const narrative = buildTradeNarrative(order, rr, slippage, effectiveRR);
  if (!narrative) return null;

  const paras = [
    { label: "Entry",   text: narrative.entry,   color: "text-sky-400" },
    { label: "The plan",text: narrative.plan,     color: "text-purple-400" },
    { label: "Exit",    text: narrative.exit,     color: "text-amber-400" },
    narrative.outcome
      ? { label: "Outcome", text: narrative.outcome,
          color: Number(order.unrealized_pl) >= 0 ? "text-emerald-400" : "text-red-400" }
      : null,
  ].filter(Boolean);

  return (
    <div className="border-b border-slate-700/60 bg-slate-950/30 shrink-0">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-2 px-5 py-2 text-left hover:bg-slate-800/30 transition"
      >
        <span className="text-[10px] font-bold uppercase tracking-[0.15em] text-slate-500">
          Trade Summary
        </span>
        <span className="text-[10px] text-slate-600 ml-1">— plain-English recap</span>
        <span className={`ml-auto text-slate-500 text-xs transition-transform ${open ? "rotate-180" : ""}`}>▾</span>
      </button>
      {open && (
        <div className="px-5 pb-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
          {paras.map(({ label, text, color }) => (
            <div key={label} className="flex flex-col gap-1">
              <span className={`text-[10px] font-bold uppercase tracking-wider ${color}`}>{label}</span>
              <p className="text-xs text-slate-300 leading-relaxed">{text}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Exit method badge ─────────────────────────────────────────────────────────
const EXIT_METHOD_META = {
  bracket_tp:    { label: "Bracket TP",    cls: "text-emerald-400 bg-emerald-900/30 border-emerald-700/50", desc: "Exchange filled your take-profit limit order" },
  bracket_sl:    { label: "Bracket SL",    cls: "text-red-400    bg-red-900/30    border-red-700/50",    desc: "Exchange filled your stop-loss order" },
  auto_close_tp: { label: "Auto-Close TP", cls: "text-amber-400  bg-amber-900/30  border-amber-700/50",  desc: "System sent a market order after 3 consecutive TP breaches" },
  auto_close_sl: { label: "Auto-Close SL", cls: "text-orange-400 bg-orange-900/30 border-orange-700/50", desc: "System sent a market order after 3 consecutive SL breaches" },
  manual:        { label: "Manual",        cls: "text-slate-300  bg-slate-800/60  border-slate-600/50",  desc: "Closed manually via Close Trade button" },
};

function ExitMethodBadge({ method }) {
  const meta = method ? EXIT_METHOD_META[method] : null;
  if (!meta) {
    return <span className="text-slate-600 text-xs italic">Unknown</span>;
  }
  return (
    <span
      title={meta.desc}
      className={`inline-flex items-center px-2 py-0.5 rounded border text-[11px] font-semibold ${meta.cls}`}
    >
      {meta.label}
    </span>
  );
}

// ── Forensic Digest ───────────────────────────────────────────────────────────
// Full table of every metric collected for a closed trade: planned vs actual
// pricing, slippage breakdown, P/L derivation, risk parameters, timestamps.
function DigestSection({ title, children }) {
  return (
    <div className="min-w-0">
      <div className="text-[8px] font-bold uppercase tracking-[0.22em] text-slate-600 pb-1 mb-1 border-b border-slate-800/70">
        {title}
      </div>
      {children}
    </div>
  );
}

function DigestRow({ label, value, color, italic }) {
  return (
    <div className="flex items-baseline justify-between gap-2 py-[2.5px] border-b border-slate-800/30 last:border-0">
      <span className="text-[10px] text-slate-500 shrink-0 leading-snug">{label}</span>
      <span className={`font-mono text-[11px] text-right leading-snug break-all ${color ?? "text-slate-200"} ${italic ? "italic" : ""}`}>
        {value}
      </span>
    </div>
  );
}

function TradeForensicDigest({
  order, rr, slippage, effectiveRR, rAchieved, tradeDuration,
  closedPl, closedPctChange, exitType, isClosedWin, isBreakeven,
}) {
  const isLong = order.direction === "long";

  const fmtTs = (v) => {
    if (!v) return "—";
    try {
      const ms = typeof v === "number" ? v : new Date(v.endsWith("Z") ? v : v + "Z").getTime();
      return etTime.dateFormat("%b %e %Y, %H:%M ET", ms);
    } catch { return String(v); }
  };

  const slipClr = (v) =>
    v == null ? "text-slate-400" : v > 0.005 ? "text-red-400" : v < -0.005 ? "text-emerald-400" : "text-slate-400";
  const plClr = (v) =>
    v == null ? "text-slate-400" : v > 0.005 ? "text-emerald-400" : v < -0.005 ? "text-red-400" : "text-slate-400";
  const sign$ = (v, d = 2) =>
    v != null ? `${Number(v) >= 0 ? "+" : "−"}$${Math.abs(Number(v)).toFixed(d)}` : "—";
  const signN = (v, d = 4) =>
    v != null ? `${Number(v) >= 0 ? "+" : ""}${Number(v).toFixed(d)}` : "—";

  // Entry fill color: bad if fill is worse than chart entry for the direction
  const entryFillClr = (() => {
    if (rr?.fillPrice == null || rr?.entry == null) return "text-slate-200";
    const diff = isLong ? rr.fillPrice - rr.entry : rr.entry - rr.fillPrice;
    return diff > 0.005 ? "text-red-400" : diff < -0.005 ? "text-emerald-400" : "text-slate-300";
  })();

  return (
    <div className="border-b border-slate-700/80 bg-[#070d19] shrink-0 overflow-y-auto" style={{ maxHeight: "310px" }}>
      {/* Digest header */}
      <div className="flex items-center gap-2 px-4 py-1.5 border-b border-slate-800/80 bg-slate-950/60 sticky top-0">
        <ClipboardList className="w-3 h-3 text-slate-500" />
        <span className="text-[9px] font-bold uppercase tracking-[0.2em] text-slate-400">Forensic Trade Digest</span>
        <span className="text-[9px] text-slate-600 ml-1">— every recorded &amp; derived metric</span>
      </div>

      {/* 4-column grid */}
      <div className="grid grid-cols-2 xl:grid-cols-4 divide-x divide-slate-800/50">

        {/* ── Col 1: Identity + Setup ── */}
        <div className="px-4 py-3 flex flex-col gap-4">
          <DigestSection title="Trade Identity">
            <DigestRow label="DB Order ID"     value={order.id ?? "—"} color="text-slate-400" />
            <DigestRow label="Alpaca Order ID" value={order.alpaca_order_id ?? "—"} color="text-slate-400" />
            <DigestRow label="Ticker"          value={order.ticker ?? "—"} color="text-brand-400" />
            <DigestRow label="Direction"       value={isLong ? "Long  ↑" : "Short ↓"} color={isLong ? "text-emerald-400" : "text-red-400"} />
            <DigestRow label="Mode"            value={order.paper_mode ? "Paper" : "Live"} color={order.paper_mode ? "text-blue-400" : "text-slate-300"} />
            <DigestRow label="Status"          value={order.status ?? "—"} />
          </DigestSection>
          <DigestSection title="Setup Metadata">
            <DigestRow label="Signal Bar"      value={order.bar_time ?? "—"} color="text-slate-400" />
            <DigestRow label="Threshold Ref"   value={order.threshold != null ? `$${Number(order.threshold).toFixed(2)}` : "—"} color="text-fuchsia-400" />
            <DigestRow label="Entry Time"      value={fmtTs(order.entry_time)} color="text-slate-400" />
          </DigestSection>
        </div>

        {/* ── Col 2: Order Parameters + Execution ── */}
        <div className="px-4 py-3 flex flex-col gap-4">
          <DigestSection title="Order Parameters (Requested)">
            <DigestRow label="Entry Limit"               value={order.entry_price != null ? `$${Number(order.entry_price).toFixed(2)}` : "—"} color="text-blue-300" />
            <DigestRow label="Stop Price"                value={order.stop_price != null  ? `$${Number(order.stop_price).toFixed(2)}`  : "—"} color="text-red-400" />
            <DigestRow label="Target Price"              value={order.target_price != null ? `$${Number(order.target_price).toFixed(2)}` : "—"} color="text-emerald-400" />
            <DigestRow label="Qty (ordered)"             value={String(order.qty ?? "—")} />
            <DigestRow label="Risk to Reward (planned)"  value={order.rr_ratio != null ? Number(order.rr_ratio).toFixed(2) : "—"} />
            <DigestRow label="Risk to Reward (effective)" value={order.rr_ratio_effective != null ? Number(order.rr_ratio_effective).toFixed(2) : "—"} />
            <DigestRow label="Risk Amount"               value={order.risk_amt != null  ? `$${Number(order.risk_amt).toFixed(2)}`  : "—"} color="text-red-400" />
            <DigestRow label="Reward Amount"             value={order.reward_amt != null ? `$${Number(order.reward_amt).toFixed(2)}` : "—"} color="text-emerald-400" />
          </DigestSection>
          <DigestSection title="Execution (Actual Fills)">
            <DigestRow label="Chart Entry"     value={rr?.entry != null ? `$${rr.entry.toFixed(2)}` : "—"} color="text-slate-300" />
            <DigestRow label="Entry Fill"      value={rr?.fillPrice != null ? `$${Number(rr.fillPrice).toFixed(2)}` : "—"} color={entryFillClr} />
            <DigestRow label="Fill vs Limit"   value={slippage?.fillVsLimitPerShare != null ? `${signN(slippage.fillVsLimitPerShare)}/sh` : "—"} color={slipClr(slippage?.fillVsLimitPerShare)} />
            <DigestRow label="Exit Fill"       value={rr?.exitPrice != null ? `$${Number(rr.exitPrice).toFixed(2)}` : "—"} color={isClosedWin ? "text-emerald-400" : "text-red-400"} />
            <DigestRow label="Exit Reference"  value={exitType === "target" ? `$${rr?.target?.toFixed(2)} (target)` : exitType === "stop" ? `$${rr?.stop?.toFixed(2)} (stop)` : "—"} color="text-slate-400" />
            <DigestRow label="Qty (filled)"    value={rr?.qty != null ? String(rr.qty) : "—"} />
          </DigestSection>
        </div>

        {/* ── Col 3: Slippage + Outcome ── */}
        <div className="px-4 py-3 flex flex-col gap-4">
          <DigestSection title="Slippage Analysis">
            <DigestRow label="Entry Slip/sh"   value={slippage?.entryCostPerShare != null ? `${signN(slippage.entryCostPerShare)}/sh` : "—"} color={slipClr(slippage?.entryCostPerShare)} />
            <DigestRow label="Entry Slip $"    value={slippage?.entryCostDollar != null ? sign$(slippage.entryCostDollar) : "—"} color={slipClr(slippage?.entryCostDollar)} />
            <DigestRow label="Fill vs Limit/sh" value={slippage?.fillVsLimitPerShare != null ? `${signN(slippage.fillVsLimitPerShare)}/sh` : "—"} color={slipClr(slippage?.fillVsLimitPerShare)} />
            <DigestRow label="Exit Slip/sh"    value={slippage?.exitCostPerShare != null ? `${signN(slippage.exitCostPerShare)}/sh` : slippage?.hasExitRef === false ? "n/a (manual)" : "—"} color={slippage?.exitCostPerShare != null ? slipClr(slippage.exitCostPerShare) : "text-slate-600"} italic={slippage?.exitCostPerShare == null} />
            <DigestRow label="Exit Slip $"     value={slippage?.exitCostDollar != null ? sign$(slippage.exitCostDollar) : "—"} color={slipClr(slippage?.exitCostDollar)} />
            <DigestRow label="Round-trip $"    value={slippage?.totalCostDollar != null ? sign$(slippage.totalCostDollar) : "—"} color={slipClr(slippage?.totalCostDollar)} />
            <DigestRow label="Slip % of Risk"  value={slippage?.pctOfRisk != null ? `${slippage.pctOfRisk > 0 ? "+" : ""}${slippage.pctOfRisk.toFixed(2)}%` : "—"} color={slippage?.pctOfRisk != null ? (slippage.pctOfRisk > 5 ? "text-red-400" : slippage.pctOfRisk < -5 ? "text-emerald-400" : "text-slate-400") : "text-slate-400"} />
          </DigestSection>
          <DigestSection title="Outcome &amp; Profit and Loss">
            <DigestRow label="Profit and Loss"  value={closedPl != null ? sign$(closedPl) : "—"} color={plClr(closedPl)} />
            <DigestRow label="% Change"        value={closedPctChange != null ? `${closedPctChange >= 0 ? "+" : ""}${closedPctChange.toFixed(3)}%` : "—"} color={plClr(closedPctChange)} />
            <DigestRow label="R Achieved"      value={rAchieved != null ? `${rAchieved >= 0 ? "+" : ""}${rAchieved.toFixed(3)}R` : "—"} color={plClr(rAchieved)} />
            <DigestRow label="Outcome"         value={isBreakeven ? "Breakeven" : isClosedWin ? "Win" : "Loss"} color={isBreakeven ? "text-slate-300" : isClosedWin ? "text-emerald-400" : "text-red-400"} />
            <DigestRow label="Exit Category"   value={exitType === "target" ? "Target Hit" : exitType === "stop" ? "Stopped Out" : exitType === "manual" ? "Manual Exit" : "—"} />
            <DigestRow label="Exit Method (raw)" value={order.exit_method ?? "—"} color="text-slate-400" />
            <DigestRow label="Duration"        value={tradeDuration ?? "—"} />
          </DigestSection>
          <DigestSection title="Close Mechanism">
            {(() => {
              const m = order.exit_method;
              const meta = m ? EXIT_METHOD_META[m] : null;
              const isBracket   = m === "bracket_tp"    || m === "bracket_sl";
              const isAutoClose = m === "auto_close_tp" || m === "auto_close_sl";
              const isManual    = m === "manual";
              const executor = isBracket   ? "Alpaca broker (bracket order)"
                             : isAutoClose ? "TradeFinder automation service"
                             : isManual    ? "Trader (manual close)"
                             : "Unknown";
              const executorColor = isBracket   ? "text-blue-400"
                                  : isAutoClose ? "text-amber-400"
                                  : isManual    ? "text-slate-300"
                                  : "text-slate-600";
              const sideColor = (m === "bracket_tp" || m === "auto_close_tp") ? "text-emerald-400"
                              : (m === "bracket_sl" || m === "auto_close_sl") ? "text-red-400"
                              : "text-slate-400";
              return (
                <>
                  <DigestRow label="Closed by"     value={executor} color={executorColor} />
                  <DigestRow label="TP or SL side" value={
                    m === "bracket_tp"    ? "Take-profit hit" :
                    m === "auto_close_tp" ? "Take-profit breach ×3" :
                    m === "bracket_sl"    ? "Stop-loss hit" :
                    m === "auto_close_sl" ? "Stop-loss breach ×3" :
                    isManual              ? "N/A (manual)" : "—"
                  } color={sideColor} />
                  <DigestRow label="How it works"  value={meta?.desc ?? "—"} color="text-slate-400" italic />
                  {isBracket && (
                    <DigestRow label="Bracket type"  value="Native OCO bracket — broker fills TP/SL leg when price reaches level" color="text-slate-500" italic />
                  )}
                  {isAutoClose && (
                    <DigestRow label="Auto-close"    value="System polled price, detected ≥3 consecutive level breaches, sent market order to Alpaca" color="text-slate-500" italic />
                  )}
                </>
              );
            })()}
          </DigestSection>
        </div>

        {/* ── Col 4: Risk Parameters + Timestamps ── */}
        <div className="px-4 py-3 flex flex-col gap-4">
          <DigestSection title="Risk Parameters">
            <DigestRow label="Stop Dist/sh"              value={rr ? `$${Math.abs(rr.entry - rr.stop).toFixed(2)}` : "—"} color="text-red-400" />
            <DigestRow label="Target Dist/sh"            value={rr ? `$${Math.abs(rr.target - rr.entry).toFixed(2)}` : "—"} color="text-emerald-400" />
            <DigestRow label="Planned Risk"              value={rr ? `$${(Math.abs(rr.entry - rr.stop) * rr.qty).toFixed(2)}` : "—"} color="text-red-400" />
            <DigestRow label="Planned Reward"            value={rr ? `$${(Math.abs(rr.target - rr.entry) * rr.qty).toFixed(2)}` : "—"} color="text-emerald-400" />
            <DigestRow label="Effective Risk to Reward"  value={effectiveRR ?? "—"} />
            <DigestRow label="Risk Amount (stored)"      value={order.risk_amt != null  ? `$${Number(order.risk_amt).toFixed(2)}`  : "—"} color="text-slate-400" />
            <DigestRow label="Reward Amount (stored)"    value={order.reward_amt != null ? `$${Number(order.reward_amt).toFixed(2)}` : "—"} color="text-slate-400" />
          </DigestSection>
          <DigestSection title="Timestamps (ET)">
            <DigestRow label="Order Created"   value={fmtTs(order.created_at)} color="text-slate-400" />
            <DigestRow label="Entry Time"      value={fmtTs(order.entry_time)} color="text-slate-400" />
            <DigestRow label="Close / Sync"    value={fmtTs(order.synced_at)} color="text-slate-400" />
            <DigestRow label="Close Time (drv)" value={fmtTs(rr?.closeTime)} color="text-slate-400" />
            <DigestRow label="Signal Bar"      value={order.bar_time ?? "—"} color="text-slate-400" />
          </DigestSection>
        </div>

      </div>
    </div>
  );
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function TradeReviewModal({ order, onClose, onTradeClosed }) {
  const [bars,      setBars]      = useState([]);
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState(null);
  const [rr,        setRr]        = useState(null);
  const [liveQuote, setLiveQuote] = useState(null);
  const [activeZoom, setActiveZoom] = useState("1D");
  const [chartH, setChartH] = useState(null);

  // Sanity check (closed trades only)
  const [sanityCheck, setSanityCheck] = useState(null); // null | { loading } | { error } | { checks }

  // Close-trade flow
  const [closeConfirm, setCloseConfirm] = useState(false); // show confirm prompt
  const [closeLoading, setCloseLoading] = useState(false);
  const [closeError,   setCloseError]   = useState(null);
  const [dayTradeWarn, setDayTradeWarn] = useState(false); // day-trade warning interstitial

  // Edit-levels flow
  const [editLevels,      setEditLevels]      = useState(false);
  const [editTarget,      setEditTarget]      = useState("");
  const [editStop,        setEditStop]        = useState("");
  const [editLevelsLoading, setEditLevelsLoading] = useState(false);
  const [editLevelsError,   setEditLevelsError]   = useState(null);
  const [editLevelsSaved,   setEditLevelsSaved]   = useState(false);

  // Forensic digest toggle (closed trades only)
  const [showDigest, setShowDigest] = useState(false);

  // Exit-method backfill state (for "Unknown" trades)
  const [fixingExitMethod, setFixingExitMethod]   = useState(false);
  const [fixExitMethodResult, setFixExitMethodResult] = useState(null); // null | {exit_method, source} | {error}

  // AI trade analysis (closed trades only)
  const [aiAnalysis, setAiAnalysis] = useState(null); // null | {loading} | {text} | {error}

  // TTS audio playback
  const [audioState, setAudioState] = useState(null); // null | {loading} | {playing, url} | {error}
  const audioRef = useRef(null);

  // True when the order was opened (created_at) on today's Eastern-Time date.
  const isDayTrade = useMemo(() => {
    if (!order?.created_at) return false;
    const etNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
    const etOpen = new Date(new Date(order.created_at + "Z").toLocaleString("en-US", { timeZone: "America/New_York" }));
    return (
      etNow.getFullYear() === etOpen.getFullYear() &&
      etNow.getMonth()    === etOpen.getMonth()    &&
      etNow.getDate()     === etOpen.getDate()
    );
  }, [order?.created_at]);

  // ── Sanity check: fetch Alpaca order and compare to DB (closed trades only) ──
  useEffect(() => {
    if (order.is_open === true || !order.alpaca_order_id) return;
    setSanityCheck({ loading: true });

    alpacaApi.getOrderDetail(order.alpaca_order_id)
      .then(res => {
        const a = res.data;                          // Alpaca order object
        const legs = Array.isArray(a.legs) ? a.legs : [];
        const stopLeg   = legs.find(l => l.type === "stop" || l.type === "stop_limit");
        const profitLeg = legs.find(l => l.type === "limit" && l !== stopLeg);

        const priceTol = 0.01;
        const priceMatch = (db, al) => {
          if (db == null || al == null) return null; // unknown
          return Math.abs(Number(db) - Number(al)) <= priceTol;
        };

        const checks = [
          {
            label:  "Symbol",
            db:     order.ticker,
            alpaca: a.symbol ?? "—",
            match:  order.ticker === a.symbol,
          },
          {
            label:  "Qty",
            db:     String(order.qty),
            alpaca: a.filled_qty != null ? String(parseInt(a.filled_qty, 10)) : "—",
            match:  a.filled_qty != null
                      ? order.qty === parseInt(a.filled_qty, 10)
                      : null,
          },
          {
            label:  "Fill Price",
            db:     order.filled_avg_price != null ? `$${Number(order.filled_avg_price).toFixed(2)}` : "—",
            alpaca: a.filled_avg_price != null ? `$${Number(a.filled_avg_price).toFixed(2)}` : "—",
            match:  priceMatch(order.filled_avg_price, a.filled_avg_price),
          },
          {
            label:  "Status",
            db:     order.status ?? "—",
            alpaca: a.status ?? "—",
            match:  (order.status ?? "") === (a.status ?? ""),
          },
          {
            label:  "Entry Limit",
            db:     order.entry_price != null ? `$${Number(order.entry_price).toFixed(2)}` : "—",
            alpaca: a.limit_price    != null ? `$${Number(a.limit_price).toFixed(2)}`    : "—",
            match:  priceMatch(order.entry_price, a.limit_price),
          },
          {
            label:  "Stop",
            db:     order.stop_price != null ? `$${Number(order.stop_price).toFixed(2)}` : "—",
            alpaca: stopLeg?.stop_price  != null ? `$${Number(stopLeg.stop_price).toFixed(2)}`  : "—",
            match:  priceMatch(order.stop_price, stopLeg?.stop_price),
          },
          {
            label:  "Target",
            db:     order.target_price != null ? `$${Number(order.target_price).toFixed(2)}` : "—",
            alpaca: profitLeg?.limit_price != null ? `$${Number(profitLeg.limit_price).toFixed(2)}` : "—",
            match:  priceMatch(order.target_price, profitLeg?.limit_price),
          },
        ];

        setSanityCheck({ checks });
      })
      .catch(err => {
        const msg = err?.response?.data?.error || "Could not fetch from Alpaca";
        setSanityCheck({ error: msg });
      });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [order.alpaca_order_id, order.is_open]);

  const chartRef    = useRef(null);
  const rrRef       = useRef(null);
  const containerRef = useRef(null);

  const ticker    = order.ticker;
  const barTimeMs = etStringToUtcMs(order.bar_time);
  const isLong    = order.direction === "long";
  const fillPrice = order.filled_avg_price ?? order.entry_price;

  const ZOOM_PRESETS = [
    { label: "1D", days: 1 },
    { label: "2D", days: 1, entryRelative: true }, // ±1 day around entry
    { label: "3D", days: 3 },
    { label: "1W", days: 7 }, { label: "2W", days: 14 },
    { label: "All", days: null },
  ];

  // Measure chart container height
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      const h = entries[0]?.contentRect.height;
      if (h) setChartH(Math.floor(h));
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  // Reconstruct rr using the same entry price ModalChart uses: bar.close at entry_time.
  // order.entry_price is the Alpaca limit price (e.g. smartEntry) which differs from
  // the bar the user clicked.  We need the bar's close to reproduce the original drawing.
  // Falls back to order.entry_price when bars haven't loaded or entry_time is absent.
  useEffect(() => {
    if (!order.stop_price) return;

    const stop    = Number(order.stop_price);
    const rrRatio = Number(order.rr_ratio ?? 2);

    // Locate the entry bar by finding the nearest bar to entry_time
    let chartEntry = null;
    if (order.entry_time && bars.length) {
      const entryBar = bars.reduce((best, b) =>
        Math.abs(b.t - order.entry_time) < Math.abs(best.t - order.entry_time) ? b : best
      , bars[0]);
      if (entryBar) chartEntry = parseFloat(entryBar.c.toFixed(2));
    }

    const entry = chartEntry ?? Number(order.entry_price ?? order.filled_avg_price);
    if (!entry) return;

    // Use the actual stored target_price when available — this is the level that was
    // submitted to Alpaca as the take-profit limit price and is what the auto-close
    // service reads from the DB.  Reconstructing from chartEntry introduces a drift
    // (chartEntry ≠ the smartEntry used when placing the order) that makes the
    // displayed target lower than the real TP, causing confusing "it should have
    // closed" situations.  Only fall back to reconstruction for legacy rows.
    const target = order.target_price
      ? Number(order.target_price)
      : parseFloat((entry + (entry - stop) * rrRatio).toFixed(2));

    // Derive close time and exit price for closed trades
    let closeTime = null;
    let exitPrice = null;
    if (!order.is_open && order.synced_at) {
      const raw = order.synced_at.endsWith("Z") ? order.synced_at : order.synced_at + "Z";
      closeTime = new Date(raw).getTime();
      // Snap to nearest 5-min bar boundary
      const BAR_MS = 5 * 60 * 1000;
      closeTime = Math.round(closeTime / BAR_MS) * BAR_MS;
    }
    if (!order.is_open && order.unrealized_pl != null && order.filled_avg_price != null && order.qty) {
      const fill = Number(order.filled_avg_price);
      const pl   = Number(order.unrealized_pl);
      const qty  = Number(order.qty);
      const dir  = order.direction === "long" ? 1 : -1;
      exitPrice = fill + dir * (pl / qty);
    }

    setRr({
      entry,
      stop,
      target,
      qty:       order.qty ?? 1,
      rrRatio,
      entryTime: order.entry_time ?? null,
      threshold: order.threshold ?? null,
      fillPrice: order.filled_avg_price != null ? Number(order.filled_avg_price) : null,
      closeTime,
      exitPrice,
    });
  }, [bars, order]);

  // Fetch bars — for closed trades use a tight window around the trade;
  // for open trades fall back to a 28-day rolling window.
  const fetchBars = useCallback(() => {
    if (!ticker) return;
    setLoading(true); setError(null);

    const fmt  = d => d.toISOString().slice(0, 10);
    const today = new Date();
    let from, to;

    if (!order.is_open && order.created_at) {
      // Anchor on the trade's entry date minus 1 calendar day
      const entryRaw = order.created_at.endsWith("Z")
        ? order.created_at : order.created_at + "Z";
      from = new Date(entryRaw);
      from.setDate(from.getDate() - 1);

      // Anchor the end on the close date plus 1 calendar day (capped at today)
      const closeRaw = order.synced_at
        ? (order.synced_at.endsWith("Z") ? order.synced_at : order.synced_at + "Z")
        : entryRaw;
      to = new Date(closeRaw);
      to.setDate(to.getDate() + 1);
      if (to > today) to = today;
    } else {
      // Open trade — rolling 28-day window
      from = new Date(today);
      from.setDate(from.getDate() - 28);
      to = today;
    }

    stockApi.history(ticker, { multiplier: 5, timespan: "minute", from: fmt(from), to: fmt(to), limit: 3000 })
      .then(r => {
        const raw = (r.data.bars || []).sort((a, b) => a.t - b.t);
        setBars(raw.map(b => ({ t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v })));
      })
      .catch(() => setError("Failed to load chart data"))
      .finally(() => setLoading(false));
  }, [ticker, order.is_open, order.created_at, order.synced_at]);

  useEffect(() => { fetchBars(); }, [fetchBars]);

  // Live quote polling
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      setLiveQuote(prev => prev ? { ...prev, fetching: true } : { fetching: true });
      try {
        const res = await alpacaApi.quote(ticker);
        if (cancelled) return;
        const d = res.data;
        setLiveQuote({ bid: d.bid > 0 ? d.bid : null, ask: d.ask > 0 ? d.ask : null, last: d.last > 0 ? d.last : null, updatedAt: Date.now(), fetching: false });
      } catch { if (!cancelled) setLiveQuote(prev => prev ? { ...prev, fetching: false } : null); }
    };
    poll();
    const id = setInterval(poll, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [ticker]);

  // Zoom to signal bar after bars load
  const paddedLastT = useCallback(() => bars.length ? bars[bars.length - 1].t + 30 * 5 * 60 * 1000 : 0, [bars]);

  const handleCloseTrade = useCallback(async () => {
    setCloseLoading(true);
    setCloseError(null);
    try {
      await alpacaApi.closeTrade(order.id);
      setCloseConfirm(false);
      (onTradeClosed ?? onClose)(); // dismiss + trigger immediate order list refresh
    } catch (err) {
      const msg = err?.response?.data?.error || err?.response?.data?.errors?.join(", ") || "Failed to close trade.";
      setCloseError(msg);
    } finally {
      setCloseLoading(false);
    }
  }, [order.id, onClose, onTradeClosed]);

  const openEditLevels = useCallback(() => {
    setEditTarget(order.target_price != null ? Number(order.target_price).toFixed(2) : "");
    setEditStop(order.stop_price != null ? Number(order.stop_price).toFixed(2) : "");
    setEditLevelsError(null);
    setEditLevelsSaved(false);
    setEditLevels(true);
  }, [order.target_price, order.stop_price]);

  const saveEditLevels = useCallback(async () => {
    const newTarget = parseFloat(editTarget);
    const newStop   = parseFloat(editStop);
    if (isNaN(newTarget) && isNaN(editTarget === "" ? NaN : 0)) {
      setEditLevelsError("Enter a valid target price."); return;
    }
    if (isNaN(newStop) && isNaN(editStop === "" ? NaN : 0)) {
      setEditLevelsError("Enter a valid stop price."); return;
    }
    setEditLevelsLoading(true);
    setEditLevelsError(null);
    try {
      const payload = {};
      if (!isNaN(newTarget)) payload.target_price = newTarget;
      if (!isNaN(newStop))   payload.stop_price   = newStop;
      await alpacaApi.patchLevels(order.id, payload);
      setEditLevelsSaved(true);
      // Persist locally so the modal reflects the new values immediately
      if (!isNaN(newTarget)) order.target_price = newTarget;
      if (!isNaN(newStop))   order.stop_price   = newStop;
      setTimeout(() => setEditLevels(false), 1200);
    } catch (err) {
      setEditLevelsError(err?.response?.data?.error || "Failed to save levels.");
    } finally {
      setEditLevelsLoading(false);
    }
  }, [order, editTarget, editStop]);

  const applyZoom = useCallback((preset) => {
    const chart = chartRef.current?.chart;
    if (!chart || !bars.length) return;
    setActiveZoom(preset.label);
    const padT   = paddedLastT();
    const DAY_MS = 24 * 60 * 60 * 1000;

    if (!preset.days) {
      // "All"
      chart.xAxis[0].setExtremes(bars[0].t, padT, true, false);
      return;
    }

    if (preset.entryRelative && order.entry_time) {
      // ±N days centred on entry time
      const fromT = Math.max(order.entry_time - preset.days * DAY_MS, bars[0].t);
      const toT   = Math.min(order.entry_time + preset.days * DAY_MS, padT);
      chart.xAxis[0].setExtremes(fromT, toT, true, false);
      return;
    }

    // Standard: last N days relative to the final bar
    const lastT   = bars[bars.length - 1].t;
    const fromT   = lastT - preset.days * DAY_MS;
    const fromIdx = bars.findIndex(b => b.t >= fromT);
    chart.xAxis[0].setExtremes(bars[Math.max(0, fromIdx)].t, padT, true, false);
  }, [bars, paddedLastT, order.entry_time]);

  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart || !bars.length) return;
    chart.reflow();
    if (!order.is_open) {
      // Closed trade: 1 day before entry → 1 day after entry
      const DAY_MS = 24 * 60 * 60 * 1000;
      if (order.entry_time) {
        const fromT = Math.max(order.entry_time - DAY_MS, bars[0].t);
        const toT   = Math.min(order.entry_time + DAY_MS, paddedLastT());
        setActiveZoom("2D");
        chart.xAxis[0].setExtremes(fromT, toT, true, false);
      } else {
        setActiveZoom("All");
        chart.xAxis[0].setExtremes(bars[0].t, paddedLastT(), true, false);
      }
    } else {
      const preset = ZOOM_PRESETS.find(p => p.label === activeZoom);
      const windowMs = preset?.days ? preset.days * 24 * 60 * 60 * 1000 : null;
      const lastT = bars[bars.length - 1].t;
      const windowStart = windowMs ? lastT - windowMs : bars[0].t;
      chart.xAxis[0].setExtremes(windowStart, paddedLastT(), true, false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bars, barTimeMs]);

  // Keep rrRef in sync
  useEffect(() => { rrRef.current = rr; }, [rr]);

  // Draw R/R levels
  useEffect(() => { applyRR(chartRef.current?.chart, rr); }, [rr, bars]);

  // Re-clip + redraw on render (zoom / scroll)
  useEffect(() => {
    const chart = chartRef.current?.chart;
    if (!chart) return;
    const onRender = () => {
      const cur = rrRef.current;
      if (!cur?.entryTime) return;
      applyClipsAtEntry(chart, cur.entryTime);
      drawGreyLeftLines(chart, cur);
      drawColoredZones(chart, cur);
    };
    Highcharts.addEvent(chart, "render", onRender);
    return () => Highcharts.removeEvent(chart, "render", onRender);
  }, [bars, barTimeMs]);

  const chartOptions = useMemo(
    () => buildOptions(ticker, bars, barTimeMs),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [ticker, bars, barTimeMs]
  );

  const ready = !loading && !error && bars.length > 0 && chartH != null;

  // ── R/R calculations ──────────────────────────────────────────────────────
  const effectiveRR = useMemo(() => {
    if (!rr) return null;
    const risk   = Math.abs(rr.entry - rr.stop);
    const reward = Math.abs(rr.target - rr.entry);
    return risk > 0 ? (reward / risk).toFixed(2) : null;
  }, [rr]);

  // ── P/L calculations ──────────────────────────────────────────────────────
  const currentPrice = liveQuote?.last ?? liveQuote?.bid ?? null;
  const livePl = currentPrice != null && fillPrice != null
    ? (isLong ? 1 : -1) * (currentPrice - Number(fillPrice)) * (order.qty ?? 0)
    : null;
  const plColor   = livePl == null ? "text-slate-500" : livePl > 0 ? "text-emerald-400" : livePl < 0 ? "text-red-400" : "text-slate-400";
  const plPrefix  = livePl != null && livePl > 0 ? "+" : "";
  const pctChange = currentPrice != null && fillPrice != null
    ? ((currentPrice - Number(fillPrice)) / Number(fillPrice)) * 100 * (isLong ? 1 : -1)
    : null;

  // Progress between stop → entry → target for the progress bar
  const plProgress = useMemo(() => {
    if (!rr || currentPrice == null) return null;
    const { entry, stop, target } = rr;
    const totalRange = Math.abs(target - stop);
    if (totalRange === 0) return null;
    const pos = isLong
      ? (currentPrice - stop) / totalRange
      : (stop - currentPrice) / totalRange;
    return Math.max(0, Math.min(1, pos)); // clamp 0–1
  }, [rr, currentPrice, isLong]);

  // ── Closed trade summary values ──────────────────────────────────────────
  const closedPl        = !order.is_open && order.unrealized_pl != null ? Number(order.unrealized_pl) : null;
  const isClosedWin     = closedPl != null ? closedPl  >  0.005 : null;
  const isBreakeven     = closedPl != null ? Math.abs(closedPl) <= 0.005 : false;

  const rAchieved = useMemo(() => {
    if (order.is_open || !rr?.exitPrice || !rr?.entry || !rr?.stop) return null;
    const dir  = isLong ? 1 : -1;
    const risk = Math.abs(rr.entry - rr.stop);
    if (risk === 0) return null;
    return dir * (Number(rr.exitPrice) - rr.entry) / risk;
  }, [order.is_open, rr, isLong]);

  const tradeDuration = useMemo(() => {
    if (!order.entry_time || !rr?.closeTime) return null;
    const ms = rr.closeTime - order.entry_time;
    if (ms <= 0) return null;
    const totalMin = Math.round(ms / 60000);
    if (totalMin < 60) return `${totalMin}m`;
    const h = Math.floor(totalMin / 60);
    const m = totalMin % 60;
    if (h < 24) return m > 0 ? `${h}h ${m}m` : `${h}h`;
    const d = Math.floor(h / 24);
    const rh = h % 24;
    return rh > 0 ? `${d}d ${rh}h` : `${d}d`;
  }, [order.entry_time, rr?.closeTime]);

  const exitType = useMemo(() => {
    if (order.is_open) return null;

    // exit_method is the authoritative source — use it first.
    // bracket_tp / auto_close_tp  → TP side ("target")
    // bracket_sl / auto_close_sl  → SL side ("stop")
    // manual                      → manual
    const m = order.exit_method;
    if (m === "bracket_tp"    || m === "auto_close_tp") return "target";
    if (m === "bracket_sl"    || m === "auto_close_sl") return "stop";
    if (m === "manual") return "manual";

    // Fallback: derive from exit-price proximity for legacy rows that
    // pre-date exit_method tracking.
    if (!rr?.exitPrice || !rr?.target || !rr?.stop || !rr?.entry) return null;
    const ep  = Number(rr.exitPrice);
    const tol = Math.max(Math.abs(rr.entry - rr.stop) * 0.10, 0.05);
    if (Math.abs(ep - rr.target) <= tol) return "target";
    if (Math.abs(ep - rr.stop)   <= tol) return "stop";
    return "manual";
  }, [order.is_open, order.exit_method, rr]);

  const closedPctChange = useMemo(() => {
    if (order.is_open || !rr?.exitPrice || !fillPrice) return null;
    return ((Number(rr.exitPrice) - Number(fillPrice)) / Number(fillPrice)) * 100 * (isLong ? 1 : -1);
  }, [order.is_open, rr, fillPrice, isLong]);

  // ── Round-trip slippage ───────────────────────────────────────────────────
  // Positive cost values = slippage hurt you. Negative = you got a better fill.
  const slippage = useMemo(() => {
    if (order.is_open || !rr) return null;
    const dir = isLong ? 1 : -1;

    // Entry: fill vs the bar-close price used to draw the R/R (rr.entry)
    const entryFill = rr.fillPrice;
    const entryCostPerShare = entryFill != null
      ? dir * (entryFill - rr.entry)     // positive = bad
      : null;

    // Entry: fill vs the Alpaca limit order price (execution quality)
    const limitPrice = order.entry_price != null ? Number(order.entry_price) : null;
    const fillVsLimitPerShare = entryFill != null && limitPrice != null
      ? dir * (entryFill - limitPrice)   // positive = paid more than limit (unusual)
      : null;

    // Exit: exit vs the reference price (target/stop); manual has no clean reference
    let refExitPrice = null;
    if      (exitType === "target") refExitPrice = rr.target;
    else if (exitType === "stop")   refExitPrice = rr.stop;

    const exitCostPerShare = rr.exitPrice != null && refExitPrice != null
      ? dir * (refExitPrice - Number(rr.exitPrice))  // positive = received less / paid more
      : null;

    // Dollar totals
    const qty = rr.qty ?? 1;
    const entryCostDollar = entryCostPerShare != null ? entryCostPerShare * qty : null;
    const exitCostDollar  = exitCostPerShare  != null ? exitCostPerShare  * qty : null;
    const totalCostDollar =
      entryCostDollar != null || exitCostDollar != null
        ? (entryCostDollar ?? 0) + (exitCostDollar ?? 0)
        : null;

    // Slippage as % of planned risk (how many "risk dollars" did slippage eat?)
    const plannedRisk = Math.abs(rr.entry - rr.stop) * qty;
    const pctOfRisk = totalCostDollar != null && plannedRisk > 0
      ? (totalCostDollar / plannedRisk) * 100
      : null;

    return {
      entryCostPerShare, fillVsLimitPerShare,
      exitCostPerShare,  exitCostDollar,
      entryCostDollar,   totalCostDollar,
      pctOfRisk,         hasExitRef: refExitPrice != null,
    };
  }, [order.is_open, order.entry_price, rr, exitType, isLong]);

  // ── AI Trade Analysis ─────────────────────────────────────────────────────
  // Defined after slippage useMemo so it can close over the computed value.
  const handleAIAnalysis = useCallback(async () => {
    if (!rr) return;
    setAiAnalysis({ loading: true });

    const isLongDir = order.direction === "long";
    const n = (v, d = 2) => v != null ? Number(v).toFixed(d) : "N/A";
    const s$ = (v, d = 2) => v != null ? `${Number(v) >= 0 ? "+" : "−"}$${Math.abs(Number(v)).toFixed(d)}` : "N/A";
    const fmtTs = (v) => {
      if (!v) return "N/A";
      try {
        const ms = typeof v === "number" ? v : new Date(v.endsWith("Z") ? v : v + "Z").getTime();
        return etTime.dateFormat("%b %e %Y %H:%M ET", ms);
      } catch { return String(v); }
    };

    const sl = slippage;
    const plannedRisk$   = rr ? (Math.abs(rr.entry - rr.stop)   * rr.qty).toFixed(2) : "N/A";
    const plannedReward$ = rr ? (Math.abs(rr.target - rr.entry) * rr.qty).toFixed(2) : "N/A";

    const prompt = `You are a friendly but honest trading coach giving a trader a quick debrief on their just-closed trade. \
Your tone is warm, clear, and encouraging — like a knowledgeable friend who tells it straight without being harsh. \
Write 2–3 short paragraphs that give a high-level human summary of the trade: \
open with what the trader was trying to do and whether the core idea worked out, \
then highlight what went right (good setup, clean execution, tight slippage, disciplined exit, etc.), \
and finish with what went wrong or could be improved (bad fill, oversize risk, stop too tight, price moved against them, etc.). \
If nothing went wrong, say so honestly. If nothing went right beyond following the plan, say that too. \
Keep it conversational and easy to understand for someone who is not a professional. \
STRICT STYLE RULES — you must follow these exactly: \
(a) All dollar amounts must be rounded to exactly 2 decimal places (e.g. $74.35, not $74.3500). \
(b) Never write "R/R" — always write "Risk to Reward" in full. \
(c) Never write "P/L" — always write "Profit and Loss" in full. \
(d) Do not use bullet points — write in flowing prose paragraphs only. \
(e) Do not start with "This trade" — vary the opening.

=== FORENSIC TRADE DATA ===

IDENTITY
Ticker: ${order.ticker}  |  Direction: ${isLongDir ? "Long (buy)" : "Short (sell)"}  |  Mode: ${order.paper_mode ? "Paper" : "Live"}  |  Status: ${order.status ?? "N/A"}

ORDER PARAMETERS (what was requested)
Entry Limit Price: $${n(order.entry_price)}
Stop Price: $${n(order.stop_price)}
Target Price: $${n(order.target_price)}
Quantity: ${order.qty ?? "N/A"} shares
Planned Risk to Reward: ${n(order.rr_ratio)}  |  Effective Risk to Reward: ${effectiveRR ?? "N/A"}
Planned Risk: $${plannedRisk$}  |  Planned Reward: $${plannedReward$}

EXECUTION (what actually happened)
Chart Entry (bar close): $${n(rr.entry)}
Entry Fill Price: $${rr.fillPrice != null ? n(rr.fillPrice) : "N/A"}
Fill vs Limit delta: ${sl?.fillVsLimitPerShare != null ? `${sl.fillVsLimitPerShare > 0 ? "+" : ""}${sl.fillVsLimitPerShare.toFixed(2)}/sh` : "N/A"}
Exit Fill Price: $${rr.exitPrice != null ? n(rr.exitPrice) : "N/A"}
Exit Reference: ${exitType === "target" ? `$${n(rr.target)} (take-profit target)` : exitType === "stop" ? `$${n(rr.stop)} (stop-loss)` : "manual — no clean reference"}

SLIPPAGE ANALYSIS
Entry slippage vs chart entry: ${sl?.entryCostPerShare != null ? `${sl.entryCostPerShare > 0 ? "+" : ""}${sl.entryCostPerShare.toFixed(2)}/sh (${s$(sl.entryCostDollar)} total)` : "N/A"}
Entry fill vs submitted limit: ${sl?.fillVsLimitPerShare != null ? `${sl.fillVsLimitPerShare > 0 ? "+" : ""}${sl.fillVsLimitPerShare.toFixed(2)}/sh` : "N/A"}
Exit slippage vs reference: ${sl?.exitCostPerShare != null ? `${sl.exitCostPerShare > 0 ? "+" : ""}${sl.exitCostPerShare.toFixed(2)}/sh (${s$(sl.exitCostDollar)} total)` : sl?.hasExitRef === false ? "N/A (manual exit, no reference)" : "N/A"}
Round-trip slippage cost: ${sl?.totalCostDollar != null ? s$(sl.totalCostDollar) : "N/A"}
Slippage as % of planned risk: ${sl?.pctOfRisk != null ? `${sl.pctOfRisk.toFixed(2)}%` : "N/A"}

OUTCOME & PROFIT AND LOSS
Final Realized Profit and Loss: ${closedPl != null ? s$(closedPl) : "N/A"}
% Change (fill to exit): ${closedPctChange != null ? `${closedPctChange >= 0 ? "+" : ""}${closedPctChange.toFixed(2)}%` : "N/A"}
Risk Units Achieved: ${rAchieved != null ? `${rAchieved >= 0 ? "+" : ""}${rAchieved.toFixed(2)}R` : "N/A"}
Outcome: ${isBreakeven ? "Breakeven" : isClosedWin ? "WIN" : "LOSS"}
Exit Category: ${exitType === "target" ? "Target Hit" : exitType === "stop" ? "Stopped Out (stop-loss triggered)" : exitType === "manual" ? "Manual Exit" : "Unknown"}
Exit Method: ${order.exit_method ?? "N/A"}
Trade Duration: ${tradeDuration ?? "N/A"}

TIMESTAMPS
Order placed: ${fmtTs(order.created_at)}
Entry filled: ${fmtTs(order.entry_time)}
Trade closed: ${fmtTs(order.synced_at)}
`;

    try {
      const { data } = await aiApi.chat(
        [{ role: "user", content: prompt }],
        { model: "gpt-4o-mini", max_completion_tokens: 800, temperature: 0.4 },
      );
      const text = data.choices?.[0]?.message?.content?.trim() ?? "No response returned.";
      setAiAnalysis({ text });
    } catch (err) {
      const msg = err.response?.data?.error?.message || err.message || "Failed to fetch AI analysis.";
      setAiAnalysis({ error: msg });
    }
  }, [order, rr, slippage, effectiveRR, exitType, rAchieved, tradeDuration,
      closedPl, closedPctChange, isClosedWin, isBreakeven]);

  // ── Text-to-Speech (Replicate / MiniMax Speech 2.8 Turbo) ─────────────────
  const stopAudio = useCallback(() => {
    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current.src = "";
      audioRef.current = null;
    }
    setAudioState(prev => prev?.url ? { ...prev, playing: false } : null);
  }, []);

  const handleTTS = useCallback(async (text) => {
    stopAudio();
    setAudioState({ loading: true });
    try {
      const res = await aiApi.tts(text, { voice_id: "English_MatureBoss", speed: 1.0, emotion: "neutral" });
      let prediction = res.data;

      // If Prefer:wait timed out server-side, poll until complete
      while (prediction.status === "starting" || prediction.status === "processing") {
        await new Promise(r => setTimeout(r, 1200));
        const poll = await aiApi.pollTts(prediction.id);
        prediction = poll.data;
      }
      if (prediction.status === "failed") {
        throw new Error(prediction.error || "Speech generation failed");
      }

      const audioUrl = Array.isArray(prediction.output) ? prediction.output[0] : prediction.output;
      if (!audioUrl) throw new Error("No audio URL returned from Replicate");

      const audio = new Audio(audioUrl);
      audioRef.current = audio;
      audio.onended = () => setAudioState({ url: audioUrl, playing: false });
      audio.onerror = () => setAudioState({ url: audioUrl, playing: false, error: "Playback error" });
      setAudioState({ url: audioUrl, playing: true });
      await audio.play();
    } catch (err) {
      const msg = err?.response?.data?.error || err?.response?.data?.detail || err.message || "TTS failed";
      setAudioState({ error: msg });
    }
  }, [stopAudio]);

  // Auto-play TTS whenever a fresh AI analysis text arrives
  useEffect(() => {
    if (aiAnalysis?.text) {
      handleTTS(aiAnalysis.text);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aiAnalysis?.text]);

  // Stop audio when AI panel is dismissed
  useEffect(() => {
    if (!aiAnalysis) stopAudio();
  }, [aiAnalysis, stopAudio]);

  // Cleanup on modal unmount
  useEffect(() => () => stopAudio(), [stopAudio]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="relative bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl flex flex-col"
        style={{ width: "95vw", height: "95vh" }}
        onClick={e => e.stopPropagation()}
      >
        {/* ── Modal header ── */}
        <div className="flex items-center gap-3 px-5 py-3 border-b border-slate-800 shrink-0">
          <span className="text-lg font-bold text-brand-400">{ticker}</span>

          {isLong
            ? <span className="flex items-center gap-1 text-[11px] font-bold px-2 py-0.5 rounded uppercase tracking-wider bg-green-900/50 text-green-400 border border-green-800"><TrendingUp className="w-3 h-3" /> Long</span>
            : <span className="flex items-center gap-1 text-[11px] font-bold px-2 py-0.5 rounded uppercase tracking-wider bg-red-900/50 text-red-400 border border-red-800"><TrendingDown className="w-3 h-3" /> Short</span>
          }

          {order.paper_mode && (
            <span className="text-[10px] font-semibold text-blue-400 bg-blue-900/30 border border-blue-700/40 rounded px-1.5 py-0.5">PAPER</span>
          )}

          <span className={`text-[10px] font-semibold rounded px-1.5 py-0.5 capitalize ${
            order.status === "filled"   ? "bg-emerald-900/40 text-emerald-400 border border-emerald-700/40" :
            order.status === "accepted" || order.status === "new" ? "bg-yellow-900/40 text-yellow-400 border border-yellow-700/40" :
            order.status === "canceled" ? "bg-slate-800 text-slate-500 border border-slate-700" :
            "bg-slate-800 text-slate-400 border border-slate-700"
          }`}>{order.status ?? "—"}</span>

          {/* Outcome badge — closed trades only */}
          {!order.is_open && closedPl != null && (
            <span className={`flex items-center gap-1.5 text-xs font-bold px-2.5 py-1 rounded-lg border ${
              isBreakeven  ? "bg-slate-800 text-slate-300 border-slate-600" :
              isClosedWin  ? "bg-emerald-950/70 text-emerald-300 border-emerald-700/60" :
                             "bg-red-950/70 text-red-300 border-red-700/60"
            }`}>
              {isBreakeven ? "↔ B/E" : isClosedWin ? "✓ WIN" : "✗ LOSS"}
              <span className="font-mono">
                {closedPl > 0 ? "+" : ""}{closedPl < 0 ? "" : ""}${Math.abs(closedPl).toFixed(2)}
              </span>
            </span>
          )}

          <button
            onClick={onClose}
            className="ml-auto p-1.5 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* ── Sanity check strip (closed trades only) ── */}
        {order.is_open !== true && (
          <div className="px-4 py-2 border-b border-slate-700 bg-slate-950 shrink-0">
            <div className="flex items-center gap-2 mb-1.5">
              <ShieldCheck className="w-3.5 h-3.5 text-slate-500 shrink-0" />
              <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">DB ↔ Alpaca Sanity Check</span>
            </div>

            {/* No Alpaca ID */}
            {!order.alpaca_order_id && (
              <div className="flex items-center gap-2 text-xs text-slate-500 italic">
                <AlertCircle className="w-3.5 h-3.5 shrink-0" />
                No Alpaca order ID on file — cannot verify
              </div>
            )}

            {/* Loading */}
            {order.alpaca_order_id && (!sanityCheck || sanityCheck.loading) && (
              <div className="flex items-center gap-2 text-xs text-slate-400">
                <Loader2 className="w-3 h-3 animate-spin" />
                Fetching from Alpaca…
              </div>
            )}

            {/* Fetch error */}
            {order.alpaca_order_id && sanityCheck?.error && (
              <div className="flex items-center gap-2 text-xs text-amber-400">
                <AlertCircle className="w-3.5 h-3.5 shrink-0" />
                {sanityCheck.error}
              </div>
            )}

            {/* Results */}
            {order.alpaca_order_id && sanityCheck?.checks && (() => {
              const mismatches = sanityCheck.checks.filter(c => c.match === false);
              const unknowns   = sanityCheck.checks.filter(c => c.match === null);
              const allGood    = mismatches.length === 0;
              return (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
                  {/* Overall badge */}
                  <div className={`flex items-center gap-1.5 text-[11px] font-semibold shrink-0 ${allGood ? "text-emerald-400" : "text-amber-400"}`}>
                    {allGood
                      ? <ShieldCheck className="w-3.5 h-3.5" />
                      : <ShieldAlert  className="w-3.5 h-3.5" />}
                    {allGood ? "All fields match" : `${mismatches.length} mismatch${mismatches.length !== 1 ? "es" : ""} detected`}
                  </div>

                  <span className="text-slate-700 text-xs">|</span>

                  {/* Per-field chips */}
                  {sanityCheck.checks.map(({ label, db, alpaca, match }) => (
                    <div key={label} className={`flex items-center gap-1 text-[10px] rounded px-1.5 py-0.5 border ${
                      match === true  ? "bg-emerald-900/20 border-emerald-800/50 text-emerald-400" :
                      match === false ? "bg-amber-900/30  border-amber-600/50  text-amber-300"     :
                                        "bg-slate-800/60  border-slate-700      text-slate-500"
                    }`}>
                      <span className="text-slate-500 mr-0.5">{label}:</span>
                      <span className="font-mono font-semibold">{db}</span>
                      {match === false && (
                        <span className="ml-1 text-slate-500">↔ <span className="text-amber-400 font-mono">{alpaca}</span></span>
                      )}
                    </div>
                  ))}

                  {unknowns.length > 0 && (
                    <span className="text-[10px] text-slate-600 italic">{unknowns.length} unavailable from Alpaca</span>
                  )}
                </div>
              );
            })()}
          </div>
        )}

        {/* ── Toolbar ── */}
        <div className="flex items-center gap-3 px-3 py-1.5 border-b border-slate-800 bg-slate-900/60 shrink-0">
          {/* Zoom presets */}
          <div className="flex items-center rounded overflow-hidden border border-slate-700 shrink-0">
            {ZOOM_PRESETS.map((preset, i) => (
              <button
                key={preset.label}
                onClick={() => applyZoom(preset)}
                className={`px-2.5 py-1 text-xs font-semibold transition ${i > 0 ? "border-l border-slate-700" : ""} ${
                  activeZoom === preset.label ? "bg-brand-700/40 text-brand-300" : "bg-slate-800 text-slate-500 hover:text-slate-200"
                }`}
              >
                {preset.label}
              </button>
            ))}
          </div>

          {/* Last price + refresh indicator */}
          <div className="flex items-center gap-x-3 text-xs ml-2">
            {currentPrice != null && (
              <span className="text-slate-500">Last <span className="font-mono text-slate-300">${currentPrice.toFixed(2)}</span></span>
            )}
            {liveQuote?.fetching && <RefreshCw className="w-3 h-3 text-slate-600 animate-spin" />}
          </div>

          {/* ── Edit Levels + Close Trade buttons ── */}
          {order.is_open && !dayTradeWarn && !closeConfirm && !editLevels && (
            <div className="ml-auto flex items-center gap-2">
              <button
                onClick={openEditLevels}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-slate-700/50 hover:bg-slate-700 text-slate-300 border border-slate-600/60 transition"
                title="Correct the stored TP / SL levels for auto-close"
              >
                <Pencil className="w-3 h-3" />
                Edit Levels
              </button>
              <button
                onClick={() => {
                  setCloseError(null);
                  if (isDayTrade) { setDayTradeWarn(true); }
                  else            { setCloseConfirm(true); }
                }}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-red-500/15 hover:bg-red-500/25 text-red-400 border border-red-500/40 transition"
              >
                <LogOut className="w-3.5 h-3.5" />
                Close Trade
              </button>
            </div>
          )}

          {/* ── Edit Levels panel ── */}
          {order.is_open && editLevels && (
            <div className="ml-auto flex items-center gap-3 px-3 py-1.5 rounded border border-slate-600/60 bg-slate-800/80">
              <span className="text-[11px] text-slate-400 font-semibold whitespace-nowrap">Edit Levels</span>
              <label className="flex items-center gap-1.5 text-[11px] text-emerald-400">
                TP
                <input
                  type="number" step="0.01"
                  value={editTarget}
                  onChange={e => { setEditTarget(e.target.value); setEditLevelsSaved(false); }}
                  className="w-22 bg-slate-900 border border-slate-600 rounded px-2 py-0.5 text-xs font-mono text-emerald-300 focus:outline-none focus:border-emerald-500"
                  placeholder={order.target_price != null ? Number(order.target_price).toFixed(2) : "—"}
                />
              </label>
              <label className="flex items-center gap-1.5 text-[11px] text-red-400">
                SL
                <input
                  type="number" step="0.01"
                  value={editStop}
                  onChange={e => { setEditStop(e.target.value); setEditLevelsSaved(false); }}
                  className="w-22 bg-slate-900 border border-slate-600 rounded px-2 py-0.5 text-xs font-mono text-red-300 focus:outline-none focus:border-red-500"
                  placeholder={order.stop_price != null ? Number(order.stop_price).toFixed(2) : "—"}
                />
              </label>
              {editLevelsError && (
                <span className="text-[11px] text-red-400 max-w-[180px] truncate">{editLevelsError}</span>
              )}
              {editLevelsSaved && (
                <span className="flex items-center gap-1 text-[11px] text-emerald-400">
                  <Check className="w-3 h-3" /> Saved
                </span>
              )}
              <button
                onClick={saveEditLevels}
                disabled={editLevelsLoading}
                className="flex items-center gap-1 px-2.5 py-1 rounded text-xs font-semibold bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white transition"
              >
                {editLevelsLoading ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                Save
              </button>
              <button
                onClick={() => setEditLevels(false)}
                className="text-slate-500 hover:text-slate-300 transition"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
          )}

          {/* ── Day-trade warning interstitial ── */}
          {dayTradeWarn && !closeConfirm && (
            <div className="ml-auto flex items-center gap-2 px-3 py-1.5 rounded border border-amber-500/50 bg-amber-500/10">
              <AlertTriangle className="w-3.5 h-3.5 text-amber-400 shrink-0" />
              <span className="text-xs text-amber-300 font-semibold">
                Day Trade Warning — this position was opened today.
                Closing it counts as a Day Trade (PDT rule applies).
              </span>
              <button
                onClick={() => { setDayTradeWarn(false); setCloseConfirm(true); }}
                className="px-3 py-1 rounded text-xs font-semibold bg-amber-500/20 hover:bg-amber-500/35 text-amber-300 border border-amber-500/50 transition whitespace-nowrap"
              >
                Understood, continue
              </button>
              <button
                onClick={() => setDayTradeWarn(false)}
                className="px-2.5 py-1 rounded text-xs text-slate-400 hover:text-slate-200 border border-slate-700 hover:border-slate-500 transition"
              >
                Cancel
              </button>
            </div>
          )}

          {/* ── Confirm close prompt ── */}
          {closeConfirm && (
            <div className="ml-auto flex items-center gap-2">
              {closeError && (
                <span className="text-[11px] text-red-400 max-w-xs truncate">{closeError}</span>
              )}
              <span className="text-xs text-slate-400">
                Cancel brackets &amp; close at market?
              </span>
              <button
                onClick={handleCloseTrade}
                disabled={closeLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-semibold bg-red-600 hover:bg-red-500 disabled:opacity-50 text-white transition"
              >
                {closeLoading
                  ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Closing…</>
                  : <><LogOut className="w-3.5 h-3.5" /> Confirm Close</>}
              </button>
              <button
                onClick={() => { setCloseConfirm(false); setCloseError(null); }}
                disabled={closeLoading}
                className="px-2.5 py-1.5 rounded text-xs text-slate-400 hover:text-slate-200 border border-slate-700 hover:border-slate-500 transition"
              >
                Cancel
              </button>
            </div>
          )}
        </div>

        {/* ── CLOSED TRADE: Summary panel ── */}
        {!order.is_open && rr && (
          <div className="flex items-stretch border-b border-slate-700 shrink-0 bg-slate-950/40 divide-x divide-slate-700/50 text-xs overflow-x-auto">

            {/* Outcome */}
            <div className={`flex flex-col items-center justify-center px-6 py-3 min-w-[110px] shrink-0 ${
              isBreakeven ? "" : isClosedWin ? "bg-emerald-950/30" : "bg-red-950/30"
            }`}>
              <span className={`text-[9px] font-bold uppercase tracking-[0.15em] mb-1 ${
                isBreakeven ? "text-slate-500" : isClosedWin ? "text-emerald-600" : "text-red-600"
              }`}>outcome</span>
              <span className={`text-3xl font-black leading-none ${
                isBreakeven ? "text-slate-300" : isClosedWin ? "text-emerald-400" : "text-red-400"
              }`}>
                {isBreakeven ? "B/E" : isClosedWin ? "WIN" : "LOSS"}
              </span>
              {rAchieved != null && (
                <span className={`text-sm font-bold font-mono mt-1.5 ${
                  rAchieved > 0 ? "text-emerald-400" : rAchieved < 0 ? "text-red-400" : "text-slate-400"
                }`}>
                  {rAchieved > 0 ? "+" : ""}{rAchieved.toFixed(2)}R
                </span>
              )}
              {exitType && (
                <span className={`text-[9px] mt-2 font-semibold uppercase tracking-wide ${
                  exitType === "target" ? "text-emerald-600" :
                  exitType === "stop"   ? "text-red-600"     : "text-slate-500"
                }`}>
                  {exitType === "target" ? "Target Hit" :
                   exitType === "stop"   ? "Stopped Out" : "Manual Exit"}
                </span>
              )}
            </div>

            {/* Final P/L */}
            <div className="flex flex-col justify-center px-5 py-3 min-w-[145px] shrink-0">
              <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">Final P / L</span>
              <span className={`text-2xl font-bold font-mono leading-none ${
                closedPl == null ? "text-slate-600" :
                isBreakeven ? "text-slate-300" :
                isClosedWin ? "text-emerald-400" : "text-red-400"
              }`}>
                {closedPl != null
                  ? `${closedPl > 0 ? "+" : ""}$${Math.abs(closedPl).toFixed(2)}`
                  : "—"}
              </span>
              {closedPctChange != null && (
                <span className={`text-sm font-mono mt-1 ${
                  closedPctChange > 0 ? "text-emerald-500" : closedPctChange < 0 ? "text-red-500" : "text-slate-400"
                }`}>
                  {closedPctChange >= 0 ? "+" : ""}{closedPctChange.toFixed(2)}%
                </span>
              )}
              <span className="text-[10px] text-slate-600 font-mono mt-1.5">{rr.qty} sh</span>
            </div>

            {/* Prices */}
            <div className="flex flex-col justify-center px-5 py-3 min-w-[210px] shrink-0">
              <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">Prices</span>
              <div className="flex flex-col gap-1">
                <div className="flex items-center gap-2">
                  <span className="w-10 text-right text-[10px] text-slate-500 shrink-0">Entry</span>
                  <span className="font-mono font-semibold text-slate-200">${rr.entry.toFixed(2)}</span>
                  {rr.fillPrice != null && Math.abs(rr.fillPrice - rr.entry) > 0.005 && (
                    <span className="text-[10px] text-yellow-500/80 font-mono">(fill ${rr.fillPrice.toFixed(2)})</span>
                  )}
                </div>
                <div className="flex items-center gap-2 ml-[3.2rem]">
                  <div className="w-px h-3 bg-slate-700" />
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-10 text-right text-[10px] text-slate-500 shrink-0">Exit</span>
                  <span className={`font-mono font-semibold ${
                    rr.exitPrice == null ? "text-slate-600" :
                    isClosedWin ? "text-emerald-400" : "text-red-400"
                  }`}>
                    {rr.exitPrice != null ? `$${Number(rr.exitPrice).toFixed(2)}` : "—"}
                  </span>
                </div>
                <div className="border-t border-slate-800/80 my-0.5" />
                <div className="flex items-center gap-3">
                  <span className="text-[10px] text-slate-600">
                    Stop <span className="font-mono text-red-800">${rr.stop.toFixed(2)}</span>
                  </span>
                  <span className="text-slate-700">·</span>
                  <span className="text-[10px] text-slate-600">
                    Target <span className="font-mono text-emerald-800">${rr.target.toFixed(2)}</span>
                  </span>
                </div>
              </div>
            </div>

            {/* Timeline */}
            <div className="flex flex-col justify-center px-5 py-3 min-w-[215px] shrink-0">
              <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">Timeline</span>
              <div className="flex flex-col">
                {/* Entry dot + time */}
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-blue-400 ring-2 ring-blue-400/20 shrink-0" />
                  <span className="font-mono text-slate-300">
                    {order.entry_time != null
                      ? etTime.dateFormat("%b %e, %H:%M ET", order.entry_time)
                      : <span className="text-slate-600 italic">unknown</span>}
                  </span>
                  <span className="text-[10px] text-slate-600">entry</span>
                </div>

                {/* Connector + duration */}
                <div className="flex items-center gap-2 my-0.5 pl-[3px]">
                  <div className="w-px h-5 bg-slate-700 shrink-0" />
                  {tradeDuration && (
                    <span className="text-[10px] font-mono text-slate-400 bg-slate-800/70 px-1.5 py-0.5 rounded ml-0.5">
                      {tradeDuration}
                    </span>
                  )}
                </div>

                {/* Exit dot + time */}
                <div className="flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ring-2 shrink-0 ${
                    isBreakeven  ? "bg-slate-400 ring-slate-400/20" :
                    isClosedWin  ? "bg-emerald-400 ring-emerald-400/20" :
                                   "bg-red-400 ring-red-400/20"
                  }`} />
                  <span className="font-mono text-slate-300">
                    {rr.closeTime != null
                      ? etTime.dateFormat("%b %e, %H:%M ET", rr.closeTime)
                      : <span className="text-slate-600 italic">unknown</span>}
                  </span>
                  <span className="text-[10px] text-slate-600">exit</span>
                </div>
              </div>
            </div>

            {/* Setup reference */}
            <div className="flex flex-col justify-center px-5 py-3 shrink-0">
              <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">Setup</span>
              <div className="flex flex-col gap-1.5">
                <div className="flex items-center gap-2">
                  <span className="w-14 text-[10px] text-slate-500">R/R</span>
                  <span className="font-mono font-bold text-white text-sm">{effectiveRR ?? "—"}</span>
                  {rr.rrRatio != null && effectiveRR != null && Math.abs(Number(effectiveRR) - Number(rr.rrRatio)) > 0.05 && (
                    <span className="text-[10px] text-slate-600">/ {Number(rr.rrRatio).toFixed(1)} planned</span>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-14 text-[10px] text-slate-500">Qty</span>
                  <span className="font-mono text-slate-300">{rr.qty}</span>
                </div>
                {rr.threshold != null && (
                  <div className="flex items-center gap-2">
                    <span className="w-14 text-[10px] text-slate-500">Ref</span>
                    <span className="font-mono text-fuchsia-400">${Number(rr.threshold).toFixed(2)}</span>
                  </div>
                )}
              </div>
            </div>

            {/* Slippage */}
            {slippage && (
              <div className="flex flex-col justify-center px-5 py-3 shrink-0 min-w-[210px]">
                <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">
                  Round-trip Slippage
                </span>
                <div className="flex flex-col gap-1.5">

                  {/* Entry fill vs bar entry */}
                  <div className="flex items-center gap-2">
                    <span className="w-16 text-[10px] text-slate-500 shrink-0">Entry fill</span>
                    {slippage.entryCostPerShare != null ? (
                      <>
                        <span className={`font-mono text-xs font-semibold ${
                          slippage.entryCostPerShare > 0.005  ? "text-red-400"     :
                          slippage.entryCostPerShare < -0.005 ? "text-emerald-400" : "text-slate-400"
                        }`}>
                          {slippage.entryCostPerShare > 0 ? "+" : ""}{slippage.entryCostPerShare.toFixed(3)}/sh
                        </span>
                        <span className={`text-[10px] font-mono ${
                          slippage.entryCostDollar > 0.01  ? "text-red-500/70"     :
                          slippage.entryCostDollar < -0.01 ? "text-emerald-500/70" : "text-slate-600"
                        }`}>
                          ({slippage.entryCostDollar > 0 ? "+" : ""}${slippage.entryCostDollar.toFixed(2)})
                        </span>
                      </>
                    ) : <span className="text-slate-600 text-[10px]">—</span>}
                  </div>

                  {/* Fill vs limit order price */}
                  {slippage.fillVsLimitPerShare != null && Math.abs(slippage.fillVsLimitPerShare) > 0.001 && (
                    <div className="flex items-center gap-2">
                      <span className="w-16 text-[10px] text-slate-500 shrink-0">vs limit</span>
                      <span className={`font-mono text-xs ${
                        slippage.fillVsLimitPerShare > 0.005  ? "text-red-400"     :
                        slippage.fillVsLimitPerShare < -0.005 ? "text-emerald-400" : "text-slate-400"
                      }`}>
                        {slippage.fillVsLimitPerShare > 0 ? "+" : ""}{slippage.fillVsLimitPerShare.toFixed(3)}/sh
                      </span>
                    </div>
                  )}

                  {/* Exit fill vs target/stop */}
                  <div className="flex items-center gap-2">
                    <span className="w-16 text-[10px] text-slate-500 shrink-0">Exit fill</span>
                    {slippage.hasExitRef && slippage.exitCostPerShare != null ? (
                      <>
                        <span className={`font-mono text-xs font-semibold ${
                          slippage.exitCostPerShare > 0.005  ? "text-red-400"     :
                          slippage.exitCostPerShare < -0.005 ? "text-emerald-400" : "text-slate-400"
                        }`}>
                          {slippage.exitCostPerShare > 0 ? "+" : ""}{slippage.exitCostPerShare.toFixed(3)}/sh
                        </span>
                        <span className={`text-[10px] font-mono ${
                          slippage.exitCostDollar > 0.01  ? "text-red-500/70"     :
                          slippage.exitCostDollar < -0.01 ? "text-emerald-500/70" : "text-slate-600"
                        }`}>
                          ({slippage.exitCostDollar > 0 ? "+" : ""}${slippage.exitCostDollar.toFixed(2)})
                        </span>
                      </>
                    ) : (
                      <span className="text-slate-600 text-[10px] italic">
                        {exitType === "manual" ? "manual exit" : "—"}
                      </span>
                    )}
                  </div>

                  {/* Divider */}
                  <div className="border-t border-slate-800 my-0.5" />

                  {/* Total round-trip cost */}
                  <div className="flex items-center gap-2">
                    <span className="w-16 text-[10px] text-slate-500 shrink-0">Total cost</span>
                    {slippage.totalCostDollar != null ? (
                      <span className={`font-mono text-sm font-bold ${
                        slippage.totalCostDollar > 0.01  ? "text-red-400"     :
                        slippage.totalCostDollar < -0.01 ? "text-emerald-400" : "text-slate-400"
                      }`}>
                        {slippage.totalCostDollar > 0 ? "+" : ""}${slippage.totalCostDollar.toFixed(2)}
                      </span>
                    ) : <span className="text-slate-600 text-xs">—</span>}
                    {slippage.pctOfRisk != null && (
                      <span className={`text-[10px] font-mono ${
                        slippage.pctOfRisk > 5   ? "text-red-500/80"     :
                        slippage.pctOfRisk < -5  ? "text-emerald-500/80" : "text-slate-600"
                      }`}>
                        {slippage.pctOfRisk > 0 ? "+" : ""}{slippage.pctOfRisk.toFixed(1)}% of risk
                      </span>
                    )}
                  </div>

                </div>
              </div>
            )}

            {/* Exit Method */}
            {!order.is_open && (
              <div className="flex flex-col justify-center px-5 py-3 shrink-0 border-l border-slate-700/50">
                <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-slate-500 mb-2">
                  Exit Method
                </span>
                <ExitMethodBadge method={fixExitMethodResult?.exit_method ?? order.exit_method} />

                {/* Backfill button — only shown when exit_method is unrecorded */}
                {!order.exit_method && !fixExitMethodResult?.exit_method && (
                  <button
                    onClick={async () => {
                      setFixingExitMethod(true);
                      setFixExitMethodResult(null);
                      try {
                        const res = await alpacaApi.fixExitMethod(order.id);
                        setFixExitMethodResult(res.data);
                        // Patch the local order object so badge + digest both update
                        if (res.data.exit_method) order.exit_method = res.data.exit_method;
                      } catch (err) {
                        setFixExitMethodResult({ error: err?.response?.data?.error || "Failed to detect exit method" });
                      } finally {
                        setFixingExitMethod(false);
                      }
                    }}
                    disabled={fixingExitMethod}
                    className="mt-2 flex items-center gap-1 text-[9px] text-amber-400/80 hover:text-amber-300 border border-amber-700/40 hover:border-amber-600 bg-amber-900/20 hover:bg-amber-900/30 rounded px-2 py-1 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {fixingExitMethod
                      ? <><Loader2 className="w-2.5 h-2.5 animate-spin" /> Detecting…</>
                      : <><RefreshCw className="w-2.5 h-2.5" /> Detect from Alpaca</>
                    }
                  </button>
                )}
                {fixExitMethodResult?.error && (
                  <span className="mt-1 text-[9px] text-red-400/80">{fixExitMethodResult.error}</span>
                )}
                {fixExitMethodResult?.exit_method && (
                  <span className="mt-1 text-[9px] text-emerald-400/70 italic">
                    {fixExitMethodResult.source === "inferred" ? "Detected & saved" : "Saved"}
                  </span>
                )}
              </div>
            )}

            {/* Forensic Digest + AI Analysis toggles */}
            {!order.is_open && (
              <div className="flex items-center gap-2 px-4 py-3 shrink-0 border-l border-slate-700/50 ml-auto">
                {/* Digest toggle */}
                <div className="flex flex-col items-center gap-1">
                  <button
                    onClick={() => setShowDigest(v => !v)}
                    className={`flex items-center gap-1.5 text-[10px] font-semibold px-3 py-1.5 rounded border transition-colors ${
                      showDigest
                        ? "bg-indigo-900/50 border-indigo-600/60 text-indigo-300 hover:bg-indigo-900/70"
                        : "bg-slate-800/60 border-slate-600/50 text-slate-400 hover:bg-slate-700/60 hover:text-slate-200"
                    }`}
                  >
                    <ClipboardList className="w-3 h-3" />
                    {showDigest ? "Hide Digest" : "Trade Digest"}
                  </button>
                </div>

                {/* AI Analysis button */}
                <div className="flex flex-col items-center gap-1">
                  <button
                    onClick={() => {
                      if (aiAnalysis?.text || aiAnalysis?.error) {
                        setAiAnalysis(null);
                      } else {
                        handleAIAnalysis();
                      }
                    }}
                    disabled={aiAnalysis?.loading}
                    className={`flex items-center gap-1.5 text-[10px] font-semibold px-3 py-1.5 rounded border transition-colors disabled:opacity-60 disabled:cursor-not-allowed ${
                      aiAnalysis?.text
                        ? "bg-violet-900/50 border-violet-600/60 text-violet-300 hover:bg-violet-900/70"
                        : aiAnalysis?.error
                        ? "bg-red-900/40 border-red-700/50 text-red-400 hover:bg-red-900/60"
                        : "bg-slate-800/60 border-slate-600/50 text-slate-400 hover:bg-slate-700/60 hover:text-slate-200"
                    }`}
                  >
                    {aiAnalysis?.loading
                      ? <Loader2 className="w-3 h-3 animate-spin" />
                      : <Sparkles className="w-3 h-3" />
                    }
                    {aiAnalysis?.loading ? "Analyzing…"
                      : aiAnalysis?.text  ? "Hide"
                      : aiAnalysis?.error ? "Retry"
                      : "Analysis"}
                  </button>
                </div>
              </div>
            )}

          </div>
        )}

        {/* ── CLOSED TRADE: Forensic Digest ── */}
        {!order.is_open && rr && showDigest && (
          <TradeForensicDigest
            order={order}
            rr={rr}
            slippage={slippage}
            effectiveRR={effectiveRR}
            rAchieved={rAchieved}
            tradeDuration={tradeDuration}
            closedPl={closedPl}
            closedPctChange={closedPctChange}
            exitType={exitType}
            isClosedWin={isClosedWin}
            isBreakeven={isBreakeven}
          />
        )}

        {/* ── CLOSED TRADE: AI Analysis panel ── */}
        {!order.is_open && aiAnalysis && (
          <div className="border-b border-slate-700/80 bg-[#0a0718] shrink-0 overflow-y-auto" style={{ maxHeight: "280px" }}>
            {/* Header */}
            <div className="flex items-center gap-2 px-4 py-1.5 border-b border-slate-800/80 bg-slate-950/60 sticky top-0">
              <Sparkles className="w-3 h-3 text-violet-400" />
              <span className="text-[9px] font-bold uppercase tracking-[0.2em] text-violet-300">AI Trade Analysis</span>

              {/* Audio status indicator */}
              <div className="flex items-center gap-1.5 ml-3">
                {audioState?.loading && (
                  <span className="flex items-center gap-1 text-[9px] text-amber-400/80">
                    <Loader2 className="w-2.5 h-2.5 animate-spin" /> Generating audio…
                  </span>
                )}
                {audioState?.playing && (
                  <span className="flex items-center gap-1 text-[9px] text-emerald-400/80 animate-pulse">
                    <Volume2 className="w-2.5 h-2.5" /> Playing
                  </span>
                )}
                {audioState?.url && !audioState.playing && !audioState.loading && (
                  <span className="flex items-center gap-1 text-[9px] text-slate-500">
                    <VolumeX className="w-2.5 h-2.5" /> Audio ready
                  </span>
                )}
                {audioState?.error && (
                  <span className="flex items-center gap-1 text-[9px] text-red-400/80">
                    <AlertCircle className="w-2.5 h-2.5" /> Audio error
                  </span>
                )}
              </div>

              {/* Audio controls */}
              <div className="flex items-center gap-1 ml-1">
                {audioState?.playing && (
                  <button
                    onClick={stopAudio}
                    title="Stop audio"
                    className="flex items-center gap-1 text-[9px] text-slate-400 hover:text-red-400 transition-colors px-1.5 py-0.5 rounded border border-slate-700/50 hover:border-red-700/50"
                  >
                    <Square className="w-2.5 h-2.5" /> Stop
                  </button>
                )}
                {audioState?.url && !audioState.playing && !audioState.loading && (
                  <button
                    onClick={() => {
                      if (audioRef.current) {
                        audioRef.current.currentTime = 0;
                        audioRef.current.play();
                        setAudioState(prev => ({ ...prev, playing: true }));
                      } else if (audioState.url) {
                        const audio = new Audio(audioState.url);
                        audioRef.current = audio;
                        audio.onended = () => setAudioState(prev => ({ ...prev, playing: false }));
                        audio.play();
                        setAudioState(prev => ({ ...prev, playing: true }));
                      }
                    }}
                    title="Replay audio"
                    className="flex items-center gap-1 text-[9px] text-slate-400 hover:text-emerald-400 transition-colors px-1.5 py-0.5 rounded border border-slate-700/50 hover:border-emerald-700/50"
                  >
                    <Volume2 className="w-2.5 h-2.5" /> Replay
                  </button>
                )}
                {aiAnalysis.text && (
                  <button
                    onClick={() => handleTTS(aiAnalysis.text)}
                    disabled={audioState?.loading}
                    title="Re-generate audio"
                    className="flex items-center gap-1 text-[9px] text-slate-500 hover:text-amber-400 transition-colors disabled:opacity-40 disabled:cursor-not-allowed px-1.5 py-0.5 rounded border border-slate-700/50 hover:border-amber-700/50"
                  >
                    <RefreshCw className="w-2.5 h-2.5" /> New audio
                  </button>
                )}
              </div>

              {aiAnalysis.text && (
                <button
                  onClick={handleAIAnalysis}
                  className="ml-auto flex items-center gap-1 text-[9px] text-slate-500 hover:text-slate-300 transition-colors"
                >
                  <RefreshCw className="w-2.5 h-2.5" /> Regenerate text
                </button>
              )}
            </div>

            {/* Loading */}
            {aiAnalysis.loading && (
              <div className="flex items-center gap-2 px-5 py-6 text-slate-500 text-xs">
                <Loader2 className="w-4 h-4 animate-spin text-violet-400" />
                <span>Analyzing…</span>
              </div>
            )}

            {/* Error */}
            {aiAnalysis.error && (
              <div className="flex items-start gap-2 px-5 py-4 text-red-400 text-xs">
                <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
                <span>{aiAnalysis.error}</span>
              </div>
            )}

            {/* Result — only shown once audio has started (or failed to generate) */}
            {aiAnalysis.text && (audioState?.playing || audioState?.url || audioState?.error) && (
              <div className="px-5 py-4 text-[12px] text-slate-300 leading-relaxed space-y-3">
                {aiAnalysis.text.split(/\n\n+/).map((para, i) => (
                  <p key={i}>{para}</p>
                ))}
              </div>
            )}

            {/* Waiting for audio — show a holding message */}
            {aiAnalysis.text && audioState?.loading && (
              <div className="flex items-center gap-2 px-5 py-6 text-slate-600 text-xs italic">
                <Volume2 className="w-3.5 h-3.5 text-amber-500/60" />
                <span>Generating audio — analysis will appear when playback begins…</span>
              </div>
            )}
          </div>
        )}

        {/* ── CLOSED TRADE: Plain-English narrative ── */}
        {!order.is_open && rr && (
          <TradeNarrative
            order={order}
            rr={rr}
            slippage={slippage}
            effectiveRR={effectiveRR}
          />
        )}

        {/* ── OPEN TRADE: Level strip ── */}
        {order.is_open && rr && (
          <div className="flex items-center gap-x-5 px-4 py-1.5 border-b border-slate-700/60 bg-slate-950/50 shrink-0 text-xs">
            <span className="text-slate-500">Entry <span className="font-mono font-bold text-slate-200">${rr.entry.toFixed(2)}</span></span>
            {rr.fillPrice != null && Math.abs(rr.fillPrice - rr.entry) > 0.005 && (
              <span className="text-slate-500">Fill <span className="font-mono text-yellow-400">${rr.fillPrice.toFixed(2)}</span></span>
            )}
            <span className="text-slate-500">Stop  <span className="font-mono font-bold text-red-400">${rr.stop.toFixed(2)}</span></span>
            <span className="text-slate-500">Target <span className="font-mono font-bold text-emerald-400">${rr.target.toFixed(2)}</span></span>
            <span className="text-slate-500">
              R/R <span className="font-mono font-bold text-white">{effectiveRR ?? "—"}</span>
              {rr.rrRatio != null && effectiveRR != null && Math.abs(Number(effectiveRR) - Number(rr.rrRatio)) > 0.05 && (
                <span className="text-slate-600 ml-1">(intended {Number(rr.rrRatio).toFixed(1)})</span>
              )}
            </span>
            <span className="text-slate-500">Qty <span className="font-mono text-slate-300">{rr.qty}</span></span>
            {rr.threshold != null && (
              <span className="text-slate-500">Ref <span className="font-mono text-fuchsia-400">${Number(rr.threshold).toFixed(2)}</span></span>
            )}
            {order.entry_time != null && (
              <span className="ml-auto text-slate-500">
                Start <span className="font-mono text-slate-300">{etTime.dateFormat("%b %e %H:%M ET", order.entry_time)}</span>
              </span>
            )}
          </div>
        )}

        {/* ── OPEN TRADE: Live P/L bar ── */}
        {order.is_open && (
          <div className="flex items-center gap-6 px-5 py-2.5 border-b border-slate-700/60 shrink-0">
            {/* Big P/L number */}
            <div className="flex flex-col">
              <span className="text-[10px] text-slate-500 uppercase tracking-wider leading-none mb-0.5">Unrealized P/L</span>
              <span className={`text-xl font-bold font-mono leading-none ${plColor}`}>
                {livePl != null
                  ? `${plPrefix}$${Math.abs(livePl).toFixed(2)}`
                  : <span className="text-slate-600 text-sm">—</span>}
              </span>
            </div>

            {/* Pct change */}
            {pctChange != null && (
              <div className="flex flex-col">
                <span className="text-[10px] text-slate-500 uppercase tracking-wider leading-none mb-0.5">Change</span>
                <span className={`text-sm font-semibold font-mono leading-none ${plColor}`}>
                  {pctChange >= 0 ? "+" : ""}{pctChange.toFixed(2)}%
                </span>
              </div>
            )}

            {/* Current price */}
            {currentPrice != null && (
              <div className="flex flex-col">
                <span className="text-[10px] text-slate-500 uppercase tracking-wider leading-none mb-0.5">Current</span>
                <span className="text-sm font-semibold font-mono text-slate-200 leading-none">
                  ${currentPrice.toFixed(2)}
                </span>
              </div>
            )}

            {/* Fill / limit price */}
            {fillPrice != null && (
              <div className="flex flex-col">
                <span className="text-[10px] text-slate-500 uppercase tracking-wider leading-none mb-0.5">
                  {order.filled_avg_price != null ? "Fill" : "Limit"}
                </span>
                <span className="text-sm font-mono text-slate-400 leading-none">
                  ${Number(fillPrice).toFixed(2)}
                </span>
              </div>
            )}

            {/* Stop → Target progress bar */}
            {plProgress != null && rr && (
              <div className="flex-1 flex flex-col gap-1 ml-2">
                <div className="flex justify-between text-[10px] text-slate-600 font-mono">
                  <span className="text-red-500">Stop ${rr.stop.toFixed(2)}</span>
                  <span className="text-slate-500">Entry ${rr.entry.toFixed(2)}</span>
                  <span className="text-emerald-500">Target ${rr.target.toFixed(2)}</span>
                </div>
                <div className="relative h-2 rounded-full bg-slate-800 overflow-hidden">
                  <div className="absolute left-0 top-0 h-full bg-red-900/60 rounded-l-full"
                    style={{ width: `${(Math.abs(rr.entry - rr.stop) / Math.abs(rr.target - rr.stop)) * 100}%` }} />
                  <div className="absolute top-0 h-full bg-emerald-900/60 rounded-r-full"
                    style={{ left: `${(Math.abs(rr.entry - rr.stop) / Math.abs(rr.target - rr.stop)) * 100}%`, right: "0" }} />
                  <div
                    className={`absolute top-0 w-0.5 h-full ${livePl != null && livePl >= 0 ? "bg-emerald-400" : "bg-red-400"}`}
                    style={{ left: `${plProgress * 100}%` }}
                  />
                </div>
              </div>
            )}

            {liveQuote?.fetching && <RefreshCw className="w-3 h-3 text-slate-600 animate-spin ml-auto" />}
          </div>
        )}

        {/* ── Chart ── */}
        <div ref={containerRef} className="flex-1 min-h-0 overflow-hidden">
          {(loading || chartH == null) && !error && (
            <div className="h-full flex items-center justify-center gap-2 text-slate-400">
              <Loader2 className="w-5 h-5 animate-spin" />
              <span className="text-sm">Loading chart…</span>
            </div>
          )}
          {error && (
            <div className="h-full flex items-center justify-center gap-2 text-red-400">
              <AlertCircle className="w-4 h-4" />
              <span className="text-sm">{error}</span>
            </div>
          )}
          {ready && (
            <HighchartsReact
              ref={chartRef}
              highcharts={Highcharts}
              constructorType="stockChart"
              options={chartOptions}
              containerProps={{ style: { height: `${chartH}px`, width: "100%" } }}
            />
          )}
        </div>
      </div>
    </div>
  );
}
