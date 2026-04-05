/**
 * AnalyticsPanel — trading performance analytics dashboard.
 * Receives `orders` (already synced) and `loading` from AdminPanel.
 */
import React, { useMemo, useState } from "react";
import Highcharts from "highcharts";
import HighchartsReact from "highcharts-react-official";
import {
  TrendingUp, TrendingDown, Activity, Clock,
  Award, AlertTriangle, BarChart2, Zap, Target,
} from "lucide-react";

// ── Highcharts dark theme ─────────────────────────────────────────────────────
Highcharts.setOptions({
  chart:   { backgroundColor: "transparent", style: { fontFamily: "inherit" } },
  title:   { text: null },
  credits: { enabled: false },
  xAxis:   { labels: { style: { color: "#64748b", fontSize: "10px" } }, lineColor: "#334155", tickColor: "#334155", gridLineColor: "transparent" },
  yAxis:   { labels: { style: { color: "#64748b", fontSize: "10px" } }, gridLineColor: "#1e293b", title: { text: null } },
  legend:  { itemStyle: { color: "#94a3b8", fontSize: "11px" }, itemHoverStyle: { color: "#e2e8f0" } },
  tooltip: { backgroundColor: "#0f172a", borderColor: "#334155", borderRadius: 6, style: { color: "#e2e8f0", fontSize: "11px" } },
  plotOptions: { series: { animation: false } },
});

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmt$ = (v, digits = 2) => {
  if (v == null || isNaN(v)) return "—";
  const abs = Math.abs(v);
  const str = abs >= 1000 ? `$${(abs / 1000).toFixed(1)}k` : `$${abs.toFixed(digits)}`;
  return v < 0 ? `-${str}` : str;
};

function histogram(values, buckets = 16) {
  if (!values.length) return { data: [] };
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const size = range / buckets;
  const bins = Array(buckets).fill(0);
  values.forEach(v => {
    const idx = Math.min(Math.floor((v - min) / size), buckets - 1);
    bins[idx]++;
  });
  return {
    data: bins.map((count, i) => {
      const mid = min + i * size + size / 2;
      return { x: mid, y: count, color: mid >= 0 ? "#34d399" : "#f87171" };
    }),
  };
}

function etHour(utcMs) {
  const et = new Date(new Date(utcMs).toLocaleString("en-US", { timeZone: "America/New_York" }));
  return et.getHours() + et.getMinutes() / 60;
}
function etDow(utcMs) {
  return new Date(new Date(utcMs).toLocaleString("en-US", { timeZone: "America/New_York" })).getDay();
}
function sessionLabel(h) {
  if (h < 9.5) return "Pre-market";
  if (h < 16)  return "Regular";
  return "Post-market";
}
function stdDev(arr) {
  if (!arr.length) return 0;
  const mean = arr.reduce((s, v) => s + v, 0) / arr.length;
  return Math.sqrt(arr.reduce((s, v) => s + (v - mean) ** 2, 0) / arr.length);
}

// ── Sub-components ────────────────────────────────────────────────────────────
function SectionHeader({ title, sub }) {
  return (
    <div className="mb-3 pb-2 border-b border-slate-800/60">
      <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  );
}

function ChartBox({ title, children, className = "" }) {
  return (
    <div className={`bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 ${className}`}>
      {title && <p className="text-xs font-semibold text-slate-400 mb-2">{title}</p>}
      {children}
    </div>
  );
}

function StatTile({ label, value, sub, color = "text-slate-100", icon: Icon }) {
  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 flex flex-col gap-1.5">
      <div className="flex items-center justify-between">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">{label}</span>
        {Icon && <Icon className="w-3.5 h-3.5 text-slate-500" />}
      </div>
      <span className={`text-xl font-bold font-mono leading-tight ${color}`}>{value}</span>
      {sub && <span className="text-[11px] text-slate-400 leading-tight">{sub}</span>}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function AnalyticsPanel({ orders, loading }) {

  const closed = useMemo(() =>
    orders
      .filter(o => !o.is_open && o.unrealized_pl != null && o.synced_at)
      .sort((a, b) => new Date(a.synced_at) - new Date(b.synced_at)),
    [orders]
  );

  // ── Core stats ──────────────────────────────────────────────────────────────
  const stats = useMemo(() => {
    if (!closed.length) return null;
    const pls     = closed.map(o => Number(o.unrealized_pl));
    const winners = pls.filter(p => p > 0);
    const losers  = pls.filter(p => p < 0);
    const totalWin  = winners.reduce((s, p) => s + p, 0);
    const totalLoss = losers.reduce((s, p) => s + p, 0);
    const profitFactor = totalLoss !== 0 ? totalWin / Math.abs(totalLoss) : Infinity;
    const winRate  = winners.length / pls.length;
    const avgWin   = winners.length ? totalWin  / winners.length : 0;
    const avgLoss  = losers.length  ? totalLoss / losers.length  : 0;
    const expectancy = winRate * avgWin + (1 - winRate) * avgLoss;

    // Cumulative P&L + drawdown
    let cum = 0, peak = 0, maxDD = 0, maxDDPct = 0;
    closed.forEach(o => {
      cum += Number(o.unrealized_pl);
      if (cum > peak) peak = cum;
      const dd = cum - peak;
      const ddPct = peak !== 0 ? (dd / peak) * 100 : 0;
      if (dd < maxDD) { maxDD = dd; maxDDPct = ddPct; }
    });

    // Streaks
    let curW = 0, curL = 0, bestWin = 0, bestLoss = 0;
    pls.forEach(p => {
      if (p > 0) { curW++; curL = 0; if (curW > bestWin)  bestWin  = curW; }
      else       { curL++; curW = 0; if (curL > bestLoss) bestLoss = curL; }
    });

    // Daily returns for Sharpe / Sortino
    const byDay = {};
    closed.forEach(o => {
      const day = o.synced_at.slice(0, 10);
      byDay[day] = (byDay[day] || 0) + Number(o.unrealized_pl);
    });
    const dailyRets = Object.values(byDay);
    const meanDay   = dailyRets.reduce((s, r) => s + r, 0) / (dailyRets.length || 1);
    const std       = stdDev(dailyRets);
    const downRets  = dailyRets.filter(r => r < 0);
    const stdDown   = stdDev(downRets.map(r => r)); // downside std
    const sharpe    = std    > 0 ? (meanDay / std)    * Math.sqrt(252) : null;
    const sortino   = stdDown > 0 ? (meanDay / stdDown) * Math.sqrt(252) : null;

    return {
      totalWin, totalLoss,
      profitFactor, winRate, avgWin, avgLoss, expectancy,
      maxDD, maxDDPct,
      bestWin, bestLoss,
      sharpe, sortino,
      best:  Math.max(...pls),
      worst: Math.min(...pls),
    };
  }, [closed]);

  // ── Cumulative P&L + drawdown chart ─────────────────────────────────────────
  const cumulativeOpts = useMemo(() => {
    if (!closed.length) return null;
    let cum = 0, peak = 0;
    const cumData = [], ddData = [];
    closed.forEach((o, i) => {
      cum  += Number(o.unrealized_pl);
      if (cum > peak) peak = cum;
      cumData.push([i, parseFloat(cum.toFixed(2))]);
      ddData.push([i, parseFloat((cum - peak).toFixed(2))]);
    });
    return {
      chart:  { height: 220, type: "line", marginTop: 10 },
      xAxis:  { visible: false },
      yAxis: [
        { title: { text: null }, labels: { formatter() { return fmt$(this.value, 0); } } },
        { title: { text: null }, labels: { formatter() { return fmt$(this.value, 0); }, style: { color: "#f87171" } }, opposite: true, max: 0 },
      ],
      series: [
        { name: "Cum. P&L", data: cumData, color: "#34d399", lineWidth: 2, marker: { enabled: false }, yAxis: 0 },
        { name: "Drawdown", data: ddData, type: "area", color: "#f87171", fillOpacity: 0.12, lineWidth: 1, marker: { enabled: false }, yAxis: 1 },
      ],
      legend: { enabled: true, align: "right", verticalAlign: "top" },
      tooltip: {
        shared: true,
        formatter() {
          return this.points.map(p => `<b>${p.series.name}</b>: ${fmt$(p.y)}`).join("<br>");
        },
      },
    };
  }, [closed]);

  // ── P&L by Symbol (top 12 horizontal bar) ────────────────────────────────────
  const symbolOpts = useMemo(() => {
    if (!closed.length) return null;
    const map = {};
    closed.forEach(o => { map[o.ticker] = (map[o.ticker] || 0) + Number(o.unrealized_pl); });
    const sorted = Object.entries(map).sort(([, a], [, b]) => Math.abs(b) - Math.abs(a)).slice(0, 12);
    return {
      chart:  { height: 260, type: "bar", marginTop: 10 },
      xAxis:  { categories: sorted.map(([t]) => t), labels: { style: { fontSize: "10px" } } },
      yAxis:  { title: { text: null }, labels: { formatter() { return fmt$(this.value, 0); } },
        plotLines: [{ value: 0, color: "#334155", width: 1 }] },
      series: [{
        name: "P&L", showInLegend: false,
        data: sorted.map(([, v]) => ({ y: parseFloat(v.toFixed(2)), color: v >= 0 ? "#34d399" : "#f87171" })),
      }],
      tooltip: { formatter() { return `<b>${this.x}</b>: ${fmt$(this.y)}`; } },
    };
  }, [closed]);

  // ── P&L by Day of Week (Mon–Fri net P&L + win rate dots) ─────────────────────
  const dowOpts = useMemo(() => {
    if (!closed.length) return null;
    const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"];
    const plMap  = { Mon: 0, Tue: 0, Wed: 0, Thu: 0, Fri: 0 };
    const winMap = { Mon: 0, Tue: 0, Wed: 0, Thu: 0, Fri: 0 };
    const cntMap = { Mon: 0, Tue: 0, Wed: 0, Thu: 0, Fri: 0 };
    closed.forEach(o => {
      const d = new Date(o.synced_at).getDay(); // 1=Mon…5=Fri
      const label = ["", "Mon", "Tue", "Wed", "Thu", "Fri"][d];
      if (!label) return;
      const pl = Number(o.unrealized_pl);
      plMap[label]  += pl;
      cntMap[label] += 1;
      if (pl > 0) winMap[label] += 1;
    });
    const plData  = DAYS.map(d => ({ y: parseFloat(plMap[d].toFixed(2)), color: plMap[d] >= 0 ? "#34d399" : "#f87171" }));
    const wrData  = DAYS.map(d => cntMap[d] ? parseFloat(((winMap[d] / cntMap[d]) * 100).toFixed(1)) : null);
    const cntData = DAYS.map(d => cntMap[d]);
    return {
      chart:  { height: 260, marginTop: 10 },
      xAxis:  { categories: DAYS },
      yAxis: [
        { title: { text: null }, labels: { formatter() { return fmt$(this.value, 0); } },
          plotLines: [{ value: 0, color: "#334155", width: 1 }] },
        { title: { text: null }, opposite: true, min: 0, max: 100,
          labels: { formatter() { return `${this.value}%`; }, style: { color: "#60a5fa" } } },
      ],
      series: [
        { name: "Net P&L",  type: "column", data: plData,  yAxis: 0, borderWidth: 0 },
        { name: "Win Rate", type: "line",   data: wrData,  yAxis: 1, color: "#60a5fa",
          lineWidth: 2, marker: { enabled: true, radius: 4 },
          tooltip: { valueSuffix: "%" } },
      ],
      legend: { enabled: true, align: "right", verticalAlign: "top" },
      tooltip: {
        shared: true,
        formatter() {
          const idx = this.points[0]?.point.index ?? 0;
          const n = cntData[idx];
          return [
            `<b>${this.x}</b> (${n} trade${n !== 1 ? "s" : ""})`,
            ...this.points.map(p =>
              `${p.series.name}: <b>${p.series.name === "Win Rate" ? `${p.y}%` : fmt$(p.y)}</b>`
            ),
          ].join("<br>");
        },
      },
    };
  }, [closed]);

  // ── R-Multiple breakdown ──────────────────────────────────────────────────────
  const rMultipleData = useMemo(() => {
    const buckets = [
      { label: "≤ -2R", min: -Infinity, max: -2 },
      { label: "-2R",   min: -2,        max: -1 },
      { label: "-1R",   min: -1,        max:  0 },
      { label: "0–1R",  min:  0,        max:  1 },
      { label: "1–2R",  min:  1,        max:  2 },
      { label: "2–3R",  min:  2,        max:  3 },
      { label: "≥ 3R",  min:  3,        max: Infinity },
    ];
    const trades = closed.filter(o => o.risk_amt != null && Number(o.risk_amt) > 0);
    const counts = buckets.map(() => 0);
    trades.forEach(o => {
      const r = Number(o.unrealized_pl) / Number(o.risk_amt);
      const i = buckets.findIndex(b => r >= b.min && r < b.max);
      if (i >= 0) counts[i]++;
    });
    return { buckets: buckets.map(b => b.label), counts, total: trades.length };
  }, [closed]);

  // ── P&L distribution histogram ────────────────────────────────────────────────
  const plHistOpts = useMemo(() => {
    if (!closed.length) return null;
    const { data } = histogram(closed.map(o => Number(o.unrealized_pl)));
    return {
      chart:  { height: 185, type: "column", marginTop: 10 },
      xAxis:  { title: { text: "P&L ($)", style: { color: "#64748b", fontSize: "10px" } }, labels: { formatter() { return fmt$(this.value, 0); } } },
      yAxis:  { title: { text: "Trades", style: { color: "#64748b", fontSize: "10px" } }, allowDecimals: false },
      series: [{ name: "Trades", showInLegend: false, data, borderWidth: 0 }],
      tooltip: { formatter() { return `~${fmt$(this.x)}: <b>${this.y}</b> trade${this.y !== 1 ? "s" : ""}`; } },
    };
  }, [closed]);

  // ── Duration histogram ────────────────────────────────────────────────────────
  const durationOpts = useMemo(() => {
    const withDur = orders.filter(o => o.created_at && o.synced_at);
    if (!withDur.length) return null;
    const mins = withDur
      .map(o => (new Date(o.synced_at) - new Date(o.created_at + (o.created_at.endsWith("Z") ? "" : "Z"))) / 60000)
      .filter(m => m > 0);
    if (!mins.length) return null;
    const { data } = histogram(mins);
    return {
      chart:  { height: 185, type: "column", marginTop: 10 },
      xAxis:  { title: { text: "Duration (min)", style: { color: "#64748b", fontSize: "10px" } }, labels: { formatter() { return `${Math.round(this.value)}m`; } } },
      yAxis:  { title: { text: "Trades", style: { color: "#64748b", fontSize: "10px" } }, allowDecimals: false },
      series: [{ name: "Trades", showInLegend: false, data: data.map(d => ({ ...d, color: "#60a5fa" })), borderWidth: 0 }],
      tooltip: { formatter() { return `~${Math.round(this.x)}m: <b>${this.y}</b> trade${this.y !== 1 ? "s" : ""}`; } },
    };
  }, [orders]);

  // ── Slippage histogram ────────────────────────────────────────────────────────
  const slippageOpts = useMemo(() => {
    const slips = closed
      .filter(o => o.filled_avg_price != null && o.entry_price != null)
      .map(o => {
        const fill  = Number(o.filled_avg_price);
        const limit = Number(o.entry_price);
        return o.direction === "long" ? fill - limit : limit - fill;
      });
    if (!slips.length) return null;
    const { data } = histogram(slips);
    return {
      chart:  { height: 185, type: "column", marginTop: 10 },
      xAxis:  { title: { text: "Slippage ($)", style: { color: "#64748b", fontSize: "10px" } }, labels: { formatter() { return `$${this.value.toFixed(2)}`; } } },
      yAxis:  { title: { text: "Trades", style: { color: "#64748b", fontSize: "10px" } }, allowDecimals: false },
      series: [{ name: "Trades", showInLegend: false, data, borderWidth: 0 }],
      tooltip: { formatter() { return `~$${this.x.toFixed(2)} slip: <b>${this.y}</b> trade${this.y !== 1 ? "s" : ""}`; } },
    };
  }, [closed]);

  // ── Direction attribution (pie chart) ────────────────────────────────────────
  const dirPieOpts = useMemo(() => {
    if (!closed.length) return null;
    const map = {};
    const cnt = {};
    closed.forEach(o => {
      const d = o.direction ?? "unknown";
      map[d] = (map[d] || 0) + Number(o.unrealized_pl);
      cnt[d] = (cnt[d] || 0) + 1;
    });
    const DIR_COLORS = { long: "#34d399", short: "#f87171", unknown: "#64748b" };
    const data = Object.entries(map).map(([dir, pl]) => ({
      name:  dir.charAt(0).toUpperCase() + dir.slice(1),
      y:     parseFloat(Math.abs(pl).toFixed(2)),
      pl,
      count: cnt[dir],
      color: DIR_COLORS[dir] ?? "#94a3b8",
    }));
    return {
      chart:  { height: 220, type: "pie", marginTop: 0 },
      plotOptions: {
        pie: {
          innerSize: "52%",
          dataLabels: {
            enabled: true,
            format: "{point.name}",
            style: { color: "#94a3b8", fontSize: "11px", fontWeight: "600", textOutline: "none" },
            distance: 14,
          },
          borderWidth: 0,
        },
      },
      series: [{
        name: "P&L",
        data,
      }],
      tooltip: {
        formatter() {
          const sign = this.point.pl >= 0 ? "+" : "";
          return `<b>${this.point.name}</b><br>${this.point.count} trade${this.point.count !== 1 ? "s" : ""}<br>P&L: <b>${sign}${fmt$(this.point.pl)}</b>`;
        },
      },
    };
  }, [closed]);

  // ── Time-of-day heatmap ───────────────────────────────────────────────────────
  const tod = useMemo(() => {
    const DAYS  = ["Mon", "Tue", "Wed", "Thu", "Fri"];
    const HOURS = Array.from({ length: 14 }, (_, i) => i + 6); // 06–19 ET
    const grid  = {};
    closed.forEach(o => {
      const tMs = o.entry_time ?? (o.created_at ? new Date(o.created_at + (o.created_at.endsWith("Z") ? "" : "Z")).getTime() : null);
      if (!tMs) return;
      const h   = Math.floor(etHour(tMs));
      const dow = etDow(tMs);
      if (dow < 1 || dow > 5 || h < 6 || h > 19) return;
      const k = `${dow}-${h}`;
      if (!grid[k]) grid[k] = { pl: 0, count: 0 };
      grid[k].pl    += Number(o.unrealized_pl);
      grid[k].count += 1;
    });
    // find max abs pl for colour scaling
    const maxAbs = Math.max(1, ...Object.values(grid).map(c => Math.abs(c.pl)));
    return { DAYS, HOURS, grid, maxAbs };
  }, [closed]);

  // ── Empty / loading states ─────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
        Loading analytics…
      </div>
    );
  }
  if (!closed.length) {
    return (
      <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-10 text-center">
        <BarChart2 className="w-10 h-10 text-slate-500 mx-auto mb-3" />
        <p className="text-slate-400 font-medium">No closed trade data yet</p>
        <p className="text-slate-500 text-xs mt-1">Analytics will appear after trades are closed and synced</p>
      </div>
    );
  }

  const netPL = (stats?.totalWin ?? 0) + (stats?.totalLoss ?? 0);

  return (
    <div className="flex flex-col gap-8">

      {/* ══ P&L Analysis ════════════════════════════════════════════════════════ */}
      <div>
        <SectionHeader
          title="P&L Analysis"
          sub={`${closed.length} closed trade${closed.length !== 1 ? "s" : ""} · Net ${fmt$(netPL)}`}
        />

        {/* Cumulative P&L + Drawdown */}
        <ChartBox title="Cumulative P&L + Drawdown Overlay" className="mb-4">
          {cumulativeOpts && <HighchartsReact highcharts={Highcharts} options={cumulativeOpts} />}
        </ChartBox>

        {/* Symbol attribution + Day-of-week */}
        <div className="grid grid-cols-2 gap-4 mb-4">
          <ChartBox title="P&L by Symbol">
            {symbolOpts
              ? <HighchartsReact highcharts={Highcharts} options={symbolOpts} />
              : <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No data</div>}
          </ChartBox>
          <ChartBox title="P&L by Day of Week">
            {dowOpts
              ? <HighchartsReact highcharts={Highcharts} options={dowOpts} />
              : <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No data</div>}
          </ChartBox>
        </div>

        {/* Direction + R-Multiple breakdown */}
        <div className="grid grid-cols-2 gap-4">
          <ChartBox title="P&L by Direction">
            {dirPieOpts
              ? <HighchartsReact highcharts={Highcharts} options={dirPieOpts} />
              : <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No data</div>}
          </ChartBox>
          <ChartBox title="R-Multiple Breakdown">
            {rMultipleData.total === 0 ? (
              <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No risk data available</div>
            ) : (
              <div className="flex flex-col gap-2 mt-1">
                {rMultipleData.buckets.map((label, i) => {
                  const count = rMultipleData.counts[i];
                  const pct   = rMultipleData.total ? (count / rMultipleData.total) * 100 : 0;
                  const isPos = i >= 3; // 0–1R, 1–2R, 2–3R, ≥3R
                  return (
                    <div key={label} className="flex items-center gap-2">
                      <span className={`text-[11px] font-mono w-12 shrink-0 ${isPos ? "text-emerald-400" : "text-red-400"}`}>{label}</span>
                      <div className="flex-1 h-3 bg-slate-700/60 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all ${isPos ? "bg-emerald-500/70" : "bg-red-500/70"}`}
                          style={{ width: `${Math.min(pct, 100)}%` }}
                        />
                      </div>
                      <span className="text-[11px] text-slate-400 w-10 text-right tabular-nums">{count} <span className="text-slate-600">({pct.toFixed(0)}%)</span></span>
                    </div>
                  );
                })}
                <p className="text-[10px] text-slate-600 mt-1">{rMultipleData.total} trade{rMultipleData.total !== 1 ? "s" : ""} with risk data</p>
              </div>
            )}
          </ChartBox>
        </div>
      </div>

      {/* ══ Trade Statistics ════════════════════════════════════════════════════ */}
      <div>
        <SectionHeader title="Trade Statistics" />
        <div className="grid grid-cols-5 gap-3">
          <StatTile
            icon={Activity}
            label="Profit Factor"
            value={stats.profitFactor === Infinity ? "∞" : stats.profitFactor.toFixed(2)}
            sub={`${(stats.winRate * 100).toFixed(1)}% win rate`}
            color={stats.profitFactor >= 1.5 ? "text-emerald-400" : stats.profitFactor >= 1 ? "text-yellow-400" : "text-red-400"}
          />
          <StatTile
            icon={TrendingUp}
            label="Sharpe Ratio"
            value={stats.sharpe != null ? stats.sharpe.toFixed(2) : "—"}
            sub="Annualised daily"
            color={stats.sharpe == null ? "text-slate-400" : stats.sharpe >= 1 ? "text-emerald-400" : stats.sharpe >= 0 ? "text-yellow-400" : "text-red-400"}
          />
          <StatTile
            icon={TrendingDown}
            label="Sortino Ratio"
            value={stats.sortino != null ? stats.sortino.toFixed(2) : "—"}
            sub="Downside deviation"
            color={stats.sortino == null ? "text-slate-400" : stats.sortino >= 1 ? "text-emerald-400" : stats.sortino >= 0 ? "text-yellow-400" : "text-red-400"}
          />
          <StatTile
            icon={AlertTriangle}
            label="Max Drawdown"
            value={fmt$(stats.maxDD)}
            sub={`${stats.maxDDPct.toFixed(1)}% peak-to-trough`}
            color="text-red-400"
          />
          <StatTile
            icon={Zap}
            label="Expectancy"
            value={fmt$(stats.expectancy)}
            sub="Per trade"
            color={stats.expectancy >= 0 ? "text-emerald-400" : "text-red-400"}
          />
          <StatTile
            icon={Award}
            label="Best Trade"
            value={fmt$(stats.best)}
            color="text-emerald-400"
          />
          <StatTile
            icon={TrendingDown}
            label="Worst Trade"
            value={fmt$(stats.worst)}
            color="text-red-400"
          />
          <StatTile
            icon={TrendingUp}
            label="Avg Win"
            value={fmt$(stats.avgWin)}
            color="text-emerald-400"
          />
          <StatTile
            icon={TrendingDown}
            label="Avg Loss"
            value={fmt$(stats.avgLoss)}
            color="text-red-400"
          />
          <StatTile
            icon={Target}
            label="Win / Loss Streak"
            value={`${stats.bestWin} / ${stats.bestLoss}`}
            sub="Best consecutive"
          />
        </div>
      </div>

      {/* ══ Distribution Analysis ════════════════════════════════════════════════ */}
      <div>
        <SectionHeader title="Distribution Analysis" />

        <div className="grid grid-cols-2 gap-4 mb-4">
          <ChartBox title="P&L Distribution">
            {plHistOpts && <HighchartsReact highcharts={Highcharts} options={plHistOpts} />}
          </ChartBox>
          <ChartBox title="Trade Duration Distribution">
            {durationOpts
              ? <HighchartsReact highcharts={Highcharts} options={durationOpts} />
              : <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No duration data</div>
            }
          </ChartBox>
        </div>

        <div className="grid grid-cols-2 gap-4">
          {/* Time-of-day heatmap */}
          <ChartBox title="Entry Time-of-Day Heatmap (ET) — P&L">
            <div className="mt-1 w-full">
              <div className="flex gap-1 w-full">
                {/* Day labels — fixed narrow column */}
                <div className="flex flex-col gap-0.5 shrink-0 mt-[18px]">
                  {tod.DAYS.map(d => (
                    <div key={d} className="h-5 flex items-center text-[9px] text-slate-400 pr-1 w-8">{d}</div>
                  ))}
                </div>
                {/* Hour columns — each grows to fill available width */}
                <div className="flex-1 min-w-0">
                  {/* Hour labels */}
                  <div className="flex gap-0.5 mb-0.5">
                    {tod.HOURS.map(h => (
                      <div key={h} className="flex-1 text-center text-[8px] text-slate-500 truncate">{h}</div>
                    ))}
                  </div>
                  {/* Grid rows */}
                  {tod.DAYS.map((d, di) => (
                    <div key={d} className="flex gap-0.5 mb-0.5">
                      {tod.HOURS.map(h => {
                        const k    = `${di + 1}-${h}`;
                        const cell = tod.grid[k];
                        const intensity = cell ? Math.min(Math.abs(cell.pl) / tod.maxAbs, 1) : 0;
                        const bg = cell
                          ? cell.pl >= 0
                            ? `rgba(52,211,153,${0.12 + intensity * 0.65})`
                            : `rgba(248,113,113,${0.12 + intensity * 0.65})`
                          : "rgba(30,41,59,0.4)";
                        return (
                          <div
                            key={h}
                            className="flex-1 h-5 rounded-sm cursor-default transition-opacity hover:opacity-80"
                            style={{ backgroundColor: bg }}
                            title={cell
                              ? `${d} ${String(h).padStart(2, "0")}:00 ET — ${cell.count} trade${cell.count !== 1 ? "s" : ""}, P&L: ${fmt$(cell.pl)}`
                              : `${d} ${String(h).padStart(2, "0")}:00 ET — no trades`
                            }
                          />
                        );
                      })}
                    </div>
                  ))}
                </div>
              </div>
              <div className="flex items-center gap-3 mt-2 text-[9px] text-slate-500">
                <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "rgba(248,113,113,0.7)" }} /> Loss</span>
                <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: "rgba(52,211,153,0.7)" }} /> Win</span>
                <span className="text-slate-700">· darker = larger magnitude · hover for details</span>
              </div>
            </div>
          </ChartBox>

          {/* Slippage distribution */}
          <ChartBox title="Fill Slippage Distribution">
            {slippageOpts
              ? <HighchartsReact highcharts={Highcharts} options={slippageOpts} />
              : <div className="flex items-center justify-center h-36 text-slate-500 text-xs">No fill price data available</div>
            }
          </ChartBox>
        </div>
      </div>

    </div>
  );
}
