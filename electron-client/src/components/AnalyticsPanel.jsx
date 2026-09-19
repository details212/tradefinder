/**
 * AnalyticsPanel — trading performance analytics dashboard.
 * P&L curve is Alpaca portfolio history (same feed as My Trades Net P/L).
 * Distribution charts still use local closed-trade rows.
 */
import React, { useMemo, useRef, useEffect, useState } from "react";
import Highcharts from "highcharts";
import HighchartsReact from "highcharts-react-official";
import { BarChart2 } from "lucide-react";
import { alpacaApi } from "../api/client";
import {
  formatSinceLabel,
  hasTradeOnYmd,
  historySinceParam,
  historyToCurve,
  historyUntilParam,
  isRegularEquitySessionYmd,
  todaySessionYmd,
} from "../utils/alpacaPortfolio";
import { entrySlippagePerShare, executionFromTrade } from "../utils/tradeExecution";

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

function fmtDays(v) {
  if (v == null || isNaN(v)) return "—";
  if (v < 1) return `${v.toFixed(2)}d`;
  if (v < 10) return `${v.toFixed(1)}d`;
  return `${Math.round(v)}d`;
}

// ── Sub-components ────────────────────────────────────────────────────────────
function SectionHeader({ title, sub }) {
  return (
    <div className="pb-2 border-b border-slate-800/60">
      <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  );
}

const NET_PERIODS = [
  { id: "day", label: "Day" },
  { id: "week", label: "Week" },
  { id: "month", label: "Month" },
  { id: "all", label: "All" },
];

function PeriodToggle({ value, onChange }) {
  return (
    <span className="flex items-center rounded border border-slate-700/70 overflow-hidden shrink-0">
      {NET_PERIODS.map((p) => (
        <button
          key={p.id}
          type="button"
          onClick={() => onChange(p.id)}
          className={`px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide transition ${
            value === p.id
              ? "bg-brand-500/20 text-brand-400"
              : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
        >
          {p.label}
        </button>
      ))}
    </span>
  );
}

function ChartBox({ title, children, className = "", fill = false, containerRef }) {
  return (
    <div className={`bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 flex flex-col ${fill ? "flex-1 min-h-0" : ""} ${className}`}>
      {title && <p className="text-xs font-semibold text-slate-400 mb-2 shrink-0">{title}</p>}
      <div
        ref={containerRef}
        className={fill ? "flex-1 min-h-[200px]" : "min-h-[185px] flex flex-col justify-center flex-1"}
      >
        {children}
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function AnalyticsPanel({ orders, loading, section = "all", footer }) {

  const showPnl = section === "all" || section === "pnl";
  const showDistribution = section === "all" || section === "distribution";
  const pnlFill = section === "pnl";

  const pnlChartRef = useRef(null);
  const [pnlChartHeight, setPnlChartHeight] = useState(null);
  const [pnlPeriod, setPnlPeriod] = useState("all");
  const [alpacaHist, setAlpacaHist] = useState(null);
  const [alpacaErr, setAlpacaErr] = useState(null);
  const [alpacaLoading, setAlpacaLoading] = useState(showPnl);

  const closed = useMemo(() =>
    orders
      .filter(o => !o.is_open && o.unrealized_pl != null && o.synced_at)
      .sort((a, b) => new Date(a.synced_at) - new Date(b.synced_at)),
    [orders]
  );

  const since = useMemo(() => historySinceParam(pnlPeriod, orders), [pnlPeriod, orders]);
  const until = useMemo(() => historyUntilParam(pnlPeriod, orders), [pnlPeriod, orders]);
  const tradedToday = useMemo(() => {
    const today = todaySessionYmd();
    return isRegularEquitySessionYmd(today) && hasTradeOnYmd(orders, today);
  }, [orders]);

  useEffect(() => {
    if (!showPnl) return undefined;
    if (loading && !orders.length) return undefined;
    if (pnlPeriod === "day" && !tradedToday) {
      setAlpacaHist({ ok: true, timestamps: [], profit_loss: [], equity: [] });
      setAlpacaErr(null);
      setAlpacaLoading(false);
      return undefined;
    }
    let cancelled = false;
    setAlpacaLoading(true);
    alpacaApi.portfolioHistory(pnlPeriod, since, until)
      .then((r) => {
        if (cancelled) return;
        if (r.data?.ok) {
          setAlpacaHist(r.data);
          setAlpacaErr(null);
        } else {
          setAlpacaHist(null);
          setAlpacaErr(r.data?.error || "Alpaca history unavailable");
        }
      })
      .catch(() => {
        if (!cancelled) {
          setAlpacaHist(null);
          setAlpacaErr("Could not load Alpaca P/L");
        }
      })
      .finally(() => {
        if (!cancelled) setAlpacaLoading(false);
      });
    return () => { cancelled = true; };
  }, [showPnl, pnlPeriod, since, until, tradedToday, loading, orders.length]);

  const alpacaCurve = useMemo(() => historyToCurve(alpacaHist, until), [alpacaHist, until]);

  useEffect(() => {
    if (!pnlFill) {
      setPnlChartHeight(null);
      return;
    }
    const el = pnlChartRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h > 0) setPnlChartHeight(Math.floor(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [pnlFill, alpacaLoading, alpacaCurve.points.length]);

  // ── Cumulative P&L + drawdown chart (Alpaca portfolio history) ─────────────
  const cumulativeOpts = useMemo(() => {
    const points = alpacaCurve.points;
    if (!points.length) return null;
    const cumData = points.map((p) => [p.ms, parseFloat(p.pl.toFixed(2))]);
    const ddData = points.map((p) => [p.ms, parseFloat((p.drawdown ?? 0).toFixed(2))]);
    return {
      time: { timezone: "America/New_York" },
      chart:  {
        height: pnlFill && pnlChartHeight ? pnlChartHeight : 220,
        type: "line",
        marginTop: 10,
      },
      xAxis:  { type: "datetime" },
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
        xDateFormat: "%Y-%m-%d %H:%M ET",
        formatter() {
          const when = new Date(this.x).toLocaleString("en-US", { timeZone: "America/New_York" });
          return [`<span class="text-slate-400">${when}</span>`]
            .concat(this.points.map(p => `<b>${p.series.name}</b>: ${fmt$(p.y)}`))
            .join("<br>");
        },
      },
    };
  }, [alpacaCurve, pnlFill, pnlChartHeight]);

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
    const days = withDur
      .map(o => (new Date(o.synced_at) - new Date(o.created_at + (o.created_at.endsWith("Z") ? "" : "Z"))) / 86400000)
      .filter(d => d > 0);
    if (!days.length) return null;
    const { data } = histogram(days, 32);
    return {
      chart:  { height: 185, type: "column", marginTop: 10 },
      xAxis:  { title: { text: "Duration (days)", style: { color: "#64748b", fontSize: "10px" } }, labels: { formatter() { return fmtDays(this.value); } } },
      yAxis:  { title: { text: "Trades", style: { color: "#64748b", fontSize: "10px" } }, allowDecimals: false },
      series: [{ name: "Trades", showInLegend: false, data: data.map(d => ({ ...d, color: "#60a5fa" })), borderWidth: 0 }],
      tooltip: { formatter() { return `~${fmtDays(this.x)}: <b>${this.y}</b> trade${this.y !== 1 ? "s" : ""}`; } },
    };
  }, [orders]);

  // ── Slippage histogram ────────────────────────────────────────────────────────
  const slippageOpts = useMemo(() => {
    const slips = closed
      .map((o) => {
        const exec = executionFromTrade(o);
        return entrySlippagePerShare({
          limitPrice: exec.limitPrice,
          fillPrice: exec.fillPrice,
          direction: o.direction,
          isMarket: exec.isMarket,
        });
      })
      .filter((v) => v != null && !Number.isNaN(v));
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

  // Distribution still needs local closed rows. P&L Analysis does not.
  if (showDistribution && !showPnl && loading) {
    return (
      <div className="flex flex-col gap-4">
        <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
          Loading analytics…
        </div>
        {footer}
      </div>
    );
  }
  if (showDistribution && !showPnl && !closed.length) {
    return (
      <div className="flex flex-col gap-4">
        <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-10 text-center">
          <BarChart2 className="w-10 h-10 text-slate-500 mx-auto mb-3" />
          <p className="text-slate-400 font-medium">No closed trade data yet</p>
          <p className="text-slate-500 text-xs mt-1">Analytics will appear after trades are closed and synced</p>
        </div>
        {footer}
      </div>
    );
  }

  const periodLabel = NET_PERIODS.find((p) => p.id === pnlPeriod)?.label || "All";
  const sourceLabel = alpacaHist == null
    ? "Alpaca"
    : alpacaHist.paper
      ? "Alpaca paper"
      : "Alpaca live";
  const pnlSub = alpacaErr
    ? alpacaErr
    : alpacaLoading
      ? "Loading Alpaca…"
      : `${sourceLabel} · ${periodLabel}${alpacaHist?.since ? ` · since ${formatSinceLabel(alpacaHist.since)}` : ""} · Net ${fmt$(alpacaCurve.netPl)}`;

  return (
    <div className={`flex flex-col gap-6 ${pnlFill ? "flex-1 min-h-0 h-full" : ""}`}>

      {showPnl && (
      <div className={`flex flex-col gap-4 ${pnlFill ? "flex-1 min-h-0" : ""}`}>
        <div className="flex items-start justify-between gap-3 pb-2 border-b border-slate-800/60">
          <div>
            <h3 className="text-sm font-semibold text-slate-200">P&L Analysis</h3>
            <p className="text-xs text-slate-400 mt-0.5">{pnlSub}</p>
          </div>
          <PeriodToggle value={pnlPeriod} onChange={setPnlPeriod} />
        </div>

        <ChartBox
          title="Cumulative P&L + Drawdown Overlay"
          fill={pnlFill}
          containerRef={pnlFill ? pnlChartRef : undefined}
        >
          {alpacaLoading && !cumulativeOpts && (
            <p className="text-slate-400 text-sm text-center py-10">Loading Alpaca P&L…</p>
          )}
          {alpacaErr && !cumulativeOpts && (
            <p className="text-slate-400 text-sm text-center py-10">{alpacaErr}</p>
          )}
          {!alpacaLoading && !alpacaErr && !cumulativeOpts && (
            <p className="text-slate-500 text-xs text-center py-10">No Alpaca portfolio history for this period</p>
          )}
          {cumulativeOpts && (!pnlFill || pnlChartHeight != null) && (
            <HighchartsReact
              highcharts={Highcharts}
              options={cumulativeOpts}
              containerProps={
                pnlFill && pnlChartHeight
                  ? { style: { height: `${pnlChartHeight}px`, width: "100%" } }
                  : undefined
              }
            />
          )}
        </ChartBox>
      </div>
      )}

      {showDistribution && (
      <div className="flex flex-col gap-4">
        <SectionHeader title="Distribution Analysis" />

        <div className="grid grid-cols-2 gap-4">
          <ChartBox title="P&L Distribution">
            {plHistOpts && <HighchartsReact highcharts={Highcharts} options={plHistOpts} />}
          </ChartBox>
          <ChartBox title="Fill Slippage Distribution">
            {slippageOpts
              ? <HighchartsReact highcharts={Highcharts} options={slippageOpts} />
              : <p className="text-slate-500 text-xs text-center">No fill price data available</p>
            }
          </ChartBox>
          <ChartBox title="Trade Duration Distribution" className="col-span-2">
            {durationOpts
              ? <HighchartsReact highcharts={Highcharts} options={durationOpts} />
              : <p className="text-slate-500 text-xs text-center">No duration data</p>
            }
          </ChartBox>
          {footer && section === "all" && <div className="col-span-2">{footer}</div>}
        </div>
      </div>
      )}

    </div>
  );
}
