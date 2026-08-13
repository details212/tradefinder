/**
 * StrategyDetailCard — cumulative P&L chart + trade list for a single
 * strategy (or the Manual bucket). Shared by StrategyPerformancePanel
 * (drill-down) and ManualTradesPanel (dedicated tab).
 */
import { useMemo } from "react";
import Highcharts from "highcharts";
import HighchartsReact from "highcharts-react-official";
import { TrendingUp, TrendingDown, X } from "lucide-react";
import { fmt$, riskAmtFor } from "../utils/strategyMetrics";

export default function StrategyDetailCard({ title, icon, trades, stat, onClose }) {
  const sortedTrades = useMemo(() =>
    trades.slice().sort((a, b) => new Date(a.synced_at) - new Date(b.synced_at)),
    [trades]
  );

  const chartOpts = useMemo(() => {
    if (!sortedTrades.length) return null;
    let cum = 0;
    const data = sortedTrades.map((o, i) => {
      cum += Number(o.unrealized_pl);
      return [i, parseFloat(cum.toFixed(2))];
    });
    return {
      chart:  { height: 180, type: "line", marginTop: 10, backgroundColor: "transparent", style: { fontFamily: "inherit" } },
      title:  { text: null },
      credits: { enabled: false },
      xAxis:  { visible: false },
      yAxis:  {
        title: { text: null },
        gridLineColor: "#1e293b",
        labels: { style: { color: "#64748b", fontSize: "10px" }, formatter() { return fmt$(this.value, 0); } },
      },
      series: [{ name: "Cum. P&L", data, color: "#34d399", lineWidth: 2, marker: { enabled: false } }],
      legend: { enabled: false },
      tooltip: {
        backgroundColor: "#0f172a", borderColor: "#334155", borderRadius: 6,
        style: { color: "#e2e8f0", fontSize: "11px" },
        formatter() { return `Trade ${this.point.x + 1}<br><b>Cum. P&L</b>: ${fmt$(this.y)}`; },
      },
      plotOptions: { series: { animation: false } },
    };
  }, [sortedTrades]);

  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700/50">
        <div className="flex items-center gap-2 min-w-0">
          {icon}
          <h4 className="text-sm font-semibold text-slate-200 truncate">{title}</h4>
          <span className="text-xs text-slate-500 shrink-0">
            {stat?.count ?? 0} trade{stat?.count !== 1 ? "s" : ""}
            {stat?.bestPL != null && <> · best {fmt$(stat.bestPL)}</>}
            {stat?.worstPL != null && <> · worst {fmt$(stat.worstPL)}</>}
          </span>
        </div>
        {onClose && (
          <button
            type="button"
            onClick={onClose}
            className="text-slate-500 hover:text-slate-300 transition shrink-0"
            title="Close"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>

      {chartOpts && (
        <div className="px-4 pt-3">
          <HighchartsReact highcharts={Highcharts} options={chartOpts} />
        </div>
      )}

      <div className="grid grid-cols-[1fr_0.6fr_0.9fr_0.9fr_0.9fr_1fr] gap-3 px-4 py-2 border-t border-b border-slate-800/40 mt-1">
        {["Ticker", "Dir", "Fill", "P&L", "R Result", "Closed"].map((h, i) => (
          <span key={h} className={`text-[10px] font-semibold text-slate-500 uppercase tracking-wider ${i > 0 ? "text-right" : ""}`}>{h}</span>
        ))}
      </div>
      <div className="max-h-64 overflow-y-auto divide-y divide-slate-800/40">
        {sortedTrades.length === 0 && (
          <p className="text-sm text-slate-500 text-center py-8">No closed trades yet.</p>
        )}
        {sortedTrades.slice().reverse().map((o) => {
          const isLong = o.direction === "long";
          const pl = Number(o.unrealized_pl);
          const plPos = pl > 0, plNeg = pl < 0;
          const risk = riskAmtFor(o);
          const rResult = risk && risk > 0 ? pl / risk : null;
          const fillPx = o.filled_avg_price != null ? Number(o.filled_avg_price) : null;
          const date = o.closed_at ?? o.synced_at ?? o.created_at;
          const dateStr = date ? new Date(date).toLocaleDateString([], { month: "short", day: "numeric" }) : "—";
          return (
            <div key={o.id} className="grid grid-cols-[1fr_0.6fr_0.9fr_0.9fr_0.9fr_1fr] gap-3 items-center px-4 py-2 hover:bg-slate-800/30 transition">
              <span className="text-sm font-bold text-slate-100 truncate">{o.ticker}</span>
              <span className="flex items-center justify-end">
                {isLong ? <TrendingUp className="w-3.5 h-3.5 text-emerald-400" /> : <TrendingDown className="w-3.5 h-3.5 text-red-400" />}
              </span>
              <span className="text-xs font-mono text-slate-300 text-right tabular-nums">{fillPx != null ? `$${fillPx.toFixed(2)}` : "—"}</span>
              <span className={`text-xs font-mono font-semibold text-right tabular-nums ${plPos ? "text-emerald-400" : plNeg ? "text-red-400" : "text-slate-400"}`}>
                {plPos ? "+" : ""}{fmt$(pl)}
              </span>
              <span className={`text-xs font-mono text-right tabular-nums ${rResult > 0 ? "text-emerald-400" : rResult < 0 ? "text-red-400" : "text-slate-500"}`}>
                {rResult != null ? `${rResult > 0 ? "+" : ""}${rResult.toFixed(2)}R` : "—"}
              </span>
              <span className="text-[11px] text-slate-400 text-right">{dateStr}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
