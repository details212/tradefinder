/**
 * ManualTradesPanel — performance for trades placed without a Trade Ideas
 * source (Order.trade_idea_name is null/empty). Sibling tab to the
 * strategy-grouped "Trade Ideas" view.
 *
 * Receives `orders` (already synced) and `loading` from AdminPanel.
 */
import { useMemo } from "react";
import { User } from "lucide-react";
import { fmt$, computeGroupStats } from "../utils/strategyMetrics";
import StrategyDetailCard from "./StrategyDetailCard";

export default function ManualTradesPanel({ orders, loading }) {
  const closed = useMemo(() =>
    orders.filter(o => !o.is_open && o.unrealized_pl != null && o.synced_at && !o.trade_idea_name),
    [orders]
  );

  const openNow = useMemo(() =>
    orders.filter(o => o.is_open && !o.trade_idea_name).length,
    [orders]
  );

  const stat = useMemo(() => computeGroupStats(closed), [closed]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
        Loading manual trades…
      </div>
    );
  }

  if (!closed.length && !openNow) {
    return (
      <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-10 text-center">
        <User className="w-10 h-10 text-slate-500 mx-auto mb-3" />
        <p className="text-slate-400 font-medium">No manual trades yet</p>
        <p className="text-slate-500 text-xs mt-1">Trades placed without a Trade Ideas source will appear here</p>
      </div>
    );
  }

  const netPos = stat.netPL > 0, netNeg = stat.netPL < 0;

  return (
    <div className="flex flex-col gap-4">

      <div className="pb-2 border-b border-slate-800/60 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold text-slate-200">Manual Trades</h3>
          <p className="text-xs text-slate-400 mt-0.5">
            {stat.count} closed trade{stat.count !== 1 ? "s" : ""}
            {openNow > 0 && <> · {openNow} open now</>}
            {" · "}Net <span className={netPos ? "text-emerald-400" : netNeg ? "text-red-400" : ""}>{fmt$(stat.netPL)}</span>
          </p>
        </div>
      </div>

      {/* ── Summary stat cards ── */}
      <div className="grid grid-cols-4 gap-3">
        {[
          { label: "Win Rate",   value: stat.winRate != null ? `${stat.winRate.toFixed(0)}%` : "—", sub: stat.count > 0 ? `${stat.wins}/${stat.losses}` : null },
          { label: "Net P&L",    value: fmt$(stat.netPL), color: netPos ? "text-emerald-400" : netNeg ? "text-red-400" : "text-slate-200" },
          { label: "Avg / Trade", value: stat.avgPL != null ? fmt$(stat.avgPL) : "—", color: stat.avgPL > 0 ? "text-emerald-400" : stat.avgPL < 0 ? "text-red-400" : "text-slate-200" },
          { label: "Avg R",      value: stat.avgR != null ? `${stat.avgR > 0 ? "+" : ""}${stat.avgR.toFixed(2)}R` : "—", color: stat.avgR > 0 ? "text-emerald-400" : stat.avgR < 0 ? "text-red-400" : "text-slate-200" },
        ].map(({ label, value, sub, color }) => (
          <div key={label} className="bg-slate-800/50 border border-slate-700/50 rounded-xl p-4">
            <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</p>
            <p className={`text-lg font-bold font-mono mt-1 ${color ?? "text-slate-200"}`}>{value}</p>
            {sub && <p className="text-[11px] text-slate-500 mt-0.5">{sub}</p>}
          </div>
        ))}
      </div>

      <StrategyDetailCard
        title="Manual Trades"
        icon={<User className="w-4 h-4 text-slate-400 shrink-0" />}
        trades={closed}
        stat={stat}
      />

    </div>
  );
}
