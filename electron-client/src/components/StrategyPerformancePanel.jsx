/**
 * StrategyPerformancePanel — breaks down historical trade performance by the
 * strategy that generated each trade (Order.trade_idea_name). Manual trades
 * (no trade_idea_name) live on their own "Manual" tab, not here.
 *
 * Receives `orders` (already synced) and `loading` from AdminPanel.
 */
import { useMemo, useState } from "react";
import { Lightbulb, ArrowUpDown } from "lucide-react";
import { fmt$, computeGroupStats } from "../utils/strategyMetrics";
import StrategyDetailCard from "./StrategyDetailCard";

const SORT_FIELDS = {
  name:     (s) => s.name.toLowerCase(),
  trades:   (s) => s.count,
  openNow:  (s) => s.openNow,
  winRate:  (s) => s.winRate ?? -1,
  netPL:    (s) => s.netPL,
  avgPL:    (s) => s.avgPL ?? -Infinity,
  avgR:     (s) => s.avgR ?? -Infinity,
};

export default function StrategyPerformancePanel({ orders, loading }) {
  const [sortField, setSortField] = useState("netPL");
  const [sortDir,   setSortDir]   = useState("desc"); // "asc" | "desc"
  const [selected,  setSelected]  = useState(null);    // strategy name | null

  const closed = useMemo(() =>
    orders.filter(o => !o.is_open && o.unrealized_pl != null && o.synced_at && o.trade_idea_name),
    [orders]
  );

  const strategies = useMemo(() => {
    const groups = new Map();
    const ensure = (name) => {
      if (!groups.has(name)) groups.set(name, { name, trades: [], openNow: 0 });
      return groups.get(name);
    };

    closed.forEach((o) => {
      ensure(o.trade_idea_name).trades.push(o);
    });
    orders.filter(o => o.is_open && o.trade_idea_name).forEach((o) => {
      ensure(o.trade_idea_name).openNow += 1;
    });

    return [...groups.values()].map((g) => ({
      name: g.name,
      openNow: g.openNow,
      trades: g.trades,
      ...computeGroupStats(g.trades),
    }));
  }, [closed, orders]);

  const sortedStrategies = useMemo(() => {
    const getVal = SORT_FIELDS[sortField] ?? SORT_FIELDS.netPL;
    const list = [...strategies].sort((a, b) => {
      const va = getVal(a), vb = getVal(b);
      if (va < vb) return -1;
      if (va > vb) return 1;
      return 0;
    });
    if (sortDir === "desc") list.reverse();
    return list;
  }, [strategies, sortField, sortDir]);

  const toggleSort = (field) => {
    if (sortField === field) {
      setSortDir(d => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  };

  const selectedStat = strategies.find(s => s.name === selected) ?? null;

  const SortHeader = ({ field, label, className = "" }) => (
    <button
      type="button"
      onClick={() => toggleSort(field)}
      className={`flex items-center gap-1 text-[10px] font-semibold text-slate-500 uppercase tracking-wider hover:text-slate-300 transition ${className}`}
    >
      {label}
      <ArrowUpDown className={`w-2.5 h-2.5 shrink-0 ${sortField === field ? "text-brand-400" : "text-slate-700"}`} />
    </button>
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
        Loading strategy performance…
      </div>
    );
  }

  if (!strategies.length) {
    return (
      <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-10 text-center">
        <Lightbulb className="w-10 h-10 text-slate-500 mx-auto mb-3" />
        <p className="text-slate-400 font-medium">No trade idea data yet</p>
        <p className="text-slate-500 text-xs mt-1">Performance by strategy will appear after trades are closed and synced</p>
      </div>
    );
  }

  const totalNetPL = strategies.reduce((s, g) => s + g.netPL, 0);
  const totalTrades = strategies.reduce((s, g) => s + g.count, 0);

  return (
    <div className="flex flex-col gap-4">

      <div className="pb-2 border-b border-slate-800/60 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold text-slate-200">Performance by Strategy</h3>
          <p className="text-xs text-slate-400 mt-0.5">
            {strategies.length} strateg{strategies.length !== 1 ? "ies" : "y"} · {totalTrades} closed trade{totalTrades !== 1 ? "s" : ""} · Net {fmt$(totalNetPL)}
          </p>
        </div>
      </div>

      {/* ── Strategy breakdown table ── */}
      <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl overflow-hidden">
        <div className="grid grid-cols-[1.6fr_0.7fr_0.7fr_0.9fr_1fr_1fr_0.8fr] gap-3 px-4 py-2.5 border-b border-slate-700/50 bg-slate-800/60">
          <SortHeader field="name"    label="Strategy" />
          <SortHeader field="trades"  label="Trades" className="justify-end" />
          <SortHeader field="openNow" label="Open" className="justify-end" />
          <SortHeader field="winRate" label="Win Rate" className="justify-end" />
          <SortHeader field="netPL"   label="Net P&L" className="justify-end" />
          <SortHeader field="avgPL"   label="Avg / Trade" className="justify-end" />
          <SortHeader field="avgR"    label="Avg R" className="justify-end" />
        </div>
        <div className="divide-y divide-slate-800/60">
          {sortedStrategies.map((s) => {
            const isSelected = selected === s.name;
            const netPos = s.netPL > 0, netNeg = s.netPL < 0;
            return (
              <button
                key={s.name}
                type="button"
                onClick={() => setSelected(isSelected ? null : s.name)}
                className={`w-full grid grid-cols-[1.6fr_0.7fr_0.7fr_0.9fr_1fr_1fr_0.8fr] gap-3 items-center px-4 py-3 text-left transition ${
                  isSelected ? "bg-brand-500/10 border-l-2 border-brand-500 pl-3.5" : "border-l-2 border-transparent hover:bg-slate-800/40"
                }`}
              >
                <span className="flex items-center gap-1.5 min-w-0">
                  <Lightbulb className="w-3.5 h-3.5 text-yellow-400 shrink-0" />
                  <span className="text-sm font-medium text-slate-200 truncate">{s.name}</span>
                </span>
                <span className="text-xs font-mono text-slate-300 text-right tabular-nums">{s.count}</span>
                <span className="text-xs font-mono text-right tabular-nums">
                  {s.openNow > 0
                    ? <span className="text-emerald-400 font-semibold">{s.openNow}</span>
                    : <span className="text-slate-600">0</span>}
                </span>
                <span className="text-xs font-mono text-right tabular-nums text-slate-300">
                  {s.winRate != null ? `${s.winRate.toFixed(0)}%` : "—"}
                  {s.count > 0 && (
                    <span className="text-slate-600"> ({s.wins}/{s.losses})</span>
                  )}
                </span>
                <span className={`text-xs font-mono font-bold text-right tabular-nums ${netPos ? "text-emerald-400" : netNeg ? "text-red-400" : "text-slate-400"}`}>
                  {netPos ? "+" : ""}{fmt$(s.netPL)}
                </span>
                <span className={`text-xs font-mono text-right tabular-nums ${s.avgPL > 0 ? "text-emerald-400/80" : s.avgPL < 0 ? "text-red-400/80" : "text-slate-500"}`}>
                  {s.avgPL != null ? `${s.avgPL > 0 ? "+" : ""}${fmt$(s.avgPL)}` : "—"}
                </span>
                <span className={`text-xs font-mono text-right tabular-nums ${s.avgR > 0 ? "text-emerald-400/80" : s.avgR < 0 ? "text-red-400/80" : "text-slate-500"}`}>
                  {s.avgR != null ? `${s.avgR > 0 ? "+" : ""}${s.avgR.toFixed(2)}R` : "—"}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Selected strategy detail ── */}
      {selected && selectedStat && (
        <StrategyDetailCard
          title={selected}
          icon={<Lightbulb className="w-4 h-4 text-yellow-400 shrink-0" />}
          trades={selectedStat.trades}
          stat={selectedStat}
          onClose={() => setSelected(null)}
        />
      )}

    </div>
  );
}
