import { useEffect, useState, useCallback } from "react";
import { leaderboardApi } from "../api/client";
import {
  Trophy, Loader2, RefreshCw,
  ChevronDown, ChevronUp, ArrowUp, ArrowDown,
  TrendingUp, Target, Zap, BarChart2,
} from "lucide-react";

// ── Formatters ────────────────────────────────────────────────────────────────
function fmtMoney(v, compact = false) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  const n = Number(v);
  const abs = Math.abs(n);
  let s;
  if (compact && abs >= 1000) {
    s = `$${(abs / 1000).toFixed(1)}k`;
  } else {
    s = `$${abs.toFixed(2)}`;
  }
  return n < 0 ? `-${s}` : s;
}

function fmtPct(v) {
  if (v == null) return "—";
  return `${(Number(v) * 100).toFixed(1)}%`;
}

function fmtNum(v, digits = 2) {
  if (v == null) return "—";
  return Number(v).toFixed(digits);
}

// ── Colour helpers ────────────────────────────────────────────────────────────
function plColor(v) {
  if (v == null) return "text-slate-400";
  return Number(v) > 0 ? "text-emerald-400" : Number(v) < 0 ? "text-red-400" : "text-slate-400";
}

// ── Medal ─────────────────────────────────────────────────────────────────────
function Medal({ rank }) {
  if (rank === 1) return <span className="text-base leading-none">🥇</span>;
  if (rank === 2) return <span className="text-base leading-none">🥈</span>;
  if (rank === 3) return <span className="text-base leading-none">🥉</span>;
  return <span className="text-sm text-slate-500 tabular-nums font-mono">{rank}</span>;
}

// ── Stat tile (summary bar) ───────────────────────────────────────────────────
function Tile({ label, value, sub, accent = false, icon: Icon }) {
  return (
    <div className={`rounded-xl border px-4 py-3 flex flex-col gap-1 ${
      accent
        ? "bg-amber-900/20 border-amber-700/40"
        : "bg-slate-800/50 border-slate-700/50"
    }`}>
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</span>
        {Icon && <Icon className="w-3.5 h-3.5 text-slate-600" />}
      </div>
      <span className={`text-lg font-bold font-mono leading-tight ${accent ? "text-amber-300" : "text-slate-100"}`}>
        {value}
      </span>
      {sub && <span className="text-[11px] text-slate-500">{sub}</span>}
    </div>
  );
}

// ── Sortable column header ────────────────────────────────────────────────────
function ColHeader({ label, col, sortCol, sortDir, onSort, right = false }) {
  const active = sortCol === col;
  return (
    <th
      className={`px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider cursor-pointer select-none whitespace-nowrap transition ${
        right ? "text-right" : "text-left"
      } ${active ? "text-brand-400" : "text-slate-500 hover:text-slate-300"}`}
      onClick={() => onSort(col)}
    >
      <span className="inline-flex items-center gap-1">
        {right && active && (
          sortDir === "desc"
            ? <ArrowDown className="w-3 h-3 inline" />
            : <ArrowUp className="w-3 h-3 inline" />
        )}
        {label}
        {!right && active && (
          sortDir === "desc"
            ? <ArrowDown className="w-3 h-3 inline" />
            : <ArrowUp className="w-3 h-3 inline" />
        )}
      </span>
    </th>
  );
}

// ── Expanded detail card ──────────────────────────────────────────────────────
function DetailCard({ e }) {
  const metrics = [
    { label: "Net P&L",       value: fmtMoney(e.net_pl),         color: plColor(e.net_pl) },
    { label: "Avg trade",     value: fmtMoney(e.avg_trade),       color: plColor(e.avg_trade) },
    { label: "Avg win",       value: fmtMoney(e.avg_win),         color: "text-emerald-400" },
    { label: "Avg loss",      value: fmtMoney(e.avg_loss),        color: "text-red-400" },
    { label: "Best trade",    value: fmtMoney(e.best_trade),      color: "text-emerald-400" },
    { label: "Worst trade",   value: fmtMoney(e.worst_trade),     color: "text-red-400" },
    { label: "Profit factor", value: fmtNum(e.profit_factor, 2),  color: Number(e.profit_factor) > 1 ? "text-emerald-400" : "text-red-400" },
    { label: "Expectancy",    value: fmtMoney(e.expectancy),      color: plColor(e.expectancy) },
    { label: "Avg R/R",       value: fmtNum(e.avg_rr, 2),         color: "text-slate-200" },
    { label: "Win rate",      value: fmtPct(e.win_rate),          color: Number(e.win_rate) >= 0.5 ? "text-emerald-400" : "text-red-400" },
    { label: "Wins",          value: e.wins ?? "—",               color: "text-emerald-400" },
    { label: "Losses",        value: e.losses ?? "—",             color: "text-red-400" },
    { label: "Closed trades", value: e.closed_trades ?? "—",      color: "text-slate-200" },
    { label: "Open trades",   value: e.open_trades ?? "—",        color: "text-blue-400" },
  ];

  return (
    <tr className={e.is_you ? "bg-brand-900/10" : "bg-slate-800/30"}>
      <td colSpan={8} className="px-4 pb-4 pt-2">
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2">
          {metrics.map(({ label, value, color }) => (
            <div key={label} className="rounded-lg bg-slate-900/60 border border-slate-800 px-3 py-2">
              <p className="text-[9px] font-semibold uppercase tracking-wider text-slate-500 mb-1">{label}</p>
              <p className={`text-sm font-bold font-mono ${color}`}>{value}</p>
            </div>
          ))}
        </div>
      </td>
    </tr>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function LeaderBoard() {
  const [data,    setData]    = useState(null);
  const [error,   setError]   = useState(null);
  const [loading, setLoading] = useState(true);

  // client-side sort (server provides initial rank_by order)
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("desc");

  // expanded row
  const [expanded, setExpanded] = useState(null);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    leaderboardApi
      .get()
      .then((r) => {
        setData(r.data);
        setSortCol(r.data.rank_by || "net_pl");
      })
      .catch((e) => {
        setError(e.response?.data?.error || e.message || "Failed to load");
        setData(null);
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleSort = useCallback((col) => {
    setSortCol(prev => {
      if (prev === col) setSortDir(d => d === "desc" ? "asc" : "desc");
      else setSortDir("desc");
      return col;
    });
    setExpanded(null);
  }, []);

  const toggleExpand = useCallback((id) => {
    setExpanded(prev => prev === id ? null : id);
  }, []);

  // Derived: sorted entries
  const entries = (() => {
    if (!data?.entries) return [];
    const arr = [...data.entries];
    if (!sortCol) return arr;
    arr.sort((a, b) => {
      const av = a[sortCol] ?? (sortDir === "desc" ? -Infinity : Infinity);
      const bv = b[sortCol] ?? (sortDir === "desc" ? -Infinity : Infinity);
      return sortDir === "desc" ? bv - av : av - bv;
    });
    return arr;
  })();

  // Summary across all users (aggregate)
  const summary = (() => {
    if (!data?.entries?.length) return null;
    const es = data.entries;
    const active = es.filter(e => e.closed_trades > 0);

    // Avg win rate across active traders
    const avgWr = active.length
      ? active.reduce((s, e) => s + (e.win_rate || 0), 0) / active.length
      : null;
    const topTrader = [...es].sort((a, b) => b.net_pl - a.net_pl)[0];

    // Avg profit factor across traders who have one
    const pfEntries = active.filter(e => e.profit_factor != null);
    const avgPf = pfEntries.length
      ? pfEntries.reduce((s, e) => s + e.profit_factor, 0) / pfEntries.length
      : null;

    // Best single trade across all traders
    let bestTrade = null;
    let bestTrader = null;
    for (const e of es) {
      if (e.best_trade != null && (bestTrade == null || e.best_trade > bestTrade)) {
        bestTrade = e.best_trade;
        bestTrader = e.username;
      }
    }

    return { users: es.length, avgWr, topTrader, avgPf, bestTrade, bestTrader };
  })();

  const rankLabel = data?.rank_labels?.[data?.rank_by] ?? "Net P&L";

  const sortProps = { sortCol, sortDir, onSort: handleSort };

  return (
    <div className="h-full overflow-y-auto bg-slate-900">
      <div className="max-w-6xl mx-auto px-6 py-8">

        {/* ── Header ── */}
        <div className="flex items-start justify-between gap-4 mb-6">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <Trophy className="w-6 h-6 text-amber-400" />
              <h1 className="text-xl font-bold text-white tracking-tight">Leader Board</h1>
            </div>
            <p className="text-sm text-slate-400">
              Comparing closed-trade performance across all accounts.{" "}
              {data?.rank_by && (
                <>Server rank: <span className="text-slate-300 font-medium">{rankLabel}</span>.</>
              )}{" "}
              Click any column header to re-sort.
            </p>
          </div>
          <button
            type="button"
            onClick={load}
            disabled={loading}
            className="flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium text-slate-300 bg-slate-800 border border-slate-700 hover:bg-slate-700/80 transition shrink-0 disabled:opacity-50"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Refresh
          </button>
        </div>

        {error && (
          <div className="mb-6 rounded-xl border border-red-800/60 bg-red-950/40 px-4 py-3 text-sm text-red-300">
            {error}
          </div>
        )}

        {/* ── Loading state ── */}
        {loading && !data && (
          <div className="flex items-center justify-center py-24 text-slate-500 gap-2">
            <Loader2 className="w-5 h-5 animate-spin" />
            <span className="text-sm">Loading…</span>
          </div>
        )}

        {data && (
          <>
            {/* ── Summary tiles ── */}
            {summary && (
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-8">
                <Tile
                  label="Traders"
                  value={summary.users}
                  icon={BarChart2}
                />
                <Tile
                  label="Avg profit factor"
                  value={summary.avgPf != null ? fmtNum(summary.avgPf, 2) : "—"}
                  sub="wins ÷ losses across all traders"
                  icon={Target}
                />
                <Tile
                  label="Best single trade"
                  value={fmtMoney(summary.bestTrade, true)}
                  sub={summary.bestTrader ? `by ${summary.bestTrader}` : undefined}
                  icon={TrendingUp}
                />
                <Tile
                  label="Avg win rate"
                  value={fmtPct(summary.avgWr)}
                  sub={summary.topTrader ? `Top: ${summary.topTrader.username}` : undefined}
                  icon={Zap}
                  accent={!!summary.topTrader}
                />
              </div>
            )}

            {/* ── Traders table ── */}
            <section className="mb-10">
              <div className="rounded-xl border border-slate-800 overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-slate-800/80 border-b border-slate-800">
                        <th className="px-3 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 w-12">#</th>
                        <th className="px-3 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500">User</th>
                        <ColHeader label="Net P&L"       col="net_pl"        right {...sortProps} />
                        <ColHeader label="Closed"        col="closed_trades" right {...sortProps} />
                        <ColHeader label="Win rate"      col="win_rate"      right {...sortProps} />
                        <ColHeader label="Profit factor" col="profit_factor" right {...sortProps} />
                        <ColHeader label="Expectancy"    col="expectancy"    right {...sortProps} />
                        <ColHeader label="Avg R/R"       col="avg_rr"        right {...sortProps} />
                        <th className="w-8" />
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60">
                      {entries.length === 0 && (
                        <tr>
                          <td colSpan={9} className="px-4 py-12 text-center text-sm text-slate-500">
                            No users found.
                          </td>
                        </tr>
                      )}
                      {entries.map((e, idx) => {
                        const isExp = expanded === e.user_id;
                        const serverRank = data.entries.find(x => x.user_id === e.user_id)?.rank ?? idx + 1;
                        return [
                          <tr
                            key={e.user_id}
                            onClick={() => toggleExpand(e.user_id)}
                            className={`cursor-pointer transition ${
                              e.is_you
                                ? "bg-brand-900/20 border-l-2 border-brand-500 hover:bg-brand-900/30"
                                : "bg-slate-900/40 hover:bg-slate-800/40"
                            }`}
                          >
                            <td className="px-3 py-3 text-center">
                              <Medal rank={serverRank} />
                            </td>
                            <td className="px-3 py-3 font-medium text-slate-200 whitespace-nowrap">
                              {e.username}
                              {e.is_you && (
                                <span className="ml-2 text-[10px] font-semibold uppercase text-brand-400 bg-brand-900/40 px-1.5 py-0.5 rounded">
                                  You
                                </span>
                              )}
                            </td>
                            {/* Net P&L */}
                            <td className={`px-3 py-3 text-right font-mono font-semibold tabular-nums ${plColor(e.net_pl)}`}>
                              {fmtMoney(e.net_pl, true)}
                            </td>
                            {/* Closed trades */}
                            <td className="px-3 py-3 text-right text-slate-400 tabular-nums">
                              <span className="font-mono">{e.closed_trades}</span>
                              {e.open_trades > 0 && (
                                <span className="ml-1.5 text-[10px] text-blue-400 tabular-nums">+{e.open_trades} open</span>
                              )}
                            </td>
                            {/* Win rate */}
                            <td className="px-3 py-3 text-right tabular-nums">
                              {e.win_rate != null ? (
                                <span className={`font-mono font-medium ${Number(e.win_rate) >= 0.5 ? "text-emerald-400" : "text-red-400"}`}>
                                  {fmtPct(e.win_rate)}
                                </span>
                              ) : (
                                <span className="text-slate-600">—</span>
                              )}
                            </td>
                            {/* Profit factor */}
                            <td className="px-3 py-3 text-right tabular-nums">
                              {e.profit_factor != null ? (
                                <span className={`font-mono font-medium ${Number(e.profit_factor) >= 1 ? "text-emerald-400" : "text-red-400"}`}>
                                  {fmtNum(e.profit_factor, 2)}
                                </span>
                              ) : (
                                <span className="text-slate-600">—</span>
                              )}
                            </td>
                            {/* Expectancy */}
                            <td className={`px-3 py-3 text-right font-mono tabular-nums ${plColor(e.expectancy)}`}>
                              {fmtMoney(e.expectancy)}
                            </td>
                            {/* Avg R/R */}
                            <td className="px-3 py-3 text-right text-slate-300 font-mono tabular-nums">
                              {e.avg_rr != null ? fmtNum(e.avg_rr, 2) : <span className="text-slate-600">—</span>}
                            </td>
                            {/* Expand toggle */}
                            <td className="px-3 py-3 text-right">
                              {isExp
                                ? <ChevronUp className="w-4 h-4 text-slate-500 inline" />
                                : <ChevronDown className="w-4 h-4 text-slate-600 inline" />
                              }
                            </td>
                          </tr>,
                          isExp && <DetailCard key={`${e.user_id}-detail`} e={e} />,
                        ];
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </section>

          </>
        )}
      </div>
    </div>
  );
}
