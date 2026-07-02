import React, { useMemo } from "react";
import { Loader2 } from "lucide-react";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { fmtEtDate } from "../utils/timeUtils";

const WIN_COLOR  = "#4ade80";
const LOSS_COLOR = "#f87171";
const LONG_COLOR = "#60a5fa";
const SHORT_COLOR = "#fb923c";

function fmtPct(v) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  const n = Number(v);
  return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
}

function pctOf(part, total) {
  if (!total) return 0;
  return Math.round((part / total) * 1000) / 10;
}

function StatTile({ label, value, accent }) {
  return (
    <div className="rounded-lg bg-slate-800/60 border border-slate-700/50 px-3 py-2">
      <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</div>
      <div className={`text-sm font-bold tabular-nums mt-0.5 ${accent ?? "text-slate-200"}`}>{value}</div>
    </div>
  );
}

function ChartCard({ title, subtitle, children }) {
  return (
    <div className="rounded-xl bg-slate-800/40 border border-slate-700/50 p-3 flex flex-col">
      <div className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.12em]">{title}</div>
      {subtitle && <div className="text-[11px] text-slate-600 mt-0.5 mb-2">{subtitle}</div>}
      <div className="flex-1 min-h-[148px]">{children}</div>
    </div>
  );
}

function PieTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const p = payload[0]?.payload;
  if (!p) return null;
  return (
    <div className="rounded-lg border border-slate-600 bg-slate-900 px-3 py-2 text-xs shadow-xl">
      <div className="font-semibold text-slate-200">{p.name}</div>
      <div className="text-slate-400 tabular-nums">{p.value} trade{p.value !== 1 ? "s" : ""} · {p.pct}%</div>
    </div>
  );
}

function MiniDonut({ data, colors }) {
  const total = data.reduce((s, d) => s + d.value, 0);
  if (!total) {
    return <div className="h-full flex items-center justify-center text-xs text-slate-600">No data</div>;
  }
  return (
    <ResponsiveContainer width="100%" height={148}>
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius={38}
          outerRadius={58}
          paddingAngle={2}
          stroke="none"
        >
          {data.map((entry, i) => (
            <Cell key={entry.name} fill={colors[i % colors.length]} />
          ))}
        </Pie>
        <Tooltip content={<PieTooltip />} />
      </PieChart>
    </ResponsiveContainer>
  );
}

function LegendRow({ items }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 justify-center">
      {items.map(({ name, value, pct, color }) => (
        <div key={name} className="flex items-center gap-1.5 text-[11px] text-slate-400">
          <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
          <span>{name}</span>
          <span className="tabular-nums text-slate-500">{value} ({pct}%)</span>
        </div>
      ))}
    </div>
  );
}

function fmtPrice(v) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function PlSummarySection({ summary }) {
  if (!summary) return null;
  const { netPct, grossWin, grossLoss, winCount, lossCount, total, withReturns } = summary;
  const netAccent = netPct > 0 ? "text-green-400" : netPct < 0 ? "text-red-400" : "text-slate-200";

  return (
    <div className="rounded-xl bg-slate-800/40 border border-slate-700/50 p-3">
      <div className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.12em]">Net P/L Summary</div>
      <div className="text-[11px] text-slate-600 mt-0.5 mb-3">
        Based on {total} completed trade{total !== 1 ? "s" : ""} shown
      </div>
      <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-[11px]">
        <div className="flex items-baseline justify-between col-span-2 pb-2 mb-1 border-b border-slate-700/50">
          <span className="text-slate-400 font-semibold uppercase tracking-wider text-[10px]">Net Return</span>
          <span className={`text-base font-bold tabular-nums ${netAccent}`}>{fmtPct(netPct)}</span>
        </div>
        <div className="flex items-baseline justify-between">
          <span className="text-slate-500">Gross Wins</span>
          <span className="font-semibold tabular-nums text-green-400">
            {fmtPct(grossWin)} <span className="text-slate-600 font-normal">({winCount})</span>
          </span>
        </div>
        <div className="flex items-baseline justify-between">
          <span className="text-slate-500">Gross Losses</span>
          <span className="font-semibold tabular-nums text-red-400">
            {fmtPct(grossLoss)} <span className="text-slate-600 font-normal">({lossCount})</span>
          </span>
        </div>
        {withReturns > 0 && (
          <div className="flex items-baseline justify-between col-span-2 pt-1">
            <span className="text-slate-500">Avg per Trade</span>
            <span className={`font-semibold tabular-nums ${netAccent}`}>{fmtPct(netPct / withReturns)}</span>
          </div>
        )}
      </div>
    </div>
  );
}

function summarizeTradesPl(trades) {
  if (!trades?.length) return null;
  let netPct = 0;
  let grossWin = 0;
  let grossLoss = 0;
  let winCount = 0;
  let lossCount = 0;
  let withReturns = 0;

  for (const t of trades) {
    const r = Number(t.return_pct);
    if (Number.isNaN(r)) continue;
    withReturns++;
    netPct += r;
    if (r >= 0) {
      grossWin += r;
      winCount++;
    } else {
      grossLoss += r;
      lossCount++;
    }
  }

  return { netPct, grossWin, grossLoss, winCount, lossCount, total: trades.length, withReturns };
}

function TradesColumn({ trades }) {
  if (!trades?.length) {
    return (
      <div className="flex-1 flex items-center justify-center px-4 text-xs text-slate-600 text-center">
        No completed trades in lookback window.
      </div>
    );
  }

  return (
    <div className="flex-1 min-h-0 overflow-y-auto">
      <table className="w-full text-left border-collapse text-[11px]">
        <thead className="sticky top-0 z-10 bg-slate-900">
          <tr className="border-b border-slate-700/60">
            {["Dir", "Entry", "Exit", "Return", "Days Held"].map((h) => (
              <th key={h} className="px-2 py-2 font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {trades.map((t, i) => {
            const isLong = t.direction?.toLowerCase() === "long";
            const ret = t.return_pct;
            const win = t.win === 1 || t.win === true;
            return (
              <tr key={`${t.entry_time}-${t.direction}-${i}`} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                <td className="px-2 py-1.5 whitespace-nowrap">
                  <span className={`text-[9px] font-bold px-1 py-px rounded uppercase ${
                    isLong ? "bg-green-900/50 text-green-400" : "bg-red-900/50 text-red-400"
                  }`}>
                    {isLong ? "L" : "S"}
                  </span>
                </td>
                <td className="px-2 py-1.5 whitespace-nowrap text-slate-400 tabular-nums">
                  <div>{fmtEtDate(t.entry_time)}</div>
                  <div className="text-slate-500">{fmtPrice(t.entry_price)}</div>
                </td>
                <td className="px-2 py-1.5 whitespace-nowrap text-slate-400 tabular-nums">
                  <div>{fmtEtDate(t.exit_time)}</div>
                  <div className="text-slate-500">{fmtPrice(t.exit_price)}</div>
                </td>
                <td className={`px-2 py-1.5 whitespace-nowrap font-semibold tabular-nums ${
                  ret == null ? "text-slate-600" : win ? "text-green-400" : "text-red-400"
                }`}>
                  {fmtPct(ret)}
                </td>
                <td className="px-2 py-1.5 whitespace-nowrap text-slate-500 tabular-nums text-center">
                  {t.bars_held ?? "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function LorentzianStatsPopover({
  popover,
  snapPrices,
  onMouseEnter,
  onMouseLeave,
}) {
  const { ticker, x, y, stats, allocation, trades, signalDirection, loading } = popover ?? {};
  const price = snapPrices?.[ticker] ?? null;

  const winLossData = useMemo(() => {
    const wins   = stats?.wins   ?? 0;
    const losses = stats?.losses ?? 0;
    const total  = wins + losses;
    if (!total) return [];
    return [
      { name: "Wins",   value: wins,   pct: pctOf(wins, total),   color: WIN_COLOR  },
      { name: "Losses", value: losses, pct: pctOf(losses, total), color: LOSS_COLOR },
    ];
  }, [stats]);

  const allocationData = useMemo(() => {
    const long  = allocation?.long  ?? 0;
    const short = allocation?.short ?? 0;
    const total = long + short;
    if (total > 0) {
      return [
        { name: "Long",  value: long,  pct: pctOf(long, total),  color: LONG_COLOR  },
        { name: "Short", value: short, pct: pctOf(short, total), color: SHORT_COLOR },
      ];
    }
    const wins   = stats?.wins   ?? 0;
    const losses = stats?.losses ?? 0;
    const wlTotal = wins + losses;
    if (!wlTotal) return [];
    return [
      { name: "Win %",  value: wins,   pct: pctOf(wins, wlTotal),   color: WIN_COLOR  },
      { name: "Loss %", value: losses, pct: pctOf(losses, wlTotal), color: LOSS_COLOR },
    ];
  }, [allocation, stats]);

  const allocationTitle = (allocation?.long ?? 0) + (allocation?.short ?? 0) > 0
    ? "Direction Allocation"
    : "Outcome Allocation";

  const plSummary = useMemo(() => summarizeTradesPl(trades), [trades]);

  if (!popover) return null;

  const sig = signalDirection?.toLowerCase();
  const sigLong = sig === "long";

  return (
    <div
      className="fixed z-[9999] bg-slate-900 border border-slate-700/80 rounded-xl shadow-2xl overflow-hidden select-none flex flex-col"
      style={{ left: x, top: y, width: 920, maxHeight: "85vh", transform: "translate(-50%, -50%)" }}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3.5 bg-slate-800/80 border-b border-slate-700/60">
        <div className="flex items-center gap-3">
          <span className="text-lg font-bold text-yellow-400 tracking-wide">{ticker}</span>
          {signalDirection && (
            <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider ${
              sigLong ? "bg-green-900/50 text-green-400" : "bg-red-900/50 text-red-400"
            }`}>
              {signalDirection}
            </span>
          )}
          {stats?.lookback_days != null && (
            <span className="text-[10px] text-slate-500">{stats.lookback_days}d lookback</span>
          )}
        </div>
        <div className="text-right">
          {price != null
            ? <span className="text-base font-mono font-semibold text-cyan-400">${Number(price).toFixed(2)}</span>
            : <span className="text-sm text-slate-600">no live price</span>}
          <div className="text-[10px] text-slate-600 mt-0.5 uppercase tracking-wider">live price</div>
        </div>
      </div>

      {loading && !stats && !trades?.length && (
        <div className="flex items-center justify-center py-14">
          <Loader2 className="w-5 h-5 text-slate-400 animate-spin" />
        </div>
      )}

      {!loading && !stats && !trades?.length && (
        <div className="px-5 py-10 text-center text-sm text-slate-500">
          No Lorentzian stats for this symbol yet.
        </div>
      )}

      {(stats || trades?.length > 0 || loading) && (
        <div className="flex flex-1 min-h-[380px] max-h-[calc(85vh-72px)]">
          {/* Left — summary + charts */}
          <div className="flex-1 min-w-0 overflow-y-auto px-5 py-4 space-y-4">
            {!stats && !loading && (
              <p className="text-xs text-slate-600">No symbol summary available.</p>
            )}

            {stats && (
              <>
                <div>
                  <div className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.15em] mb-3">
                    Symbol Summary
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    <StatTile label="Total Trades" value={stats.total_trades ?? 0} />
                    <StatTile
                      label="Win Rate"
                      value={stats.win_rate != null ? `${(stats.win_rate * 100).toFixed(1)}%` : "—"}
                      accent="text-green-400"
                    />
                    <StatTile
                      label="W / L Ratio"
                      value={stats.win_loss_ratio != null ? Number(stats.win_loss_ratio).toFixed(2) : "—"}
                    />
                    <StatTile label="Avg Return" value={fmtPct(stats.avg_return_pct)} accent={
                      stats.avg_return_pct > 0 ? "text-green-400" : stats.avg_return_pct < 0 ? "text-red-400" : "text-slate-200"
                    } />
                    <StatTile label="Avg Win" value={fmtPct(stats.avg_win_pct)} accent="text-green-400" />
                    <StatTile label="Avg Loss" value={fmtPct(stats.avg_loss_pct)} accent="text-red-400" />
                    <StatTile label="Best Trade" value={fmtPct(stats.best_return_pct)} accent="text-green-400" />
                    <StatTile label="Worst Trade" value={fmtPct(stats.worst_return_pct)} accent="text-red-400" />
                    <StatTile
                      label="Open Position"
                      value={stats.open_position ? stats.open_position.toUpperCase() : "None"}
                      accent={stats.open_position === "long" ? "text-green-400" : stats.open_position === "short" ? "text-red-400" : "text-slate-400"}
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <ChartCard title="Win / Loss" subtitle="Completed trades in lookback window">
                    <MiniDonut data={winLossData} colors={[WIN_COLOR, LOSS_COLOR]} />
                    <LegendRow items={winLossData} />
                  </ChartCard>
                  <ChartCard title={allocationTitle} subtitle="Share of historical signals">
                    <MiniDonut
                      data={allocationData}
                      colors={allocationTitle === "Direction Allocation" ? [LONG_COLOR, SHORT_COLOR] : [WIN_COLOR, LOSS_COLOR]}
                    />
                    <LegendRow items={allocationData} />
                  </ChartCard>
                </div>

                <PlSummarySection summary={plSummary} />
              </>
            )}

            {!stats && plSummary && (
              <PlSummarySection summary={plSummary} />
            )}

            {loading && !stats && (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="w-4 h-4 text-slate-500 animate-spin" />
              </div>
            )}
          </div>

          {/* Right — trade history */}
          <div className="w-[360px] shrink-0 border-l border-slate-700/60 flex flex-col min-h-0 self-stretch bg-slate-900/50">
            <div className="px-3 py-2.5 border-b border-slate-700/60 shrink-0">
              <div className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.12em]">
                Trade History
              </div>
              <div className="text-[11px] text-slate-600 mt-0.5">
                {trades?.length ?? 0} completed round trip{trades?.length !== 1 ? "s" : ""}
              </div>
            </div>
            {loading && !trades?.length ? (
              <div className="flex-1 flex items-center justify-center">
                <Loader2 className="w-4 h-4 text-slate-500 animate-spin" />
              </div>
            ) : (
              <TradesColumn trades={trades} />
            )}
          </div>
        </div>
      )}
    </div>
  );
}
