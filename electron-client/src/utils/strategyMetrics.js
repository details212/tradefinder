/** Shared helpers for strategy / manual trade performance breakdowns. */

export const fmt$ = (v, digits = 2) => {
  if (v == null || isNaN(v)) return "—";
  const abs = Math.abs(v);
  const str = abs >= 1000 ? `$${(abs / 1000).toFixed(1)}k` : `$${abs.toFixed(digits)}`;
  return v < 0 ? `-${str}` : str;
};

export function riskAmtFor(o) {
  if (o.risk_amt != null) return Number(o.risk_amt);
  const entry = o.entry_price != null ? Number(o.entry_price) : null;
  const stop  = o.stop_price  != null ? Number(o.stop_price)  : null;
  if (entry == null || stop == null) return null;
  return Math.abs(entry - stop) * (o.qty ?? 1);
}

/** Compute win-rate / P&L / R-multiple stats for a list of closed trades. */
export function computeGroupStats(trades) {
  const count = trades.length;
  const wins = trades.filter((o) => Number(o.unrealized_pl) > 0).length;
  const losses = trades.filter((o) => Number(o.unrealized_pl) < 0).length;
  const netPL = trades.reduce((s, o) => s + Number(o.unrealized_pl), 0);
  const avgPL = count ? netPL / count : null;
  const winRate = count ? (wins / count) * 100 : null;

  const rResults = trades
    .map((o) => {
      const risk = riskAmtFor(o);
      return risk && risk > 0 ? Number(o.unrealized_pl) / risk : null;
    })
    .filter((r) => r != null);
  const avgR = rResults.length ? rResults.reduce((s, r) => s + r, 0) / rResults.length : null;

  const plValues = trades.map((o) => Number(o.unrealized_pl));
  const bestPL = plValues.length ? Math.max(...plValues) : null;
  const worstPL = plValues.length ? Math.min(...plValues) : null;

  return { count, wins, losses, netPL, avgPL, winRate, avgR, bestPL, worstPL };
}
