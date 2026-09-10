/**
 * Closed-trade execution audit.
 *
 * Uses order.filled_avg_price and order.exit_price (Alpaca snapshots on sync)
 * vs entry_price (intended/limit at placement) and TP/SL levels.
 */

import { exitTypeOf, roundTripSlippageFromOrder } from "./tradeExecution";

export const FLAG_DEFS = {
  missing_fill:         { severity: "error", label: "No fill",           group: "data",  detail: "Closed with no filled_avg_price." },
  missing_pl:           { severity: "error", label: "No P/L",            group: "data",  detail: "Closed with no stored P/L — exit cannot be reconstructed." },
  missing_exit_method:  { severity: "warn",  label: "No exit method",    group: "data",  detail: "exit_method was never stamped." },
  canceled_with_fill:   { severity: "error", label: "Canceled + fill",   group: "data",  detail: "Status is canceled/expired but a fill exists — likely a close tagged as cancel." },
  no_closed_at:         { severity: "info",  label: "No close time",     group: "data",  detail: "is_open is false but closed_at is empty." },
  status_not_filled:    { severity: "warn",  label: "Status ≠ filled",   group: "data",  detail: "Position closed with a fill, but status is not filled." },
  zero_qty:             { severity: "error", label: "Qty 0",             group: "data",  detail: "Quantity is missing or zero." },
  missing_levels:       { severity: "warn",  label: "Missing levels",    group: "data",  detail: "Stop or target is missing." },
  implausible_exit:     { severity: "error", label: "Implausible exit",  group: "data",  detail: "Inferred exit is ≤ 0 or more than 50% away from the fill." },
  entry_slip_large:     { severity: "warn",  label: "Entry slip",        group: "entry", detail: "Adverse fill vs intended/limit is large relative to planned risk." },
  entry_slip_severe:    { severity: "error", label: "Severe entry slip", group: "entry", detail: "Adverse fill vs intended/limit ate a large share of planned risk." },
  fill_worse_than_limit:{ severity: "error", label: "Fill past limit",   group: "entry", detail: "Limit order filled worse than the limit — should not happen on a true limit." },
  exit_slip_large:      { severity: "warn",  label: "Exit slip",         group: "exit",  detail: "Exit fill is worse than the TP/SL for this exit method." },
  exit_slip_severe:     { severity: "error", label: "Severe exit slip",  group: "exit",  detail: "Exit fill is far worse than the recorded TP/SL." },
  exit_method_mismatch: { severity: "error", label: "Exit vs method",    group: "exit",  detail: "Exit fill is closer to the opposite level than the recorded method." },
  bad_entry_reference:  { severity: "warn",  label: "Bad entry ref",     group: "data",  detail: "Stored intended entry is far from the Alpaca fill — entry slip skipped." },
  stale_pl:             { severity: "error", label: "Stale P/L?",        group: "exit",  detail: "Stored P/L looks like a last open mark, not a close at the recorded TP/SL." },
};

const DEAD_STATUSES = new Set(["canceled", "expired", "rejected", "done_for_day"]);

// Scale-free: fraction of planned risk, or fraction of price when risk is missing.
const ENTRY_SLIP_WARN_R   = 0.15;
const ENTRY_SLIP_ERR_R    = 0.35;
const ENTRY_SLIP_WARN_PCT = 0.0010;
const ENTRY_SLIP_ERR_PCT  = 0.0035;
const EXIT_SLIP_WARN_R    = 0.15;
const EXIT_SLIP_ERR_R     = 0.35;
const EXIT_SLIP_WARN_PCT  = 0.0010;
const EXIT_SLIP_ERR_PCT   = 0.0035;
const STALE_PL_RATIO      = 0.50;

export function isDeadOrder(o) {
  if (!DEAD_STATUSES.has(o.status)) return false;
  if (o.exit_method || o.closed_at || o.filled_avg_price != null) return false;
  return true;
}

/** Realized close — do not rely on is_open alone (crypto sync can leave it true). */
export function isRealizedClose(o) {
  if (!o) return false;
  if (o.exit_method || o.closed_at || o.exit_price != null) return true;
  return o.is_open === false || o.is_open === 0;
}

export function parseTs(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s = String(v);
  const hasTz = /[zZ]|[+-]\d{2}:?\d{2}$/.test(s);
  const d = new Date(hasTz ? s : `${s}Z`);
  return Number.isNaN(d.getTime()) ? null : d.getTime();
}

/** Unique exit-method copy. `code` is what we store on orders.exit_method. */
export const EXIT_METHOD_INFO = {
  bracket_tp: {
    label: "Bracket TP",
    message: "Closed by the Alpaca bracket take-profit limit.",
    cls: "text-emerald-400 bg-emerald-900/30 border-emerald-700/50",
  },
  bracket_sl: {
    label: "Bracket SL",
    message: "Closed by the Alpaca bracket stop-loss.",
    cls: "text-red-400 bg-red-900/30 border-red-700/50",
  },
  auto_close_tp: {
    label: "Auto TP",
    message: "Closed by Trade automation after a completed 5-minute bar closed beyond target.",
    cls: "text-amber-400 bg-amber-900/30 border-amber-700/50",
  },
  auto_close_sl: {
    label: "Auto SL",
    message: "Closed by Trade automation after a completed 5-minute bar closed beyond stop.",
    cls: "text-orange-400 bg-orange-900/30 border-orange-700/50",
  },
  scalp_tp: {
    label: "Scalp P/L profit",
    message: "Closed by Scalp Profit / Loss: the 5-minute Alpaca snapshot P/L reached your profit threshold.",
    cls: "text-emerald-300 bg-teal-950/50 border-teal-700/60",
  },
  scalp_sl: {
    label: "Scalp P/L loss",
    message: "Closed by Scalp Profit / Loss: the 5-minute Alpaca snapshot P/L reached your loss threshold.",
    cls: "text-rose-300 bg-rose-950/50 border-rose-700/60",
  },
  manual: {
    label: "Manual",
    message: "Closed manually with Close Trade.",
    cls: "text-slate-300 bg-slate-800/60 border-slate-600/50",
  },
};

export const EXIT_LABELS = Object.fromEntries(
  Object.entries(EXIT_METHOD_INFO).map(([code, info]) => [code, info.label]),
);

export function exitMethodLabel(method) {
  if (!method) return "—";
  return EXIT_METHOD_INFO[method]?.label ?? method;
}

export function exitMethodMessage(method) {
  if (!method) return null;
  return EXIT_METHOD_INFO[method]?.message ?? null;
}

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function addFlag(flags, key) {
  if (!FLAG_DEFS[key] || flags.some((f) => f.key === key)) return;
  flags.push({ key, ...FLAG_DEFS[key] });
}

function severityRank(flags) {
  if (flags.some((f) => f.severity === "error")) return 3;
  if (flags.some((f) => f.severity === "warn")) return 2;
  if (flags.some((f) => f.severity === "info")) return 1;
  return 0;
}

/** Flag only adverse (positive) slip. Prefer planned-risk; % of price is fallback. */
function flagAdverseSlip(flags, slipPerShare, { riskPerShare, price, warnR, errR, warnPct, errPct, largeKey, severeKey }) {
  if (slipPerShare == null || !(slipPerShare > 0)) return;
  if (riskPerShare > 0) {
    const vsRisk = slipPerShare / riskPerShare;
    if (vsRisk >= errR) addFlag(flags, severeKey);
    else if (vsRisk >= warnR) addFlag(flags, largeKey);
    return;
  }
  const vsPx = price > 0 ? slipPerShare / price : 0;
  if (vsPx >= errPct) addFlag(flags, severeKey);
  else if (vsPx >= warnPct) addFlag(flags, largeKey);
}

/**
 * Analyze one closed order. Open / never-filled dead orders return null.
 */
export function analyzeClosedTrade(o) {
  if (!o || isDeadOrder(o)) return null;

  const flags = [];
  const isLong = o.direction === "long";
  const dir = isLong ? 1 : -1;
  const qty = num(o.qty);
  const limit = num(o.entry_price);
  const fill = num(o.filled_avg_price);
  const stop = num(o.stop_price);
  const target = num(o.target_price);
  const pl = num(o.unrealized_pl);
  const riskAmt = num(o.risk_amt);
  const rewardAmt = num(o.reward_amt);

  if (qty == null || qty <= 0) addFlag(flags, "zero_qty");
  if (fill == null) addFlag(flags, "missing_fill");
  if (pl == null) addFlag(flags, "missing_pl");
  if (!o.exit_method) addFlag(flags, "missing_exit_method");
  if (!o.closed_at) addFlag(flags, "no_closed_at");
  if (stop == null || target == null) addFlag(flags, "missing_levels");

  if (DEAD_STATUSES.has(o.status) && fill != null) addFlag(flags, "canceled_with_fill");
  if (fill != null && o.status && o.status !== "filled" && !DEAD_STATUSES.has(o.status)) {
    addFlag(flags, "status_not_filled");
  }
  if (fill != null && DEAD_STATUSES.has(o.status) && o.status !== "canceled") {
    addFlag(flags, "status_not_filled");
  }

  const storedExit = num(o.exit_price);
  const inferredExit = fill != null && pl != null && qty
    ? fill + dir * (pl / qty)
    : null;
  const exitPrice = storedExit ?? inferredExit;

  if (exitPrice != null && fill != null) {
    const far = Math.abs(exitPrice - fill) / Math.max(Math.abs(fill), 0.01);
    if (exitPrice <= 0 || far > 0.50) addFlag(flags, "implausible_exit");
  }

  const slip = roundTripSlippageFromOrder(o);
  const {
    isMarket,
    intendedEntry,
    entryFill,
    exitFill,
    refExit,
    exitType,
    entrySlipPerShare,
    entrySlipDollar,
    exitSlipPerShare,
    exitSlipDollar,
    roundTripDollar,
    entryRefBad,
    exitMethodMismatch,
    riskPerShare,
    fillVsLimitPerShare,
  } = slip;

  if (entryRefBad) addFlag(flags, "bad_entry_reference");

  const fillVsLimitDollar = fillVsLimitPerShare != null && qty ? fillVsLimitPerShare * qty : null;
  const px = Math.abs(entryFill ?? intendedEntry ?? 0);

  flagAdverseSlip(flags, entrySlipPerShare, {
    riskPerShare, price: px,
    warnR: ENTRY_SLIP_WARN_R, errR: ENTRY_SLIP_ERR_R,
    warnPct: ENTRY_SLIP_WARN_PCT, errPct: ENTRY_SLIP_ERR_PCT,
    largeKey: "entry_slip_large", severeKey: "entry_slip_severe",
  });

  if (exitMethodMismatch) addFlag(flags, "exit_method_mismatch");

  const expectedPl = entryFill != null && refExit != null && qty
    ? dir * (refExit - entryFill) * qty
    : (exitType === "target" ? rewardAmt : exitType === "stop" && riskAmt != null ? -Math.abs(riskAmt) : null);

  if (!exitMethodMismatch) {
    flagAdverseSlip(flags, exitSlipPerShare, {
      riskPerShare, price: px,
      warnR: EXIT_SLIP_WARN_R, errR: EXIT_SLIP_ERR_R,
      warnPct: EXIT_SLIP_WARN_PCT, errPct: EXIT_SLIP_ERR_PCT,
      largeKey: "exit_slip_large", severeKey: "exit_slip_severe",
    });
  }

  if (
    storedExit == null
    && expectedPl != null
    && pl != null
    && Math.abs(expectedPl) >= 1
    && exitType
    && exitType !== "manual"
  ) {
    const ratio = pl / expectedPl;
    const absGap = Math.abs(pl - expectedPl);
    if (absGap > Math.max(Math.abs(expectedPl) * 0.35, 5) && ratio < STALE_PL_RATIO) {
      addFlag(flags, "stale_pl");
    }
  }

  const shownExitSlipPs = exitMethodMismatch ? null : exitSlipPerShare;
  const shownExitSlipDollar = exitMethodMismatch ? null : exitSlipDollar;
  const shownRoundTrip =
    entrySlipDollar != null || shownExitSlipDollar != null
      ? (entrySlipDollar ?? 0) + (shownExitSlipDollar ?? 0)
      : null;

  const plannedRisk = riskAmt != null
    ? Math.abs(riskAmt)
    : (riskPerShare != null && qty ? riskPerShare * qty : null);
  const rResult = pl != null && plannedRisk > 0 ? pl / plannedRisk : null;
  const slipPctOfRisk =
    shownRoundTrip != null && plannedRisk > 0
      ? (shownRoundTrip / plannedRisk) * 100
      : null;

  const closedAtMs = parseTs(o.closed_at) ?? parseTs(o.synced_at) ?? parseTs(o.created_at);

  return {
    order: o,
    isLong,
    isMarket,
    qty,
    limit: intendedEntry,
    fill: entryFill,
    stop,
    target,
    pl,
    exitPrice: exitFill,
    exitType,
    refExit,
    expectedPl,
    fillVsLimitPerShare,
    fillVsLimitDollar,
    entrySlipPerShare,
    entrySlipDollar,
    exitSlipPerShare: shownExitSlipPs,
    exitSlipDollar: shownExitSlipDollar,
    roundTripDollar: shownRoundTrip,
    slipPctOfRisk,
    rResult,
    plannedRisk,
    flags,
    severity: severityRank(flags),
    hasEntryIssue: flags.some((f) => f.group === "entry"),
    hasExitIssue: flags.some((f) => f.group === "exit"),
    hasDataIssue: flags.some((f) => f.group === "data"),
    closedAtMs,
  };
}

export function analyzeClosedTrades(orders) {
  return (orders || [])
    .map(analyzeClosedTrade)
    .filter(Boolean)
    .sort((a, b) => (b.closedAtMs ?? 0) - (a.closedAtMs ?? 0));
}

export function summarizeAudits(rows) {
  const n = rows.length;
  const sum = (fn) => rows.reduce((s, r) => {
    const v = fn(r);
    return v != null && Number.isFinite(v) ? s + v : s;
  }, 0);
  const mean = (fn) => {
    const vals = rows.map(fn).filter((v) => v != null && Number.isFinite(v));
    if (!vals.length) return null;
    return vals.reduce((s, v) => s + v, 0) / vals.length;
  };

  return {
    count: n,
    netPl: sum((r) => r.pl),
    wins: rows.filter((r) => (r.pl ?? 0) > 0.005).length,
    losses: rows.filter((r) => (r.pl ?? 0) < -0.005).length,
    entrySlipDollar: sum((r) => r.entrySlipDollar),
    fillVsLimitDollar: sum((r) => r.fillVsLimitDollar),
    exitSlipDollar: sum((r) => r.exitSlipDollar),
    roundTripDollar: sum((r) => r.roundTripDollar),
    avgEntrySlipPs: mean((r) => r.entrySlipPerShare),
    avgFillVsLimitPs: mean((r) => r.fillVsLimitPerShare),
    avgExitSlipPs: mean((r) => r.exitSlipPerShare),
    flagged: rows.filter((r) => r.flags.length).length,
    errors: rows.filter((r) => r.severity === 3).length,
    entryIssues: rows.filter((r) => r.hasEntryIssue).length,
    exitIssues: rows.filter((r) => r.hasExitIssue).length,
    dataIssues: rows.filter((r) => r.hasDataIssue).length,
    stalePl: rows.filter((r) => r.flags.some((f) => f.key === "stale_pl")).length,
    missingMethod: rows.filter((r) => r.flags.some((f) => f.key === "missing_exit_method")).length,
  };
}

export function compareAlpacaDetail(row, alpaca) {
  if (!row || !alpaca) return null;
  const legs = Array.isArray(alpaca.legs) ? alpaca.legs : [];
  const stopLeg = legs.find((l) => l.type === "stop" || l.type === "stop_limit") || null;
  const profitLeg = legs.find((l) => l.type === "limit" && l !== stopLeg) || null;
  const filledLeg = legs.find((l) => l.status === "filled") || null;

  const alpacaFill = num(alpaca.filled_avg_price);
  const alpacaExit = filledLeg ? num(filledLeg.filled_avg_price) : null;
  const inferredMethod = (() => {
    if (profitLeg?.status === "filled") return "bracket_tp";
    if (stopLeg?.status === "filled") return "bracket_sl";
    if (legs.length) return "manual";
    return null;
  })();

  const fillDelta = row.fill != null && alpacaFill != null ? alpacaFill - row.fill : null;
  const impliedPl = alpacaExit != null && row.fill != null && row.qty
    ? row.order.direction === "long"
      ? (alpacaExit - row.fill) * row.qty
      : (row.fill - alpacaExit) * row.qty
    : null;
  const plDelta = impliedPl != null && row.pl != null ? row.pl - impliedPl : null;

  return {
    symbol: alpaca.symbol ?? null,
    status: alpaca.status ?? null,
    filledQty: alpaca.filled_qty != null ? Number(alpaca.filled_qty) : null,
    alpacaFill,
    alpacaExit,
    fillDelta,
    impliedPl,
    plDelta,
    inferredMethod,
    stopLeg,
    profitLeg,
    filledLeg,
    legs,
  };
}
