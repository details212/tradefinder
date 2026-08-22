/**
 * Closed-trade execution audit.
 * Positive slippage = adverse (hurt the trader). Exit price is inferred from
 * stored P/L when Alpaca did not persist an exit fill — that inference is
 * itself a finding when it disagrees with the recorded exit method.
 */

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
  entry_slip_large:     { severity: "warn",  label: "Entry slip",        group: "entry", detail: "Fill vs limit is large relative to planned risk." },
  entry_slip_severe:    { severity: "error", label: "Severe entry slip", group: "entry", detail: "Fill vs limit is extreme — check whether the limit was honored." },
  fill_worse_than_limit:{ severity: "error", label: "Fill past limit",   group: "entry", detail: "Limit order filled worse than the limit — should not happen on a true limit." },
  exit_slip_large:      { severity: "warn",  label: "Exit slip",         group: "exit",  detail: "Inferred exit is far from the TP/SL implied by exit_method." },
  exit_slip_severe:     { severity: "error", label: "Severe exit slip",  group: "exit",  detail: "Inferred exit is far from the recorded exit level." },
  exit_method_mismatch: { severity: "error", label: "Exit vs method",    group: "exit",  detail: "Inferred exit is closer to the opposite level than the recorded method." },
  stale_pl:             { severity: "error", label: "Stale P/L?",        group: "exit",  detail: "Stored P/L looks like a last open mark, not a close at the recorded TP/SL." },
};

const DEAD_STATUSES = new Set(["canceled", "expired", "rejected", "done_for_day"]);

const ENTRY_SLIP_WARN_PS  = 0.05;
const ENTRY_SLIP_ERR_PS   = 0.15;
const ENTRY_SLIP_WARN_R   = 0.08;
const ENTRY_SLIP_ERR_R    = 0.20;
const EXIT_SLIP_WARN_PS   = 0.10;
const EXIT_SLIP_ERR_PS    = 0.25;
const EXIT_SLIP_WARN_R    = 0.12;
const EXIT_SLIP_ERR_R     = 0.30;
const STALE_PL_RATIO      = 0.50;
const LIMIT_VIOLATION_PS  = 0.01;

export function isDeadOrder(o) {
  if (!DEAD_STATUSES.has(o.status)) return false;
  if (o.exit_method || o.closed_at || o.filled_avg_price != null) return false;
  return true;
}

export function parseTs(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s = String(v);
  const hasTz = /[zZ]|[+-]\d{2}:?\d{2}$/.test(s);
  const d = new Date(hasTz ? s : `${s}Z`);
  return Number.isNaN(d.getTime()) ? null : d.getTime();
}

export function exitTypeOf(method) {
  if (method === "bracket_tp" || method === "auto_close_tp") return "target";
  if (method === "bracket_sl" || method === "auto_close_sl") return "stop";
  if (method === "manual") return "manual";
  return null;
}

export const EXIT_LABELS = {
  bracket_tp: "Bracket TP",
  bracket_sl: "Bracket SL",
  auto_close_tp: "Auto TP",
  auto_close_sl: "Auto SL",
  manual: "Manual",
};

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

/**
 * Analyze one closed order. Open / never-filled dead orders return null.
 */
export function analyzeClosedTrade(o) {
  if (!o || o.is_open || isDeadOrder(o)) return null;

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

  const exitPrice = fill != null && pl != null && qty
    ? fill + dir * (pl / qty)
    : null;

  if (exitPrice != null && fill != null) {
    const far = Math.abs(exitPrice - fill) / Math.max(Math.abs(fill), 0.01);
    if (exitPrice <= 0 || far > 0.50) addFlag(flags, "implausible_exit");
  }

  const entrySlipPerShare = fill != null && limit != null ? dir * (fill - limit) : null;
  const entrySlipDollar = entrySlipPerShare != null && qty ? entrySlipPerShare * qty : null;

  const riskPerShare = (() => {
    if (riskAmt != null && qty) return Math.abs(riskAmt) / qty;
    const ref = fill ?? limit;
    if (ref != null && stop != null) return Math.abs(ref - stop);
    return null;
  })();

  if (entrySlipPerShare != null) {
    const vsRisk = riskPerShare > 0 ? Math.abs(entrySlipPerShare) / riskPerShare : 0;
    if (Math.abs(entrySlipPerShare) >= ENTRY_SLIP_ERR_PS || vsRisk >= ENTRY_SLIP_ERR_R) {
      addFlag(flags, "entry_slip_severe");
    } else if (Math.abs(entrySlipPerShare) >= ENTRY_SLIP_WARN_PS || vsRisk >= ENTRY_SLIP_WARN_R) {
      addFlag(flags, "entry_slip_large");
    }
    if (
      (o.order_type || "").toLowerCase() === "limit"
      && entrySlipPerShare > LIMIT_VIOLATION_PS
    ) {
      addFlag(flags, "fill_worse_than_limit");
    }
  }

  const exitType = exitTypeOf(o.exit_method);
  const refExit = exitType === "target" ? target : exitType === "stop" ? stop : null;
  const exitSlipPerShare = exitPrice != null && refExit != null
    ? dir * (refExit - exitPrice)
    : null;
  const exitSlipDollar = exitSlipPerShare != null && qty ? exitSlipPerShare * qty : null;

  if (exitPrice != null && target != null && stop != null) {
    const dTarget = Math.abs(exitPrice - target);
    const dStop = Math.abs(exitPrice - stop);
    if (exitType === "target" && dStop + 0.01 < dTarget) addFlag(flags, "exit_method_mismatch");
    if (exitType === "stop" && dTarget + 0.01 < dStop) addFlag(flags, "exit_method_mismatch");
  }

  const expectedPl = fill != null && refExit != null && qty
    ? dir * (refExit - fill) * qty
    : (exitType === "target" ? rewardAmt : exitType === "stop" && riskAmt != null ? -Math.abs(riskAmt) : null);

  if (exitSlipPerShare != null) {
    const vsRisk = riskPerShare > 0 ? Math.abs(exitSlipPerShare) / riskPerShare : 0;
    if (Math.abs(exitSlipPerShare) >= EXIT_SLIP_ERR_PS || vsRisk >= EXIT_SLIP_ERR_R) {
      addFlag(flags, "exit_slip_severe");
    } else if (Math.abs(exitSlipPerShare) >= EXIT_SLIP_WARN_PS || vsRisk >= EXIT_SLIP_WARN_R) {
      addFlag(flags, "exit_slip_large");
    }
  }

  if (
    expectedPl != null
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

  const plannedRisk = riskAmt != null
    ? Math.abs(riskAmt)
    : (riskPerShare != null && qty ? riskPerShare * qty : null);
  const rResult = pl != null && plannedRisk > 0 ? pl / plannedRisk : null;
  const roundTripDollar =
    entrySlipDollar != null || exitSlipDollar != null
      ? (entrySlipDollar ?? 0) + (exitSlipDollar ?? 0)
      : null;
  const slipPctOfRisk =
    roundTripDollar != null && plannedRisk > 0
      ? (roundTripDollar / plannedRisk) * 100
      : null;

  const closedAtMs = parseTs(o.closed_at) ?? parseTs(o.synced_at) ?? parseTs(o.created_at);

  return {
    order: o,
    isLong,
    qty,
    limit,
    fill,
    stop,
    target,
    pl,
    exitPrice,
    exitType,
    refExit,
    expectedPl,
    entrySlipPerShare,
    entrySlipDollar,
    exitSlipPerShare,
    exitSlipDollar,
    roundTripDollar,
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
    exitSlipDollar: sum((r) => r.exitSlipDollar),
    roundTripDollar: sum((r) => r.roundTripDollar),
    avgEntrySlipPs: mean((r) => r.entrySlipPerShare),
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
