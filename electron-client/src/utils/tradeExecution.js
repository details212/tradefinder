function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

/** Strip Polygon/Alpaca crypto punctuation so BTCUSD matches BTC/USD. */
export function compactTicker(ticker) {
  return String(ticker || "")
    .toUpperCase()
    .replace(/^X:/, "")
    .replace(/[-/_]/g, "");
}

export function exitTypeOf(method) {
  if (method === "bracket_tp" || method === "auto_close_tp") return "target";
  if (method === "bracket_sl" || method === "auto_close_sl") return "stop";
  if (method === "manual") return "manual";
  return null;
}

/** Limit + fill from a trade row (optional live Alpaca overlay). */
export function executionFromTrade(order, alpacaOrder = null) {
  const liveLimit = num(alpacaOrder?.limit_price);
  const liveFill = num(alpacaOrder?.filled_avg_price);
  const orderType = String(alpacaOrder?.type || alpacaOrder?.order_type || order?.order_type || "").toLowerCase();
  const isMarket = orderType === "market";
  const fill = liveFill ?? num(order?.filled_avg_price);
  // Limit orders: Alpaca limit when known, else DB entry_price sent as limit.
  // Market orders: entry_price is intended price at click (no Alpaca limit exists).
  const limitPrice = isMarket
    ? num(order?.entry_price)
    : (liveLimit ?? num(order?.entry_price));
  return { limitPrice, fillPrice: fill, isMarket };
}

/** True when stored entry reference is too far from Alpaca fill to use for slip. */
export function entryReferenceImplausible({ fillPrice, limitPrice }) {
  if (fillPrice == null || limitPrice == null) return false;
  return Math.abs(fillPrice - limitPrice) / Math.max(Math.abs(fillPrice), 1) > 0.15;
}

/** Per-share entry slippage; positive = adverse (paid more on a long). */
export function entrySlippagePerShare({ limitPrice, fillPrice, direction, isMarket = false }) {
  if (limitPrice == null || fillPrice == null) return null;
  if (entryReferenceImplausible({ fillPrice, limitPrice })) return null;
  const isLong = direction === "long";
  return isLong ? fillPrice - limitPrice : limitPrice - fillPrice;
}

export function entrySlippageDollar({ limitPrice, fillPrice, direction, qty = 1, isMarket = false }) {
  const perShare = entrySlippagePerShare({ limitPrice, fillPrice, direction, isMarket });
  return perShare != null ? perShare * qty : null;
}

/**
 * Round-trip slip from order row fields (filled_avg_price / exit_price synced from Alpaca).
 * entry_price is the intended/limit reference at placement — not a stored slip column.
 */
export function roundTripSlippageFromOrder(order) {
  const isLong = order?.direction === "long";
  const dir = isLong ? 1 : -1;
  const qty = num(order?.qty) ?? 1;
  const exec = executionFromTrade(order);
  const { fillPrice: entryFill, limitPrice: intendedEntry, isMarket } = exec;

  const stop = num(order?.stop_price);
  const target = num(order?.target_price);
  const pl = num(order?.unrealized_pl);
  const riskAmt = num(order?.risk_amt);

  const entryRefBad = entryReferenceImplausible({
    fillPrice: entryFill,
    limitPrice: intendedEntry,
  });

  const entrySlipPerShare = entrySlippagePerShare({
    limitPrice: intendedEntry,
    fillPrice: entryFill,
    direction: order?.direction,
    isMarket,
  });

  const exitType = exitTypeOf(order?.exit_method);
  const refExit = exitType === "target" ? target : exitType === "stop" ? stop : null;
  const storedExit = num(order?.exit_price);
  const inferredExit = entryFill != null && pl != null && qty
    ? entryFill + dir * (pl / qty)
    : null;
  const exitFill = storedExit ?? inferredExit;

  let exitMethodMismatch = false;
  if (exitFill != null && target != null && stop != null && exitType) {
    const dTarget = Math.abs(exitFill - target);
    const dStop = Math.abs(exitFill - stop);
    if (exitType === "target" && dStop + 0.01 < dTarget) exitMethodMismatch = true;
    if (exitType === "stop" && dTarget + 0.01 < dStop) exitMethodMismatch = true;
  }

  const exitSlipPerShare = !exitMethodMismatch && exitFill != null && refExit != null
    ? dir * (refExit - exitFill)
    : null;

  const riskPerShare = (() => {
    if (intendedEntry != null && stop != null) return Math.abs(intendedEntry - stop);
    if (riskAmt != null && qty) return Math.abs(riskAmt) / qty;
    if (entryFill != null && stop != null) return Math.abs(entryFill - stop);
    return null;
  })();

  const entrySlipDollar = entrySlipPerShare != null ? entrySlipPerShare * qty : null;
  const exitSlipDollar = exitSlipPerShare != null ? exitSlipPerShare * qty : null;
  const roundTripDollar = entrySlipDollar != null || exitSlipDollar != null
    ? (entrySlipDollar ?? 0) + (exitSlipDollar ?? 0)
    : null;

  return {
    isLong,
    isMarket,
    qty,
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
    fillVsLimitPerShare: entryFill != null && intendedEntry != null
      ? entryFill - intendedEntry
      : null,
  };
}

/** Raw fill − limit (unsigned), for table display. */
export function fillVsLimitRaw(limitPrice, fillPrice) {
  if (limitPrice == null || fillPrice == null) return null;
  return fillPrice - limitPrice;
}

export function slippageDisplayColor(slip, isLong) {
  if (slip == null) return "text-slate-500";
  const bad = isLong ? slip > 0.005 : slip < -0.005;
  const good = isLong ? slip < -0.005 : slip > 0.005;
  if (bad) return "text-red-400";
  if (good) return "text-emerald-400";
  return "text-slate-500";
}

/** Color for direction-adjusted slip where positive = adverse. */
export function adverseSlipColor(slip, loose = 0.005) {
  if (slip == null || Number.isNaN(slip)) return "text-slate-500";
  if (slip > loose) return "text-red-400";
  if (slip < -loose) return "text-emerald-400";
  return "text-slate-400";
}
