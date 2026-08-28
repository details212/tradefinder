function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

/** Limit + fill from a trade row (and optional live Alpaca overlay). */
export function executionFromTrade(order, alpacaOrder = null) {
  const liveLimit = num(alpacaOrder?.limit_price);
  const liveFill = num(alpacaOrder?.filled_avg_price);
  return {
    limitPrice: liveLimit ?? num(order?.entry_price),
    fillPrice: liveFill ?? num(order?.filled_avg_price),
  };
}

/** Per-share entry slippage; positive = adverse (paid more on a long). */
export function entrySlippagePerShare({ limitPrice, fillPrice, direction }) {
  if (limitPrice == null || fillPrice == null || Number.isNaN(limitPrice) || Number.isNaN(fillPrice)) {
    return null;
  }
  const isLong = direction === "long";
  return isLong ? fillPrice - limitPrice : limitPrice - fillPrice;
}

export function entrySlippageDollar({ limitPrice, fillPrice, direction, qty = 1 }) {
  const perShare = entrySlippagePerShare({ limitPrice, fillPrice, direction });
  return perShare != null ? perShare * qty : null;
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
