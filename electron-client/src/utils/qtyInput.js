/** Keep intermediate strings like "0." while typing a decimal qty. */
export function qtyFromInput(raw) {
  const v = String(raw ?? "");
  if (v === "") return "";
  if (!/^\d*\.?\d*$/.test(v)) return null;
  return v;
}

export function qtyNumber(qty) {
  const n = parseFloat(qty);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

/** Whole shares when qty >= 1; fractional when risk only covers a slice (crypto). */
export function deriveRiskQty(entry, riskPrefs, portfolioValue) {
  if (!riskPrefs || entry <= 0) return null;
  let riskDollars = 0;
  if (riskPrefs.risk_mode === "dollar") {
    riskDollars = parseFloat(riskPrefs.risk_value) || 0;
  } else if (riskPrefs.risk_mode === "percent") {
    const pv = parseFloat(portfolioValue) || 0;
    riskDollars = ((parseFloat(riskPrefs.risk_value) || 0) / 100) * pv;
  }
  if (riskDollars <= 0) return null;
  const q = riskDollars / entry;
  if (q <= 0) return null;
  if (q < 1) {
    const rounded = Math.round(q * 1e8) / 1e8;
    return rounded >= 1e-8 ? rounded : null;
  }
  return Math.floor(q);
}
