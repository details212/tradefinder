/** Match Alpaca crypto symbols so 24/7 names are not gated by the equity session. */
export function looksLikeCrypto(ticker) {
  const raw = String(ticker || "");
  const compact = raw.replace(/[-/]/g, "").toUpperCase();
  return /^X:/i.test(raw) || /^(?:[A-Z]{2,})(USD|USDT|USDC|EUR|GBP)$/.test(compact);
}

export function formatNextOpen(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleString("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

export function equityMarketClosedMessage(nextOpen) {
  const when = formatNextOpen(nextOpen);
  return when
    ? `US equity market is closed. Next open: ${when}.`
    : "US equity market is closed. New stock orders can be placed during regular hours (9:30 AM–4:00 PM ET).";
}

/** True when new equity (not crypto) orders should be blocked. Unknown session stays allowed; the server still enforces. */
export function blockEquityOrders(ticker, marketIsOpen) {
  return marketIsOpen === false && !looksLikeCrypto(ticker);
}
