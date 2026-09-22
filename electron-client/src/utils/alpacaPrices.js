/**
 * Mark price for live P/L — matches Alpaca's own unrealized_pl convention
 * (last trade price), not a conservative bid/ask estimate. Using bid/ask
 * here made the open-trade cards disagree with Alpaca's authoritative
 * position P/L by the full spread, on every open ticket.
 */
export function exitPrice(direction, quote) {
  if (!quote) return null;
  const last = quote.last > 0 ? quote.last : null;
  const bid  = quote.bid  > 0 ? quote.bid  : null;
  const ask  = quote.ask  > 0 ? quote.ask  : null;
  if (direction === "long")  return last ?? bid;
  if (direction === "short") return last ?? ask;
  return last;
}
