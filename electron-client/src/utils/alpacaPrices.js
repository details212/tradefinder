/** Actionable exit price — long sells at bid, short buys at ask. */
export function exitPrice(direction, quote) {
  if (!quote) return null;
  const last = quote.last > 0 ? quote.last : null;
  const bid  = quote.bid  > 0 ? quote.bid  : null;
  const ask  = quote.ask  > 0 ? quote.ask  : null;
  if (direction === "long")  return bid ?? last;
  if (direction === "short") return ask ?? last;
  return last;
}
