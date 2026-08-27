import { useState, useEffect } from "react";
import { fetchAlpacaQuote } from "../utils/fetchAlpacaQuote";

/**
 * Renders a live Alpaca price. Fetches from the API on mount and whenever
 * `ticker` changes — no polling and no shared price cache.
 */
export default function LiveAlpacaPrice({
  ticker,
  className = "",
  loadingClassName = "text-slate-600 text-xs animate-pulse",
  emptyClassName = "text-slate-600 text-xs",
  fmt = (p) => `$${Number(p).toFixed(2)}`,
}) {
  const [price, setPrice] = useState(null);
  const [loading, setLoading] = useState(!!ticker);

  useEffect(() => {
    if (!ticker) {
      setPrice(null);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    fetchAlpacaQuote(ticker)
      .then((q) => { if (!cancelled) setPrice(q?.price ?? null); })
      .catch(() => { if (!cancelled) setPrice(null); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [ticker]);

  if (loading) return <span className={loadingClassName}>…</span>;
  if (price == null) return <span className={emptyClassName}>—</span>;
  return <span className={`tabular-nums ${className}`.trim()}>{fmt(price)}</span>;
}
