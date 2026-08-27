import { useState, useEffect, useCallback, useMemo } from "react";
import { fetchAlpacaQuotes } from "../utils/fetchAlpacaQuote";

/**
 * Batch-fetch Alpaca quotes when the ticker list changes.
 * No polling — call `refetch()` to force a fresh network request.
 */
export function useFreshAlpacaQuotes(tickers, enabled = true) {
  const key = useMemo(
    () => [...new Set((tickers || []).map((t) => String(t).trim().toUpperCase()).filter(Boolean))].sort().join(","),
    [tickers],
  );
  const [quotes, setQuotes] = useState({});
  const [fetching, setFetching] = useState(false);

  const refetch = useCallback(async () => {
    if (!key || !enabled) {
      setQuotes({});
      return {};
    }
    setFetching(true);
    try {
      const next = await fetchAlpacaQuotes(key.split(","));
      setQuotes(next);
      return next;
    } catch {
      setQuotes({});
      return {};
    } finally {
      setFetching(false);
    }
  }, [key, enabled]);

  useEffect(() => {
    refetch();
  }, [refetch]);

  return { quotes, fetching, refetch };
}
