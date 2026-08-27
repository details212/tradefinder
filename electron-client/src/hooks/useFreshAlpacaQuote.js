import { useState, useEffect, useCallback } from "react";
import { fetchAlpacaQuote } from "../utils/fetchAlpacaQuote";

/**
 * Single-ticker Alpaca quote. Fetches on mount / ticker change only.
 * Call `refetch()` before trading actions that need a fresh price.
 */
export function useFreshAlpacaQuote(ticker, enabled = true) {
  const [quote, setQuote] = useState(null);
  const [fetching, setFetching] = useState(false);

  const refetch = useCallback(async () => {
    if (!ticker || !enabled) {
      setQuote(null);
      return null;
    }
    setFetching(true);
    try {
      const next = await fetchAlpacaQuote(ticker);
      setQuote(next);
      return next;
    } catch {
      setQuote(null);
      return null;
    } finally {
      setFetching(false);
    }
  }, [ticker, enabled]);

  useEffect(() => {
    refetch();
  }, [refetch]);

  return { quote, fetching, refetch };
}
