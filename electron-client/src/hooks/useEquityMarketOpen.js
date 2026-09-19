import { useCallback, useEffect, useState } from "react";
import { stockApi } from "../api/client";

const POLL_MS = 60_000;

/**
 * Regular-session open flag from GET /api/stocks/market-status.
 * Only `market === "open"` counts — extended hours are treated as closed.
 */
export function useEquityMarketOpen() {
  const [isOpen, setIsOpen] = useState(null);
  const [nextOpen, setNextOpen] = useState(null);

  const refresh = useCallback(async () => {
    try {
      const { data } = await stockApi.marketStatus();
      setIsOpen(data?.market === "open");
      setNextOpen(data?.next_open || null);
    } catch {
      // Keep last known value; place_order still enforces on the server.
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  }, [refresh]);

  return { isOpen, nextOpen, refresh };
}
