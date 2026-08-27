import { alpacaApi } from "../api/client";

/** Normalize a single-ticker Alpaca /quote response into a consistent shape. */
export function normalizeAlpacaQuoteResponse(d) {
  if (!d) return null;
  const bid = d.bid > 0 ? d.bid : null;
  const ask = d.ask > 0 ? d.ask : null;
  const last = d.last > 0 ? d.last : null;
  const spread =
    d.spread != null && Number.isFinite(Number(d.spread))
      ? Number(d.spread)
      : bid != null && ask != null
        ? Number((ask - bid).toFixed(4))
        : null;
  const price =
    last ??
    (bid != null && ask != null ? (bid + ask) / 2 : bid ?? ask ?? null);

  return {
    bid,
    ask,
    last,
    spread,
    price,
    bidSize: d.bid_size || null,
    askSize: d.ask_size || null,
    lastSize: d.last_size || null,
    updatedAt: Date.now(),
    fetching: false,
    error: null,
  };
}

/** Fresh Alpaca quote — always hits the network; no client-side cache. */
export async function fetchAlpacaQuote(ticker) {
  if (!ticker) return null;
  const res = await alpacaApi.quote(String(ticker).trim().toUpperCase());
  return normalizeAlpacaQuoteResponse(res.data);
}

/** Fresh batch Alpaca quotes — always hits the network; no client-side cache. */
export async function fetchAlpacaQuotes(tickers) {
  const list = [...new Set((tickers || []).map((t) => String(t).trim().toUpperCase()).filter(Boolean))];
  if (!list.length) return {};
  const res = await alpacaApi.quotes(list.join(","));
  const raw = res.data.quotes || {};
  const out = {};
  Object.entries(raw).forEach(([sym, q]) => {
    out[sym] = {
      ...q,
      price: q.price ?? q.last ?? (q.bid != null && q.ask != null ? (q.bid + q.ask) / 2 : q.bid ?? q.ask ?? null),
    };
  });
  return out;
}
