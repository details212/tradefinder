/** Per-ticket P/L from Alpaca prices — not the stored unrealized_pl column. */

import { exitPrice } from "./alpacaPrices";
import { isRealizedClose } from "./closedTradeAudit";

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function signedPl(direction, qty, entry, exit) {
  const q = Math.abs(qty);
  if (!(q > 0) || entry == null || exit == null) return null;
  const dir = direction === "short" ? -1 : 1;
  return dir * (exit - entry) * q;
}

/**
 * Money figure for one TradeFinder ticket.
 * Open: live Alpaca quote (or position mark) vs entry fill.
 * Closed: Alpaca exit fill vs entry fill.
 */
export function ticketPl(order, quote, position) {
  if (!order) return null;
  const qty = num(order.qty);
  const fill = num(order.filled_avg_price);
  // isRealizedClose() already excludes dead (never-filled) orders, so this
  // branch can trust that the entry genuinely filled.
  const closed = isRealizedClose(order);

  if (closed) {
    if (fill == null) return null; // no real entry fill — nothing to realize
    const exit = num(order.exit_price);
    const computed = signedPl(order.direction, qty, fill, exit);
    if (computed != null) return computed;
    return null;
  }

  // Still open: fall back to the intended/limit price only for a live mark
  // estimate — never for a realized (closed) P/L figure.
  const liveFill = fill ?? num(order.entry_price);
  const live =
    exitPrice(order.direction, quote) ??
    num(position?.current_price) ??
    num(order.current_price);
  const fromMark = signedPl(order.direction, qty, liveFill, live);
  if (fromMark != null) return fromMark;

  const posPl = num(position?.unrealized_pl);
  const posQty = Math.abs(num(position?.qty) ?? 0);
  if (posPl != null && posQty > 0 && qty != null && qty > 0) {
    return posPl * (Math.abs(qty) / posQty);
  }
  return null;
}

export function withTicketPl(order, quote, position) {
  const pl = ticketPl(order, quote, position);
  if (pl == null) return order;
  if (order.unrealized_pl != null && Math.abs(Number(order.unrealized_pl) - pl) < 0.00005) {
    return order;
  }
  return { ...order, unrealized_pl: pl };
}
