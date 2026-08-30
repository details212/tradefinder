import {
  BarChart2,
  Search,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import { exitPrice } from "../utils/alpacaPrices";

function fmt$(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return "—";
  return `$${Number(v).toFixed(digits)}`;
}

function fmtPl(v) {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v).toFixed(2);
  if (Math.abs(v) < 0.005) return `$${abs}`;
  return v > 0 ? `+$${abs}` : `-$${abs}`;
}

function fmtR(r) {
  if (r == null || Number.isNaN(r)) return "—";
  const n = Number(r);
  if (Math.abs(n) < 0.005) return "0R";
  const abs = Math.abs(n);
  const body = (abs >= 10 ? abs.toFixed(1) : abs.toFixed(2)).replace(/\.?0+$/, "");
  return n > 0 ? `+${body}R` : `-${body}R`;
}

/** Current R from P/L ÷ risk, or mark vs fill ÷ stop distance. */
function currentR({ isLong, fillPx, livePx, stopPx, pl, riskAmt }) {
  if (riskAmt != null && riskAmt > 0 && pl != null) {
    return pl / riskAmt;
  }
  if (fillPx == null || stopPx == null) return null;
  const riskPerShare = Math.abs(Number(fillPx) - Number(stopPx));
  if (riskPerShare <= 0) return null;
  const mark = livePx ?? fillPx;
  const dir = isLong ? 1 : -1;
  return dir * (Number(mark) - Number(fillPx)) / riskPerShare;
}

function rColor(r) {
  if (r == null || Number.isNaN(r)) return "text-slate-400";
  if (r > 0.005) return "text-emerald-400";
  if (r < -0.005) return "text-red-400";
  return "text-slate-400";
}

/** True when last mark is between stop and target (inclusive). */
function tradeInBounds(isLong, livePx, stopPx, targetPx) {
  if (livePx == null || stopPx == null || targetPx == null) return null;
  const px = Number(livePx);
  const stop = Number(stopPx);
  const target = Number(targetPx);
  if (isLong) return px >= stop && px <= target;
  return px <= stop && px >= target;
}

function OpenTradeCard({ order, quote, onDetail, onChart, daysOpen }) {
  const isLong = order.direction === "long";
  const isPaper = order.paper_mode;
  const pl = order.unrealized_pl != null ? Number(order.unrealized_pl) : null;
  const plPos = pl != null && pl > 0;
  const plNeg = pl != null && pl < 0;
  const fillPx = order.filled_avg_price ?? order.entry_price;
  const livePx =
    exitPrice(order.direction, quote) ??
    (order.current_price != null ? Number(order.current_price) : null);
  const stopPx = order.stop_price != null ? Number(order.stop_price) : null;
  const targetPx = order.target_price != null ? Number(order.target_price) : null;
  const inBounds = tradeInBounds(isLong, livePx, stopPx, targetPx);
  const rr = order.rr_ratio_effective ?? order.rr_ratio;
  const planR = rr != null ? Number(rr) : null;
  const riskAmt = order.risk_amt != null ? Number(order.risk_amt) : null;
  const rNow = currentR({
    isLong,
    fillPx,
    livePx,
    stopPx,
    pl,
    riskAmt,
  });

  return (
    <article
      className={`rounded-xl border bg-slate-900/50 flex flex-col min-w-0 transition ${
        inBounds === false
          ? "oob-card-flash border-amber-500/55"
          : "border-slate-700/70 hover:border-slate-600/80"
      }`}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-2 px-3.5 pt-3.5 pb-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h3 className="font-bold text-slate-100 text-base truncate">{order.ticker}</h3>
            <span
              className={`inline-flex items-center gap-0.5 text-[10px] font-semibold px-1.5 py-0.5 rounded border ${
                isLong
                  ? "text-emerald-400 bg-emerald-950/40 border-emerald-800/50"
                  : "text-red-400 bg-red-950/40 border-red-800/50"
              }`}
            >
              {isLong ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              {isLong ? "Long" : "Short"}
            </span>
            {rNow != null && (
              <span
                className={`text-xs font-bold font-mono tabular-nums px-1.5 py-0.5 rounded border border-slate-700/80 bg-slate-950/60 ${rColor(rNow)}`}
                title={planR != null ? `Target: ${planR.toFixed(1)}R` : "Current R"}
              >
                {fmtR(rNow)}
              </span>
            )}
          </div>
          <p className="text-[11px] text-slate-500 mt-0.5 truncate" title={order.trade_idea_name ?? "Manual"}>
            {order.trade_idea_name ?? "Manual trade"}
          </p>
        </div>
        <span
          className={`shrink-0 text-[9px] font-semibold px-1.5 py-0.5 rounded border ${
            isPaper
              ? "text-blue-400 bg-blue-900/30 border-blue-700/40"
              : "text-emerald-400 bg-emerald-900/20 border-emerald-700/30"
          }`}
        >
          {isPaper ? "PAPER" : "LIVE"}
        </span>
      </div>

      {/* P/L + R hero */}
      <div className="px-3.5 py-2 border-y border-slate-800/60 bg-slate-950/30 grid grid-cols-2 gap-3">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Open P/L</p>
          <p
            className={`text-xl font-bold font-mono tabular-nums leading-tight mt-0.5 ${
              plPos ? "text-emerald-400" : plNeg ? "text-red-400" : "text-slate-400"
            }`}
          >
            {fmtPl(pl)}
          </p>
        </div>
        <div className="text-right">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">At R</p>
          <p className={`text-xl font-bold font-mono tabular-nums leading-tight mt-0.5 ${rColor(rNow)}`}>
            {fmtR(rNow)}
          </p>
          {planR != null && (
            <p className="text-[10px] font-mono text-slate-500 mt-0.5">
              target {planR.toFixed(1).replace(/\.0$/, "")}R
            </p>
          )}
        </div>
      </div>

      {/* Price levels */}
      <div className="px-3.5 py-2.5 grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
        <div>
          <span className="text-slate-500">Fill</span>
          <p className="font-mono text-slate-200 tabular-nums">{fmt$(fillPx)}</p>
        </div>
        <div>
          <span className="text-slate-500">Mark</span>
          <p className="font-mono text-slate-200 tabular-nums">{fmt$(livePx)}</p>
        </div>
        <div>
          <span className="text-slate-500">Stop</span>
          <p className="font-mono text-red-400/90 tabular-nums">{fmt$(stopPx)}</p>
        </div>
        <div>
          <span className="text-slate-500">Target</span>
          <p className="font-mono text-emerald-400/90 tabular-nums">{fmt$(targetPx)}</p>
        </div>
        <div>
          <span className="text-slate-500">Risk $</span>
          <p className="font-mono text-slate-300 tabular-nums">
            {riskAmt != null ? fmt$(riskAmt) : "—"}
          </p>
        </div>
        <div>
          <span className="text-slate-500">Plan R</span>
          <p className="font-mono text-brand-400/90 tabular-nums">
            {planR != null ? `${planR.toFixed(1).replace(/\.0$/, "")}R` : "—"}
          </p>
        </div>
        <div>
          <span className="text-slate-500">Qty</span>
          <p className="font-mono text-slate-300 tabular-nums">{order.qty ?? "—"}</p>
        </div>
        <div>
          <span className="text-slate-500">Days</span>
          <p
            className={`font-mono tabular-nums ${
              order.trade_idea_name === "Tradefinder AI" && daysOpen != null && daysOpen >= 4
                ? "text-yellow-400 font-semibold"
                : "text-slate-300"
            }`}
          >
            {daysOpen ?? "—"}
          </p>
        </div>
      </div>

      {/* Footer — bounds validator + actions */}
      <div className="mt-auto flex items-center justify-between gap-2 px-3.5 py-2.5 border-t border-slate-800/60">
        <div className="min-w-0">
          {inBounds === null ? (
            <span className="text-[11px] font-bold uppercase tracking-wide text-slate-500">
              Awaiting quote
            </span>
          ) : inBounds ? (
            <span className="text-xs font-bold uppercase tracking-wide text-emerald-400">
              TRADE OK
            </span>
          ) : (
            <span className="text-xs font-bold uppercase tracking-wide text-red-400">
              TRADE OUT OF BOUNDS
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          <button
            type="button"
            onClick={() => onDetail(order)}
            disabled={!order.alpaca_order_id}
            title="Bracket details"
            className="p-2 rounded-lg text-brand-400 hover:text-brand-300 hover:bg-slate-800 disabled:opacity-25 disabled:cursor-not-allowed transition"
          >
            <Search className="w-5 h-5" strokeWidth={2.5} />
          </button>
          <button
            type="button"
            onClick={() => onChart(order)}
            title="Trade chart"
            className="p-2 rounded-lg text-emerald-400 hover:text-emerald-300 hover:bg-slate-800 transition"
          >
            <BarChart2 className="w-5 h-5" strokeWidth={2.5} />
          </button>
        </div>
      </div>
    </article>
  );
}

export default function OpenTradesCards({ orders, liveQuotes = {}, onDetail, onChart, daysOpenFn }) {
  if (!orders.length) return null;

  return (
    <div className="p-4 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-3">
      {orders.map((o) => {
        const tUpper = o.ticker ? String(o.ticker).trim().toUpperCase() : "";
        return (
          <OpenTradeCard
            key={o.id}
            order={o}
            quote={tUpper ? liveQuotes[tUpper] : null}
            onDetail={onDetail}
            onChart={onChart}
            daysOpen={daysOpenFn ? daysOpenFn(o.created_at) : null}
          />
        );
      })}
    </div>
  );
}
