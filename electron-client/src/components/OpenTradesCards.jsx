import { useId, useMemo } from "react";
import {
  BarChart2,
  Search,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import { exitPrice } from "../utils/alpacaPrices";
import { parseTs } from "../utils/closedTradeAudit";

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

function plColor(v) {
  if (v == null || Number.isNaN(v)) return "text-slate-400";
  if (v > 0.005) return "text-emerald-400";
  if (v < -0.005) return "text-red-400";
  return "text-slate-400";
}

function orderMark(order, quote) {
  return (
    exitPrice(order.direction, quote) ??
    (order.current_price != null ? Number(order.current_price) : null)
  );
}

function orderPl(order, quote) {
  const fillPx = order.filled_avg_price ?? order.entry_price;
  const livePx = orderMark(order, quote);
  const qty = order.qty != null ? Number(order.qty) : null;
  if (fillPx != null && livePx != null && qty != null && !Number.isNaN(qty)) {
    const dir = order.direction === "long" ? 1 : -1;
    return dir * (Number(livePx) - Number(fillPx)) * qty;
  }
  if (order.unrealized_pl == null) return null;
  const v = Number(order.unrealized_pl);
  return Number.isNaN(v) ? null : v;
}

function clamp01(v) {
  if (v == null || Number.isNaN(v)) return 0.5;
  return Math.min(1, Math.max(0, v));
}

function etDayKey(ms) {
  return new Date(ms).toLocaleDateString("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
}

function isTodayEt(dateVal) {
  const ms = parseTs(dateVal);
  if (ms == null) return false;
  return etDayKey(ms) === etDayKey(Date.now());
}

function computeDayPl(closedOrders) {
  let realized = 0;
  let realizedCount = 0;
  let wins = 0;
  let losses = 0;
  for (const o of closedOrders) {
    if (!isTodayEt(o.closed_at ?? o.synced_at)) continue;
    if (o.unrealized_pl == null) continue;
    const v = Number(o.unrealized_pl);
    if (Number.isNaN(v)) continue;
    realized += v;
    realizedCount += 1;
    if (v > 0.005) wins += 1;
    else if (v < -0.005) losses += 1;
  }
  return { realized, realizedCount, wins, losses };
}

function fmtHold(ms) {
  if (ms == null || Number.isNaN(ms) || ms < 0) return "—";
  const min = ms / 60000;
  if (min < 1) return "<1m";
  if (min < 60) return `${Math.round(min)}m`;
  const hours = min / 60;
  if (hours < 24) {
    const h = Math.floor(hours);
    const m = Math.round(min - h * 60);
    return m ? `${h}h ${m}m` : `${h}h`;
  }
  const days = hours / 24;
  if (days < 10) {
    const d = Math.floor(days);
    const h = Math.round((days - d) * 24);
    return h ? `${d}d ${h}h` : `${d}d`;
  }
  return `${days.toFixed(1)}d`;
}

const HOLD_WARN_MS = 4 * 24 * 3600 * 1000;

function computeOpenBookMetrics(orders, liveQuotes) {
  let pl = 0;
  let plCount = 0;
  let risk = 0;
  let reward = 0;
  let winners = 0;
  let losers = 0;
  let longs = 0;
  let shorts = 0;
  let holdSum = 0;
  let holdCount = 0;
  let longestHold = 0;
  const now = Date.now();

  for (const o of orders) {
    if (o.direction === "short") shorts += 1;
    else longs += 1;

    const tUpper = o.ticker ? String(o.ticker).trim().toUpperCase() : "";
    const quote = tUpper ? liveQuotes[tUpper] : null;
    const riskAmt = o.risk_amt != null ? Number(o.risk_amt) : null;
    const rewardAmt = o.reward_amt != null ? Number(o.reward_amt) : null;
    const tradePl = orderPl(o, quote);

    if (tradePl != null) {
      pl += tradePl;
      plCount += 1;
      if (tradePl > 0.005) winners += 1;
      else if (tradePl < -0.005) losers += 1;
    }
    if (riskAmt != null && !Number.isNaN(riskAmt) && riskAmt > 0) risk += riskAmt;
    if (rewardAmt != null && !Number.isNaN(rewardAmt) && rewardAmt > 0) reward += rewardAmt;

    const openMs = parseTs(o.created_at);
    if (openMs != null && now >= openMs) {
      const hold = now - openMs;
      holdSum += hold;
      holdCount += 1;
      if (hold > longestHold) longestHold = hold;
    }
  }

  const count = orders.length;
  return {
    count,
    pl: plCount ? pl : null,
    risk,
    reward,
    winners,
    losers,
    longs,
    shorts,
    inProfitPct: count ? (winners / count) * 100 : null,
    avgHoldMs: holdCount ? holdSum / holdCount : null,
    longestHoldMs: holdCount ? longestHold : null,
  };
}

/** Semicircle needle gauge. `t` is 0 (left) … 1 (right). */
function ArcGauge({ label, valueText, valueClass, sub, t, minLabel, maxLabel, track, zeroT, title }) {
  const uid = useId().replace(/[^a-zA-Z0-9]/g, "");
  const cx = 80;
  const cy = 78;
  const r = 56;
  const needleR = 48;
  const clamped = clamp01(t);
  const angle = Math.PI * (1 - clamped);
  const nx = cx + needleR * Math.cos(angle);
  const ny = cy - needleR * Math.sin(angle);
  const zeroAngle = zeroT == null ? null : Math.PI * (1 - clamp01(zeroT));
  const zx = zeroAngle == null ? null : cx + (r + 1) * Math.cos(zeroAngle);
  const zy = zeroAngle == null ? null : cy - (r + 1) * Math.sin(zeroAngle);
  const zix = zeroAngle == null ? null : cx + (r - 8) * Math.cos(zeroAngle);
  const ziy = zeroAngle == null ? null : cy - (r - 8) * Math.sin(zeroAngle);
  const stops = track ?? [
    { offset: "0%", color: "#f87171" },
    { offset: "50%", color: "#94a3b8" },
    { offset: "100%", color: "#34d399" },
  ];

  return (
    <div
      className="rounded-xl border border-slate-700/70 bg-slate-900/50 px-3 pt-3 pb-2.5 flex flex-col items-center min-w-0"
      title={title}
    >
      <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 self-start">
        {label}
      </p>
      <svg viewBox="0 0 160 96" className="w-full max-w-[200px] -mt-1" aria-hidden="true">
        <defs>
          <linearGradient id={`g-${uid}`} x1="0%" y1="0%" x2="100%" y2="0%">
            {stops.map((s) => (
              <stop key={s.offset} offset={s.offset} stopColor={s.color} />
            ))}
          </linearGradient>
        </defs>
        <path
          d={`M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`}
          fill="none"
          stroke={`url(#g-${uid})`}
          strokeWidth="10"
          strokeLinecap="round"
          opacity="0.9"
        />
        <path
          d={`M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`}
          fill="none"
          stroke="#0f172a"
          strokeWidth="2"
          strokeLinecap="round"
          opacity="0.25"
        />
        {zx != null && (
          <line x1={zix} y1={ziy} x2={zx} y2={zy} stroke="#94a3b8" strokeWidth="1.5" />
        )}
        <line
          x1={cx}
          y1={cy}
          x2={nx}
          y2={ny}
          stroke="#e2e8f0"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <circle cx={cx} cy={cy} r="4.5" fill="#e2e8f0" />
        <circle cx={cx} cy={cy} r="2" fill="#0f172a" />
      </svg>
      <p className={`text-lg font-bold font-mono tabular-nums leading-none -mt-1 ${valueClass ?? "text-slate-200"}`}>
        {valueText}
      </p>
      {sub && <p className="text-[11px] text-slate-500 mt-1 text-center leading-snug">{sub}</p>}
      {(minLabel || maxLabel) && (
        <div className="flex items-center justify-between w-full mt-1.5 text-[9px] font-medium text-slate-600 tabular-nums">
          <span>{minLabel}</span>
          <span>{maxLabel}</span>
        </div>
      )}
    </div>
  );
}

export function OpenTradesGauges({ orders, closedOrders = [], liveQuotes = {} }) {
  const metrics = useMemo(
    () => computeOpenBookMetrics(orders, liveQuotes),
    [orders, liveQuotes],
  );
  const day = useMemo(
    () => computeDayPl(closedOrders),
    [closedOrders],
  );

  if (!orders.length) return null;

  const absPl = Math.abs(metrics.pl ?? 0);
  const plMin = metrics.risk > 0 ? -metrics.risk : -Math.max(absPl, 1);
  const plMax = metrics.reward > 0 ? metrics.reward : Math.max(metrics.risk, absPl, 1);
  const plT = metrics.pl == null ? 0.5 : (metrics.pl - plMin) / (plMax - plMin || 1);

  const dayAbs = Math.abs(day.realized);
  const dayScale = Math.max(dayAbs, 1);
  const dayT = (day.realized - (-dayScale)) / (2 * dayScale);

  const holdScale = Math.max(metrics.longestHoldMs || 0, HOLD_WARN_MS);
  const holdT = metrics.avgHoldMs == null ? 0 : metrics.avgHoldMs / holdScale;
  const holdHot = metrics.avgHoldMs != null && metrics.avgHoldMs >= HOLD_WARN_MS;

  const winT = metrics.count ? metrics.winners / metrics.count : 0;
  const sideSub = `${metrics.longs} long · ${metrics.shorts} short`;

  return (
    <div className="px-4 pt-4 pb-3 border-b border-slate-800/60">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <ArcGauge
          label="Open P/L"
          valueText={fmtPl(metrics.pl)}
          valueClass={plColor(metrics.pl)}
          sub={`${metrics.count} open · ${sideSub}`}
          t={plT}
          minLabel={metrics.risk > 0 ? `-${fmt$(metrics.risk, 0)} risk` : "loss"}
          maxLabel={metrics.reward > 0 ? `+${fmt$(metrics.reward, 0)} tgt` : "gain"}
          zeroT={(0 - plMin) / (plMax - plMin || 1)}
          title="Unrealized P/L across all open trades. Needle is scaled from total stop risk to total target reward."
        />
        <ArcGauge
          label="Day P/L"
          valueText={fmtPl(day.realized)}
          valueClass={plColor(day.realized)}
          sub={
            day.realizedCount > 0
              ? `${day.realizedCount} closed today · ${day.wins}W / ${day.losses}L`
              : "No closes today"
          }
          t={dayT}
          minLabel={`-${fmt$(dayScale, 0)}`}
          maxLabel={`+${fmt$(dayScale, 0)}`}
          zeroT={0.5}
          title="Realized P/L from trades closed today (US/Eastern). Open trades are not included."
        />
        <ArcGauge
          label="Avg Hold"
          valueText={fmtHold(metrics.avgHoldMs)}
          valueClass={holdHot ? "text-yellow-400" : "text-slate-200"}
          sub={metrics.longestHoldMs != null ? `longest ${fmtHold(metrics.longestHoldMs)}` : "Time in trade"}
          t={holdT}
          minLabel="now"
          maxLabel={fmtHold(holdScale)}
          track={[
            { offset: "0%", color: "#53c3ff" },
            { offset: "55%", color: "#34d399" },
            { offset: "100%", color: "#fbbf24" },
          ]}
          title="Average time open trades have been held, from fill/placement to now. Needle reaches the right at 4 days (or the longest hold, if greater)."
        />
        <ArcGauge
          label="In Profit"
          valueText={metrics.inProfitPct == null ? "—" : `${Math.round(metrics.inProfitPct)}%`}
          valueClass={
            metrics.inProfitPct == null
              ? "text-slate-400"
              : metrics.inProfitPct >= 50
                ? "text-emerald-400"
                : metrics.inProfitPct > 0
                  ? "text-yellow-400"
                  : "text-red-400"
          }
          sub={`${metrics.winners} up · ${metrics.losers} down`}
          t={winT}
          minLabel="0%"
          maxLabel="100%"
          track={[
            { offset: "0%", color: "#f87171" },
            { offset: "50%", color: "#fbbf24" },
            { offset: "100%", color: "#34d399" },
          ]}
          title="Share of open trades currently in profit."
        />
      </div>
    </div>
  );
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
            <span
              className="text-[11px] font-bold uppercase tracking-wide text-slate-500"
              title={
                stopPx == null || targetPx == null
                  ? "This order has no stop-loss/target set (e.g. a Quick Open market order), so there's nothing to check bounds against."
                  : "Waiting on a live quote to check trade bounds."
              }
            >
              {stopPx == null || targetPx == null ? "No stop/target set" : "Awaiting quote"}
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
