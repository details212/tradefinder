import { useEffect, useId, useMemo, useState } from "react";
import {
  BarChart2,
  Search,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import { alpacaApi } from "../api/client";
import { exitPrice } from "../utils/alpacaPrices";
import {
  formatSinceLabel,
  hasTradeOnYmd,
  historySinceParam,
  historyToNet,
  historyUntilParam,
  isRegularEquitySessionYmd,
  todaySessionYmd,
} from "../utils/alpacaPortfolio";
import { parseTs } from "../utils/closedTradeAudit";
import { ticketPl } from "../utils/ticketPl";

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

function orderPl(order, quote) {
  return ticketPl(order, quote);
}

function clamp01(v) {
  if (v == null || Number.isNaN(v)) return 0.5;
  return Math.min(1, Math.max(0, v));
}

const NET_PERIODS = [
  { id: "week", label: "Week" },
  { id: "month", label: "Month" },
  { id: "all", label: "All" },
];

function PeriodToggle({ value, onChange }) {
  return (
    <span className="flex items-center rounded border border-slate-700/70 overflow-hidden shrink-0">
      {NET_PERIODS.map((p) => (
        <button
          key={p.id}
          type="button"
          onClick={() => onChange(p.id)}
          className={`px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide transition ${
            value === p.id
              ? "bg-brand-500/20 text-brand-400"
              : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
        >
          {p.label}
        </button>
      ))}
    </span>
  );
}

function PnlBarChart({ buckets }) {
  const [hover, setHover] = useState(null);
  if (!buckets?.length) return null;
  const max = Math.max(...buckets.map((b) => Math.abs(b.value)), 1);
  const tip = hover != null ? buckets[hover] : null;

  return (
    <div className="w-full mt-1.5" role="img" aria-label="Net P/L bar chart">
      <div
        className="relative flex items-stretch gap-px w-full h-12"
        onMouseLeave={() => setHover(null)}
      >
        <div className="absolute left-0 right-0 top-1/2 h-px bg-slate-700 pointer-events-none" />
        {buckets.map((b, i) => {
          const pct = (Math.abs(b.value) / max) * 50;
          const h = Math.abs(b.value) < 0.005 ? 0 : Math.max(pct, 4);
          const positive = b.value >= 0;
          const color = b.value > 0.005
            ? "bg-emerald-400"
            : b.value < -0.005
              ? "bg-red-400"
              : "bg-slate-600";
          return (
            <div
              key={b.key}
              className="relative flex-1 min-w-0 h-full cursor-crosshair"
              onMouseEnter={() => setHover(i)}
            >
              {h === 0 ? (
                <div className="absolute left-[15%] right-[15%] top-1/2 -translate-y-1/2 h-px bg-slate-600" />
              ) : (
                <div
                  className={`absolute left-[10%] right-[10%] rounded-[1px] ${color} ${b.isCurrent || hover === i ? "opacity-100" : "opacity-80"}`}
                  style={positive
                    ? { bottom: "50%", height: `${h}%` }
                    : { top: "50%", height: `${h}%` }}
                />
              )}
            </div>
          );
        })}
        {tip && (
          <div className="absolute -top-6 left-1/2 -translate-x-1/2 z-10 pointer-events-none whitespace-nowrap rounded bg-slate-950/95 border border-slate-700 px-1.5 py-0.5 text-[10px] font-mono tabular-nums text-slate-100 shadow-lg">
            <span className="text-slate-400 mr-1">{tip.title}</span>
            <span className={plColor(tip.value)}>{fmtPl(tip.value)}</span>
          </div>
        )}
      </div>
      <div className="flex w-full mt-0.5">
        {buckets.map((b) => (
          <span
            key={`${b.key}-lbl`}
            className={`flex-1 min-w-0 text-center text-[8px] leading-none truncate ${
              b.isCurrent ? "text-slate-400" : "text-slate-600"
            }`}
          >
            {b.label || "\u00a0"}
          </span>
        ))}
      </div>
    </div>
  );
}

function NetPlCard({ netPl, period, onPeriod, buckets, loading, error, paper, since }) {
  return (
    <div
      className="rounded-xl border border-slate-700/70 bg-slate-900/50 px-3 pt-3 pb-2 flex flex-col min-w-0 overflow-visible"
    >
      <div className="flex items-center justify-between gap-1 min-h-[18px]">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 truncate">
          Net P/L
        </p>
        <PeriodToggle value={period} onChange={onPeriod} />
      </div>
      <p className={`text-lg font-bold font-mono tabular-nums mt-1.5 leading-none ${
        loading ? "text-slate-500" : plColor(netPl)
      }`}>
        {loading ? "…" : fmtPl(netPl)}
      </p>
      <p className="text-[11px] text-slate-500 mt-1 leading-snug">
        {error
          ? error
          : loading
            ? "Loading Alpaca…"
            : `Alpaca ${paper ? "paper" : "live"}${since ? ` · since ${formatSinceLabel(since)}` : " account"}`}
      </p>
      <div className="w-full mt-auto">
        {!loading && !error && <PnlBarChart buckets={buckets} />}
      </div>
    </div>
  );
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
function ArcGauge({ label, valueText, valueClass, sub, t, minLabel, maxLabel, track, zeroT, title, action }) {
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
      title={action ? undefined : title}
    >
      <div className="flex items-center justify-between w-full gap-1 min-h-[18px]">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 truncate" title={action ? title : undefined}>
          {label}
        </p>
        {action}
      </div>
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

export function OpenTradesGauges({ orders, closedOrders = [], allOrders = [], liveQuotes = {} }) {
  const [netPeriod, setNetPeriod] = useState("week");
  const [alpacaHist, setAlpacaHist] = useState(null);
  const [alpacaErr, setAlpacaErr] = useState(null);
  const [alpacaLoading, setAlpacaLoading] = useState(true);
  const [dayHist, setDayHist] = useState(null);
  const [dayErr, setDayErr] = useState(null);
  const [dayLoading, setDayLoading] = useState(true);

  const sinceOrders = useMemo(
    () => (allOrders.length ? allOrders : [...orders, ...closedOrders]),
    [allOrders, orders, closedOrders],
  );
  const since = useMemo(
    () => historySinceParam(netPeriod, sinceOrders),
    [netPeriod, sinceOrders],
  );
  const until = useMemo(
    () => historyUntilParam(netPeriod, sinceOrders),
    [netPeriod, sinceOrders],
  );
  const tradedToday = useMemo(() => {
    const today = todaySessionYmd();
    return isRegularEquitySessionYmd(today) && hasTradeOnYmd(sinceOrders, today);
  }, [sinceOrders]);

  useEffect(() => {
    let cancelled = false;
    setAlpacaLoading(true);
    alpacaApi.portfolioHistory(netPeriod, since, until)
      .then((r) => {
        if (cancelled) return;
        if (r.data?.ok) {
          setAlpacaHist(r.data);
          setAlpacaErr(null);
        } else {
          setAlpacaHist(null);
          setAlpacaErr(r.data?.error || "Alpaca history unavailable");
        }
      })
      .catch(() => {
        if (!cancelled) {
          setAlpacaHist(null);
          setAlpacaErr("Could not load Alpaca P/L");
        }
      })
      .finally(() => {
        if (!cancelled) setAlpacaLoading(false);
      });
    return () => { cancelled = true; };
  }, [netPeriod, since, until]);

  useEffect(() => {
    if (!tradedToday) {
      setDayHist(null);
      setDayErr(null);
      setDayLoading(false);
      return undefined;
    }

    let cancelled = false;
    let firstLoad = true;

    const load = () => {
      if (firstLoad) setDayLoading(true);
      alpacaApi.portfolioHistory("day")
        .then((r) => {
          if (cancelled) return;
          if (r.data?.ok) {
            setDayHist(r.data);
            setDayErr(null);
          } else {
            setDayHist(null);
            setDayErr(r.data?.error || "Alpaca day P/L unavailable");
          }
        })
        .catch(() => {
          if (!cancelled) {
            setDayHist(null);
            setDayErr("Could not load Alpaca day P/L");
          }
        })
        .finally(() => {
          if (!cancelled && firstLoad) {
            setDayLoading(false);
            firstLoad = false;
          }
        });
    };

    load();
    // `tradedToday` only flips false -> true once per session (on the day's
    // first trade) and never changes again, so without a periodic refetch
    // here the gauge would freeze at whatever Alpaca reported at that one
    // moment - stale for the rest of the day as positions keep moving.
    const id = setInterval(load, 60_000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [tradedToday]);

  const metrics = useMemo(
    () => computeOpenBookMetrics(orders, liveQuotes),
    [orders, liveQuotes],
  );
  const alpacaNet = useMemo(
    () => historyToNet(alpacaHist, netPeriod, until),
    [alpacaHist, netPeriod, until],
  );
  const dayNet = useMemo(
    () => historyToNet(dayHist, "day"),
    [dayHist],
  );
  const dayPl = tradedToday ? dayNet.netPl : 0;

  const dayAbs = Math.abs(dayPl ?? 0);
  const dayScale = Math.max(dayAbs, 1);
  const dayT = dayPl == null ? 0.5 : (dayPl - (-dayScale)) / (2 * dayScale);

  const holdScale = Math.max(metrics.longestHoldMs || 0, HOLD_WARN_MS);
  const holdT = metrics.avgHoldMs == null ? 0 : metrics.avgHoldMs / holdScale;
  const holdHot = metrics.avgHoldMs != null && metrics.avgHoldMs >= HOLD_WARN_MS;

  const winT = metrics.count ? metrics.winners / metrics.count : 0;

  return (
    <div className="px-4 pt-4 pb-3 border-b border-slate-800/60">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <NetPlCard
          netPl={alpacaNet.netPl}
          period={netPeriod}
          onPeriod={setNetPeriod}
          buckets={alpacaNet.buckets}
          loading={alpacaLoading}
          error={alpacaErr}
          paper={alpacaHist?.paper}
          since={alpacaHist?.since}
        />
        <ArcGauge
          label="Day P/L"
          valueText={dayLoading ? "…" : fmtPl(dayPl)}
          valueClass={dayLoading ? "text-slate-500" : plColor(dayPl)}
          sub={
            dayErr
              ? dayErr
              : dayLoading
                ? "Loading Alpaca…"
                : tradedToday
                  ? `Alpaca ${dayHist?.paper ? "paper" : "live"} · today`
                  : "No trades today"
          }
          t={dayT}
          minLabel={`-${fmt$(dayScale, 0)}`}
          maxLabel={`+${fmt$(dayScale, 0)}`}
          zeroT={0.5}
          title="Account day P/L from Alpaca GET /v2/account/portfolio/history (1D, regular hours). Local trade rows are not used."
        />
        <ArcGauge
          label="Avg Hold"
          valueText={fmtHold(metrics.avgHoldMs)}
          valueClass={holdHot ? "text-yellow-400" : "text-slate-200"}
          sub={
            metrics.longestHoldMs != null
              ? `longest ${fmtHold(metrics.longestHoldMs)}`
              : orders.length ? "Time in trade" : "No open trades"
          }
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
          sub={orders.length ? `${metrics.winners} up · ${metrics.losers} down` : "No open trades"}
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
  const pl = ticketPl(order, quote);
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
