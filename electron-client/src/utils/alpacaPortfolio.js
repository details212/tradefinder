/** Alpaca GET /v2/account/portfolio/history — shared mapping for Net P/L and P&L Analysis. */

import { parseTs } from "./closedTradeAudit";

function pad2(n) {
  return String(n).padStart(2, "0");
}

function etYmd(ms) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date(ms));
  const get = (t) => Number(parts.find((p) => p.type === t)?.value);
  return { y: get("year"), m: get("month"), d: get("day") };
}

function etWeekday(ymd) {
  return new Date(Date.UTC(ymd.y, ymd.m - 1, ymd.d)).getUTCDay();
}

function ymdKey({ y, m, d }) {
  return `${y}-${pad2(m)}-${pad2(d)}`;
}

function weekdayLetter(ymd) {
  return ["S", "M", "T", "W", "T", "F", "S"][etWeekday(ymd)];
}

function monthShort(ym) {
  return new Date(Date.UTC(ym.y, ym.m - 1, 1)).toLocaleDateString("en-US", {
    month: "short",
    timeZone: "UTC",
  });
}

function tradeTimes(order) {
  const times = [];
  const openMs = parseTs(order?.created_at);
  const closeMs = parseTs(order?.closed_at);
  if (openMs != null) times.push(openMs);
  if (closeMs != null) times.push(closeMs);
  return times;
}

/** Eastern civil date of the oldest TradeFinder ticket (YYYY-MM-DD). */
export function oldestTradeYmd(orders) {
  let min = null;
  for (const o of orders ?? []) {
    for (const ms of tradeTimes(o)) {
      if (min == null || ms < min) min = ms;
    }
  }
  return min == null ? null : ymdKey(etYmd(min));
}

/** Eastern civil date of the newest TradeFinder open or close. */
export function newestTradeYmd(orders) {
  let max = null;
  for (const o of orders ?? []) {
    for (const ms of tradeTimes(o)) {
      if (max == null || ms > max) max = ms;
    }
  }
  return max == null ? null : ymdKey(etYmd(max));
}

export function todaySessionYmd(now = Date.now()) {
  return ymdKey(etYmd(now));
}

export function isRegularEquitySessionYmd(ymd) {
  if (!ymd) return false;
  const [y, m, d] = String(ymd).split("-").map(Number);
  if (!y || !m || !d) return false;
  const dow = etWeekday({ y, m, d });
  return dow !== 0 && dow !== 6;
}

export function hasTradeOnYmd(orders, ymd) {
  if (!ymd) return false;
  for (const o of orders ?? []) {
    for (const ms of tradeTimes(o)) {
      if (ymdKey(etYmd(ms)) === ymd) return true;
    }
  }
  return false;
}

/** Eastern civil date `n` weekdays (Mon–Fri) before `now`. Ignores market holidays. */
function tradingDaysAgoYmd(n, now = Date.now()) {
  let cur = etYmd(now);
  let count = 0;
  while (count < n) {
    const dt = new Date(Date.UTC(cur.y, cur.m - 1, cur.d - 1));
    cur = { y: dt.getUTCFullYear(), m: dt.getUTCMonth() + 1, d: dt.getUTCDate() };
    const dow = etWeekday(cur);
    if (dow !== 0 && dow !== 6) count++;
  }
  return ymdKey(cur);
}

function periodStartYmdEt(period, now = Date.now()) {
  const today = etYmd(now);
  if (period === "month") return ymdKey({ y: today.y, m: today.m, d: 1 });
  // "Week" is a rolling last-10-trading-day window, not the calendar week —
  // newest bar on the right, oldest on the left, regardless of what weekday it is today.
  if (period === "week") return tradingDaysAgoYmd(10, now);
  return null;
}

/**
 * Floor Alpaca history at the first TradeFinder trade so pre-client
 * account activity is excluded. Null means use Alpaca's native period.
 */
export function historySinceParam(period, orders) {
  const oldest = oldestTradeYmd(orders);
  if (!oldest) return null;
  if (period === "all") return oldest;
  if (period === "day") return null;
  const start = periodStartYmdEt(period);
  if (!start) return null;
  // "Week" (last 10 trading days) is always an explicit window, not just a
  // floor — otherwise it would fall back to Alpaca's native calendar-week
  // period instead of the rolling 10-trading-day range.
  if (period === "week") return oldest > start ? oldest : start;
  return oldest > start ? oldest : null;
}

/** Stop Alpaca history on the last ticket day so idle days do not look like trades. */
export function historyUntilParam(period, orders) {
  const newest = newestTradeYmd(orders);
  if (!newest) return null;
  const today = todaySessionYmd();
  if (newest >= today) return null;
  if (period === "day") return newest;
  return newest;
}

export function formatSinceLabel(ymd) {
  if (!ymd) return null;
  const [y, m, d] = String(ymd).split("-").map(Number);
  if (!y || !m || !d) return null;
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  });
}

export function alpacaTsMs(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  return n < 1e12 ? n * 1000 : n;
}

function walkHistory(hist, onPoint) {
  const stamps = hist?.timestamps || [];
  const pls = hist?.profit_loss || [];
  const eqs = hist?.equity || [];
  // Alpaca's profit_loss is `equity - base_value` — it is NOT adjusted for
  // deposits/withdrawals. Each entry in `cashflow` is a per-bucket amount by
  // activity type (CSD deposit +, CSW withdrawal -, etc.), same length as
  // timestamps. Subtract the running total so "P/L" reflects trading only,
  // not cash moved in/out of the account.
  const cashflowArrays = hist?.cashflow && typeof hist.cashflow === "object"
    ? Object.values(hist.cashflow)
    : [];
  const n = Math.min(stamps.length, pls.length);
  let cumCash = 0;
  for (let i = 0; i < n; i++) {
    const ms = alpacaTsMs(stamps[i]);
    const plRaw = pls[i] == null ? null : Number(pls[i]);
    if (ms == null || plRaw == null || Number.isNaN(plRaw)) continue;
    const eqRaw = eqs[i] == null ? null : Number(eqs[i]);
    const equity = eqRaw == null || Number.isNaN(eqRaw) ? null : eqRaw;
    for (const arr of cashflowArrays) {
      const v = arr?.[i];
      if (v != null && Number.isFinite(Number(v))) cumCash += Number(v);
    }
    const pl = plRaw - cumCash;
    onPoint({ i, n, ms, pl, equity });
  }
}

/** Incremental bars + period net (last Alpaca profit_loss). */
export function historyToNet(hist, period, untilYmd = null) {
  const tf = String(hist?.timeframe || "").toLowerCase();
  const isIntraday = tf !== "" && tf !== "1d";
  const nowKey = ymdKey(etYmd(Date.now()));
  const buckets = [];
  let prev = 0;
  let lastPl = null;

  walkHistory(hist, ({ i, n, ms, pl }) => {
    const ymd = etYmd(ms);
    if (untilYmd && ymdKey(ymd) > untilYmd) return;
    lastPl = pl;
    const incremental = pl - prev;
    prev = pl;
    let label = "";
    if (isIntraday) {
      if (buckets.length === 0 || i === n - 1 || buckets.length % 4 === 0) {
        label = new Date(ms).toLocaleTimeString("en-US", {
          timeZone: "America/New_York",
          hour: "numeric",
          minute: "2-digit",
        });
      }
    } else if (period !== "month" && period !== "all") {
      label = weekdayLetter(ymd);
    }
    buckets.push({
      key: String(ms),
      label,
      title: new Date(ms).toLocaleString("en-US", { timeZone: "America/New_York" }),
      value: incremental,
      isCurrent: isIntraday ? i === n - 1 : ymdKey(ymd) === nowKey,
    });
  });

  if (!isIntraday && buckets.length && (period === "month" || period === "all")) {
    buckets.forEach((b, i) => {
      const ymd = etYmd(Number(b.key));
      const show = i === 0 || i === buckets.length - 1 || b.isCurrent;
      if (!show) return;
      b.label = period === "all" ? monthShort(ymd) : String(ymd.d);
    });
  }

  return { netPl: lastPl, buckets };
}

/**
 * Cumulative P/L + dollar drawdown from the same Alpaca series.
 * Drawdown tracks the plotted cumulative P/L itself (peak-to-trough of `pl`),
 * not raw account equity — equity also moves with cash/margin/short proceeds
 * unrelated to trading P/L, which made the drawdown line show swings far
 * larger than the P/L curve it was drawn next to.
 */
export function historyToCurve(hist, untilYmd = null) {
  const points = [];
  let lastPl = null;
  let peakPl = 0;

  walkHistory(hist, ({ ms, pl, equity }) => {
    if (untilYmd && ymdKey(etYmd(ms)) > untilYmd) return;
    lastPl = pl;
    if (pl > peakPl) peakPl = pl;
    const drawdown = pl - peakPl;
    points.push({ ms, pl, equity, drawdown });
  });

  return { netPl: lastPl, points };
}

/** Daily equity / return series for Performance metrics (period=all). */
export function historyToAccountSeries(hist, untilYmd = null) {
  const { netPl, points } = historyToCurve(hist, untilYmd);
  const baseRaw = hist?.base_value == null ? null : Number(hist.base_value);
  const baseValue = baseRaw != null && !Number.isNaN(baseRaw) && baseRaw > 0 ? baseRaw : null;
  const equityPoints = [];
  const returns = [];
  const incrementalPl = [];
  let prevEq = null;
  let prevPl = 0;

  for (const p of points) {
    incrementalPl.push({ ms: p.ms, value: p.pl - prevPl });
    prevPl = p.pl;
    const equity = p.equity ?? (baseValue != null ? baseValue + p.pl : null);
    if (equity == null) continue;
    if (prevEq != null && prevEq > 0) {
      returns.push((equity - prevEq) / prevEq);
    }
    equityPoints.push({ equity, timeMs: p.ms });
    prevEq = equity;
  }

  const firstMs = equityPoints[0]?.timeMs ?? points[0]?.ms ?? null;
  const lastMs = equityPoints.at(-1)?.timeMs ?? points.at(-1)?.ms ?? null;
  const years = firstMs != null && lastMs != null && lastMs > firstMs
    ? (lastMs - firstMs) / (365.25 * 24 * 3600 * 1000)
    : null;

  return {
    netPl,
    baseValue,
    lastEquity: prevEq,
    paper: !!hist?.paper,
    equityPoints,
    returns,
    incrementalPl,
    firstMs,
    lastMs,
    years,
    periodsPerYear: 252,
  };
}
