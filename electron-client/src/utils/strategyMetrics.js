/** Shared helpers for strategy / manual trade performance breakdowns. */

export const fmt$ = (v, digits = 2) => {
  if (v == null || isNaN(v)) return "—";
  const abs = Math.abs(v);
  const str = abs >= 1000 ? `$${(abs / 1000).toFixed(1)}k` : `$${abs.toFixed(digits)}`;
  return v < 0 ? `-${str}` : str;
};

export function riskAmtFor(o) {
  if (o.risk_amt != null) return Number(o.risk_amt);
  const entry = o.entry_price != null ? Number(o.entry_price) : null;
  const stop  = o.stop_price  != null ? Number(o.stop_price)  : null;
  if (entry == null || stop == null) return null;
  return Math.abs(entry - stop) * (o.qty ?? 1);
}

/** Compute win-rate / P&L / R-multiple stats for a list of closed trades. */
export function computeGroupStats(trades) {
  const count = trades.length;
  const wins = trades.filter((o) => Number(o.unrealized_pl) > 0).length;
  const losses = trades.filter((o) => Number(o.unrealized_pl) < 0).length;
  const netPL = trades.reduce((s, o) => s + Number(o.unrealized_pl), 0);
  const avgPL = count ? netPL / count : null;
  const winRate = count ? (wins / count) * 100 : null;

  const rResults = trades
    .map((o) => {
      const risk = riskAmtFor(o);
      return risk && risk > 0 ? Number(o.unrealized_pl) / risk : null;
    })
    .filter((r) => r != null);
  const avgR = rResults.length ? rResults.reduce((s, r) => s + r, 0) / rResults.length : null;

  const plValues = trades.map((o) => Number(o.unrealized_pl));
  const bestPL = plValues.length ? Math.max(...plValues) : null;
  const worstPL = plValues.length ? Math.min(...plValues) : null;

  return { count, wins, losses, netPL, avgPL, winRate, avgR, bestPL, worstPL };
}

export const fmtPct = (v, digits = 1) => {
  if (v == null || isNaN(v)) return "—";
  return `${Number(v).toFixed(digits)}%`;
};

export const fmtRatio = (v, digits = 2) => {
  if (v == null || isNaN(v) || !Number.isFinite(v)) return "—";
  return Number(v).toFixed(digits);
};

const RISK_FREE_RATE_ANNUAL = 0.05;
const BENCHMARK_TICKER = "SPY";

function tradeCloseTime(o) {
  const d = o.closed_at ?? o.synced_at ?? o.created_at;
  return d ? new Date(d).getTime() : NaN;
}

function tradeOpenTime(o) {
  const d = o.created_at;
  return d ? new Date(d).getTime() : NaN;
}

function mean(values) {
  if (!values.length) return null;
  return values.reduce((s, v) => s + v, 0) / values.length;
}

function sampleStd(values) {
  if (values.length < 2) return null;
  const m = mean(values);
  const variance = values.reduce((s, v) => s + (v - m) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function downsideDeviation(values, threshold = 0) {
  const downside = values.filter((v) => v < threshold);
  if (!downside.length) return null;
  // Standard Sortino/LPM2 convention: average squared shortfall over ALL
  // periods (values.length), not just the periods that fell below the
  // threshold. Dividing by the below-threshold count only overstates the
  // downside deviation and understates the Sortino ratio.
  const sq = downside.reduce((s, v) => s + (v - threshold) ** 2, 0) / values.length;
  return Math.sqrt(sq);
}

function maxDrawdownPct(equitySeries) {
  if (!equitySeries.length) return null;
  let peak = equitySeries[0];
  let maxDd = 0;
  for (const eq of equitySeries) {
    if (eq > peak) peak = eq;
    if (peak > 0) {
      const dd = (peak - eq) / peak;
      if (dd > maxDd) maxDd = dd;
    }
  }
  return maxDd * 100;
}

export function fmtDuration(ms) {
  if (ms == null || ms <= 0 || isNaN(ms)) return "—";
  const days = ms / (24 * 3600 * 1000);
  if (days < 1) return `${Math.round(days * 24)}h`;
  if (days < 30) return `${days.toFixed(1)} days`;
  if (days < 365) return `${(days / 30.4).toFixed(1)} mo`;
  return `${(days / 365.25).toFixed(1)} yr`;
}

/** Analyze drawdown episodes from an equity curve with timestamps. */
function analyzeDrawdowns(equityPoints) {
  if (!equityPoints.length) {
    return {
      maxDrawdownPct: null,
      maxDrawdownDollars: null,
      avgDrawdownPct: null,
      longestDrawdownMs: null,
      ulcerIndex: null,
    };
  }

  let peak = equityPoints[0].equity;
  let maxDrawdownPctVal = 0;
  let maxDrawdownDollars = 0;
  const instantaneousDdPct = [];
  const episodeDepths = [];
  const episodeDurations = [];

  let inDrawdown = false;
  let episodePeakTime = equityPoints[0].timeMs;
  let episodeMaxDdPct = 0;

  for (const pt of equityPoints) {
    if (pt.equity >= peak) {
      if (inDrawdown) {
        episodeDepths.push(episodeMaxDdPct);
        const endTime = pt.timeMs ?? episodePeakTime;
        if (episodePeakTime != null && endTime != null) {
          episodeDurations.push(Math.max(0, endTime - episodePeakTime));
        }
        inDrawdown = false;
        episodeMaxDdPct = 0;
      }
      peak = pt.equity;
      episodePeakTime = pt.timeMs ?? episodePeakTime;
    } else if (peak > 0) {
      inDrawdown = true;
      const ddPct = ((peak - pt.equity) / peak) * 100;
      instantaneousDdPct.push(ddPct);
      if (ddPct > episodeMaxDdPct) episodeMaxDdPct = ddPct;
      if (ddPct > maxDrawdownPctVal) {
        maxDrawdownPctVal = ddPct;
        maxDrawdownDollars = peak - pt.equity;
      }
    }
  }

  if (inDrawdown) {
    episodeDepths.push(episodeMaxDdPct);
    const lastTime = equityPoints[equityPoints.length - 1].timeMs;
    if (episodePeakTime != null && lastTime != null) {
      episodeDurations.push(Math.max(0, lastTime - episodePeakTime));
    }
  }

  const avgDrawdownPct = episodeDepths.length
    ? mean(episodeDepths)
    : instantaneousDdPct.length
      ? mean(instantaneousDdPct)
      : null;

  const longestDrawdownMs = episodeDurations.length ? Math.max(...episodeDurations) : null;

  const ulcerIndex = instantaneousDdPct.length
    ? Math.sqrt(mean(instantaneousDdPct.map((d) => d ** 2)))
    : null;

  return {
    maxDrawdownPct: maxDrawdownPctVal,
    maxDrawdownDollars,
    avgDrawdownPct,
    longestDrawdownMs,
    ulcerIndex,
  };
}

function computeVaRMetrics(returns, confidence = 0.95) {
  if (returns.length < 2) return { varPct: null, cvarPct: null };

  const sorted = [...returns].sort((a, b) => a - b);
  const idx = Math.max(0, Math.ceil((1 - confidence) * sorted.length) - 1);
  const varReturn = sorted[idx];
  const tail = sorted.slice(0, idx + 1);
  const cvarReturn = tail.length ? mean(tail) : null;

  return {
    varPct: varReturn != null ? varReturn * 100 : null,
    cvarPct: cvarReturn != null ? cvarReturn * 100 : null,
  };
}

/** Build per-trade return series from a chronologically sorted closed-trade list. */
function buildTradeReturnSeries(closedTrades, beginningEquity) {
  const sorted = closedTrades
    .slice()
    .filter((o) => o.unrealized_pl != null && !isNaN(Number(o.unrealized_pl)))
    .sort((a, b) => tradeCloseTime(a) - tradeCloseTime(b));

  if (!sorted.length || beginningEquity == null || beginningEquity <= 0) {
    return { returns: [], equitySeries: [], equityPoints: [], trades: [], tradesPerYear: null };
  }

  const firstOpenMs = sorted
    .map((o) => tradeOpenTime(o))
    .filter((t) => !isNaN(t))
    .sort((a, b) => a - b)[0];
  const firstCloseMs = tradeCloseTime(sorted[0]);

  let equity = beginningEquity;
  const returns = [];
  const equitySeries = [equity];
  const equityPoints = [{
    equity,
    timeMs: firstOpenMs ?? firstCloseMs ?? Date.now(),
  }];
  const trades = [];

  for (const trade of sorted) {
    const pl = Number(trade.unrealized_pl);
    const ret = equity > 0 ? pl / equity : 0;
    returns.push(ret);
    equity += pl;
    equitySeries.push(equity);
    equityPoints.push({
      equity,
      timeMs: tradeCloseTime(trade),
    });
    trades.push({
      trade,
      portfolioReturn: ret,
      openMs: tradeOpenTime(trade),
      closeMs: tradeCloseTime(trade),
    });
  }

  const firstMs = tradeCloseTime(sorted[0]);
  const lastMs = tradeCloseTime(sorted[sorted.length - 1]);
  const years = firstMs && lastMs && lastMs > firstMs
    ? (lastMs - firstMs) / (365.25 * 24 * 3600 * 1000)
    : null;
  const tradesPerYear = years && years > 0 ? sorted.length / years : null;

  return { returns, equitySeries, equityPoints, trades, tradesPerYear };
}

/** Map SPY bar list to { dateKey: dailyReturn }. */
function benchmarkDailyReturns(bars) {
  const byDate = new Map();
  for (const bar of bars ?? []) {
    if (bar.t == null || bar.c == null) continue;
    const key = new Date(bar.t).toISOString().slice(0, 10);
    byDate.set(key, Number(bar.c));
  }
  const dates = [...byDate.keys()].sort();
  const returns = new Map();
  for (let i = 1; i < dates.length; i++) {
    const prev = byDate.get(dates[i - 1]);
    const curr = byDate.get(dates[i]);
    if (prev > 0) returns.set(dates[i], (curr - prev) / prev);
  }
  return returns;
}

function compoundedBenchmarkReturn(dailyReturns, startMs, endMs) {
  if (!startMs || !endMs || endMs <= startMs) return null;
  let product = 1;
  let count = 0;
  const start = new Date(startMs);
  const end = new Date(endMs);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const key = d.toISOString().slice(0, 10);
    const r = dailyReturns.get(key);
    if (r != null) {
      product *= 1 + r;
      count++;
    }
  }
  return count > 0 ? product - 1 : null;
}

function alignedBenchmarkReturns(trades, benchmarkBars) {
  const dailyReturns = benchmarkDailyReturns(benchmarkBars);
  const pairs = [];

  for (const t of trades) {
    if (!t.openMs || !t.closeMs || t.closeMs <= t.openMs) continue;
    const benchmarkReturn = compoundedBenchmarkReturn(dailyReturns, t.openMs, t.closeMs);
    if (benchmarkReturn != null) {
      pairs.push({ portfolioReturn: t.portfolioReturn, benchmarkReturn });
    }
  }

  return pairs;
}

function computeBeta(pReturns, bReturns) {
  if (pReturns.length < 2 || bReturns.length < 2) return null;
  const bMean = mean(bReturns);
  const bVar = bReturns.reduce((s, r) => s + (r - bMean) ** 2, 0) / (bReturns.length - 1);
  if (bVar <= 0) return null;
  const pMean = mean(pReturns);
  const cov = pReturns.reduce(
    (s, r, i) => s + (r - pMean) * (bReturns[i] - bMean),
    0
  ) / (pReturns.length - 1);
  return cov / bVar;
}

function computeCorrelation(x, y) {
  if (x.length < 2 || y.length < 2) return null;
  const sx = sampleStd(x);
  const sy = sampleStd(y);
  if (!sx || !sy) return null;
  const mx = mean(x);
  const my = mean(y);
  const cov = x.reduce((s, v, i) => s + (v - mx) * (y[i] - my), 0) / (x.length - 1);
  return cov / (sx * sy);
}

function sampleSkewness(values) {
  if (values.length < 3) return null;
  const m = mean(values);
  const s = sampleStd(values);
  if (!s) return null;
  const n = values.length;
  const cubed = values.reduce((sum, v) => sum + ((v - m) / s) ** 3, 0);
  return (n / ((n - 1) * (n - 2))) * cubed;
}

function sampleExcessKurtosis(values) {
  if (values.length < 4) return null;
  const m = mean(values);
  const s = sampleStd(values);
  if (!s) return null;
  const n = values.length;
  const fourth = values.reduce((sum, v) => sum + ((v - m) / s) ** 4, 0);
  const kurt = (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * fourth;
  const correction = (3 * (n - 1) ** 2) / ((n - 2) * (n - 3));
  return kurt - correction;
}

function annualizeVolatility(periodStd, periodsPerYear) {
  if (periodStd == null || !periodsPerYear) return null;
  return periodStd * Math.sqrt(periodsPerYear);
}

/**
 * Risk-adjusted performance ratios from closed trades and optional benchmark bars.
 * Returns are computed per closed trade relative to running equity.
 */
export function computeRiskAdjustedMetrics({
  closedTrades,
  portfolioValue,
  netPL,
  cagr,
  benchmarkBars = null,
}) {
  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  const { returns, equitySeries, equityPoints, trades, tradesPerYear } = buildTradeReturnSeries(
    closedTrades,
    beginningEquity
  );

  const drawdowns = analyzeDrawdowns(equityPoints);

  if (returns.length < 2 || !tradesPerYear) {
    return {
      sharpe: null,
      sortino: null,
      calmar: null,
      treynor: null,
      omega: null,
      informationRatio: null,
      maxDrawdownPct: drawdowns.maxDrawdownPct,
      benchmarked: false,
    };
  }

  const meanReturn = mean(returns);
  const stdReturn = sampleStd(returns);
  const annualizedMean = meanReturn * tradesPerYear;
  const annualizedStd = stdReturn != null ? stdReturn * Math.sqrt(tradesPerYear) : null;
  const rfPerTrade = RISK_FREE_RATE_ANNUAL / tradesPerYear;

  const sharpe = annualizedStd && annualizedStd > 0
    ? (annualizedMean - RISK_FREE_RATE_ANNUAL) / annualizedStd
    : null;

  const downDev = downsideDeviation(returns, rfPerTrade);
  const annualizedDownDev = downDev != null ? downDev * Math.sqrt(tradesPerYear) : null;
  const sortino = annualizedDownDev && annualizedDownDev > 0
    ? (annualizedMean - RISK_FREE_RATE_ANNUAL) / annualizedDownDev
    : null;

  const maxDdPct = drawdowns.maxDrawdownPct;
  const calmar = maxDdPct != null && maxDdPct > 0 && cagr != null
    ? cagr / maxDdPct
    : null;

  const threshold = rfPerTrade;
  const omegaGains = returns.reduce((s, r) => s + Math.max(r - threshold, 0), 0);
  const omegaLosses = returns.reduce((s, r) => s + Math.max(threshold - r, 0), 0);
  const omega = omegaLosses > 0 ? omegaGains / omegaLosses : null;

  let treynor = null;
  let informationRatio = null;
  let benchmarked = false;

  const pairs = benchmarkBars?.length ? alignedBenchmarkReturns(trades, benchmarkBars) : [];
  if (pairs.length >= 2) {
    benchmarked = true;
    const pReturns = pairs.map((p) => p.portfolioReturn);
    const bReturns = pairs.map((p) => p.benchmarkReturn);
    const beta = computeBeta(pReturns, bReturns);

    if (beta != null && Math.abs(beta) > 1e-8) {
      treynor = (annualizedMean - RISK_FREE_RATE_ANNUAL) / beta;
    }

    const activeReturns = pairs.map((p) => p.portfolioReturn - p.benchmarkReturn);
    const activeStd = sampleStd(activeReturns);
    if (activeStd && activeStd > 0) {
      informationRatio = (mean(activeReturns) / activeStd) * Math.sqrt(tradesPerYear);
    }
  }

  return {
    sharpe,
    sortino,
    calmar,
    treynor,
    omega,
    informationRatio,
    maxDrawdownPct: maxDdPct,
    benchmarked,
    benchmarkTicker: BENCHMARK_TICKER,
  };
}

/**
 * Drawdown and downside risk metrics from the equity curve and per-trade returns.
 */
export function computeDrawdownMetrics({ closedTrades, portfolioValue, netPL }) {
  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  const { returns, equityPoints } = buildTradeReturnSeries(closedTrades, beginningEquity);
  const drawdowns = analyzeDrawdowns(equityPoints);
  const { varPct, cvarPct } = computeVaRMetrics(returns);

  const recoveryFactor = drawdowns.maxDrawdownDollars != null
    && drawdowns.maxDrawdownDollars > 0
    && netPL != null
    ? netPL / drawdowns.maxDrawdownDollars
    : null;

  return {
    maxDrawdownPct: drawdowns.maxDrawdownPct,
    avgDrawdownPct: drawdowns.avgDrawdownPct,
    longestDrawdownMs: drawdowns.longestDrawdownMs,
    recoveryFactor,
    ulcerIndex: drawdowns.ulcerIndex,
    varPct,
    cvarPct,
  };
}

/**
 * Volatility and statistical risk metrics from per-trade returns.
 * Daily/monthly figures are trade-frequency equivalents scaled to calendar periods.
 */
export function computeVolatilityMetrics({
  closedTrades,
  portfolioValue,
  netPL,
  benchmarkBars = null,
}) {
  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  const { returns, trades, tradesPerYear } = buildTradeReturnSeries(closedTrades, beginningEquity);

  if (returns.length < 2 || !tradesPerYear) {
    return {
      stdDevDailyPct: null,
      stdDevMonthlyPct: null,
      stdDevAnnualPct: null,
      downsideDevPct: null,
      downsideDevAnnualPct: null,
      beta: null,
      skewness: null,
      kurtosis: null,
      benchmarked: false,
      benchmarkTicker: BENCHMARK_TICKER,
    };
  }

  const tradeStd = sampleStd(returns);
  const rfPerTrade = RISK_FREE_RATE_ANNUAL / tradesPerYear;
  const downDev = downsideDeviation(returns, rfPerTrade);

  const stdDevDailyPct = tradeStd != null
    ? annualizeVolatility(tradeStd, tradesPerYear / 252) * 100
    : null;
  const stdDevMonthlyPct = tradeStd != null
    ? annualizeVolatility(tradeStd, tradesPerYear / 12) * 100
    : null;
  const stdDevAnnualPct = tradeStd != null
    ? annualizeVolatility(tradeStd, tradesPerYear) * 100
    : null;

  const downsideDevPct = downDev != null ? downDev * 100 : null;
  const downsideDevAnnualPct = downDev != null
    ? annualizeVolatility(downDev, tradesPerYear) * 100
    : null;

  let beta = null;
  let benchmarked = false;
  const pairs = benchmarkBars?.length ? alignedBenchmarkReturns(trades, benchmarkBars) : [];
  if (pairs.length >= 2) {
    benchmarked = true;
    beta = computeBeta(
      pairs.map((p) => p.portfolioReturn),
      pairs.map((p) => p.benchmarkReturn)
    );
  }

  return {
    stdDevDailyPct,
    stdDevMonthlyPct,
    stdDevAnnualPct,
    downsideDevPct,
    downsideDevAnnualPct,
    beta,
    skewness: sampleSkewness(returns),
    kurtosis: sampleExcessKurtosis(returns),
    benchmarked,
    benchmarkTicker: BENCHMARK_TICKER,
  };
}

function holdingPeriodMs(o) {
  const open = tradeOpenTime(o);
  const close = tradeCloseTime(o);
  if (isNaN(open) || isNaN(close) || close <= open) return null;
  return close - open;
}

function longestOutcomeStreak(trades, outcome) {
  const sorted = trades
    .slice()
    .filter((o) => o.unrealized_pl != null && !isNaN(Number(o.unrealized_pl)))
    .sort((a, b) => tradeCloseTime(a) - tradeCloseTime(b));

  let max = 0;
  let current = 0;

  for (const trade of sorted) {
    const pl = Number(trade.unrealized_pl);
    const matches = outcome === "win" ? pl > 0 : pl < 0;
    if (matches) {
      current += 1;
      if (current > max) max = current;
    } else {
      current = 0;
    }
  }

  return max;
}

/** Closed-trade activity stats: counts, holding periods, extremes, streaks. */
export function computeTradeLevelMetrics({ closedTrades, openTrades }) {
  const closed = closedTrades.filter(
    (o) => o.unrealized_pl != null && !isNaN(Number(o.unrealized_pl))
  );

  const winners = closed.filter((o) => Number(o.unrealized_pl) > 0);
  const losers = closed.filter((o) => Number(o.unrealized_pl) < 0);

  const winCount = winners.length;
  const lossCount = losers.length;

  const winnerHolds = winners.map(holdingPeriodMs).filter((ms) => ms != null);
  const loserHolds = losers.map(holdingPeriodMs).filter((ms) => ms != null);

  const avgWin = winCount
    ? winners.reduce((s, o) => s + Number(o.unrealized_pl), 0) / winCount
    : null;
  const avgLoss = lossCount
    ? losers.reduce((s, o) => s + Number(o.unrealized_pl), 0) / lossCount
    : null;

  const payoffRatio = avgWin != null && avgLoss != null && avgLoss < 0
    ? avgWin / Math.abs(avgLoss)
    : null;

  const winPLs = winners.map((o) => Number(o.unrealized_pl));
  const lossPLs = losers.map((o) => Number(o.unrealized_pl));

  return {
    totalTrades: closed.length + openTrades.length,
    closedTrades: closed.length,
    openTrades: openTrades.length,
    winCount,
    lossCount,
    avgHoldWinnersMs: winnerHolds.length ? mean(winnerHolds) : null,
    avgHoldLosersMs: loserHolds.length ? mean(loserHolds) : null,
    largestWin: winPLs.length ? Math.max(...winPLs) : null,
    largestLoss: lossPLs.length ? Math.min(...lossPLs) : null,
    longestWinStreak: longestOutcomeStreak(closed, "win"),
    longestLossStreak: longestOutcomeStreak(closed, "loss"),
    payoffRatio,
  };
}

/** Dollar notional for an order (qty × fill or entry price). */
export function positionNotional(o) {
  const qty = o.qty != null ? Number(o.qty) : null;
  if (!qty || qty <= 0) return null;
  const px = o.filled_avg_price != null
    ? Number(o.filled_avg_price)
    : o.entry_price != null
      ? Number(o.entry_price)
      : null;
  if (px == null || px <= 0 || isNaN(px)) return null;
  return qty * px;
}

function computeExposureStats(orders, beginningEquity) {
  if (beginningEquity == null || beginningEquity <= 0) {
    return { avgCapitalUtilPct: null, avgLeverage: null, maxLeverage: null };
  }

  const events = [];
  for (const o of orders) {
    const notional = positionNotional(o);
    const openMs = tradeOpenTime(o);
    if (!notional || isNaN(openMs)) continue;

    events.push({ t: openMs, type: "open", id: o.id, notional });

    if (o.is_open) continue;
    const closeMs = tradeCloseTime(o);
    if (!isNaN(closeMs) && closeMs >= openMs) {
      const pl = o.unrealized_pl != null ? Number(o.unrealized_pl) : 0;
      events.push({
        t: closeMs,
        type: "close",
        id: o.id,
        notional,
        pl: isNaN(pl) ? 0 : pl,
      });
    }
  }

  if (!events.length) {
    return { avgCapitalUtilPct: null, avgLeverage: null, maxLeverage: null };
  }

  events.sort((a, b) => a.t - b.t || (a.type === "close" ? 1 : -1));

  const openMap = new Map();
  let equity = beginningEquity;
  let weightedUtil = 0;
  let weightedLev = 0;
  let totalWeight = 0;
  let maxLeverage = 0;
  let prevT = events[0].t;

  const snapshot = () => {
    const deployed = [...openMap.values()].reduce((s, n) => s + n, 0);
    if (equity <= 0) return null;
    const util = (deployed / equity) * 100;
    const lev = deployed / equity;
    if (lev > maxLeverage) maxLeverage = lev;
    return { util, lev };
  };

  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    if (i > 0) {
      const dt = ev.t - prevT;
      if (dt > 0) {
        const snap = snapshot();
        if (snap) {
          weightedUtil += snap.util * dt;
          weightedLev += snap.lev * dt;
          totalWeight += dt;
        }
      }
    }

    if (ev.type === "open") openMap.set(ev.id, ev.notional);
    else {
      openMap.delete(ev.id);
      equity += ev.pl ?? 0;
    }
    prevT = ev.t;
  }

  if (openMap.size > 0) {
    const dt = Date.now() - prevT;
    if (dt > 0) {
      const snap = snapshot();
      if (snap) {
        weightedUtil += snap.util * dt;
        weightedLev += snap.lev * dt;
        totalWeight += dt;
      }
    }
  }

  return {
    avgCapitalUtilPct: totalWeight > 0 ? weightedUtil / totalWeight : null,
    avgLeverage: totalWeight > 0 ? weightedLev / totalWeight : null,
    maxLeverage: maxLeverage > 0 ? maxLeverage : null,
  };
}

/** Position sizing, capital deployment, leverage, and Kelly estimate. */
export function computePositionSizingMetrics({
  orders,
  closedTrades,
  portfolioValue,
  netPL,
}) {
  const notionals = orders.map(positionNotional).filter((n) => n != null && n > 0);

  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  const exposure = computeExposureStats(orders, beginningEquity);

  const closed = closedTrades.filter(
    (o) => o.unrealized_pl != null && !isNaN(Number(o.unrealized_pl))
  );
  const winners = closed.filter((o) => Number(o.unrealized_pl) > 0);
  const losers = closed.filter((o) => Number(o.unrealized_pl) < 0);
  const winRate = closed.length ? winners.length / closed.length : null;
  const avgWin = winners.length
    ? winners.reduce((s, o) => s + Number(o.unrealized_pl), 0) / winners.length
    : null;
  const avgLoss = losers.length
    ? losers.reduce((s, o) => s + Number(o.unrealized_pl), 0) / losers.length
    : null;
  const payoffRatio = avgWin != null && avgLoss != null && avgLoss < 0
    ? avgWin / Math.abs(avgLoss)
    : null;

  let kellyFraction = null;
  if (winRate != null && payoffRatio != null && payoffRatio > 0) {
    kellyFraction = winRate - (1 - winRate) / payoffRatio;
  }

  const kellyPct = kellyFraction != null ? kellyFraction * 100 : null;
  const kellyOptimalSize = kellyFraction != null && pv != null && kellyFraction > 0
    ? kellyFraction * pv
    : null;

  return {
    avgPositionSize: notionals.length ? mean(notionals) : null,
    maxPositionSize: notionals.length ? Math.max(...notionals) : null,
    avgCapitalUtilPct: exposure.avgCapitalUtilPct,
    avgLeverage: exposure.avgLeverage,
    maxLeverage: exposure.maxLeverage,
    kellyPct,
    kellyOptimalSize,
  };
}

export const fmtR = (v, digits = 2) => {
  if (v == null || isNaN(v)) return "—";
  return `${v > 0 ? "+" : ""}${Number(v).toFixed(digits)}R`;
};

function monthKey(ms) {
  const d = new Date(ms);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}

function weekKey(ms) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - d.getDay());
  // Build the key from local date parts directly (not toISOString, which
  // converts to UTC and can roll the date back a day for positive UTC
  // offsets, misassigning trades to the wrong week).
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function profitablePeriodPct(closedTrades, granularity) {
  const groups = new Map();

  for (const o of closedTrades) {
    const closeMs = tradeCloseTime(o);
    if (isNaN(closeMs) || o.unrealized_pl == null) continue;
    const pl = Number(o.unrealized_pl);
    if (isNaN(pl)) continue;

    const key = granularity === "week" ? weekKey(closeMs) : monthKey(closeMs);
    groups.set(key, (groups.get(key) ?? 0) + pl);
  }

  if (!groups.size) return null;
  let profitable = 0;
  for (const sum of groups.values()) {
    if (sum > 0) profitable += 1;
  }
  return (profitable / groups.size) * 100;
}

function rMultiplesFor(trades) {
  return trades
    .map((o) => {
      const risk = riskAmtFor(o);
      if (!risk || risk <= 0 || o.unrealized_pl == null) return null;
      const r = Number(o.unrealized_pl) / risk;
      return isNaN(r) ? null : r;
    })
    .filter((r) => r != null);
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** Consistency metrics: periodic profitability, SQN, R distribution, trade cadence. */
export function computeConsistencyMetrics({ closedTrades }) {
  const closed = closedTrades.filter(
    (o) => o.unrealized_pl != null && !isNaN(Number(o.unrealized_pl))
  );

  const profitableWeeksPct = profitablePeriodPct(closed, "week");
  const profitableMonthsPct = profitablePeriodPct(closed, "month");

  const rList = rMultiplesFor(closed);
  const avgR = rList.length ? mean(rList) : null;
  const stdR = rList.length >= 2 ? sampleStd(rList) : null;
  const sqn = stdR && stdR > 0 && rList.length >= 2
    ? (avgR / stdR) * Math.sqrt(rList.length)
    : null;

  let rBelow0Pct = null;
  let rZeroToOnePct = null;
  let rAboveOnePct = null;
  let rMedian = null;

  if (rList.length) {
    rMedian = median(rList);
    rBelow0Pct = (rList.filter((r) => r < 0).length / rList.length) * 100;
    rZeroToOnePct = (rList.filter((r) => r >= 0 && r < 1).length / rList.length) * 100;
    rAboveOnePct = (rList.filter((r) => r >= 1).length / rList.length) * 100;
  }

  const closeTimes = closed
    .map((o) => tradeCloseTime(o))
    .filter((t) => !isNaN(t))
    .sort((a, b) => a - b);

  let tradesPerDay = null;
  let tradesPerWeek = null;
  let tradesPerMonth = null;

  if (closeTimes.length) {
    const spanDays = closeTimes.length >= 2
      ? Math.max(1, (closeTimes[closeTimes.length - 1] - closeTimes[0]) / (24 * 3600 * 1000))
      : 1;
    tradesPerDay = closed.length / spanDays;
    tradesPerWeek = tradesPerDay * 7;
    tradesPerMonth = tradesPerDay * 30.4;
  }

  return {
    profitableWeeksPct,
    profitableMonthsPct,
    sqn,
    avgR,
    rMedian,
    rBelow0Pct,
    rZeroToOnePct,
    rAboveOnePct,
    rSampleSize: rList.length,
    tradesPerDay,
    tradesPerWeek,
    tradesPerMonth,
  };
}

/** Entry slippage cost in dollars (positive = adverse). */
function entrySlippageCost(o) {
  const fill = o.filled_avg_price != null ? Number(o.filled_avg_price) : null;
  const limit = o.entry_price != null ? Number(o.entry_price) : null;
  const qty = o.qty ?? 1;
  if (fill == null || limit == null || isNaN(fill) || isNaN(limit)) return null;
  const isLong = o.direction === "long";
  const perShare = isLong ? fill - limit : limit - fill;
  return perShare * qty;
}

/** Entry slippage per share (positive = adverse). */
function entrySlippagePerShare(o) {
  const fill = o.filled_avg_price != null ? Number(o.filled_avg_price) : null;
  const limit = o.entry_price != null ? Number(o.entry_price) : null;
  if (fill == null || limit == null || isNaN(fill) || isNaN(limit)) return null;
  const isLong = o.direction === "long";
  return isLong ? fill - limit : limit - fill;
}

/** Execution costs from fill vs limit slippage and net vs gross return. */
export function computeCostsMetrics({ closedTrades, portfolioValue, netPL }) {
  const slippageCosts = closedTrades
    .map(entrySlippageCost)
    .filter((v) => v != null && !isNaN(v));
  const slippagePerShare = closedTrades
    .map(entrySlippagePerShare)
    .filter((v) => v != null && !isNaN(v));

  const totalSlippage = slippageCosts.length
    ? slippageCosts.reduce((s, v) => s + v, 0)
    : null;
  const avgSlippagePerTrade = slippageCosts.length ? mean(slippageCosts) : null;
  const avgSlippagePerShare = slippagePerShare.length ? mean(slippagePerShare) : null;

  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  let netReturnPct = null;
  let grossReturnPct = null;

  if (beginningEquity != null && beginningEquity > 0 && netPL != null) {
    netReturnPct = (netPL / beginningEquity) * 100;
    grossReturnPct = totalSlippage != null
      ? ((netPL + totalSlippage) / beginningEquity) * 100
      : netReturnPct;
  }

  return {
    totalSlippage,
    avgSlippagePerTrade,
    avgSlippagePerShare,
    slippageSampleSize: slippageCosts.length,
    netReturnPct,
    grossReturnPct,
  };
}

/**
 * Benchmark comparison vs SPY using per-trade hold-period returns.
 */
export function computeBenchmarkMetrics({
  closedTrades,
  portfolioValue,
  netPL,
  benchmarkBars = null,
}) {
  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  const beginningEquity = pv != null && !isNaN(pv) ? pv - (netPL ?? 0) : null;

  const { trades, tradesPerYear } = buildTradeReturnSeries(closedTrades, beginningEquity);
  const pairs = benchmarkBars?.length ? alignedBenchmarkReturns(trades, benchmarkBars) : [];

  if (pairs.length < 2 || !tradesPerYear) {
    return {
      alpha: null,
      beta: null,
      correlation: null,
      excessReturnPct: null,
      trackingErrorPct: null,
      benchmarked: false,
      benchmarkTicker: BENCHMARK_TICKER,
    };
  }

  const pReturns = pairs.map((p) => p.portfolioReturn);
  const bReturns = pairs.map((p) => p.benchmarkReturn);
  const beta = computeBeta(pReturns, bReturns);
  const correlation = computeCorrelation(pReturns, bReturns);

  const annualizedPortfolioPct = mean(pReturns) * tradesPerYear * 100;
  const annualizedBenchmarkPct = mean(bReturns) * tradesPerYear * 100;
  const rfPct = RISK_FREE_RATE_ANNUAL * 100;

  const alpha = beta != null
    ? annualizedPortfolioPct - (rfPct + beta * (annualizedBenchmarkPct - rfPct))
    : null;

  const activeReturns = pairs.map((p) => p.portfolioReturn - p.benchmarkReturn);
  const excessReturnPct = mean(activeReturns) * tradesPerYear * 100;
  const activeStd = sampleStd(activeReturns);
  const trackingErrorPct = activeStd != null
    ? activeStd * Math.sqrt(tradesPerYear) * 100
    : null;

  return {
    alpha,
    beta,
    correlation,
    excessReturnPct,
    trackingErrorPct,
    benchmarked: true,
    benchmarkTicker: BENCHMARK_TICKER,
  };
}

/**
 * Portfolio-level performance metrics (aligned with leaderboard formulas).
 * `portfolioValue` from Alpaca enables total return and CAGR.
 */
export function computePerformanceMetrics({ closedTrades, openTrades, portfolioValue }) {
  const closedPLs = closedTrades
    .map((o) => Number(o.unrealized_pl))
    .filter((v) => !isNaN(v));

  const realizedPL = closedPLs.reduce((s, v) => s + v, 0);

  const unrealizedPL = openTrades.reduce((s, o) => {
    if (o.unrealized_pl == null) return s;
    const v = Number(o.unrealized_pl);
    return isNaN(v) ? s : s + v;
  }, 0);

  const netPL = realizedPL + unrealizedPL;

  const winners = closedPLs.filter((p) => p > 0);
  const losers = closedPLs.filter((p) => p < 0);
  const grossProfit = winners.reduce((s, v) => s + v, 0);
  const grossLoss = losers.reduce((s, v) => s + v, 0);

  const winCount = winners.length;
  const lossCount = losers.length;
  const closedCount = closedPLs.length;

  const winRate = closedCount ? (winCount / closedCount) * 100 : null;
  const avgWin = winCount ? grossProfit / winCount : null;
  const avgLoss = lossCount ? grossLoss / lossCount : null;

  const profitFactor = grossLoss < 0 ? grossProfit / Math.abs(grossLoss) : null;

  // Mean realized P&L per closed trade. Equivalent to winRate*avgWin +
  // lossRate*avgLoss, but computed directly so breakeven trades (P&L = 0,
  // counted in closedCount but not winCount/lossCount) aren't incorrectly
  // folded into the loss-rate weight via `1 - winRate`.
  const expectancy = closedCount ? realizedPL / closedCount : null;

  const pv = portfolioValue != null ? Number(portfolioValue) : null;
  let totalReturnPct = null;
  let cagr = null;

  if (pv != null && !isNaN(pv)) {
    const beginningEquity = pv - netPL;
    if (beginningEquity > 0) {
      totalReturnPct = (netPL / beginningEquity) * 100;

      const allDates = [...closedTrades, ...openTrades]
        .map((o) => o.created_at)
        .filter(Boolean)
        .map((d) => new Date(d).getTime())
        .filter((t) => !isNaN(t));

      if (allDates.length) {
        const years = (Date.now() - Math.min(...allDates)) / (365.25 * 24 * 3600 * 1000);
        if (years > 0) {
          cagr = (Math.pow(pv / beginningEquity, 1 / years) - 1) * 100;
        }
      }
    }
  }

  return {
    netPL,
    realizedPL,
    unrealizedPL,
    grossProfit,
    grossLoss,
    avgWin,
    avgLoss,
    winRate,
    profitFactor,
    expectancy,
    totalReturnPct,
    cagr,
    closedCount,
    winCount,
    lossCount,
    openCount: openTrades.length,
  };
}
