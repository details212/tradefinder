/**
 * PerformancePanel — overall trading performance summary across all trades.
 * Receives `orders` (already synced) and `loading` from AdminPanel.
 */
import { useMemo, useState, useEffect } from "react";
import { BarChart2, Shield, TrendingDown, Activity, ListOrdered, Scale, CalendarCheck, CircleDollarSign, GitCompareArrows } from "lucide-react";
import { alpacaApi, stockApi } from "../api/client";
import {
  fmt$,
  fmtPct,
  fmtRatio,
  fmtR,
  fmtDuration,
  computePerformanceMetrics,
  computeRiskAdjustedMetrics,
  computeDrawdownMetrics,
  computeVolatilityMetrics,
  computeTradeLevelMetrics,
  computePositionSizingMetrics,
  computeConsistencyMetrics,
  computeCostsMetrics,
  computeBenchmarkMetrics,
} from "../utils/strategyMetrics";

function plColor(v) {
  if (v == null || isNaN(v)) return "text-slate-200";
  return v > 0 ? "text-emerald-400" : v < 0 ? "text-red-400" : "text-slate-200";
}

function ratioColor(v, goodAbove = 1) {
  if (v == null || isNaN(v)) return "text-slate-200";
  return v >= goodAbove ? "text-emerald-400" : "text-red-400";
}

function sqnColor(v) {
  if (v == null || isNaN(v)) return "text-slate-200";
  if (v >= 2.5) return "text-emerald-400";
  if (v >= 1.6) return "text-yellow-400";
  return "text-red-400";
}

function fmtFreq(v) {
  if (v == null || isNaN(v)) return "—";
  if (v >= 10) return v.toFixed(1);
  if (v >= 1) return v.toFixed(2);
  return v.toFixed(3);
}

function MetricCard({ label, value, sub, color }) {
  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl p-4">
      <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</p>
      <p className={`text-lg font-bold font-mono mt-1 ${color ?? "text-slate-200"}`}>{value}</p>
      {sub && <p className="text-[11px] text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
}

function toDateKey(ms) {
  if (!ms || Number.isNaN(ms)) return null;
  return new Date(ms).toISOString().slice(0, 10);
}

export default function PerformancePanel({ orders, loading }) {
  const [portfolioValue, setPortfolioValue] = useState(null);
  const [benchmarkBars, setBenchmarkBars] = useState(null);

  useEffect(() => {
    alpacaApi.test()
      .then((r) => { if (r.data.ok) setPortfolioValue(r.data.portfolio_value ?? null); })
      .catch(() => {});
  }, []);

  const closed = useMemo(() =>
    orders.filter(o => !o.is_open && o.unrealized_pl != null && o.synced_at),
    [orders]
  );

  const open = useMemo(() =>
    orders.filter(o => o.is_open),
    [orders]
  );

  const metrics = useMemo(() =>
    computePerformanceMetrics({ closedTrades: closed, openTrades: open, portfolioValue }),
    [closed, open, portfolioValue]
  );

  useEffect(() => {
    if (!closed.length) {
      setBenchmarkBars(null);
      return;
    }

    const times = closed.flatMap((o) => [o.created_at, o.closed_at, o.synced_at].filter(Boolean));
    if (!times.length) return;

    const parsed = times.map((d) => new Date(d).getTime()).filter((t) => !Number.isNaN(t));
    const from = toDateKey(Math.min(...parsed));
    const to = toDateKey(Math.max(...parsed, Date.now()));
    if (!from || !to) return;

    stockApi.history("SPY", { from, to, timespan: "day", limit: "5000" })
      .then((r) => setBenchmarkBars(r.data.bars ?? []))
      .catch(() => setBenchmarkBars(null));
  }, [closed]);

  const riskMetrics = useMemo(() =>
    computeRiskAdjustedMetrics({
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
      cagr: metrics.cagr,
      benchmarkBars,
    }),
    [closed, portfolioValue, metrics.netPL, metrics.cagr, benchmarkBars]
  );

  const drawdownMetrics = useMemo(() =>
    computeDrawdownMetrics({
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
    }),
    [closed, portfolioValue, metrics.netPL]
  );

  const volatilityMetrics = useMemo(() =>
    computeVolatilityMetrics({
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
      benchmarkBars,
    }),
    [closed, portfolioValue, metrics.netPL, benchmarkBars]
  );

  const tradeStats = useMemo(() =>
    computeTradeLevelMetrics({ closedTrades: closed, openTrades: open }),
    [closed, open]
  );

  const sizingMetrics = useMemo(() =>
    computePositionSizingMetrics({
      orders,
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
    }),
    [orders, closed, portfolioValue, metrics.netPL]
  );

  const consistencyMetrics = useMemo(() =>
    computeConsistencyMetrics({ closedTrades: closed }),
    [closed]
  );

  const costsMetrics = useMemo(() =>
    computeCostsMetrics({
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
    }),
    [closed, portfolioValue, metrics.netPL]
  );

  const benchmarkMetrics = useMemo(() =>
    computeBenchmarkMetrics({
      closedTrades: closed,
      portfolioValue,
      netPL: metrics.netPL,
      benchmarkBars,
    }),
    [closed, portfolioValue, metrics.netPL, benchmarkBars]
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
        Loading performance…
      </div>
    );
  }

  if (!closed.length && !open.length) {
    return (
      <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-10 text-center">
        <BarChart2 className="w-10 h-10 text-slate-500 mx-auto mb-3" />
        <p className="text-slate-400 font-medium">No trade data yet</p>
        <p className="text-slate-500 text-xs mt-1">Performance metrics will appear after trades are placed and synced</p>
      </div>
    );
  }

  const netPos = metrics.netPL > 0;
  const pf = metrics.profitFactor;
  const benchmarkNote = riskMetrics.benchmarked
    ? `Benchmarked vs ${riskMetrics.benchmarkTicker}`
    : "Treynor & information ratio require SPY benchmark data";
  const volatilityBenchmarkNote = volatilityMetrics.benchmarked
    ? `Beta vs ${volatilityMetrics.benchmarkTicker}`
    : "Beta requires SPY benchmark data";

  return (
    <div className="flex flex-col gap-6">

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <h3 className="text-sm font-semibold text-slate-200">Key Metrics</h3>
          <p className="text-xs text-slate-400 mt-0.5">
            {metrics.closedCount} closed trade{metrics.closedCount !== 1 ? "s" : ""}
            {metrics.openCount > 0 && <> · {metrics.openCount} open now</>}
            {portfolioValue == null && <> · Connect Alpaca for return metrics</>}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <MetricCard
            label="Net P&L (realized + unrealized)"
            value={`${netPos ? "+" : ""}${fmt$(metrics.netPL)}`}
            sub={`Realized ${fmt$(metrics.realizedPL)} · Unrealized ${fmt$(metrics.unrealizedPL)}`}
            color={plColor(metrics.netPL)}
          />
          <MetricCard
            label="Total return (%)"
            value={fmtPct(metrics.totalReturnPct)}
            sub={portfolioValue != null ? `Based on ${fmt$(Number(portfolioValue) - metrics.netPL)} starting equity` : "Requires Alpaca portfolio value"}
            color={plColor(metrics.totalReturnPct)}
          />
          <MetricCard
            label="CAGR"
            value={fmtPct(metrics.cagr)}
            sub="Compound annual growth rate"
            color={plColor(metrics.cagr)}
          />
          <MetricCard
            label="Gross profit / Gross loss"
            value={`${fmt$(metrics.grossProfit)} / ${fmt$(Math.abs(metrics.grossLoss))}`}
            sub={metrics.closedCount > 0 ? `${metrics.winCount} wins · ${metrics.lossCount} losses` : null}
            color="text-slate-200"
          />
          <MetricCard
            label="Average win / Average loss"
            value={`${fmt$(metrics.avgWin)} / ${fmt$(metrics.avgLoss)}`}
            color="text-slate-200"
          />
          <MetricCard
            label="Win rate (%)"
            value={fmtPct(metrics.winRate, 0)}
            sub={metrics.closedCount > 0 ? `${metrics.winCount}W / ${metrics.lossCount}L` : null}
            color={metrics.winRate != null && metrics.winRate >= 50 ? "text-emerald-400" : "text-red-400"}
          />
          <MetricCard
            label="Profit factor"
            value={pf != null ? pf.toFixed(2) : "—"}
            sub="Gross profit ÷ gross loss"
            color={pf != null && pf > 1 ? "text-emerald-400" : pf != null ? "text-red-400" : "text-slate-200"}
          />
          <MetricCard
            label="Expectancy per trade"
            value={metrics.expectancy != null ? fmt$(metrics.expectancy) : "—"}
            sub="Expected P&L per closed trade"
            color={plColor(metrics.expectancy)}
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <Shield className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Risk-Adjusted Performance</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            How returns compare to risk taken · {benchmarkNote}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
          <MetricCard
            label="Sharpe ratio"
            value={fmtRatio(riskMetrics.sharpe)}
            sub="Excess return per unit of total volatility"
            color={ratioColor(riskMetrics.sharpe)}
          />
          <MetricCard
            label="Sortino ratio"
            value={fmtRatio(riskMetrics.sortino)}
            sub="Excess return per unit of downside volatility"
            color={ratioColor(riskMetrics.sortino)}
          />
          <MetricCard
            label="Calmar ratio"
            value={fmtRatio(riskMetrics.calmar)}
            sub="CAGR ÷ max drawdown"
            color={ratioColor(riskMetrics.calmar)}
          />
          <MetricCard
            label="Treynor ratio"
            value={fmtRatio(riskMetrics.treynor)}
            sub={riskMetrics.benchmarked ? `Excess return per unit of ${riskMetrics.benchmarkTicker} beta` : "Requires benchmark"}
            color={ratioColor(riskMetrics.treynor)}
          />
          <MetricCard
            label="Omega ratio"
            value={fmtRatio(riskMetrics.omega)}
            sub="Gain probability weight ÷ loss probability weight"
            color={ratioColor(riskMetrics.omega)}
          />
          <MetricCard
            label="Information ratio"
            value={fmtRatio(riskMetrics.informationRatio)}
            sub={riskMetrics.benchmarked ? `Active return vs ${riskMetrics.benchmarkTicker} tracking error` : "Requires benchmark"}
            color={ratioColor(riskMetrics.informationRatio)}
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <TrendingDown className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Drawdown & Downside Risk</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Peak-to-trough equity declines and tail-loss estimates per closed trade
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
          <MetricCard
            label="Maximum drawdown (%)"
            value={fmtPct(drawdownMetrics.maxDrawdownPct)}
            sub="Largest peak-to-trough equity decline"
            color="text-red-400"
          />
          <MetricCard
            label="Average drawdown"
            value={fmtPct(drawdownMetrics.avgDrawdownPct)}
            sub="Mean depth of completed drawdown episodes"
            color="text-red-400"
          />
          <MetricCard
            label="Longest drawdown duration"
            value={fmtDuration(drawdownMetrics.longestDrawdownMs)}
            sub="Peak to recovery (or latest data)"
            color="text-slate-200"
          />
          <MetricCard
            label="Recovery factor"
            value={fmtRatio(drawdownMetrics.recoveryFactor)}
            sub="Net profit ÷ max drawdown ($)"
            color={ratioColor(drawdownMetrics.recoveryFactor)}
          />
          <MetricCard
            label="Ulcer Index"
            value={fmtRatio(drawdownMetrics.ulcerIndex)}
            sub="√ mean of squared drawdown %"
            color={drawdownMetrics.ulcerIndex != null && drawdownMetrics.ulcerIndex < 5 ? "text-emerald-400" : "text-red-400"}
          />
          <MetricCard
            label="VaR (95%) / CVaR"
            value={
              drawdownMetrics.varPct != null && drawdownMetrics.cvarPct != null
                ? `${fmtPct(drawdownMetrics.varPct)} / ${fmtPct(drawdownMetrics.cvarPct)}`
                : "—"
            }
            sub="Per-trade return at 95% confidence / expected shortfall"
            color="text-red-400"
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Volatility & Statistical Risk</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Return dispersion and distribution shape · {volatilityBenchmarkNote}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <MetricCard
            label="Std dev of returns"
            value={
              volatilityMetrics.stdDevDailyPct != null
                ? `${fmtPct(volatilityMetrics.stdDevDailyPct)} / ${fmtPct(volatilityMetrics.stdDevMonthlyPct)} / ${fmtPct(volatilityMetrics.stdDevAnnualPct)}`
                : "—"
            }
            sub="Daily / monthly / annualized (equivalent)"
            color="text-slate-200"
          />
          <MetricCard
            label="Downside deviation"
            value={
              volatilityMetrics.downsideDevPct != null
                ? `${fmtPct(volatilityMetrics.downsideDevPct)} / ${fmtPct(volatilityMetrics.downsideDevAnnualPct)}`
                : "—"
            }
            sub="Per-trade / annualized vs risk-free threshold"
            color="text-red-400"
          />
          <MetricCard
            label="Beta (vs benchmark)"
            value={fmtRatio(volatilityMetrics.beta)}
            sub={volatilityMetrics.benchmarked ? `Sensitivity to ${volatilityMetrics.benchmarkTicker}` : "Requires benchmark"}
            color="text-slate-200"
          />
          <MetricCard
            label="Skewness / Kurtosis"
            value={
              volatilityMetrics.skewness != null || volatilityMetrics.kurtosis != null
                ? `${fmtRatio(volatilityMetrics.skewness)} / ${fmtRatio(volatilityMetrics.kurtosis)}`
                : "—"
            }
            sub="Return asymmetry / tail thickness (excess kurtosis)"
            color="text-slate-200"
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <ListOrdered className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Trade-Level Statistics</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            {tradeStats.closedTrades} closed trade{tradeStats.closedTrades !== 1 ? "s" : ""}
            {tradeStats.openTrades > 0 && <> · {tradeStats.openTrades} open now</>}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
          <MetricCard
            label="Total number of trades"
            value={tradeStats.totalTrades}
            sub={`${tradeStats.closedTrades} closed${tradeStats.openTrades > 0 ? ` · ${tradeStats.openTrades} open` : ""}`}
            color="text-slate-200"
          />
          <MetricCard
            label="Win count / Loss count"
            value={`${tradeStats.winCount} / ${tradeStats.lossCount}`}
            sub={tradeStats.closedTrades > 0
              ? `${fmtPct((tradeStats.winCount / tradeStats.closedTrades) * 100, 0)} win rate`
              : null}
            color="text-slate-200"
          />
          <MetricCard
            label="Average holding period"
            value={
              tradeStats.avgHoldWinnersMs != null || tradeStats.avgHoldLosersMs != null
                ? `${fmtDuration(tradeStats.avgHoldWinnersMs)} / ${fmtDuration(tradeStats.avgHoldLosersMs)}`
                : "—"
            }
            sub="Winners vs losers"
            color="text-slate-200"
          />
          <MetricCard
            label="Largest winning / losing trade"
            value={`${fmt$(tradeStats.largestWin)} / ${fmt$(tradeStats.largestLoss)}`}
            color="text-slate-200"
          />
          <MetricCard
            label="Longest winning / losing streak"
            value={`${tradeStats.longestWinStreak} / ${tradeStats.longestLossStreak}`}
            sub="Consecutive closed trades"
            color="text-slate-200"
          />
          <MetricCard
            label="Payoff ratio"
            value={fmtRatio(tradeStats.payoffRatio)}
            sub="Average win ÷ |average loss|"
            color={ratioColor(tradeStats.payoffRatio)}
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <Scale className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Position Sizing & Exposure</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Notional deployment and optimal sizing from trade history
            {portfolioValue == null && <> · Connect Alpaca for utilization & Kelly estimates</>}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
          <MetricCard
            label="Average position size"
            value={fmt$(sizingMetrics.avgPositionSize)}
            sub="Mean qty × fill/entry price"
            color="text-slate-200"
          />
          <MetricCard
            label="Max position size"
            value={fmt$(sizingMetrics.maxPositionSize)}
            sub="Largest single-trade notional"
            color="text-slate-200"
          />
          <MetricCard
            label="Capital utilization"
            value={fmtPct(sizingMetrics.avgCapitalUtilPct)}
            sub="Time-weighted % of equity deployed"
            color="text-slate-200"
          />
          <MetricCard
            label="Leverage used"
            value={
              sizingMetrics.avgLeverage != null || sizingMetrics.maxLeverage != null
                ? `${fmtRatio(sizingMetrics.avgLeverage)}× / ${fmtRatio(sizingMetrics.maxLeverage)}×`
                : "—"
            }
            sub="Average / peak gross exposure ÷ equity"
            color="text-slate-200"
          />
          <MetricCard
            label="Kelly Criterion / optimal size"
            value={
              sizingMetrics.kellyPct != null
                ? `${fmtPct(sizingMetrics.kellyPct)} / ${fmt$(sizingMetrics.kellyOptimalSize)}`
                : "—"
            }
            sub="Optimal capital fraction / estimated position ($)"
            color={
              sizingMetrics.kellyPct != null && sizingMetrics.kellyPct > 0
                ? "text-emerald-400"
                : sizingMetrics.kellyPct != null
                  ? "text-red-400"
                  : "text-slate-200"
            }
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <CalendarCheck className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Consistency & Reliability</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            How steadily the system produces results over time
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <MetricCard
            label="Profitable weeks / months"
            value={
              consistencyMetrics.profitableWeeksPct != null || consistencyMetrics.profitableMonthsPct != null
                ? `${fmtPct(consistencyMetrics.profitableWeeksPct, 0)} / ${fmtPct(consistencyMetrics.profitableMonthsPct, 0)}`
                : "—"
            }
            sub="% of calendar weeks / months with net profit"
            color={
              consistencyMetrics.profitableMonthsPct != null && consistencyMetrics.profitableMonthsPct >= 50
                ? "text-emerald-400"
                : "text-slate-200"
            }
          />
          <MetricCard
            label="System Quality Number (SQN)"
            value={fmtRatio(consistencyMetrics.sqn)}
            sub="Van Tharp: (avg R ÷ σ R) × √n"
            color={sqnColor(consistencyMetrics.sqn)}
          />
          <MetricCard
            label="R-multiple distribution"
            value={
              consistencyMetrics.rSampleSize > 0
                ? `${fmtPct(consistencyMetrics.rBelow0Pct, 0)} / ${fmtPct(consistencyMetrics.rZeroToOnePct, 0)} / ${fmtPct(consistencyMetrics.rAboveOnePct, 0)}`
                : "—"
            }
            sub={
              consistencyMetrics.rSampleSize > 0
                ? `<0R / 0–1R / ≥1R · median ${fmtR(consistencyMetrics.rMedian)}`
                : "Requires stop/risk data per trade"
            }
            color="text-slate-200"
          />
          <MetricCard
            label="Trade frequency"
            value={
              consistencyMetrics.tradesPerDay != null
                ? `${fmtFreq(consistencyMetrics.tradesPerDay)} / ${fmtFreq(consistencyMetrics.tradesPerWeek)} / ${fmtFreq(consistencyMetrics.tradesPerMonth)}`
                : "—"
            }
            sub="Trades per day / week / month"
            color="text-slate-200"
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <CircleDollarSign className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Costs & Frictions</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Entry fill quality vs limit price
            {portfolioValue == null && <> · Connect Alpaca for return comparison</>}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-2 gap-3">
          <MetricCard
            label="Slippage estimate"
            value={
              costsMetrics.totalSlippage != null
                ? `${costsMetrics.totalSlippage > 0 ? "+" : ""}${fmt$(costsMetrics.totalSlippage)}`
                : "—"
            }
            sub={
              costsMetrics.slippageSampleSize > 0
                ? `Avg ${fmt$(costsMetrics.avgSlippagePerTrade)}/trade · ${costsMetrics.avgSlippagePerShare != null ? `${costsMetrics.avgSlippagePerShare > 0 ? "+" : ""}${costsMetrics.avgSlippagePerShare.toFixed(3)}/sh` : "—"} · ${costsMetrics.slippageSampleSize} fills`
                : "Requires fill and limit price data"
            }
            color={
              costsMetrics.totalSlippage != null && costsMetrics.totalSlippage > 0
                ? "text-red-400"
                : costsMetrics.totalSlippage != null && costsMetrics.totalSlippage < 0
                  ? "text-emerald-400"
                  : "text-slate-200"
            }
          />
          <MetricCard
            label="Net-of-cost / gross return"
            value={
              costsMetrics.netReturnPct != null && costsMetrics.grossReturnPct != null
                ? `${fmtPct(costsMetrics.netReturnPct)} / ${fmtPct(costsMetrics.grossReturnPct)}`
                : "—"
            }
            sub="Actual return vs return before entry slippage"
            color={plColor(costsMetrics.netReturnPct)}
          />
        </div>
      </div>

      <div className="flex flex-col gap-4">
        <div className="pb-2 border-b border-slate-800/60">
          <div className="flex items-center gap-2">
            <GitCompareArrows className="w-4 h-4 text-slate-400" />
            <h3 className="text-sm font-semibold text-slate-200">Benchmark Comparison</h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            {benchmarkMetrics.benchmarked
              ? `Tracking against ${benchmarkMetrics.benchmarkTicker} over matched trade hold periods`
              : "Requires SPY history and at least 2 closed trades with hold-period overlap"}
          </p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <MetricCard
            label="Alpha"
            value={fmtPct(benchmarkMetrics.alpha)}
            sub="Jensen's alpha (annualized vs SPY & risk-free)"
            color={plColor(benchmarkMetrics.alpha)}
          />
          <MetricCard
            label="Beta"
            value={fmtRatio(benchmarkMetrics.beta)}
            sub={benchmarkMetrics.benchmarked ? `Sensitivity to ${benchmarkMetrics.benchmarkTicker}` : "Requires benchmark"}
            color="text-slate-200"
          />
          <MetricCard
            label="Correlation to benchmark"
            value={fmtRatio(benchmarkMetrics.correlation)}
            sub={benchmarkMetrics.benchmarked ? `Pearson vs ${benchmarkMetrics.benchmarkTicker} returns` : "Requires benchmark"}
            color="text-slate-200"
          />
          <MetricCard
            label="Excess return / tracking error"
            value={
              benchmarkMetrics.excessReturnPct != null && benchmarkMetrics.trackingErrorPct != null
                ? `${fmtPct(benchmarkMetrics.excessReturnPct)} / ${fmtPct(benchmarkMetrics.trackingErrorPct)}`
                : "—"
            }
            sub="Annualized active return / volatility of active return"
            color={plColor(benchmarkMetrics.excessReturnPct)}
          />
        </div>
      </div>

    </div>
  );
}
