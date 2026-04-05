import React, { useState, useEffect, useCallback, useRef } from "react";
import { tradeIdeasApi, stockApi, snapshotsApi, alpacaApi } from "../api/client";
import {
  Lightbulb, TrendingUp, TrendingDown, RefreshCw,
  ChevronRight, AlertCircle, Loader2, ArrowUpRight,
  Star, StarOff, X, Info,
} from "lucide-react";
import ModalChart from "./ModalChart";
import { fmtEtString } from "../utils/timeUtils";

// ── Column display config ─────────────────────────────────────────────────────
const COL_META = {
  // shared
  ticker:                { label: "Ticker",         fmt: "ticker"  },
  _live:                 { label: "Live",           fmt: "live"    },
  // strategy 1
  bar_time:              { label: "Bar Time",       fmt: "dt"      },
  close:                 { label: "Close",          fmt: "price"   },
  high_5d:               { label: "5D High",        fmt: "price"   },
  low_5d:                { label: "5D Low",         fmt: "price"   },
  range_5d:              { label: "5D Range",       fmt: "price"   },
  pct_from_5d_low:       { label: "% 5D Low",       fmt: "pct"     },
  resistance:            { label: "Resistance",     fmt: "price"   },
  prev_bar_close:        { label: "Prev Bar",       fmt: "price"   },
  high_60min:            { label: "60m High",       fmt: "price"   },
  is_near_5d_low:        { label: "Near 5D Low",    fmt: "bool"    },
  cross_above_resistance:{ label: "Cross Resist.",  fmt: "bool"    },
  new_60min_high:        { label: "New 60m High",   fmt: "bool"    },
  trigger_fired:         { label: "Trigger",        fmt: "bool"    },
  updated_at:            { label: "Updated",        fmt: "dt_dim"  },
  // strategy 2
  signal_date:           { label: "Date",           fmt: "date"    },
  first_signal_time:     { label: "First Signal",   fmt: "dt"      },
  signal_count:          { label: "Count",          fmt: "num"     },
  first_entry:           { label: "Entry",          fmt: "price"   },
  tightest_stop:         { label: "Stop",           fmt: "price"   },
  // strategy 3
  prev_day_close:        { label: "Prev Close",     fmt: "price"   },
  pct_from_prev:         { label: "% Chg",          fmt: "pct"     },
  down_from_prev:        { label: "Down",           fmt: "bool"    },
  breakdown:             { label: "Breakdown",      fmt: "bool"    },
  spy_5m_change:         { label: "SPY 5m Chg",    fmt: "pct"     },
  market_ok_to_short:    { label: "Mkt Short OK",   fmt: "bool"    },
  // strategy 5
  stop_level:            { label: "Stop Level",     fmt: "price"   },
  pts_below_support:     { label: "Pts Below",      fmt: "price"   },
  prior_day_candle_pct:  { label: "Prior Day %",    fmt: "pct"     },
  two_day_change:        { label: "2-Day Chg",      fmt: "pct"     },
  // strategy 4 (resistance shares COL_META with strategy 1)
  prev_range_mid:        { label: "Range Mid",      fmt: "price"   },
  avg_volume_10d:        { label: "Avg Vol 10D",    fmt: "vol"     },
  today_volume:          { label: "Today Vol",      fmt: "vol"     },
  relative_volume:       { label: "Rel Vol",        fmt: "rvol"    },
  above_range_mid:       { label: "Above Mid",      fmt: "bool"    },
  shares_traded_ok:      { label: "Vol ≥ 125k",     fmt: "bool"    },
  breakout:              { label: "Breakout",       fmt: "bool"    },
  spy_30m_change:        { label: "SPY 30m Chg",   fmt: "pct"     },
  market_flat:           { label: "Mkt Flat",       fmt: "bool"    },
  entry:                 { label: "Entry",          fmt: "price"   },
  stop:                  { label: "Stop",           fmt: "price"   },
  // strategy 6
  entry_time:            { label: "Entry Time",     fmt: "dt"      },
  entry_price:           { label: "Entry Price",    fmt: "price"   },
  pts_above_resistance:  { label: "Pts Above Res",  fmt: "price"   },
  pct_above_resistance:  { label: "% Above Res",    fmt: "pct"     },
};

// Defines display order; shared cols (ticker, _live, bar_time…) appear once —
// the ordering logic deduplicates before rendering.
const PREFERRED_ORDER = [
  // shared / strategy 1
  "ticker", "_live", "bar_time", "close",
  // strategy 6 aliases for bar_time / close
  "entry_time", "entry_price",
  "pct_from_5d_low", "resistance",
  "pts_above_resistance", "pct_above_resistance",
  "prev_bar_close", "high_60min",
  "is_near_5d_low", "cross_above_resistance", "new_60min_high",
  "trigger_fired", "updated_at",
  // strategy 2 (unique cols)
  "signal_date", "first_signal_time", "signal_count",
  "first_entry", "tightest_stop",
  // strategy 3 (unique cols)
  "prev_day_close", "pct_from_prev", "breakdown", "spy_5m_change",
  // strategy 5 (unique cols)
  "stop_level", "pts_below_support", "prior_day_candle_pct", "two_day_change",
  // strategy 4 (unique cols)
  "prev_range_mid",
  "today_volume", "avg_volume_10d", "relative_volume",
  "above_range_mid", "shares_traded_ok", "breakout",
  "spy_30m_change", "market_flat",
  "entry", "stop",
];

const HIDDEN_COLUMNS = new Set([
  // strategy 1 — redundant when trigger_fired=1
  "high_5d", "low_5d", "range_5d", "prev_bar_close", "updated_at",
  // strategy 2
  "price_above_emas", "consec_green_5m",
  // strategy 3 — always true when trigger fires
  "down_from_prev", "market_ok_to_short",
  // strategy 4 — all true when trigger fires; keep the numeric cols instead
  "above_range_mid", "shares_traded_ok", "breakout", "market_flat", "trigger_fired",
]);

function fmtPrice(v)  { return v == null ? "—" : `$${Number(v).toFixed(2)}`; }
function fmtPct(v)    { return v == null ? "—" : `${Number(v).toFixed(1)}%`; }
function fmtDate(v)   {
  if (!v) return "—";
  const d = new Date(v + "T00:00:00");
  return d.toLocaleDateString("en-US", { month:"2-digit", day:"2-digit", year:"2-digit" });
}
function fmtDt(v) {
  return fmtEtString(v);
}

/** Returns the best available date string from a result row for day-break grouping. */
function rowDateKey(row) {
  if (row.signal_date) return String(row.signal_date).slice(0, 10);
  if (row.bar_time)    return String(row.bar_time).slice(0, 10);
  if (row.entry_time)  return String(row.entry_time).slice(0, 10);
  return null;
}

/** Returns the best threshold price for the watchlist from a result row. */
function rowThreshold(row) {
  return row.first_entry ?? null;
}

/** Returns the best bar-time for the watchlist signal marker from a result row. */
function rowBarTime(row) {
  return row.first_signal_time ?? row.bar_time ?? row.entry_time ?? null;
}

/** ISO string for POST /watchlist (server accepts ISO or unix; avoids non-string types). */
function barTimeForWatchlistApi(row) {
  const t = rowBarTime(row);
  if (t == null || t === "") return null;
  if (typeof t === "number") return new Date(t).toISOString();
  if (t instanceof Date) return t.toISOString();
  return String(t);
}

function Cell({ col, value, livePrice, dimmed }) {
  const meta = COL_META[col] || { fmt: "raw" };
  switch (meta.fmt) {
    case "ticker":
      return (
        <span className={`inline-flex items-center gap-0.5 font-bold ${dimmed ? "text-slate-500" : "text-yellow-400"}`}>
          {value}
          <ArrowUpRight className="w-3 h-3 opacity-50" />
        </span>
      );
    case "live":
      return livePrice != null
        ? <span className="font-semibold text-cyan-400 tabular-nums">{fmtPrice(livePrice)}</span>
        : <span className="text-slate-600 text-xs">—</span>;
    case "price": {
      return <span className="text-slate-200 tabular-nums">{fmtPrice(value)}</span>;
    }
    case "pct":
      return <span className="text-slate-200 tabular-nums">{fmtPct(value)}</span>;
    case "date":
      return <span className="text-slate-300 tabular-nums text-xs">{fmtDate(value)}</span>;
    case "dt":
      return <span className="text-slate-300 tabular-nums text-xs">{fmtDt(value)}</span>;
    case "dt_dim":
      return <span className="text-slate-600 tabular-nums text-xs">{fmtDt(value)}</span>;
    case "num":
      return <span className="text-slate-300 tabular-nums">{value ?? "—"}</span>;
    case "vol":
      return <span className="text-slate-300 tabular-nums text-xs">
        {value == null ? "—" : Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 })}
      </span>;
    case "rvol": {
      const rv = value == null ? null : Number(value);
      const color = rv == null ? "text-slate-500"
        : rv >= 2   ? "text-green-400 font-semibold"
        : rv >= 1.5 ? "text-yellow-400"
        : "text-slate-300";
      return <span className={`${color} tabular-nums`}>
        {rv == null ? "—" : `${rv.toFixed(2)}x`}
      </span>;
    }
    case "bool":
      return value
        ? <span className="inline-block w-2.5 h-2.5 rounded-full bg-green-400" title="Yes" />
        : <span className="inline-block w-2.5 h-2.5 rounded-full bg-slate-700"  title="No"  />;
    default:
      return <span className="text-slate-400 text-xs">{value ?? "—"}</span>;
  }
}

// ── Strategy sidebar item ─────────────────────────────────────────────────────
function StrategyItem({ strategy, active, onClick }) {
  const isLong = strategy.direction?.toLowerCase() === "long";
  return (
    <button
      onClick={() => onClick(strategy)}
      className={`w-full text-left px-4 py-3.5 border-b border-slate-800 transition group flex items-start gap-3 ${
        active ? "bg-slate-800 border-l-2 border-brand-500 pl-3.5" : "hover:bg-slate-800/60 border-l-2 border-transparent"
      }`}
    >
      <div className={`mt-0.5 p-1.5 rounded-lg shrink-0 ${isLong ? "bg-green-900/40" : "bg-red-900/40"}`}>
        {isLong
          ? <TrendingUp  className="w-3.5 h-3.5 text-green-400" />
          : <TrendingDown className="w-3.5 h-3.5 text-red-400"  />}
      </div>
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-slate-200">{strategy.name}</span>
          <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider ${
            isLong ? "bg-green-900/50 text-green-400" : "bg-red-900/50 text-red-400"
          }`}>{strategy.direction}</span>
        </div>
        {strategy.description && (
          <p className="text-[11px] text-slate-500 mt-0.5 leading-relaxed">{strategy.description}</p>
        )}
      </div>
      <ChevronRight className={`w-4 h-4 shrink-0 mt-1 transition ${active ? "text-brand-400" : "text-slate-700 group-hover:text-slate-500"}`} />
    </button>
  );
}

// ── MA score helpers ──────────────────────────────────────────────────────────
const MA_KEYS = ["ema10", "ema20", "sma50", "sma150", "sma200"];

function maScore(data, price, isLong) {
  if (!data || price == null) return null;
  return MA_KEYS.reduce((n, k) => {
    const v = data[k];
    if (v == null) return n;
    return n + (isLong ? (price > v ? 1 : 0) : (price < v ? 1 : 0));
  }, 0);
}

function scoreColor(score) {
  if (score == null) return "text-slate-600";
  if (score === 5) return "text-green-400";
  if (score === 4) return "text-green-600";
  if (score === 3) return "text-yellow-400";
  if (score === 2) return "text-orange-400";
  if (score === 1) return "text-red-500";
  return "text-slate-600";
}

// ── MA info popover ───────────────────────────────────────────────────────────
function InfoPopover({ popover, snapPrices, isLong, onMouseEnter, onMouseLeave }) {
  if (!popover) return null;
  const { ticker, x, y, data, loading } = popover;
  const price = snapPrices[ticker] ?? null;

  const INDICATORS = data ? [
    { group: "EMA", label: "10 Day", key: "ema10", value: data.ema10 },
    { group: "EMA", label: "20 Day", key: "ema20", value: data.ema20 },
    { group: "SMA", label: "50 Day",  key: "sma50",  value: data.sma50  },
    { group: "SMA", label: "150 Day", key: "sma150", value: data.sma150 },
    { group: "SMA", label: "200 Day", key: "sma200", value: data.sma200 },
  ] : [];

  const score = maScore(data, price, isLong);
  const popW = 224;

  return (
    <div
      className="fixed z-[9999] bg-slate-800 border border-slate-700 rounded-xl shadow-2xl overflow-hidden select-none"
      style={{ left: x, top: y, width: popW, transform: "translate(-50%, -50%)" }}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 bg-slate-900/70 border-b border-slate-700/60">
        <div className="flex items-center gap-2">
          <span className="text-sm font-bold text-yellow-400">{ticker}</span>
          {score != null && (
            <span className={`text-xs font-bold font-mono ${scoreColor(score)}`}>{score}/5</span>
          )}
        </div>
        {price != null
          ? <span className="text-xs font-mono text-cyan-400">${price.toFixed(2)}</span>
          : <span className="text-xs text-slate-600">no live price</span>}
      </div>

      {/* Body */}
      <div className="px-3 py-2.5">
        {loading && !data && (
          <div className="flex items-center justify-center py-5">
            <Loader2 className="w-4 h-4 text-slate-400 animate-spin" />
          </div>
        )}

        {data && ["EMA", "SMA"].map(group => {
          const rows = INDICATORS.filter(r => r.group === group);
          return (
            <div key={group} className="mb-2 last:mb-0">
              <div className="text-[9px] font-semibold text-slate-600 uppercase tracking-widest mb-1.5">
                {group}
              </div>
              {rows.map(({ label, value }) => {
                const aboveMA = price != null && value != null ? price > value : null;
                const aligned = aboveMA !== null ? (isLong ? aboveMA : !aboveMA) : null;
                return (
                  <div key={label} className="flex items-center justify-between py-0.5">
                    <div className="flex items-center gap-2">
                      {aboveMA === null
                        ? <span className="w-3.5 h-3.5 flex items-center justify-center text-slate-600 text-[10px]">—</span>
                        : aboveMA
                        ? <TrendingUp   className="w-3.5 h-3.5 text-green-400 shrink-0" />
                        : <TrendingDown className="w-3.5 h-3.5 text-red-400   shrink-0" />}
                      <span className="text-xs text-slate-400">{label}</span>
                    </div>
                    <span className={`text-xs font-mono ${
                      aboveMA === null ? "text-slate-600"
                      : aboveMA ? "text-green-400" : "text-red-400"
                    }`}>
                      {value != null ? `$${Number(value).toFixed(2)}` : "—"}
                    </span>
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function TradeIdeas({ onSelectTicker, watchlist = [], openChartRequest, onConsumedOpenChartRequest }) {
  const [strategies,     setStrategies]     = useState([]);
  const [loadingList,    setLoadingList]     = useState(true);
  const [activeStrategy, setActiveStrategy] = useState(null);
  const [results,        setResults]        = useState(null);
  const [columns,        setColumns]        = useState([]);
  const [loadingResult,  setLoadingResult]  = useState(false);
  const [error,          setError]          = useState(null);
  const [snapPrices,     setSnapPrices]     = useState({});
  const [wlLoading,      setWlLoading]      = useState({});
  const [showPrevDays,   setShowPrevDays]   = useState(false);
  /** Minimum MA alignment score (Info column) to show; 0 = no filter. Resets to 3 when switching strategy. */
  const [minInfoScore,    setMinInfoScore]   = useState(3);
  const [infoPopover,    setInfoPopover]    = useState(null);
  const [maCache,        setMaCache]        = useState({});   // triggers re-render when MA data arrives
  const infoCacheRef       = useRef({});
  const scheduledMaRef     = useRef(new Set()); // in-flight MA fetches (avoid duplicate requests)
  const hideTimerRef       = useRef(null);
  const tableContainerRef  = useRef(null);
  // { ticker, barTime, threshold } | null
  const [chartModal,  setChartModal]  = useState(null);
  const [chartHeight, setChartHeight] = useState(null);

  // Set of ticker symbols with an open position (from local DB)
  const [openTickers, setOpenTickers] = useState(new Set());

  useEffect(() => {
    alpacaApi.openTickers()
      .then(r => setOpenTickers(new Set(r.data.tickers ?? [])))
      .catch(() => {}); // non-fatal — fail silently
  }, []);

  // Compute chart height when modal opens: modal is 95vh, header ~56px
  useEffect(() => {
    if (!chartModal) { setChartHeight(null); return; }
    setChartHeight(Math.floor(window.innerHeight * 0.95) - 56);
  }, [chartModal]);

  // Sidebar watchlist → open Trade Ideas chart modal (source tradeideas)
  useEffect(() => {
    if (!openChartRequest?.ticker || openChartRequest.key == null) return;
    setChartModal({
      ticker:    openChartRequest.ticker,
      barTime:   openChartRequest.barTime ?? null,
      threshold: openChartRequest.threshold ?? null,
    });
    onConsumedOpenChartRequest?.();
  }, [openChartRequest?.key, openChartRequest?.ticker, openChartRequest?.barTime, openChartRequest?.threshold, onConsumedOpenChartRequest]);

  const toggleWatchlist = useCallback(async (ticker, meta = {}) => {
    setWlLoading((prev) => ({ ...prev, [ticker]: true }));
    try {
      if (watchlist.includes(ticker)) {
        await stockApi.removeFromWatchlist(ticker);
      } else {
        await stockApi.addToWatchlist(ticker, meta);
      }
      // Watchlist sidebar refreshes via `tf:watchlist-changed` (axios interceptor)
    } catch { /* ignore */ } finally {
      setWlLoading((prev) => ({ ...prev, [ticker]: false }));
    }
  }, [watchlist]);

  const handleInfoEnter = useCallback((e, ticker) => {
    clearTimeout(hideTimerRef.current);
    const containerRect = tableContainerRef.current?.getBoundingClientRect();
    const cx = containerRect ? containerRect.left + containerRect.width  / 2 : window.innerWidth  / 2;
    const cy = containerRect ? containerRect.top  + containerRect.height / 2 : window.innerHeight / 2;
    const cached = infoCacheRef.current[ticker];
    setInfoPopover({ ticker, x: cx, y: cy, data: cached ?? null, loading: !cached });
    if (!cached) {
      tradeIdeasApi.maCache([ticker], { staleOk: true, queueRefresh: true })
        .then(r => {
          const data = r.data.ma[ticker] ?? null;
          if (data) {
            infoCacheRef.current[ticker] = data;
            setMaCache(prev => ({ ...prev, [ticker]: data }));
          }
          setInfoPopover(prev => prev?.ticker === ticker ? { ...prev, data, loading: false } : prev);
        }).catch(() => {
          setInfoPopover(prev => prev?.ticker === ticker ? { ...prev, loading: false } : prev);
        });
    }
  }, []);

  const handleInfoLeave = useCallback(() => {
    hideTimerRef.current = setTimeout(() => setInfoPopover(null), 120);
  }, []);

  /** Load MA scores from DB immediately (stale_ok); Polygon refresh runs in background. */
  const requestMaForTickers = useCallback((tickers) => {
    const need = tickers.filter(
      (t) => t && !infoCacheRef.current[t] && !scheduledMaRef.current.has(t),
    );
    if (!need.length) return;
    need.forEach((t) => scheduledMaRef.current.add(t));
    tradeIdeasApi.maCache(need, { staleOk: true, queueRefresh: true })
      .then((r) => {
        const ma = r.data.ma || {};
        const updates = {};
        need.forEach((ticker) => {
          if (ma[ticker] != null) {
            infoCacheRef.current[ticker] = ma[ticker];
            updates[ticker] = ma[ticker];
          } else {
            infoCacheRef.current[ticker] = null;
            updates[ticker] = null;
          }
        });
        setMaCache((prev) => ({ ...prev, ...updates }));
      })
      .catch(() => {})
      .finally(() => {
        need.forEach((t) => scheduledMaRef.current.delete(t));
      });
  }, []);

  // Load strategy list on mount
  useEffect(() => {
    tradeIdeasApi.list()
      .then((r) => {
        setStrategies(r.data.strategies || []);
        // Auto-select the first strategy
        if (r.data.strategies?.length) {
          handleSelectStrategy(r.data.strategies[0]);
        }
      })
      .catch(() => setStrategies([]))
      .finally(() => setLoadingList(false));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSelectStrategy = useCallback((strategy) => {
    setActiveStrategy(strategy);
    setError(null);
    setResults(null);
    setSnapPrices({});
    setShowPrevDays(false);
    infoCacheRef.current = {};
    setMaCache({});
    setMinInfoScore(3);
    setLoadingResult(true);
    tradeIdeasApi.run(strategy.id)
      .then((r) => {
        const raw = r.data.columns || [];
        const ordered = [...new Set([
          ...PREFERRED_ORDER.filter((c) => raw.includes(c) || c === "_live"),
          ...raw.filter((c) => !PREFERRED_ORDER.includes(c)),
        ])].filter((c) => !HIDDEN_COLUMNS.has(c));
        setColumns(ordered);
        // Sort: most-recent day first, then most-recent bar time within each day
        const rows = (r.data.results || []).slice().sort((a, b) => {
          const dayA = rowDateKey(a) ?? "";
          const dayB = rowDateKey(b) ?? "";
          if (dayB !== dayA) return dayB.localeCompare(dayA);          // day DESC
          const btA = String(rowBarTime(a) ?? "");
          const btB = String(rowBarTime(b) ?? "");
          return btB.localeCompare(btA);                               // bar_time DESC
        });
        setResults(rows);

        // Read cached prices (background service keeps the full universe current)
        const tickers = [...new Set(rows.map((row) => row.ticker).filter(Boolean))];
        if (tickers.length > 0) {
          snapshotsApi.prices(tickers.join(","))
            .then((snap) => {
              const prices = snap.data.prices || {};
              const priceMap = {};
              Object.entries(prices).forEach(([t, d]) => {
                priceMap[t] = typeof d === "object" ? d.price : d;
              });
              setSnapPrices(priceMap);
            })
            .catch(() => {});
          // MA scores: lazy-loaded per visible rows (see IntersectionObserver + requestMaForTickers)
        }
      })
      .catch((e) => setError(e.response?.data?.error || "Failed to run strategy"))
      .finally(() => setLoadingResult(false));
  }, []);

  const isLong = activeStrategy?.direction?.toLowerCase() === "long";

  // Filter to the latest trading day unless the user opts in to previous days
  const visibleResults = React.useMemo(() => {
    if (!results) return null;
    if (showPrevDays) return results;
    const latestDate = results.reduce((best, row) => {
      const d = rowDateKey(row);
      return d && d > best ? d : best;
    }, "");
    return latestDate ? results.filter((row) => rowDateKey(row) === latestDate) : results;
  }, [results, showPrevDays]);

  const filteredVisibleResults = React.useMemo(() => {
    if (!visibleResults) return null;
    if (minInfoScore <= 0) return visibleResults;
    return visibleResults.filter((row) => {
      const maData = maCache[row.ticker];
      if (maData === undefined) return true;
      const price = snapPrices[row.ticker] ?? null;
      const score = maScore(maData, price, isLong);
      return score != null && score >= minInfoScore;
    });
  }, [visibleResults, minInfoScore, maCache, snapPrices, isLong]);

  // Clear in-flight MA scheduling when switching strategies
  useEffect(() => {
    scheduledMaRef.current.clear();
  }, [activeStrategy?.id]);

  // First screen of rows: prompt MA load (IntersectionObserver can miss first paint)
  useEffect(() => {
    if (!visibleResults?.length || loadingResult) return;
    const tickers = [...new Set(visibleResults.slice(0, 28).map((r) => r.ticker).filter(Boolean))];
    requestMaForTickers(tickers);
  }, [visibleResults, loadingResult, activeStrategy?.id, requestMaForTickers]);

  // Lazy-load MA for rows as they scroll into view (observe rendered / filtered rows)
  useEffect(() => {
    if (!filteredVisibleResults?.length || loadingResult) return;
    const root = tableContainerRef.current;
    if (!root) return;

    const obs = new IntersectionObserver(
      (entries) => {
        const next = [];
        for (const e of entries) {
          if (!e.isIntersecting) continue;
          const t = e.target.getAttribute("data-ma-ticker");
          if (t) next.push(t);
        }
        if (next.length) requestMaForTickers([...new Set(next)]);
      },
      { root, rootMargin: "160px", threshold: 0.01 },
    );

    root.querySelectorAll("tr[data-ma-ticker]").forEach((n) => obs.observe(n));
    return () => obs.disconnect();
  }, [filteredVisibleResults, loadingResult, requestMaForTickers]);

  return (
    <>
    <div className="flex h-full overflow-hidden">

      {/* ── Strategy list sidebar ─────────────────────────────────────────── */}
      <aside className="w-72 shrink-0 border-r border-slate-800 flex flex-col overflow-hidden bg-slate-900">
        <div className="flex items-center gap-2 px-4 py-3.5 border-b border-slate-800 shrink-0">
          <Lightbulb className="w-4 h-4 text-yellow-400" />
          <h2 className="text-sm font-semibold text-slate-200">Trade Ideas</h2>
          <span className="ml-auto text-[10px] bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded">
            {strategies.length} / 50
          </span>
        </div>

        <div className="flex-1 overflow-y-auto">
          {loadingList ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-5 h-5 text-slate-500 animate-spin" />
            </div>
          ) : strategies.length === 0 ? (
            <p className="text-sm text-slate-500 text-center py-10 px-4">No strategies configured.</p>
          ) : (
            strategies.map((s) => (
              <StrategyItem
                key={s.id}
                strategy={s}
                active={activeStrategy?.id === s.id}
                onClick={handleSelectStrategy}
              />
            ))
          )}
        </div>
      </aside>

      {/* ── Results panel ─────────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col overflow-hidden">

        {/* Results header */}
        {activeStrategy && (
          <div className="flex items-center gap-3 px-5 py-3 border-b border-slate-800 bg-slate-900 shrink-0">
            <div className={`p-1.5 rounded-lg ${isLong ? "bg-green-900/40" : "bg-red-900/40"}`}>
              {isLong
                ? <TrendingUp  className="w-4 h-4 text-green-400" />
                : <TrendingDown className="w-4 h-4 text-red-400" />}
            </div>
            <div>
              <h3 className="text-sm font-semibold text-slate-200">{activeStrategy.name}</h3>
              {activeStrategy.description && (
                <p className="text-[11px] text-slate-500">{activeStrategy.description}</p>
              )}
            </div>
            {filteredVisibleResults && visibleResults && (
              <span className="ml-2 text-xs bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full border border-slate-700">
                {filteredVisibleResults.length}
                {visibleResults.length !== filteredVisibleResults.length
                  ? ` / ${visibleResults.length}`
                  : ""}
                {!showPrevDays && results?.length > visibleResults.length ? ` (${results.length} total)` : ""}
                {" "}
                result{filteredVisibleResults.length !== 1 ? "s" : ""}
              </span>
            )}
            <label className="flex items-center gap-2 text-xs text-slate-400 ml-2 shrink-0">
              <span className="whitespace-nowrap">Info ≥</span>
              <select
                value={minInfoScore}
                onChange={(e) => setMinInfoScore(Number(e.target.value))}
                className="bg-slate-800 border border-slate-600 rounded-lg px-2 py-1 text-slate-200 tabular-nums focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500"
                title="Show rows whose Info (MA alignment) score is at least this value. 0 = no filter."
              >
                {[0, 1, 2, 3, 4, 5].map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
            {results?.length > 0 && (
              <label className="ml-3 flex items-center gap-1.5 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={showPrevDays}
                  onChange={(e) => setShowPrevDays(e.target.checked)}
                  className="w-3.5 h-3.5 rounded accent-brand-500 cursor-pointer"
                />
                <span className="text-xs text-slate-400">Show Previous Days</span>
              </label>
            )}
            <button
              onClick={() => handleSelectStrategy(activeStrategy)}
              disabled={loadingResult}
              className="ml-auto flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200 transition px-2 py-1 rounded hover:bg-slate-800"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loadingResult ? "animate-spin" : ""}`} />
              Refresh
            </button>
          </div>
        )}

        {/* Content area */}
        <div ref={tableContainerRef} className="flex-1 overflow-auto">
          {!activeStrategy && !loadingList && (
            <div className="h-full flex flex-col items-center justify-center text-center px-4">
              <Lightbulb className="w-12 h-12 text-slate-700 mb-3" />
              <p className="text-slate-500 text-sm">Select a strategy to view results</p>
            </div>
          )}

          {loadingResult && (
            <div className="h-full flex flex-col items-center justify-center gap-3">
              <Loader2 className="w-6 h-6 text-brand-400 animate-spin" />
              <p className="text-sm text-slate-500">Running strategy…</p>
            </div>
          )}

          {error && !loadingResult && (
            <div className="m-6 flex items-start gap-3 bg-red-900/20 border border-red-800 rounded-xl p-4">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-red-300">Query error</p>
                <p className="text-xs text-red-400 mt-1 font-mono">{error}</p>
              </div>
            </div>
          )}

          {!loadingResult && !error && visibleResults && (
            visibleResults.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center text-center px-4">
                <div className="w-16 h-16 bg-slate-800 rounded-2xl flex items-center justify-center mb-3 border border-slate-700">
                  <Lightbulb className="w-8 h-8 text-slate-600" />
                </div>
                <p className="text-slate-400 text-sm font-medium">No signals at this time</p>
                <p className="text-slate-600 text-xs mt-1">The strategy returned 0 rows</p>
              </div>
            ) : filteredVisibleResults.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center text-center px-4">
                <div className="w-16 h-16 bg-slate-800 rounded-2xl flex items-center justify-center mb-3 border border-slate-700">
                  <Info className="w-8 h-8 text-slate-600" />
                </div>
                <p className="text-slate-400 text-sm font-medium">No rows match the Info filter</p>
                <p className="text-slate-600 text-xs mt-1">
                  Lower &ldquo;Info ≥&rdquo; (currently {minInfoScore}) or wait for scores to load.
                </p>
              </div>
            ) : (
              <table className="w-full text-left border-collapse">
                <thead className="sticky top-0 z-10">
                  <tr className="bg-slate-900 border-b border-slate-800">
                    {columns.map((col) => (
                      <th key={col} className="px-3 py-2.5 text-[10px] font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap">
                        {COL_META[col]?.label ?? col}
                      </th>
                    ))}
                    <th className="px-3 py-2.5 text-[10px] font-semibold text-slate-500 uppercase tracking-wider">
                      Watch
                    </th>
                    <th className="px-3 py-2.5 text-[10px] font-semibold text-slate-500 uppercase tracking-wider">
                      Info
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredVisibleResults.map((row, i) => {
                    const rowDate  = rowDateKey(row);
                    const prevDate = i > 0 ? rowDateKey(filteredVisibleResults[i - 1]) : null;
                    const showDayBreak = rowDate && rowDate !== prevDate;
                    const dateLabel = rowDate
                      ? new Date(rowDate + "T00:00:00").toLocaleDateString("en-US", {
                          weekday: "long", month: "long", day: "numeric", year: "numeric",
                        })
                      : null;
                    return (
                    <React.Fragment key={`${row.ticker}-${rowDate}-${i}`}>
                      {showDayBreak && (
                        <tr key={`break-${rowDate}`} className="bg-slate-800/70">
                          <td colSpan={columns.length + 2} className="py-2 text-center text-xs font-semibold text-slate-400 tracking-wider uppercase">
                            {dateLabel}
                          </td>
                        </tr>
                      )}
                    <tr
                      data-ma-ticker={row.ticker}
                      className="border-b border-slate-800/60 hover:bg-slate-800/40 transition"
                      title={openTickers.has(row.ticker) ? "Trade Currently Open" : undefined}
                    >
                      {columns.map((col) => {
                        const isTickerCol  = col === "ticker";
                        const inOpenTrade  = isTickerCol && openTickers.has(row.ticker);
                        return (
                        <td
                          key={col}
                          className={`px-3 py-2 whitespace-nowrap${isTickerCol && !inOpenTrade ? " cursor-pointer" : ""}${inOpenTrade ? " cursor-not-allowed opacity-50" : ""}`}
                          onClick={isTickerCol && !inOpenTrade ? () => setChartModal({
                            ticker:    row.ticker,
                            barTime:   rowBarTime(row),
                            threshold: rowThreshold(row),
                          }) : undefined}
                        >
                          {inOpenTrade ? (
                            <span className="line-through decoration-red-400 decoration-2">
                              <Cell
                                col={col}
                                value={row[col]}
                                livePrice={snapPrices[row.ticker] ?? null}
                                dimmed
                              />
                            </span>
                          ) : (
                            <Cell
                              col={col}
                              value={row[col]}
                              livePrice={snapPrices[row.ticker] ?? null}
                            />
                          )}
                        </td>
                        );
                      })}
                      <td className="px-3 py-2">
                        <button
                          onClick={() => toggleWatchlist(row.ticker, {
                            bias:      activeStrategy?.direction ?? null,
                            threshold: rowThreshold(row),
                            bar_time:  barTimeForWatchlistApi(row),
                            source:    "tradeideas",
                          })}
                          disabled={wlLoading[row.ticker]}
                          title={watchlist.includes(row.ticker) ? "Remove from watchlist" : "Add to watchlist"}
                          className="transition"
                        >
                          {watchlist.includes(row.ticker)
                            ? <Star    className="w-4 h-4 text-yellow-400 fill-current hover:text-yellow-300" />
                            : <StarOff className="w-4 h-4 text-slate-600 hover:text-yellow-400" />}
                        </button>
                      </td>
                      <td className="px-3 py-2">
                        {(() => {
                          const maData = maCache[row.ticker];
                          const price  = snapPrices[row.ticker] ?? null;
                          const score  = maScore(maData, price, isLong);
                          const canOpen = !openTickers.has(row.ticker);
                          const maPending = maData === undefined;
                          return (
                            <button
                              onMouseEnter={(e) => handleInfoEnter(e, row.ticker)}
                              onMouseLeave={handleInfoLeave}
                              onClick={canOpen ? () => setChartModal({
                                ticker:    row.ticker,
                                barTime:   rowBarTime(row),
                                threshold: rowThreshold(row),
                              }) : undefined}
                              className={`transition font-bold font-mono tabular-nums text-sm w-5 text-center${canOpen ? " cursor-pointer" : " cursor-not-allowed"}`}
                              title="MA alignment score"
                            >
                              {maPending ? (
                                <Loader2 className="w-3 h-3 text-slate-700 animate-spin inline" />
                              ) : score != null ? (
                                <span className={scoreColor(score)}>{score}</span>
                              ) : (
                                <span className="text-slate-600">—</span>
                              )}
                            </button>
                          );
                        })()}
                      </td>
                    </tr>
                    </React.Fragment>
                  );
                  })}
                </tbody>
              </table>
            )
          )}
        </div>
      </div>
    </div>

    {/* ── Chart modal ───────────────────────────────────────────────────── */}
    {chartModal && (
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
        onClick={() => setChartModal(null)}
      >
        <div
          className="relative bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl flex flex-col"
          style={{ width: "95vw", height: "95vh" }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Modal header */}
          <div className="flex items-center gap-3 px-5 py-3 border-b border-slate-800 shrink-0">
            <span className="text-lg font-bold text-brand-400">{chartModal.ticker}</span>
            {activeStrategy?.direction && (
              <span className={`text-[11px] font-bold px-2 py-0.5 rounded uppercase tracking-wider ${
                isLong
                  ? "bg-green-900/50 text-green-400 border border-green-800"
                  : "bg-red-900/50 text-red-400 border border-red-800"
              }`}>
                {activeStrategy.direction}
              </span>
            )}
            {chartModal.barTime && (
              <span className="text-xs text-slate-500">
                Signal&nbsp;{fmtEtString(chartModal.barTime)}
              </span>
            )}
            <button
              onClick={() => setChartModal(null)}
              className="ml-auto p-1.5 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Chart fills remaining space */}
          <div className="flex-1 min-h-0 overflow-hidden">
            <ModalChart
              ticker={chartModal.ticker}
              barTime={chartModal.barTime}
              threshold={chartModal.threshold}
              height={chartHeight}
              bias={activeStrategy?.direction?.toLowerCase() ?? null}
              onClose={() => {
                setChartModal(null);
                // Refresh so a newly opened trade is immediately struck through
                alpacaApi.openTickers()
                  .then(r => setOpenTickers(new Set(r.data.tickers ?? [])))
                  .catch(() => {});
              }}
            />
          </div>
        </div>
      </div>
    )}

    <InfoPopover
      popover={infoPopover}
      snapPrices={snapPrices}
      isLong={isLong}
      onMouseEnter={() => clearTimeout(hideTimerRef.current)}
      onMouseLeave={handleInfoLeave}
    />
    </>
  );
}
