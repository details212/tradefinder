import { useState, useEffect, useCallback, useRef } from "react";
import { stockApi, snapshotsApi, alpacaApi } from "../api/client";
import StockDetail from "./StockDetail";
import TradeIdeas from "./TradeIdeas";
import PatternAnalysis from "./PatternAnalysis";
import NewTrade from "./NewTrade";
import AccountSettings from "./AccountSettings";
import BrokerageSettings from "./BrokerageSettings";
import AdminPanel from "./AdminPanel";
import LeaderBoard from "./LeaderBoard";
import logo from "../assets/logo.png";
import {
  LogOut,
  Star,
  BarChart2,
  RefreshCw,
  User,
  ChevronRight,
  Loader2,
  Lightbulb,
  Home,
  Settings,
  Building2,
  PlusCircle,
  TrendingUp,
  Trophy,
} from "lucide-react";

function WatchlistTile({ ticker, bias, threshold, barTime, source, liveData, onClick, onRemove }) {
  const price     = liveData?.price     ?? null;
  const changePct = liveData?.change_pct ?? null;
  const isUp      = changePct == null ? null : changePct >= 0;
  const isLong    = bias?.toLowerCase() === "long";
  const isShort   = bias?.toLowerCase() === "short";
  const aboveThreshold = threshold != null && price != null && Number(price) > Number(threshold);

  return (
    <div
      onClick={() => onClick({ ticker, bias, threshold, bar_time: barTime, source })}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === "Enter" && onClick({ ticker, bias, threshold, bar_time: barTime, source })}
      className="flex flex-col bg-slate-800 hover:bg-slate-750 border border-slate-700 hover:border-slate-600 rounded-xl px-4 py-3 transition group w-full text-left gap-1.5 cursor-pointer"
    >
      {/* Row 1: ticker + bias + price + change */}
      <div className="flex items-center justify-between w-full gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <button
            onClick={(e) => { e.stopPropagation(); onRemove(ticker); }}
            title="Remove from watchlist"
            className="shrink-0 text-yellow-400 hover:text-slate-500 transition"
          >
            <Star className="w-3.5 h-3.5 fill-current" />
          </button>
          <span className="text-sm font-bold text-slate-100">{ticker}</span>
          {bias && (
            <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider shrink-0 ${
              isLong  ? "bg-green-900/50 text-green-400" :
              isShort ? "bg-red-900/50 text-red-400"    :
                        "bg-slate-700 text-slate-400"
            }`}>{bias}</span>
          )}
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          {price != null ? (
            <span className="text-sm font-semibold text-slate-200 tabular-nums">
              ${Number(price).toFixed(2)}
            </span>
          ) : (
            <span className="text-xs text-slate-600">—</span>
          )}
          {changePct != null && (
            <span className={`text-[11px] font-medium px-1.5 py-0.5 rounded tabular-nums ${
              isUp ? "bg-green-900/40 text-green-400" : "bg-red-900/40 text-red-400"
            }`}>
              {isUp ? "+" : ""}{Number(changePct).toFixed(2)}%
            </span>
          )}
          <ChevronRight className="w-4 h-4 text-slate-600 group-hover:text-slate-400 transition" />
        </div>
      </div>
      {/* Row 2: threshold + bar_time */}
      {(threshold != null || barTime) && (
        <div className="flex items-center justify-between pl-5 pr-1">
          {threshold != null ? (
            <div className="flex items-center gap-1.5">
              <span className="text-[10px] text-slate-500">Threshold</span>
              <span className={`text-[10px] font-semibold tabular-nums ${
                aboveThreshold ? "text-green-400" : "text-slate-400"
              }`}>
                ${Number(threshold).toFixed(2)}{aboveThreshold && " ↑"}
              </span>
            </div>
          ) : <span />}
          {barTime && (
            <span className="text-[10px] text-slate-600 tabular-nums">
              {new Date(barTime).toLocaleString("en-US", {
                month: "2-digit", day: "2-digit",
                hour: "2-digit", minute: "2-digit", hour12: false,
              })}
            </span>
          )}
        </div>
      )}
    </div>
  );
}

// ── Symbols panel ─────────────────────────────────────────────────────────────
function SymbolsPanel({ watchlist, selectedTicker, onSelect, onClose }) {
  const [query, setQuery]       = useState("");
  const [allSymbols, setAll]    = useState([]);
  const [loading, setLoading]   = useState(true);
  const inputRef = useRef(null);

  // Load full list once when panel mounts
  useEffect(() => {
    inputRef.current?.focus();
    stockApi.search("", { params: { limit: 200 } })   // empty q → top 200 by volume
      .then((res) => setAll(res.data.results || []))
      .catch(() => setAll([]))
      .finally(() => setLoading(false));
  }, []);

  // Client-side filter as user types
  const q = query.toLowerCase();
  const displayed = q
    ? allSymbols.filter(
        (r) =>
          r.ticker.toLowerCase().includes(q) ||
          (r.name || "").toLowerCase().includes(q) ||
          (r.sector || "").toLowerCase().includes(q)
      )
    : allSymbols;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700 shrink-0">
        <h2 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
          <LayoutList className="w-4 h-4 text-brand-400" />
          Symbols
        </h2>
        <button onClick={onClose} className="text-slate-500 hover:text-slate-300 transition">
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Filter input */}
      <div className="px-4 py-3 border-b border-slate-700 shrink-0">
        <div className="relative">
          {loading
            ? <Loader2 className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 animate-spin" />
            : <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          }
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter by ticker, name or sector…"
            className="w-full bg-slate-800 border border-slate-700 rounded-lg pl-9 pr-8 py-2 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand-500 transition"
          />
          {query && (
            <button onClick={() => setQuery("")}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300">
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
        <p className="text-[11px] text-slate-600 mt-1.5">
          {loading ? "Loading…" : `${displayed.length} of ${allSymbols.length} symbol${allSymbols.length !== 1 ? "s" : ""}`}
        </p>
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-[2fr_3fr_1fr_1fr] gap-2 px-4 py-2 border-b border-slate-800 shrink-0">
        {["Symbol", "Name / Sector", "Price", "Exchange"].map((h) => (
          <span key={h} className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{h}</span>
        ))}
      </div>

      {/* Rows */}
      <div className="flex-1 overflow-y-auto">
        {!loading && displayed.length === 0 && (
          <p className="text-sm text-slate-500 text-center py-10">No symbols match "{query}"</p>
        )}
        {displayed.map((r) => {
          const isActive = r.ticker === selectedTicker;
          const isWl     = watchlist.includes(r.ticker);
          return (
            <button
              key={r.ticker}
              onClick={() => { onSelect(r.ticker); onClose(); }}
              className={`w-full grid grid-cols-[2fr_3fr_1fr_1fr] gap-2 items-center px-4 py-2.5 text-left border-b border-slate-800/50 transition hover:bg-slate-800 ${
                isActive ? "bg-slate-800 border-l-2 border-brand-500 pl-3.5" : "border-l-2 border-transparent"
              }`}
            >
              <div className="flex items-center gap-1.5 min-w-0">
                {isWl && <Star className="w-3 h-3 text-yellow-400 fill-current shrink-0" />}
                <span className="text-sm font-bold text-brand-400 truncate">{r.ticker}</span>
              </div>
              <div className="min-w-0">
                <p className="text-xs text-slate-300 truncate">{r.name || "—"}</p>
                {r.sector && <p className="text-[10px] text-slate-500 truncate">{r.sector}</p>}
              </div>
              <div className="text-right">
                {r.last_day_close != null
                  ? <span className="text-xs text-slate-300">${Number(r.last_day_close).toFixed(2)}</span>
                  : <span className="text-xs text-slate-600">—</span>}
              </div>
              <div className="text-right">
                <span className="text-[10px] text-slate-500">{r.primary_exchange || r.market || "—"}</span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

const WATCHLIST_POLL_MS = 60_000;

export default function Dashboard({ user, onLogout }) {
  const [selectedTicker,   setSelectedTicker]   = useState(null);
  const [defaultInterval,  setDefaultInterval]  = useState(null);
  const [chartBarTime,     setChartBarTime]     = useState(null);
  const [chartThreshold,   setChartThreshold]   = useState(null);
  const [watchlist,        setWatchlist]         = useState([]);      // ticker strings
  const [watchlistItems,   setWatchlistItems]    = useState([]);      // full objects
  const [watchlistPrices,  setWatchlistPrices]   = useState({});      // { AAPL: { price, change_pct } }
  const [loadingWatchlist, setLoadingWatchlist]  = useState(true);
  const [activeView,       setActiveView]        = useState("stocks");
  const [brokerStatus,     setBrokerStatus]      = useState(null); // null | { ok, paper }
  /** Sidebar → Trade Ideas chart modal (key bumps so same ticker re-opens) */
  const [tradeIdeasOpenChart, setTradeIdeasOpenChart] = useState(null);
  /** Sidebar → Pattern Analysis chart modal */
  const [patternOpenChart, setPatternOpenChart] = useState(null);
  const pollRef = useRef(null);
  const wlRefreshDebounceRef = useRef(null);

  useEffect(() => {
    alpacaApi.test()
      .then(r => setBrokerStatus({ ok: r.data.ok, paper: r.data.paper }))
      .catch(() => setBrokerStatus({ ok: false, paper: null }));
  }, []);

  const handleSelectTicker = useCallback((ticker, opts = {}) => {
    setSelectedTicker(ticker);
    setDefaultInterval(opts.interval   ?? null);
    setChartBarTime(opts.barTime       ?? null);
    setChartThreshold(opts.threshold   ?? null);
    setActiveView("stocks");
  }, []);

  const clearTradeIdeasOpenChart = useCallback(() => setTradeIdeasOpenChart(null), []);
  const clearPatternOpenChart = useCallback(() => setPatternOpenChart(null), []);

  const handleWatchlistItemClick = useCallback((item) => {
    const ticker = item.ticker;
    const src = (item.source || "stocks").toLowerCase();
    if (src === "tradeideas") {
      setActiveView("tradeideas");
      setTradeIdeasOpenChart({
        key: Date.now(),
        ticker,
        barTime: item.bar_time ?? null,
        threshold: item.threshold ?? null,
      });
      return;
    }
    if (src === "patternanalysis") {
      setActiveView("patternanalysis");
      setPatternOpenChart({
        key: Date.now(),
        ticker,
      });
      return;
    }
    handleSelectTicker(ticker, {
      interval: "5m",
      barTime: item.bar_time ?? null,
      threshold: item.threshold ?? null,
    });
  }, [handleSelectTicker]);

  const fetchWatchlist = useCallback(() => {
    setLoadingWatchlist(true);
    stockApi
      .getWatchlist()
      .then((r) => {
        const items = r.data.watchlist || [];
        setWatchlistItems(items);
        setWatchlist(items.map((i) => i.ticker));
      })
      .catch(() => {})
      .finally(() => setLoadingWatchlist(false));
  }, []);

  const handleRemoveFromWatchlist = useCallback(async (ticker) => {
    try {
      await stockApi.removeFromWatchlist(ticker);
      // Sidebar refreshes via `tf:watchlist-changed` (axios interceptor)
    } catch { /* ignore */ }
  }, []);

  // ── Centralised 60-second live price polling (reads from server-side cache) ─
  const pollPrices = useCallback(() => {
    if (!watchlist.length) return;
    snapshotsApi.prices(watchlist.join(","))
      .then((r) => setWatchlistPrices(r.data.prices || {}))
      .catch(() => {});
  }, [watchlist]);

  useEffect(() => {
    clearInterval(pollRef.current);
    if (watchlist.length > 0) {
      pollPrices();                                             // immediate fetch
      pollRef.current = setInterval(pollPrices, WATCHLIST_POLL_MS);
    }
    return () => clearInterval(pollRef.current);
  }, [watchlist, pollPrices]);

  useEffect(() => {
    fetchWatchlist();
  }, [fetchWatchlist]);

  // Refresh sidebar when any screen adds/removes a watchlist item (axios + chart "+ WL" dispatch tf:watchlist-changed)
  useEffect(() => {
    const onWl = () => {
      clearTimeout(wlRefreshDebounceRef.current);
      wlRefreshDebounceRef.current = setTimeout(() => {
        wlRefreshDebounceRef.current = null;
        fetchWatchlist();
      }, 50);
    };
    window.addEventListener("tf:watchlist-changed", onWl);
    return () => {
      clearTimeout(wlRefreshDebounceRef.current);
      window.removeEventListener("tf:watchlist-changed", onWl);
    };
  }, [fetchWatchlist]);

  return (
    <div className="flex h-screen bg-slate-900 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-72 flex flex-col border-r border-slate-800 bg-slate-900 shrink-0">
        {/* App header */}
        <div className="flex items-center gap-2.5 px-5 py-4 border-b border-slate-800">
          <div className="w-8 h-8 rounded-lg overflow-hidden shrink-0">
            <img src={logo} alt="TradeFinder" className="w-full h-full object-cover" />
          </div>
          <span className="font-bold text-white tracking-tight">TradeFinder</span>
        </div>

        {/* User info */}
        <div className="px-5 py-3 border-b border-slate-800">
          <div className="flex items-center gap-2.5">
            <div className="w-7 h-7 bg-slate-700 rounded-full flex items-center justify-center shrink-0">
              <User className="w-3.5 h-3.5 text-slate-300" />
            </div>
            <div className="min-w-0">
              <p className="text-sm font-medium text-slate-200 truncate">
                {user?.first_name || user?.last_name
                  ? [user?.first_name, user?.last_name].filter(Boolean).join(" ")
                  : user?.username}
              </p>
              <p className="text-xs truncate flex items-center gap-1 mt-0.5">
                {brokerStatus === null ? (
                  <span className="text-slate-600">Checking gateway…</span>
                ) : brokerStatus.ok ? (
                  <>
                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0" />
                    <span className="text-emerald-400">
                      Alpaca {brokerStatus.paper ? "Paper" : "Live"}
                    </span>
                  </>
                ) : (
                  <>
                    <span className="w-1.5 h-1.5 rounded-full bg-slate-600 shrink-0" />
                    <span className="text-slate-500">No gateway</span>
                  </>
                )}
              </p>
            </div>
          </div>
        </div>

        {/* Watchlist */}
        <div className="flex-1 overflow-y-auto px-4 py-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1.5">
              <Star className="w-3 h-3" />
              Watchlist
            </h3>
            <button
              onClick={fetchWatchlist}
              className="text-slate-500 hover:text-slate-300 transition"
              title="Refresh"
            >
              <RefreshCw className="w-3.5 h-3.5" />
            </button>
          </div>

          {loadingWatchlist ? (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <div
                  key={i}
                  className="h-11 bg-slate-800 rounded-xl animate-pulse"
                />
              ))}
            </div>
          ) : watchlist.length === 0 ? (
            <div className="text-center py-8">
              <BarChart2 className="w-8 h-8 text-slate-700 mx-auto mb-2" />
              <p className="text-sm text-slate-600">
                Search for a stock and<br />star it to add to watchlist
              </p>
            </div>
          ) : (
            <div className="space-y-1.5">
              {watchlistItems.map((item) => (
                <WatchlistTile
                  key={item.ticker}
                  ticker={item.ticker}
                  bias={item.bias}
                  threshold={item.threshold}
                  barTime={item.bar_time ?? null}
                  source={item.source ?? null}
                  liveData={watchlistPrices[item.ticker] ?? null}
                  onRemove={handleRemoveFromWatchlist}
                  onClick={handleWatchlistItemClick}
                />
              ))}
            </div>
          )}
        </div>

        {/* Nav items */}
        <div className="px-3 py-3 border-t border-slate-800 shrink-0 flex flex-col gap-1">
          <button
            onClick={() => { setActiveView("stocks"); setSelectedTicker(null); }}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "stocks"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <Home className="w-4 h-4" />
            Main
          </button>
          <button
            onClick={() => { setActiveView("leaderboard"); setSelectedTicker(null); }}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "leaderboard"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <Trophy className="w-4 h-4" />
            Leader Board
          </button>
          <button
            onClick={() => setActiveView("newtrade")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "newtrade"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <PlusCircle className="w-4 h-4" />
            New Trade
          </button>
          <button
            onClick={() => setActiveView("tradeideas")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "tradeideas"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <Lightbulb className="w-4 h-4" />
            Trade Ideas
          </button>
          <button
            onClick={() => setActiveView("patternanalysis")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "patternanalysis"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <TrendingUp className="w-4 h-4" />
            Pattern Analysis
          </button>
          <button
            onClick={() => setActiveView("brokerage")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "brokerage"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <Building2 className="w-4 h-4" />
            Brokerage
          </button>
          <button
            onClick={() => setActiveView("account")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "account"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <Settings className="w-4 h-4" />
            Account Settings
          </button>
        </div>

        {/* Logout */}
        <div className="p-4 border-t border-slate-800">
          <button
            onClick={onLogout}
            className="flex items-center gap-2 text-sm text-slate-400 hover:text-red-400 transition w-full px-2 py-2 rounded-lg hover:bg-red-900/20"
          >
            <LogOut className="w-4 h-4" />
            Sign out
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 flex flex-col overflow-hidden">

        {/* Content row */}
        <div className="flex flex-1 overflow-hidden">
          <div className="flex-1 overflow-hidden">
            {activeView === "account" ? (
              <AccountSettings user={user} />
            ) : activeView === "brokerage" ? (
              <BrokerageSettings />
            ) : activeView === "leaderboard" ? (
              <LeaderBoard />
            ) : activeView === "newtrade" ? (
              <NewTrade onSelectTicker={handleSelectTicker} />
            ) : activeView === "tradeideas" ? (
              <TradeIdeas
                onSelectTicker={handleSelectTicker}
                watchlist={watchlist}
                openChartRequest={tradeIdeasOpenChart}
                onConsumedOpenChartRequest={clearTradeIdeasOpenChart}
              />
            ) : activeView === "patternanalysis" ? (
              <PatternAnalysis
                onSelectTicker={handleSelectTicker}
                openChartRequest={patternOpenChart}
                onConsumedOpenChartRequest={clearPatternOpenChart}
              />
            ) : selectedTicker ? (
              <StockDetail
                ticker={selectedTicker}
                onClose={() => setSelectedTicker(null)}
                watchlist={watchlist}
                defaultInterval={defaultInterval}
                barTime={chartBarTime}
                threshold={chartThreshold}
              />
            ) : (
              <AdminPanel user={user} />
            )}
          </div>

        </div>
      </main>
    </div>
  );
}
