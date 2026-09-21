import { useState, useEffect, useCallback, useRef } from "react";
import { alpacaApi, tradeIdeasApi, preferencesApi, authApi } from "../api/client";
import { fetchAlpacaQuotes } from "../utils/fetchAlpacaQuote";
import LiveStreamBar from "./LiveStreamBar";
import StockDetail from "./StockDetail";
import TradeIdeas from "./TradeIdeas";
import PatternAnalysis from "./PatternAnalysis";
import NewTrade from "./NewTrade";
import AccountSettings from "./AccountSettings";
import BrokerageSettings from "./BrokerageSettings";
import ExchangePanel from "./ExchangePanel";
import AdminPanel from "./AdminPanel";
import LeaderBoard from "./LeaderBoard";
import SubscriptionPanel from "./SubscriptionPanel";
import SupportPanel from "./SupportPanel";
import DataDownload from "./DataDownload";
import SystemPanel from "./SystemPanel";
import ClosedTradesPanel from "./ClosedTradesPanel";
import logo from "../assets/logo.png";
import {
  LogOut,
  User,
  ChevronRight,
  ChevronLeft,
  Loader2,
  Server,
  Lightbulb,
  Home,
  Settings,
  Building2,
  BarChart2,
  PlusCircle,
  TrendingUp,
  Trophy,
  CreditCard,
  LifeBuoy,
  FileDown,
  Scale,
} from "lucide-react";

const LIVE_STREAM_POLL_MS       = 60_000;
const LIVE_STREAM_MINUTES  = 15;
const LIVE_STREAM_MIN_INFO = 3;
const STREAM_NEW_FLASH_MS  = 60_000;
const CLIENT_VERSION_POLL_MS    = 30 * 60_000; // re-check for a version bump every 30 min while open

function isClientOutdated(client, required) {
  if (!client || !required) return false;
  const pa = String(client).replace(/^v/i, "").split(".").map(Number);
  const pb = String(required).replace(/^v/i, "").split(".").map(Number);
  for (let i = 0; i < 3; i++) {
    const va = pa[i] ?? 0;
    const vb = pb[i] ?? 0;
    if (va < vb) return true;
    if (va > vb) return false;
  }
  return false;
}

/** Play a soft two-tone ascending chime via the Web Audio API. No audio file required. */
function playStreamChime() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    // C5 (523 Hz) → E5 (659 Hz), each note fades out naturally
    [[523.25, 0, 0.28], [659.25, 0.20, 0.36]].forEach(([freq, delay, dur]) => {
      const osc  = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0, ctx.currentTime + delay);
      gain.gain.linearRampToValueAtTime(0.22, ctx.currentTime + delay + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + delay + dur);
      osc.start(ctx.currentTime + delay);
      osc.stop(ctx.currentTime + delay + dur + 0.02);
    });
  } catch { /* Web Audio not available — skip silently */ }
}

const _STREAM_MA_KEYS = ["ema10", "ema20", "sma50", "sma150", "sma200"];
function _streamMaScore(maData, price, isLong) {
  if (!maData || price == null) return null;
  return _STREAM_MA_KEYS.reduce((n, k) => {
    const v = maData[k];
    if (v == null) return n;
    return n + (isLong ? (price > v ? 1 : 0) : (price < v ? 1 : 0));
  }, 0);
}

export default function Dashboard({ user, onLogout }) {
  const [selectedTicker,   setSelectedTicker]   = useState(null);
  const [activeView,             setActiveView]             = useState("stocks");
  const [brokerStatus,           setBrokerStatus]           = useState(null); // null | { ok, paper }
  const [patternAnalysisEnabled, setPatternAnalysisEnabled] = useState(false);
  const [clientOutdated, setClientOutdated] = useState(false);
  const [upgradeUrl, setUpgradeUrl] = useState("");
  /** Live stream → Trade Ideas chart modal (key bumps so same ticker re-opens) */
  const [tradeIdeasOpenChart, setTradeIdeasOpenChart] = useState(null);
  const [rightFlyout, setRightFlyout] = useState(null); // null | "system" | "closed"

  // ── Live Stream state ──────────────────────────────────────────────────────
  const [streamItems,    setStreamItems]    = useState([]);
  const [streamNewCount, setStreamNewCount] = useState(0);
  const [streamLastPoll, setStreamLastPoll] = useState(null);
  const [streamNewKeys,  setStreamNewKeys]  = useState(new Set()); // keys flashed as "new"
  const streamSeenRef   = useRef(new Set());
  const streamFirstPoll = useRef(true);
  const streamPollRef   = useRef(null);

  // Live stream user preferences — kept in a ref so pollLiveStream can always
  // read the latest value without being recreated on every preference change.
  const streamPrefsRef = useRef({
    showLong:         true,
    showShort:        true,
    hiddenStrategies: new Set(),
    soundEnabled:     false,
  });

  const pollLiveStream = useCallback(async () => {
    try {
      const r = await tradeIdeasApi.recent(LIVE_STREAM_MINUTES);
      const items = r.data.recent || [];

      // Fresh Alpaca prices each poll (no client price cache)
      const tickers = [...new Set(items.map(i => i.ticker))];
      let maByTicker    = {};
      let priceByTicker = {};
      let openTickerSet = new Set();
      if (tickers.length > 0) {
        try {
          const [maRes, quotes, openRes] = await Promise.allSettled([
            tradeIdeasApi.maCache(tickers, { staleOk: true }),
            fetchAlpacaQuotes(tickers),
            alpacaApi.openTickers(),
          ]);
          maByTicker = maRes.status === "fulfilled" ? (maRes.value.data.ma || {}) : {};
          priceByTicker = quotes.status === "fulfilled" ? (quotes.value || {}) : {};
          openTickerSet = openRes.status === "fulfilled"
            ? new Set(openRes.value.data.tickers ?? [])
            : new Set();
        } catch { /* keep empty lookups — show items unfiltered if secondary calls fail */ }
      }

      const prefs = streamPrefsRef.current;

      // Apply all filters: open positions → direction → strategy → MA alignment
      const filtered = items.filter(item => {
        if (openTickerSet.has(item.ticker)) return false;
        const dir = item.direction?.toLowerCase();
        if (dir === "long"  && !prefs.showLong)  return false;
        if (dir === "short" && !prefs.showShort) return false;
        if (prefs.hiddenStrategies.has(Number(item.strategy_id))) return false;
        // MA alignment score only applies to long trades: short candidates are
        // often near highs before breaking down so price is still above MAs,
        // which gives them a misleadingly low "short" score and hides them.
        if (dir === "short") return true;
        const maData = maByTicker[item.ticker];
        if (!maData) return true; // no cache entry yet — don't drop it
        const price = priceByTicker[item.ticker]?.price ?? item.close;
        const score = _streamMaScore(maData, price, true);
        return score == null || score >= LIVE_STREAM_MIN_INFO;
      });

      if (streamFirstPoll.current) {
        // Baseline on first load — mark everything seen so nothing flashes "new"
        items.forEach(item =>
          streamSeenRef.current.add(`${item.strategy_id}:${item.ticker}:${item.bar_time}`)
        );
        streamFirstPoll.current = false;
      } else {
        // Collect genuinely new items (never seen before)
        const newItems = filtered.filter(item => {
          const key = `${item.strategy_id}:${item.ticker}:${item.bar_time}`;
          return !streamSeenRef.current.has(key);
        });
        // Mark ALL raw items as seen (prevents re-highlighting on next poll)
        items.forEach(item =>
          streamSeenRef.current.add(`${item.strategy_id}:${item.ticker}:${item.bar_time}`)
        );
        if (newItems.length > 0) {
          // Highlight only the single most recent new item (latest bar_time)
          const newest = newItems.reduce((best, item) =>
            new Date(item.bar_time) > new Date(best.bar_time) ? item : best
          );
          const newestKey = `${newest.strategy_id}:${newest.ticker}:${newest.bar_time}`;
          setStreamNewCount(prev => prev + newItems.length);
          setStreamNewKeys(new Set([newestKey]));
          if (prefs.soundEnabled) playStreamChime();
          // Clear the flash highlight after STREAM_NEW_FLASH_MS
          setTimeout(() => setStreamNewKeys(new Set()), STREAM_NEW_FLASH_MS);
        }
      }

      setStreamItems(filtered);
      setStreamLastPoll(new Date());
    } catch { /* ignore network errors */ }
  }, []);

  /** Fetch stream preferences and update the ref; re-poll so changes take effect immediately. */
  const loadStreamPrefs = useCallback(async (andRepoll = false) => {
    try {
      const r = await preferencesApi.get();
      const p = r.data.preferences ?? {};
      const hidden = (() => {
        try { return new Set(JSON.parse(p.stream_hidden_strategies || "[]").map(Number)); }
        catch { return new Set(); }
      })();
      streamPrefsRef.current = {
        showLong:         p.stream_show_long  !== "false",
        showShort:        p.stream_show_short !== "false",
        hiddenStrategies: hidden,
        soundEnabled:     p.stream_sound_enabled === "true",
      };
      setPatternAnalysisEnabled(p.pattern_analysis === "1");
      if (andRepoll) pollLiveStream();
    } catch { /* non-fatal — keep existing prefs */ }
  }, [pollLiveStream]);

  // Load preferences once on mount, then kick off the poll cycle
  useEffect(() => {
    loadStreamPrefs();
    pollLiveStream();
    streamPollRef.current = setInterval(pollLiveStream, LIVE_STREAM_POLL_MS);
    return () => clearInterval(streamPollRef.current);
  }, [pollLiveStream, loadStreamPrefs]);

  // Re-read preferences immediately when the user saves them in Account Settings
  useEffect(() => {
    const onPrefsChanged = () => loadStreamPrefs(true);
    window.addEventListener("tf:stream-prefs-changed", onPrefsChanged);
    return () => window.removeEventListener("tf:stream-prefs-changed", onPrefsChanged);
  }, [loadStreamPrefs]);

  // Re-poll the live stream immediately after a trade is opened so the newly
  // open symbol is removed from the marquee without waiting 60 seconds.
  useEffect(() => {
    const onTradeOpened = () => pollLiveStream();
    window.addEventListener("tf:trade-opened", onTradeOpened);
    return () => window.removeEventListener("tf:trade-opened", onTradeOpened);
  }, [pollLiveStream]);

  useEffect(() => {
    alpacaApi.test()
      .then(r => setBrokerStatus({ ok: r.data.ok, paper: r.data.paper }))
      .catch(() => setBrokerStatus({ ok: false, paper: null }));
  }, []);

  const handleSelectTicker = useCallback((ticker) => {
    setSelectedTicker(ticker);
    setActiveView("stocks");
  }, []);

  const clearTradeIdeasOpenChart = useCallback(() => setTradeIdeasOpenChart(null), []);

  const handleStreamItemClick = useCallback((item) => {
    setActiveView("tradeideas");
    setTradeIdeasOpenChart({
      key:       Date.now(),
      ticker:    item.ticker,
      barTime:   item.bar_time ?? null,
      threshold: null,
    });
  }, []);

  useEffect(() => {
    // Vite bakes window.APP_VERSION into the bundle from package.json at dev
    // server startup — it does not live-reload if package.json is bumped
    // afterward (e.g. a release commit) while the dev server keeps running.
    // That produces a false "outdated" reading purely from server staleness,
    // not a real version mismatch. Skip the check entirely in dev.
    if (import.meta.env.DEV) return;

    let cancelled = false;
    const checkVersion = () => {
      authApi.clientVersion()
        .then((r) => {
          if (cancelled) return;
          const required = r.data?.required_version;
          const client = window.APP_VERSION || "0.0.0";
          if (isClientOutdated(client, required)) {
            setClientOutdated(true);
            setUpgradeUrl((r.data?.download_url || "").trim());
          }
        })
        .catch(() => {});
    };
    checkVersion();
    // Re-check periodically so a version bump on the server reaches clients
    // that are already open, not just ones launched after the bump.
    const id = setInterval(checkVersion, CLIENT_VERSION_POLL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  return (
    <div className="flex flex-col h-screen bg-slate-900 overflow-hidden">

      {/* ── Live Stream top bar ───────────────────────────────────────────── */}
      <LiveStreamBar
        items={streamItems}
        newCount={streamNewCount}
        lastPoll={streamLastPoll}
        newKeys={streamNewKeys}
        onClearNew={() => setStreamNewCount(0)}
        onClickItem={handleStreamItemClick}
      />

      {/* ── Body (sidebar + content) ──────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-72 flex flex-col border-r border-slate-800 bg-slate-900 shrink-0">
        {/* App header */}
        <div className="flex flex-col px-5 py-4 border-b border-slate-800">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg overflow-hidden shrink-0">
              <img src={logo} alt="TradeFinder" className="w-full h-full object-cover" />
            </div>
            <span className="font-bold text-white tracking-tight">TradeFinder</span>
          </div>
          <div className="mt-2.5 w-full rounded-lg bg-brand-600/20 border border-brand-500/50 px-3 py-1.5 text-center">
            <span className="block text-[10px] font-semibold uppercase tracking-[0.18em] text-brand-400/80">
              Version
            </span>
            <span className="block text-xl font-black font-mono tabular-nums tracking-wide text-white leading-tight">
              {window.APP_VERSION || "—"}
            </span>
            {clientOutdated && (
              upgradeUrl ? (
                <a
                  href={upgradeUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="tf-upgrade-flash mt-1 block text-sm font-black uppercase tracking-widest text-amber-300 hover:text-amber-200"
                >
                  Upgrade
                </a>
              ) : (
                <span className="tf-upgrade-flash mt-1 block text-sm font-black uppercase tracking-widest text-amber-300">
                  Upgrade
                </span>
              )
            )}
          </div>
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

        {/* Nav items */}
        <div className="flex-1 overflow-y-auto px-3 py-3 border-t border-slate-800 flex flex-col gap-1">
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
            onClick={() => setActiveView("exchange")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "exchange"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <BarChart2 className="w-4 h-4" />
            Exchange
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
            onClick={() => setActiveView("datadownload")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "datadownload"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <FileDown className="w-4 h-4" />
            Data Download
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
          {patternAnalysisEnabled && (
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
          )}
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
            onClick={() => setActiveView("subscription")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "subscription"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <CreditCard className="w-4 h-4" />
            Subscription
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
          <button
            onClick={() => setActiveView("support")}
            className={`flex items-center gap-2.5 w-full px-3 py-2.5 rounded-lg text-sm font-medium transition ${
              activeView === "support"
                ? "bg-brand-600/20 text-brand-400 border border-brand-600/40"
                : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            }`}
          >
            <LifeBuoy className="w-4 h-4" />
            Support
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

        {/* Content row + right flyout */}
        <div className="flex flex-1 overflow-hidden min-w-0">
          <div className="flex-1 overflow-hidden min-w-0">
            {activeView === "support" ? (
              <SupportPanel user={user} />
            ) : activeView === "subscription" ? (
              <SubscriptionPanel />
            ) : activeView === "account" ? (
              <AccountSettings user={user} />
            ) : activeView === "brokerage" ? (
              <BrokerageSettings />
            ) : activeView === "exchange" ? (
              <ExchangePanel onOpenBrokerage={() => setActiveView("brokerage")} />
            ) : activeView === "leaderboard" ? (
              <LeaderBoard />
            ) : activeView === "datadownload" ? (
              <DataDownload />
            ) : activeView === "newtrade" ? (
              <NewTrade onSelectTicker={handleSelectTicker} />
            ) : activeView === "tradeideas" ? (
              <TradeIdeas
                onSelectTicker={handleSelectTicker}
                openChartRequest={tradeIdeasOpenChart}
                onConsumedOpenChartRequest={clearTradeIdeasOpenChart}
              />
            ) : activeView === "patternanalysis" ? (
              <PatternAnalysis
                onSelectTicker={handleSelectTicker}
              />
            ) : selectedTicker ? (
              <StockDetail
                ticker={selectedTicker}
                onClose={() => setSelectedTicker(null)}
              />
            ) : (
              <AdminPanel user={user} />
            )}
          </div>

          {/* Right flyouts — Closed audit + System, mutually exclusive */}
          <div className="flex shrink-0 h-full">
            <div className="flex flex-col shrink-0 border-l border-slate-800">
              <button
                type="button"
                onClick={() => setRightFlyout((cur) => (cur === "closed" ? null : "closed"))}
                title={rightFlyout === "closed" ? "Hide closed-trade audit" : "Closed-trade fill & close audit"}
                aria-expanded={rightFlyout === "closed"}
                className={`no-drag flex-1 w-9 flex flex-col items-center justify-center gap-2 transition ${
                  rightFlyout === "closed"
                    ? "bg-brand-600/15 text-brand-400 hover:bg-brand-600/25"
                    : "bg-slate-900 text-slate-500 hover:bg-slate-800 hover:text-slate-300"
                }`}
              >
                {rightFlyout === "closed"
                  ? <ChevronRight className="w-4 h-4 shrink-0" />
                  : <ChevronLeft className="w-4 h-4 shrink-0" />}
                <Scale className="w-4 h-4 shrink-0" />
                <span className="text-[10px] font-semibold uppercase tracking-wider [writing-mode:vertical-rl] rotate-180 select-none">
                  Closed
                </span>
              </button>
              <button
                type="button"
                onClick={() => setRightFlyout((cur) => (cur === "system" ? null : "system"))}
                title={rightFlyout === "system" ? "Hide system panel" : "Show system panel"}
                aria-expanded={rightFlyout === "system"}
                className={`no-drag flex-1 w-9 flex flex-col items-center justify-center gap-2 border-t border-slate-800 transition ${
                  rightFlyout === "system"
                    ? "bg-brand-600/15 text-brand-400 hover:bg-brand-600/25"
                    : "bg-slate-900 text-slate-500 hover:bg-slate-800 hover:text-slate-300"
                }`}
              >
                {rightFlyout === "system"
                  ? <ChevronRight className="w-4 h-4 shrink-0" />
                  : <ChevronLeft className="w-4 h-4 shrink-0" />}
                <Server className="w-4 h-4 shrink-0" />
                <span className="text-[10px] font-semibold uppercase tracking-wider [writing-mode:vertical-rl] rotate-180 select-none">
                  System
                </span>
              </button>
            </div>

            <div
              className={`overflow-hidden transition-[width] duration-300 ease-in-out border-l border-slate-800 bg-slate-900 ${
                rightFlyout === "system"
                  ? "w-80"
                  : rightFlyout === "closed"
                    ? "w-[min(56rem,calc(100vw-20rem))]"
                    : "w-0"
              }`}
            >
              <div className={`h-full ${rightFlyout === "closed" ? "w-[min(56rem,calc(100vw-20rem))]" : "w-80"}`}>
                {rightFlyout === "system" && (
                  <SystemPanel user={user} onClose={() => setRightFlyout(null)} />
                )}
                {rightFlyout === "closed" && (
                  <ClosedTradesPanel onClose={() => setRightFlyout(null)} />
                )}
              </div>
            </div>
          </div>
        </div>
      </main>
      </div>{/* end body */}
    </div>
  );
}
