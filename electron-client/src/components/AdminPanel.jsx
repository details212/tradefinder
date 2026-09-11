import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { alpacaApi } from "../api/client";
import { useFreshAlpacaQuotes } from "../hooks/useFreshAlpacaQuotes";
import { entrySlippagePerShare, adverseSlipColor, executionFromTrade, compactTicker } from "../utils/tradeExecution";
import { isRealizedClose } from "../utils/closedTradeAudit";
import TradeReviewModal from "./TradeReviewModal";
import OpenTradesCards, { OpenTradesGauges } from "./OpenTradesCards";
import AnalyticsPanel from "./AnalyticsPanel";
import StrategyPerformancePanel from "./StrategyPerformancePanel";
import ManualTradesPanel from "./ManualTradesPanel";
import PerformancePanel from "./PerformancePanel";
import {
  Database, Cpu,
  Zap, Globe, RefreshCw, CheckCircle, AlertTriangle,
  XCircle, Server,
  Key,
  BarChart2,
  HardDrive, Wifi, Loader2,
  TrendingUp, TrendingDown, Search, X,
  ShieldCheck, ShieldAlert, ChevronDown,
} from "lucide-react";

// Count weekdays (Mon–Fri) between a start date and now, inclusive of the start day.
function tradingDaysOpen(startDate) {
  if (!startDate) return null;
  const start = new Date(startDate);
  if (Number.isNaN(start.getTime())) return null;
  start.setHours(0, 0, 0, 0);
  const end = new Date();
  end.setHours(0, 0, 0, 0);
  if (end < start) return 0;
  let days = 0;
  const cur = new Date(start);
  while (cur <= end) {
    const dow = cur.getDay();
    if (dow !== 0 && dow !== 6) days++;
    cur.setDate(cur.getDate() + 1);
  }
  return days;
}

// ── Tiny reusable atoms ───────────────────────────────────────────────────────

function StatusDot({ status }) {
  const map = {
    online:   "bg-emerald-400 shadow-emerald-500/50",
    warning:  "bg-yellow-400  shadow-yellow-500/50",
    offline:  "bg-red-400     shadow-red-500/50",
    idle:     "bg-slate-500",
  };
  return (
    <span className={`inline-block w-2 h-2 rounded-full shadow-lg ${map[status] ?? map.idle} ${
      status === "online" ? "animate-pulse" : ""
    }`} />
  );
}

function Badge({ children, color = "slate" }) {
  const colors = {
    green:  "bg-emerald-900/50 text-emerald-400 border-emerald-800/50",
    yellow: "bg-yellow-900/50  text-yellow-400  border-yellow-800/50",
    red:    "bg-red-900/50     text-red-400     border-red-800/50",
    blue:   "bg-blue-900/50    text-blue-400    border-blue-800/50",
    slate:  "bg-slate-800      text-slate-400   border-slate-700",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold border ${colors[color]}`}>
      {children}
    </span>
  );
}

function Toggle({ on, onChange, label, sublabel }) {
  return (
    <div className="flex items-center justify-between py-3 border-b border-slate-800/80 last:border-0">
      <div>
        <p className="text-sm text-slate-300">{label}</p>
        {sublabel && <p className="text-xs text-slate-400 mt-0.5">{sublabel}</p>}
      </div>
      <button
        type="button"
        onClick={() => onChange(!on)}
        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors shrink-0 ${on ? "bg-brand-600" : "bg-slate-700"}`}
      >
        <span className={`inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform ${on ? "translate-x-4.5" : "translate-x-0.5"}`} />
      </button>
    </div>
  );
}

function ProgressBar({ value, max = 100, color = "brand" }) {
  const pct = Math.min(100, (value / max) * 100);
  const colorMap = {
    brand:   "bg-brand-500",
    green:   "bg-emerald-500",
    yellow:  "bg-yellow-500",
    red:     "bg-red-500",
  };
  const bar = pct > 80 ? "red" : pct > 60 ? "yellow" : color;
  return (
    <div className="h-1.5 w-full bg-slate-800 rounded-full overflow-hidden">
      <div className={`h-full rounded-full transition-all ${colorMap[bar]}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

// ── Section wrapper ───────────────────────────────────────────────────────────
function Card({ children, className = "" }) {
  return (
    <div className={`bg-slate-800/60 border border-slate-700/60 rounded-2xl ${className}`}>
      {children}
    </div>
  );
}

function CardHeader({ icon: Icon, title, accent, action }) {
  return (
    <div className={`flex items-center justify-between px-5 py-4 border-b border-slate-700/50 ${accent ? `border-l-2 ${accent}` : ""}`}>
      {title ? (
        <div className="flex items-center gap-2.5">
          {Icon && <Icon className="w-4 h-4 text-slate-400" />}
          <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
        </div>
      ) : (
        <span className="sr-only">My Trades</span>
      )}
      {action}
    </div>
  );
}

// ── Data pipeline row ──────────────────────────────────────────────────────────
function PipelineRow({ name, status, lastRun, records, duration }) {
  return (
    <div className="grid grid-cols-[2fr_1fr_1.5fr_1fr_1fr] items-center gap-4 px-5 py-3 border-b border-slate-800/60 last:border-0 hover:bg-slate-800/30 transition">
      <div className="flex items-center gap-2.5">
        <StatusDot status={status} />
        <span className="text-sm text-slate-300 font-medium">{name}</span>
      </div>
      <Badge color={status === "online" ? "green" : status === "warning" ? "yellow" : "red"}>
        {status === "online" ? "Running" : status === "warning" ? "Stale" : "Stopped"}
      </Badge>
      <span className="text-xs text-slate-400 tabular-nums">{lastRun}</span>
      <span className="text-xs text-slate-400 tabular-nums">{records}</span>
      <span className="text-xs text-slate-400">{duration}</span>
    </div>
  );
}

// ── System resource gauge ─────────────────────────────────────────────────────
function ResourceGauge({ icon: Icon, label, used, total, unit }) {
  const pct = Math.round((used / total) * 100);
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5">
          <Icon className="w-3.5 h-3.5 text-slate-400" />
          <span className="text-xs text-slate-400">{label}</span>
        </div>
        <span className="text-xs font-semibold text-slate-300 tabular-nums">{used}{unit} / {total}{unit}</span>
      </div>
      <ProgressBar value={pct} />
      <p className="text-[11px] text-slate-500">{pct}% utilised</p>
    </div>
  );
}

// ── Network connectivity card ─────────────────────────────────────────────────
const PING_HOSTS = [
  { label: "Local API",        host: "localhost",               icon: Server },
  { label: "Tick Data Farm",   host: "api.polygon.io",          icon: BarChart2 },
  { label: "Alpaca (paper)",   host: "paper-api.alpaca.markets",icon: Zap },
  { label: "Alpaca (live)",    host: "api.alpaca.markets",      icon: Zap },
  { label: "Yahoo Finance",    host: "query1.finance.yahoo.com",icon: Globe },
  { label: "Internet",         host: "8.8.8.8",                 icon: Wifi },
];

function LatencyBar({ ms }) {
  if (ms == null) return null;
  // Green < 70ms, yellow < 200ms, red >= 200ms
  const color = ms < 70 ? "bg-emerald-500" : ms < 200 ? "bg-yellow-500" : "bg-red-500";
  const width = Math.min(100, (ms / 300) * 100);
  return (
    <div className="flex items-center gap-2 flex-1">
      <div className="flex-1 h-1 bg-slate-800 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${width}%` }} />
      </div>
      <span className={`text-xs tabular-nums font-medium w-14 text-right ${
        ms < 70 ? "text-emerald-400" : ms < 200 ? "text-yellow-400" : "text-red-400"
      }`}>{ms} ms</span>
    </div>
  );
}

function NetworkCard() {
  const isElectron = typeof window !== "undefined" && !!window.electronAPI;

  const init = () => Object.fromEntries(PING_HOSTS.map(h => [h.host, { state: "idle" }]));
  const [results, setResults] = useState(init);
  const [pinging, setPinging] = useState(false);

  const runAll = useCallback(async () => {
    if (!isElectron) return;
    setPinging(true);
    setResults(Object.fromEntries(PING_HOSTS.map(h => [h.host, { state: "pending" }])));

    await Promise.all(PING_HOSTS.map(async ({ host }) => {
      const res = await window.electronAPI.ping(host);
      setResults(prev => ({ ...prev, [host]: { state: res.ok ? "ok" : "err", ...res } }));
    }));

    setPinging(false);
  }, [isElectron]);

  const runOne = useCallback(async (host) => {
    if (!isElectron) return;
    setResults(prev => ({ ...prev, [host]: { state: "pending" } }));
    const res = await window.electronAPI.ping(host);
    setResults(prev => ({ ...prev, [host]: { state: res.ok ? "ok" : "err", ...res } }));
  }, [isElectron]);

  return (
    <Card>
      <CardHeader
        icon={Wifi}
        title="Network Connectivity"
        action={
          isElectron ? (
            <button
              onClick={runAll}
              disabled={pinging}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-slate-600 bg-slate-800 hover:bg-slate-700 disabled:opacity-50 text-slate-300 transition"
            >
              {pinging
                ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                : <RefreshCw className="w-3.5 h-3.5" />}
              {pinging ? "Pinging…" : "Ping all"}
            </button>
          ) : (
            <span className="text-xs text-slate-500 italic">Browser mode — ping unavailable</span>
          )
        }
      />
      <div className="divide-y divide-slate-800/60">
        {PING_HOSTS.map(({ label, host, icon: Icon }) => {
          const r = results[host] ?? { state: "idle" };
          return (
            <div key={host} className="flex items-center gap-4 px-5 py-3 hover:bg-slate-800/30 transition">
              <Icon className="w-3.5 h-3.5 text-slate-500 shrink-0" />
              <div className="w-36 shrink-0">
                <p className="text-sm text-slate-300">{label}</p>
                <p className="text-[11px] text-slate-500 font-mono">{host}</p>
              </div>

              {r.state === "idle" && (
                <span className="text-xs text-slate-500 flex-1">—</span>
              )}
              {r.state === "pending" && (
                <Loader2 className="w-3.5 h-3.5 text-slate-400 animate-spin flex-1" />
              )}
              {r.state === "ok" && (
                <LatencyBar ms={r.latency} />
              )}
              {r.state === "err" && (
                <span className="text-xs text-red-400 flex-1">{r.error ?? "Unreachable"}</span>
              )}

              {isElectron && r.state !== "pending" && (
                <button
                  onClick={() => runOne(host)}
                  className="shrink-0 text-slate-500 hover:text-slate-300 transition ml-2"
                  title="Re-ping"
                >
                  <RefreshCw className="w-3 h-3" />
                </button>
              )}
            </div>
          );
        })}
      </div>
    </Card>
  );
}

// ── Main AdminPanel ───────────────────────────────────────────────────────────
export default function AdminPanel({ user }) {
  // ── Order detail modal ────────────────────────────────────────────────────
  const [detailOrder,   setDetailOrder]   = useState(null);  // { dbOrder, alpacaData } | null
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError,   setDetailError]   = useState(null);
  const [dbCheckOpen,   setDbCheckOpen]   = useState(false);

  const openDetail = async (dbOrder) => {
    if (!dbOrder.alpaca_order_id) return;
    setDetailOrder({ dbOrder, alpacaData: null });
    setDetailLoading(true);
    setDetailError(null);
    try {
      const res = await alpacaApi.getOrderDetail(dbOrder.alpaca_order_id);
      setDetailOrder({ dbOrder, alpacaData: res.data });
    } catch (err) {
      setDetailError(err.response?.data?.error || "Failed to fetch order from Alpaca.");
    } finally {
      setDetailLoading(false);
    }
  };

  const closeDetail = () => { setDetailOrder(null); setDetailError(null); };

  // ── Orders — sync with Alpaca on mount + every 60 s while page is open ───
  const [orders,        setOrders]        = useState([]);
  const [ordersLoading, setOrdersLoading] = useState(true);
  const [ordersError,   setOrdersError]   = useState(null);
  const [syncedCount,   setSyncedCount]   = useState(null);
  const [lastSyncedAt,  setLastSyncedAt]  = useState(null);
  const [ordersPage,    setOrdersPage]    = useState(0);
  const [ordersFilter,  setOrdersFilter]  = useState("open"); // "open" | "closed"
  const [closedSymbolQuery, setClosedSymbolQuery] = useState("");
  const [reviewOrder,   setReviewOrder]   = useState(null);
  const [activeTab, setActiveTab] = useState("trades");
  const OPEN_ORDERS_PER_PAGE   = 24;
  const CLOSED_ORDERS_PER_PAGE = 20;

  // Hide unfilled dead orders
  const DEAD_STATUSES = new Set(["canceled", "expired", "rejected", "done_for_day"]);
  const isDeadOrder = (o) => {
    if (!DEAD_STATUSES.has(o.status)) return false;
    if (o.exit_method || o.closed_at || o.filled_avg_price != null) return false;
    return true;
  };
  const visibleOrders = useMemo(
    () => orders.filter(o => !isDeadOrder(o)),
    [orders]
  );

  const tabOrders = useMemo(() => {
    const isOpenTab = ordersFilter === "open";
    let list = visibleOrders.filter(o => isOpenTab ? o.is_open && !isRealizedClose(o) : isRealizedClose(o));
    const needle = closedSymbolQuery.trim().toUpperCase();
    if (!isOpenTab && needle) {
      const needleCompact = compactTicker(needle);
      list = list.filter((o) => {
        const sym = String(o.ticker || "").toUpperCase();
        return sym.includes(needle) || compactTicker(sym).includes(needleCompact);
      });
    }
    return list;
  }, [visibleOrders, ordersFilter, closedSymbolQuery]);

  // The exact page of open-trade cards currently rendered (see OpenTradesCards below).
  const pagedOpenOrders = useMemo(() => {
    if (ordersFilter !== "open") return [];
    return tabOrders
      .slice()
      .sort((a, b) => new Date(b.created_at ?? 0) - new Date(a.created_at ?? 0))
      .slice(ordersPage * OPEN_ORDERS_PER_PAGE, (ordersPage + 1) * OPEN_ORDERS_PER_PAGE);
  }, [tabOrders, ordersFilter, ordersPage]);

  // Quotes for every open ticker so book gauges stay live across pages.
  // Unique symbols are typically far fewer than the open-trade count.
  const openOrderTickers = useMemo(() => {
    if (ordersFilter !== "open") return [];
    const s = new Set(
      tabOrders.map(o => String(o.ticker || "").trim().toUpperCase()).filter(Boolean)
    );
    return [...s].sort();
  }, [tabOrders, ordersFilter]);

  const { quotes: liveQuotes, refetch: refetchOpenQuotes } = useFreshAlpacaQuotes(
    openOrderTickers,
    ordersFilter === "open" && openOrderTickers.length > 0,
  );

  const syncOrders = useCallback((isBackground = false) => {
    if (!isBackground) setOrdersLoading(true);
    alpacaApi.syncOrders()
      .then(r => {
        setOrders(r.data.orders ?? []);
        setSyncedCount(r.data.synced ?? 0);
        setLastSyncedAt(Date.now());
        setOrdersError(null);
        refetchOpenQuotes();
      })
      .catch(() => setOrdersError("Could not load or sync orders."))
      .finally(() => setOrdersLoading(false));
  }, [refetchOpenQuotes]);

  useEffect(() => {
    let cancelled = false;
    setOrdersLoading(true);
    alpacaApi.getOrders()
      .then((r) => {
        if (cancelled) return;
        const list = Array.isArray(r.data) ? r.data : (r.data?.orders ?? []);
        setOrders(list);
        setOrdersError(null);
      })
      .catch(() => {
        if (!cancelled) setOrdersError("Could not load or sync orders.");
      })
      .finally(() => {
        if (!cancelled) {
          setOrdersLoading(false);
          syncOrders(true);
        }
      });
    const id = setInterval(() => syncOrders(true), 60_000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [syncOrders]);

  const PANEL_TABS = [
    { id: "trades",        label: "My Trades" },
    { id: "performance",   label: "Performance" },
    { id: "pnl",           label: "P&L Analysis" },
    { id: "distribution",  label: "Distribution Analysis" },
    { id: "strategies",    label: "Trade Ideas" },
    { id: "manual",        label: "Manual" },
  ];

  return (
    <div className={`h-full bg-slate-900 ${activeTab === "pnl" ? "flex flex-col overflow-hidden" : "overflow-y-auto"}`}>
      <div className={`max-w-6xl mx-auto px-6 py-8 flex flex-col gap-6 w-full ${activeTab === "pnl" ? "flex-1 min-h-0" : ""}`}>

        {/* ── Page header ── */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-xl font-bold text-slate-100 tracking-tight">Trading Panel</h1>
            <p className="text-sm text-slate-400 mt-0.5">Overview, system status, and configuration</p>
          </div>
          <div className="flex items-center gap-2 text-xs text-emerald-400 bg-emerald-900/20 border border-emerald-800/40 px-3 py-1.5 rounded-lg">
            <StatusDot status="online" />
            All systems operational
          </div>
        </div>

        {/* ── Panel tabs ── */}
        <div className="flex items-center gap-1 border-b border-slate-800">
          {PANEL_TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 -mb-px transition ${
                activeTab === tab.id
                  ? "border-brand-500 text-brand-400"
                  : "border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-600"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {activeTab === "trades" && (
        <Card>
          <CardHeader
            action={
              <span className="flex items-center gap-3 text-[11px] text-slate-400">
                {/* Open / Closed filter toggle */}
                <span className="flex items-center rounded-md border border-slate-700/60 overflow-hidden text-[11px] font-medium">
                  {["open", "closed"].map(f => (
                    <button
                      key={f}
                      onClick={() => { setOrdersFilter(f); setOrdersPage(0); }}
                      className={`px-3 py-1 capitalize transition ${
                        ordersFilter === f
                          ? "bg-brand-500/20 text-brand-400"
                          : "text-slate-400 hover:text-slate-300 hover:bg-slate-700/40"
                      }`}
                    >{f}</button>
                  ))}
                </span>

                {ordersLoading && (
                  <span className="flex items-center gap-1">
                    <Loader2 className="w-3 h-3 animate-spin" /> Syncing…
                  </span>
                )}
                {!ordersLoading && visibleOrders.length > 0 && (
                  <span className="whitespace-nowrap">
                    {visibleOrders.filter(o => o.is_open && !isRealizedClose(o)).length} open
                    {visibleOrders.filter(o => isRealizedClose(o)).length > 0 && (
                      <> · {visibleOrders.filter(o => isRealizedClose(o)).length} closed</>
                    )}
                  </span>
                )}
                {lastSyncedAt && (
                  <span className="text-slate-500 whitespace-nowrap">
                    · updated {new Date(lastSyncedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                  </span>
                )}
              </span>
            }
          />

          {ordersFilter === "closed" && (
            <div className="px-5 py-2.5 border-b border-slate-800/40">
              <div className="relative max-w-xs">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />
                <input
                  type="text"
                  value={closedSymbolQuery}
                  onChange={(e) => {
                    setClosedSymbolQuery(e.target.value.toUpperCase());
                    setOrdersPage(0);
                  }}
                  placeholder="Search symbol…"
                  autoComplete="off"
                  autoCorrect="off"
                  spellCheck={false}
                  className="w-full bg-slate-800 border border-slate-700 text-slate-200 placeholder-slate-600 rounded-lg pl-8 pr-8 py-1.5 text-xs font-mono uppercase focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/30"
                />
                {closedSymbolQuery && (
                  <button
                    type="button"
                    onClick={() => { setClosedSymbolQuery(""); setOrdersPage(0); }}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 transition"
                    title="Clear search"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>
            </div>
          )}

          {ordersLoading ? (
            <div className="flex items-center justify-center gap-2 py-10 text-slate-400 text-sm">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading orders…
            </div>
          ) : ordersError ? (
            <div className="px-5 py-6 text-sm text-red-400">{ordersError}</div>
          ) : visibleOrders.length === 0 ? (
            <div className="px-5 py-10 text-center text-slate-500 text-sm">
              No orders placed yet. Open the chart and place a bracket order to get started.
            </div>
          ) : tabOrders.length === 0 ? (
            <div className="px-5 py-10 text-center text-slate-500 text-sm">
              {ordersFilter === "closed" && closedSymbolQuery.trim()
                ? `No closed trades matching "${closedSymbolQuery.trim().toUpperCase()}".`
                : `No ${ordersFilter} trades.`}
            </div>
          ) : (
            <>
              {ordersFilter === "open" ? (
                <>
                  <OpenTradesGauges
                    orders={tabOrders}
                    closedOrders={visibleOrders.filter((o) => isRealizedClose(o))}
                    liveQuotes={liveQuotes}
                  />
                  <OpenTradesCards
                    orders={pagedOpenOrders}
                    liveQuotes={liveQuotes}
                    onDetail={openDetail}
                    onChart={setReviewOrder}
                    daysOpenFn={tradingDaysOpen}
                  />
                </>
              ) : (
                <>
                  {/* ── Closed trades headers ── */}
                  <div className="grid grid-cols-[1.4fr_0.5fr_0.5fr_0.4fr_0.9fr_1fr_0.5fr_0.8fr_0.7fr_1fr_0.8fr_0.9fr_1.4fr] gap-3 px-5 py-2 border-b border-slate-800/40">
                    {[
                      { h: "Ticker" }, { h: "Detail" }, { h: "Chart" }, { h: "Dir", center: true },
                      { h: "Entry" }, { h: "Fill / Slippage" }, { h: "Qty" },
                      { h: "Risk $" }, { h: "R/R" }, { h: "Final P/L" }, { h: "R Result" },
                      { h: "Status" }, { h: "Closed Date" },
                    ].map(({ h, center }) => (
                      <span key={h} className={`text-[10px] font-semibold text-slate-500 uppercase tracking-wider${center ? " text-center" : ""}`}>{h}</span>
                    ))}
                  </div>

                  {/* ── Closed trades rows ── */}
                  <div className="divide-y divide-slate-800/40">
                    {tabOrders
                      .slice().sort((a, b) => new Date(b.closed_at ?? b.synced_at ?? b.created_at ?? 0) - new Date(a.closed_at ?? a.synced_at ?? a.created_at ?? 0))
                      .slice(ordersPage * CLOSED_ORDERS_PER_PAGE, (ordersPage + 1) * CLOSED_ORDERS_PER_PAGE)
                      .map(o => {
                        const isLong    = o.direction === "long";
                        const isPaper   = o.paper_mode;
                        const exec      = executionFromTrade(o);
                        const entryLim  = exec.limitPrice;
                        const fillPx    = exec.fillPrice;
                        const stopPx    = o.stop_price   != null ? Number(o.stop_price)       : null;
                        const riskAmt   = o.risk_amt     != null ? Number(o.risk_amt)         : (entryLim != null && stopPx != null ? Math.abs(entryLim - stopPx) * (o.qty ?? 1) : null);
                        const rrEff     = o.rr_ratio_effective ?? o.rr_ratio;
                        const pl        = o.unrealized_pl != null ? Number(o.unrealized_pl) : null;
                        const plPos     = pl != null && pl > 0;
                        const plNeg     = pl != null && pl < 0;
                        const plColor   = plPos ? "text-emerald-400" : plNeg ? "text-red-400" : "text-slate-400";

                        const slip      = entrySlippagePerShare({ limitPrice: entryLim, fillPrice: fillPx, direction: o.direction, isMarket: exec.isMarket });
                        const slipColor = adverseSlipColor(slip);

                        // R result: actual P/L divided by planned risk per share × qty
                        const rResult   = pl != null && riskAmt != null && riskAmt > 0 ? (pl / riskAmt) : null;
                        const rColor    = rResult == null ? "text-slate-500" : rResult > 0 ? "text-emerald-400" : "text-red-400";

                        const statusColor = o.status === "filled" ? "text-emerald-400" : o.status === "canceled" || o.status === "expired" ? "text-slate-400" : "text-yellow-400";
                        const date = o.closed_at ?? o.synced_at ?? o.created_at;
                        const dateStr = date ? new Date(date).toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "—";

                        return (
                          <div key={o.id} className="grid grid-cols-[1.4fr_0.5fr_0.5fr_0.4fr_0.9fr_1fr_0.5fr_0.8fr_0.7fr_1fr_0.8fr_0.9fr_1.4fr] gap-3 px-5 py-3 hover:bg-slate-800/30 transition items-center">
                            {/* Ticker */}
                            <div className="flex items-center gap-2 min-w-0">
                              <span className="font-bold text-slate-100 text-sm truncate">{o.ticker}</span>
                              {isPaper && <span className="text-[9px] font-semibold text-blue-400 bg-blue-900/30 border border-blue-700/40 rounded px-1 py-0.5 shrink-0">PAPER</span>}
                            </div>

                            {/* Detail */}
                            <button onClick={() => openDetail(o)} disabled={!o.alpaca_order_id} title="View bracket details" className="text-slate-500 hover:text-brand-400 disabled:opacity-20 disabled:cursor-not-allowed transition"><Search className="w-3.5 h-3.5" /></button>

                            {/* Chart */}
                            <button title="View trade chart" onClick={() => setReviewOrder(o)} className="text-slate-500 hover:text-emerald-400 transition"><BarChart2 className="w-3.5 h-3.5" /></button>

                            {/* Direction */}
                            <div className="flex items-center justify-center">
                              {isLong ? <TrendingUp className="w-3.5 h-3.5 text-emerald-400" /> : <TrendingDown className="w-3.5 h-3.5 text-red-400" />}
                            </div>

                            {/* Entry Limit */}
                            <span className="font-mono text-xs text-slate-300">
                              {entryLim != null ? `$${entryLim.toFixed(2)}` : "—"}
                            </span>

                            {/* Fill + slippage */}
                            <div className="flex items-center gap-1.5">
                              <span className="font-mono text-xs text-slate-200">{fillPx != null ? `$${fillPx.toFixed(2)}` : "—"}</span>
                              {slip != null && Math.abs(slip) > 0.005 && (
                                <span className={`text-[10px] font-mono ${slipColor}`}>
                                  {slip > 0 ? "+" : ""}{slip.toFixed(2)}
                                </span>
                              )}
                            </div>

                            {/* Qty */}
                            <span className="font-mono text-xs text-slate-300">{o.qty}</span>

                            {/* Risk $ */}
                            <span className="font-mono text-xs text-slate-400">{riskAmt != null ? `$${riskAmt.toFixed(2)}` : "—"}</span>

                            {/* R/R */}
                            <span className="font-mono text-xs text-slate-300">{rrEff != null ? `${Number(rrEff).toFixed(1)}R` : "—"}</span>

                            {/* Final P/L */}
                            <span className={`font-mono text-xs font-bold ${plColor}`}>
                              {pl != null ? `${plPos ? "+" : ""}$${Math.abs(pl).toFixed(2)}` : "—"}
                            </span>

                            {/* R Result */}
                            <span className={`font-mono text-xs font-semibold ${rColor}`}>
                              {rResult != null ? `${rResult > 0 ? "+" : ""}${rResult.toFixed(2)}R` : "—"}
                            </span>

                            {/* Status */}
                            <span className={`text-xs font-medium capitalize ${statusColor}`}>{o.status ?? "—"}</span>

                            {/* Date */}
                            <span className="text-[11px] text-slate-400">{dateStr}</span>
                          </div>
                        );
                      })}
                  </div>
                </>
              )}

              {/* Pagination controls */}
              {tabOrders.length > (ordersFilter === "open" ? OPEN_ORDERS_PER_PAGE : CLOSED_ORDERS_PER_PAGE) && (() => {
                const filteredCount = tabOrders.length;
                const perPage = ordersFilter === "open" ? OPEN_ORDERS_PER_PAGE : CLOSED_ORDERS_PER_PAGE;
                const totalPages = Math.ceil(filteredCount / perPage);
                return (
                  <div className="flex items-center justify-between px-5 py-3 border-t border-slate-800/40">
                    <span className="text-[11px] text-slate-400">
                      {ordersPage * perPage + 1}–{Math.min((ordersPage + 1) * perPage, filteredCount)} of {filteredCount}
                    </span>
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setOrdersPage(0)}
                        disabled={ordersPage === 0}
                        className="px-2 py-1 text-[11px] rounded text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 disabled:opacity-30 disabled:cursor-not-allowed transition"
                      >«</button>
                      <button
                        onClick={() => setOrdersPage(p => p - 1)}
                        disabled={ordersPage === 0}
                        className="px-2 py-1 text-[11px] rounded text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 disabled:opacity-30 disabled:cursor-not-allowed transition"
                      >‹</button>
                      {Array.from({ length: totalPages }, (_, i) => (
                        <button
                          key={i}
                          onClick={() => setOrdersPage(i)}
                          className={`px-2 py-1 text-[11px] rounded transition ${
                            i === ordersPage
                              ? "bg-brand-500/20 text-brand-400 font-semibold"
                              : "text-slate-400 hover:text-slate-100 hover:bg-slate-700/60"
                          }`}
                        >{i + 1}</button>
                      ))}
                      <button
                        onClick={() => setOrdersPage(p => p + 1)}
                        disabled={ordersPage === totalPages - 1}
                        className="px-2 py-1 text-[11px] rounded text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 disabled:opacity-30 disabled:cursor-not-allowed transition"
                      >›</button>
                      <button
                        onClick={() => setOrdersPage(totalPages - 1)}
                        disabled={ordersPage === totalPages - 1}
                        className="px-2 py-1 text-[11px] rounded text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 disabled:opacity-30 disabled:cursor-not-allowed transition"
                      >»</button>
                    </div>
                  </div>
                );
              })()}
            </>
          )}
        </Card>
        )}

        {activeTab === "performance" && (
          <PerformancePanel orders={orders} loading={ordersLoading} />
        )}

        {activeTab === "pnl" && (
          <div className="flex-1 min-h-0 flex flex-col">
            <AnalyticsPanel orders={orders} loading={ordersLoading} section="pnl" />
          </div>
        )}

        {activeTab === "distribution" && (
          <AnalyticsPanel orders={orders} loading={ordersLoading} section="distribution" />
        )}

        {activeTab === "strategies" && (
          <StrategyPerformancePanel orders={orders} loading={ordersLoading} />
        )}

        {activeTab === "manual" && (
          <ManualTradesPanel orders={orders} loading={ordersLoading} />
        )}

      </div>

      {/* ── Order detail modal ── */}
      {detailOrder && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-[2px] p-4">
          <div className="bg-slate-800 border border-slate-700 rounded-xl shadow-2xl w-full max-w-3xl overflow-hidden flex flex-col max-h-[90vh]">

            {/* Header */}
            <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700 shrink-0">
              <p className="text-white font-bold text-sm">
                Bracket Details — {detailOrder.dbOrder.ticker}
              </p>
              <button onClick={closeDetail} className="text-slate-400 hover:text-slate-300 transition">
                <XCircle className="w-5 h-5" />
              </button>
            </div>

            {/* Body */}
            <div className="overflow-y-auto px-5 py-4 flex flex-col gap-4 text-xs">
              {detailLoading && (
                <div className="flex items-center gap-2 text-slate-400 py-6 justify-center">
                  <Loader2 className="w-4 h-4 animate-spin" /> Fetching from Alpaca…
                </div>
              )}

              {detailError && (
                <div className="text-red-400 bg-red-900/20 border border-red-800/40 rounded-lg px-3 py-2">
                  {detailError}
                </div>
              )}

              {/* ── Fill / Entry summary ── */}
              {(() => {
                const db        = detailOrder.dbOrder;
                const a         = detailOrder.alpacaData;
                const isLong    = db.direction === "long";
                const exec      = executionFromTrade(db, a);
                const entryLim  = exec.limitPrice;
                const fillPx    = exec.fillPrice;
                const fillPrice = fillPx ?? entryLim;
                const stopPx    = db.stop_price       != null ? Number(db.stop_price)       : null;
                const tgtPx     = db.target_price     != null ? Number(db.target_price)     : null;
                const slip      = entrySlippagePerShare({ limitPrice: entryLim, fillPrice: fillPx, direction: db.direction, isMarket: exec.isMarket });
                const slipColor = adverseSlipColor(slip);

                // Alpaca bracket leg values for stop / target mismatch detection
                const priceTol   = 0.01;
                const legs       = a?.legs ?? [];
                const stopLeg    = legs.find(l => l.type === "stop" || l.type === "stop_limit");
                const profitLeg  = legs.find(l => l.type === "limit" && l !== stopLeg);
                const aStopPx    = stopLeg?.stop_price   != null ? Number(stopLeg.stop_price)   : null;
                const aTgtPx     = profitLeg?.limit_price != null ? Number(profitLeg.limit_price) : null;
                const stopMatch  = stopPx != null && aStopPx != null ? Math.abs(stopPx - aStopPx) <= priceTol : null;
                const tgtMatch   = tgtPx  != null && aTgtPx  != null ? Math.abs(tgtPx  - aTgtPx)  <= priceTol : null;

                const PriceCell = ({ dbVal, aVal, match, className }) => (
                  <span className="flex items-center gap-1">
                    <span className={`font-mono ${className}`}>{dbVal}</span>
                    {match === false && aVal != null && (
                      <span className="text-[10px] text-slate-500">↔ <span className="font-mono text-amber-400">{aVal}</span></span>
                    )}
                  </span>
                );

                const rows = [
                  ["Direction", <span className={`font-semibold ${isLong ? "text-emerald-400" : "text-red-400"}`}>{isLong ? "Long" : "Short"}</span>],
                  ["Qty",       <span className="font-mono text-slate-200">{db.qty ?? "—"}</span>],
                  [((a?.type || a?.order_type) === "market" ? "Chart entry" : "Entry Limit"), entryLim != null
                    ? <span className="font-mono text-slate-300">${entryLim.toFixed(2)}</span>
                    : <span className="text-slate-500">—</span>],
                  ["Fill Price", fillPrice != null
                    ? <span className="font-mono text-slate-200">${Number(fillPrice).toFixed(2)}</span>
                    : <span className="text-slate-500">—</span>],
                  ...(slip != null && Math.abs(slip) > 0.005 ? [
                    ["Slippage", <span className={`font-mono ${slipColor}`}>{slip > 0 ? "+" : ""}{slip.toFixed(3)}</span>],
                  ] : []),
                  ["Stop", stopPx != null
                    ? <PriceCell dbVal={`$${stopPx.toFixed(2)}`} aVal={aStopPx != null ? `$${aStopPx.toFixed(2)}` : null} match={stopMatch} className="text-red-400" />
                    : <span className="text-slate-500">—</span>],
                  ["Target", tgtPx != null
                    ? <PriceCell dbVal={`$${tgtPx.toFixed(2)}`} aVal={aTgtPx != null ? `$${aTgtPx.toFixed(2)}` : null} match={tgtMatch} className="text-emerald-400" />
                    : <span className="text-slate-500">—</span>],
                ];

                return (
                  <div className="rounded-lg border border-slate-700 bg-slate-900/50">
                    <div className="px-3 py-2 border-b border-slate-700 flex items-center gap-1.5">
                      <span className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Fill / Entry</span>
                      {db.paper_mode && (
                        <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-violet-900/40 text-violet-400 border border-violet-800/50">Paper</span>
                      )}
                    </div>
                    <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 px-3 py-3">
                      {rows.map(([lbl, val]) => (
                        <div key={lbl} className="flex justify-between gap-2">
                          <span className="text-slate-400">{lbl}</span>
                          {val}
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })()}

              {/* ── Sanity check: DB vs Alpaca ── */}
              {detailOrder.alpacaData && (() => {
                const db  = detailOrder.dbOrder;
                const a   = detailOrder.alpacaData;
                const legs = a.legs ?? [];
                const stopLeg   = legs.find(l => l.type === "stop" || l.type === "stop_limit");
                const profitLeg = legs.find(l => l.type === "limit" && l !== stopLeg);

                const priceTol = 0.01;
                const priceMatch = (x, y) =>
                  x == null || y == null ? null : Math.abs(Number(x) - Number(y)) <= priceTol;

                const isMkt = (a.type || a.order_type) === "market";
                const checks = [
                  { label: "Symbol",      db: db.ticker,                                              alpaca: a.symbol ?? "—",                                                  match: compactTicker(db.ticker) === compactTicker(a.symbol) },
                  { label: "Qty",         db: String(db.qty),                                         alpaca: a.filled_qty != null ? String(Number(a.filled_qty)) : "—", match: a.filled_qty != null ? Number(db.qty) === Number(a.filled_qty) : null },
                  { label: "Fill Price",  db: db.filled_avg_price != null ? `$${Number(db.filled_avg_price).toFixed(2)}` : "—", alpaca: a.filled_avg_price != null ? `$${Number(a.filled_avg_price).toFixed(2)}` : "—", match: priceMatch(db.filled_avg_price, a.filled_avg_price) },
                  { label: "Status",      db: db.status ?? "—",                                       alpaca: a.status ?? "—",                                                  match: (db.status ?? "") === (a.status ?? "") },
                  { label: isMkt ? "Chart entry" : "Entry Limit", db: db.entry_price != null ? `$${Number(db.entry_price).toFixed(2)}` : "—",       alpaca: a.limit_price != null ? `$${Number(a.limit_price).toFixed(2)}` : "—",     match: isMkt ? null : priceMatch(db.entry_price, a.limit_price) },
                  { label: "Stop",        db: db.stop_price != null ? `$${Number(db.stop_price).toFixed(2)}` : "—", alpaca: stopLeg?.stop_price != null ? `$${Number(stopLeg.stop_price).toFixed(2)}` : "—", match: priceMatch(db.stop_price, stopLeg?.stop_price) },
                  { label: "Target",      db: db.target_price != null ? `$${Number(db.target_price).toFixed(2)}` : "—", alpaca: profitLeg?.limit_price != null ? `$${Number(profitLeg.limit_price).toFixed(2)}` : "—", match: priceMatch(db.target_price, profitLeg?.limit_price) },
                ];

                const mismatches = checks.filter(c => c.match === false);
                const unknowns   = checks.filter(c => c.match === null);
                const allGood    = mismatches.length === 0;

                return (
                  <div className={`rounded-lg border ${allGood ? "border-emerald-800/50 bg-emerald-900/10" : "border-amber-700/50 bg-amber-900/10"}`}>
                    <button
                      onClick={() => setDbCheckOpen(v => !v)}
                      className="w-full flex items-center gap-1.5 px-3 py-2.5 text-left"
                    >
                      {allGood
                        ? <ShieldCheck className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
                        : <ShieldAlert  className="w-3.5 h-3.5 text-amber-400  shrink-0" />}
                      <span className={`text-[11px] font-semibold flex-1 ${allGood ? "text-emerald-400" : "text-amber-400"}`}>
                        {allGood ? "DB matches Alpaca" : `${mismatches.length} mismatch${mismatches.length !== 1 ? "es" : ""} detected`}
                      </span>
                      {unknowns.length > 0 && (
                        <span className="text-[10px] text-slate-500 italic">({unknowns.length} unavailable)</span>
                      )}
                      <ChevronDown className={`w-3.5 h-3.5 ml-1 shrink-0 transition-transform ${dbCheckOpen ? "rotate-180" : ""} ${allGood ? "text-emerald-600" : "text-amber-600"}`} />
                    </button>
                    {dbCheckOpen && (
                      <div className="flex flex-wrap gap-x-3 gap-y-1.5 px-3 pb-2.5">
                        {checks.map(({ label, db: dv, alpaca: av, match }) => (
                          <div key={label} className={`flex items-center gap-1 text-[10px] rounded px-1.5 py-0.5 border ${
                            match === true  ? "bg-emerald-900/20 border-emerald-800/50 text-emerald-400" :
                            match === false ? "bg-amber-900/30  border-amber-600/50  text-amber-300"     :
                                              "bg-slate-800/60  border-slate-700      text-slate-500"
                          }`}>
                            <span className="text-slate-500 mr-0.5">{label}:</span>
                            <span className="font-mono font-semibold">{dv}</span>
                            {match === false && (
                              <span className="ml-1 text-slate-500">↔ <span className="text-amber-400 font-mono">{av}</span></span>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })()}

              {detailOrder.alpacaData && (() => {
                const d    = detailOrder.alpacaData;
                const legs = d.legs ?? [];

                const fmt     = (v) => v != null ? `$${parseFloat(v).toFixed(2)}` : "—";
                const fmtDate = (v) => v ? new Date(v).toLocaleString() : "—";

                const OrderBlock = ({ label, data: o, accent }) => (
                  <div className={`rounded-lg border ${accent} bg-slate-900/50`}>
                    <div className={`px-3 py-2 border-b ${accent} flex items-center justify-between`}>
                      <span className="font-semibold text-slate-300">{label}</span>
                      <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded ${
                        o.status === "filled"   ? "bg-emerald-900/40 text-emerald-400" :
                        o.status === "canceled" || o.status === "expired" ? "bg-slate-700 text-slate-400" :
                        o.status === "held"     ? "bg-yellow-900/40 text-yellow-400" :
                        "bg-slate-700/60 text-slate-400"
                      }`}>{o.status}</span>
                    </div>
                    <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 px-3 pt-3 pb-4">
                      {[
                        ["Side",          o.side],
                        ["Type",          o.type],
                        ["Qty",           o.qty],
                        ["Filled Qty",    o.filled_qty],
                        ["Limit Price",   fmt(o.limit_price)],
                        ["Stop Price",    fmt(o.stop_price)],
                        ["Fill Price",    fmt(o.filled_avg_price)],
                        ["Time In Force", o.time_in_force?.toUpperCase()],
                        ["Submitted",     fmtDate(o.submitted_at)],
                        ["Filled At",     fmtDate(o.filled_at)],
                        ["Expires At",    fmtDate(o.expires_at)],
                      ].filter(([, val]) => val != null && val !== "—").map(([lbl, val]) => (
                        <div key={lbl} className="flex justify-between gap-2">
                          <span className="text-slate-400">{lbl}</span>
                          <span className="font-mono text-slate-200 capitalize">{val}</span>
                        </div>
                      ))}
                      <div className="col-span-2 mt-1 border-t border-slate-700/50 pt-2">
                        <span className="text-slate-500">ID </span>
                        <span className="font-mono text-slate-400 break-all">{o.id}</span>
                      </div>
                    </div>
                  </div>
                );

                return (
                  <>
                    <OrderBlock label="Entry Order" data={d} accent="border-slate-700" />
                    {legs.map((leg, i) => {
                      const isStop = leg.type === "stop" || leg.type === "stop_limit";
                      const isTp   = !isStop;
                      const label  = isStop ? "Stop Loss" : isTp ? "Take Profit" : `Leg ${i + 1}`;
                      const accent = isStop ? "border-red-900/50" : "border-emerald-900/50";
                      return <OrderBlock key={leg.id} label={label} data={leg} accent={accent} />;
                    })}
                  </>
                );
              })()}
            </div>
          </div>
        </div>
      )}

      {/* ── Trade Review Chart Modal ── */}
      {reviewOrder && (
        <TradeReviewModal
          order={reviewOrder}
          onClose={() => setReviewOrder(null)}
          onTradeClosed={() => { setReviewOrder(null); syncOrders(); }}
        />
      )}

    </div>
  );
}
