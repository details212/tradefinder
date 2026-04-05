import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import api, { alpacaApi, authApi, resourcesApi, snapshotsApi } from "../api/client";
import TradeReviewModal from "./TradeReviewModal";
import AnalyticsPanel from "./AnalyticsPanel";
import {
  Activity, Database, Cpu,
  Zap, Globe, RefreshCw, CheckCircle, AlertTriangle,
  XCircle, Clock, Server,
  Key, ChevronRight,
  ArrowUpRight, ArrowDownRight, BarChart2,
  HardDrive, Wifi, Loader2,
  TrendingUp, TrendingDown, ShoppingBag, Search,
  ShieldCheck, ShieldAlert,
} from "lucide-react";

// ── Server heartbeat card ─────────────────────────────────────────────────────
const PING_INTERVAL = 10000;
const NUM_TICKS     = 80;

// Colour for a single ping result
function tickColor(entry) {
  if (!entry)              return "#1e293b";          // empty slot
  if (entry.q === "red")   return "#f87171";
  if (entry.q === "yellow")return "#fbbf24";
  return "#22c55e";
}

function ServerHeartbeatCard() {
  const [history,  setHistory]  = useState([]);        // [{ms, q}] oldest → newest
  const [latency,  setLatency]  = useState(null);
  const [quality,  setQuality]  = useState("connecting");
  const [nextIn,   setNextIn]   = useState(PING_INTERVAL / 1000);
  const lastPingRef = useRef(null);

  // Countdown timer
  useEffect(() => {
    const id = setInterval(() => {
      if (lastPingRef.current == null) return;
      const secs = Math.max(0, Math.ceil((PING_INTERVAL - (Date.now() - lastPingRef.current)) / 1000));
      setNextIn(secs);
    }, 500);
    return () => clearInterval(id);
  }, []);

  const doPing = useCallback(async () => {
    const t0 = Date.now();
    try {
      await api.get("/api/health", { timeout: 3000 });
      const ms = Date.now() - t0;
      const q  = ms < 50 ? "green" : ms < 600 ? "yellow" : "red";
      setLatency(ms);
      setQuality(q);
      setHistory(h => [...h.slice(-(NUM_TICKS - 1)), { ms, q }]);
    } catch {
      setLatency(null);
      setQuality("offline");
      setHistory(h => [...h.slice(-(NUM_TICKS - 1)), { ms: null, q: "offline" }]);
    }
    lastPingRef.current = Date.now();
    setNextIn(PING_INTERVAL / 1000);
  }, []);

  useEffect(() => {
    doPing();
    const id = setInterval(doPing, PING_INTERVAL);
    return () => clearInterval(id);
  }, [doPing]);

  const labelCls = {
    green: "text-emerald-400", yellow: "text-yellow-400",
    red: "text-red-400", offline: "text-red-400", connecting: "text-slate-400",
  }[quality] ?? "text-slate-400";

  const statusLabel = {
    green: "Healthy", yellow: "Degraded", red: "Poor",
    offline: "Offline", connecting: "Connecting…",
  }[quality] ?? "—";

  // Build display array: history slots on left, empty slots on right
  const slots = Array.from({ length: NUM_TICKS }, (_, i) => {
    const offset = NUM_TICKS - history.length;
    return i >= offset ? history[i - offset] : null;
  });

  return (
    <div className="bg-slate-800/60 border border-slate-700/60 rounded-2xl overflow-hidden">
      {/* Tick histogram */}
      <div className="flex items-stretch gap-px px-3 pt-3 pb-2" style={{ height: 52 }}>
        {/* Latest ms label */}
        <span className={`text-xs font-bold font-mono tabular-nums self-center pr-3 shrink-0 w-16 ${labelCls}`}>
          {quality === "connecting" ? "…"
           : latency != null        ? `${latency}ms`
           : "offline"}
        </span>

        {/* One tick per slot */}
        {slots.map((entry, i) => (
          <div
            key={i}
            className="flex-1 rounded-sm"
            style={{ backgroundColor: tickColor(entry) }}
            title={entry ? `${entry.ms ?? "—"}ms` : ""}
          />
        ))}
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between px-4 pb-3">
        <div className="flex items-center gap-2">
          <span className="block w-1.5 h-1.5 rounded-full" style={{ backgroundColor: tickColor(history[history.length - 1]) }} />
          <span className="text-xs font-medium text-slate-400">Your Connection to TradeFinder</span>
          <span className={`text-xs font-medium ${labelCls}`}>{statusLabel}</span>
        </div>
        <span className="text-[11px] text-slate-500 font-mono tabular-nums">
          next echo in {nextIn}s
        </span>
      </div>
    </div>
  );
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

// Sparkline using SVG path
function Sparkline({ data, color = "#6366f1", height = 32 }) {
  if (!data?.length) return null;
  const w = 80, h = height;
  const min = Math.min(...data), max = Math.max(...data);
  const range = max - min || 1;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - ((v - min) / range) * h;
    return `${x},${y}`;
  }).join(" ");
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="overflow-visible">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
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
      <div className="flex items-center gap-2.5">
        {Icon && <Icon className="w-4 h-4 text-slate-400" />}
        <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
      </div>
      {action}
    </div>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({ icon: Icon, label, value, change, changeDir, sparkData, accent }) {
  const accentMap = {
    blue:   { bg: "bg-blue-500/10",   text: "text-blue-400",   border: "border-blue-500/30",  spark: "#60a5fa" },
    purple: { bg: "bg-purple-500/10", text: "text-purple-400", border: "border-purple-500/30",spark: "#a78bfa" },
    green:  { bg: "bg-emerald-500/10",text: "text-emerald-400",border: "border-emerald-500/30",spark: "#34d399" },
    orange: { bg: "bg-orange-500/10", text: "text-orange-400", border: "border-orange-500/30",spark: "#fb923c" },
  };
  const a = accentMap[accent] ?? accentMap.blue;
  return (
    <Card className="p-5 flex flex-col gap-3">
      <div className="flex items-start justify-between">
        <div className={`w-9 h-9 rounded-xl flex items-center justify-center border ${a.bg} ${a.border}`}>
          <Icon className={`w-4 h-4 ${a.text}`} />
        </div>
        {sparkData && <Sparkline data={sparkData} color={a.spark} />}
      </div>
      <div>
        <p className="text-base font-bold text-slate-100 tabular-nums">{value}</p>
        <p className="text-xs text-slate-400 mt-0.5">{label}</p>
      </div>
      {change && (
        <div className={`flex items-center gap-1 text-xs font-medium ${changeDir === "up" ? "text-emerald-400" : "text-red-400"}`}>
          {changeDir === "up" ? <ArrowUpRight className="w-3.5 h-3.5" /> : <ArrowDownRight className="w-3.5 h-3.5" />}
          {change}
        </div>
      )}
    </Card>
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
  { label: "Polygon.io",       host: "api.polygon.io",          icon: BarChart2 },
  { label: "Alpaca (paper)",   host: "paper-api.alpaca.markets",icon: Zap },
  { label: "Alpaca (live)",    host: "api.alpaca.markets",      icon: Zap },
  { label: "Yahoo Finance",    host: "query1.finance.yahoo.com",icon: Globe },
  { label: "Internet",         host: "8.8.8.8",                 icon: Wifi },
];

function LatencyBar({ ms }) {
  if (ms == null) return null;
  // Green < 80ms, yellow < 250ms, red >= 250ms
  const color = ms < 80 ? "bg-emerald-500" : ms < 250 ? "bg-yellow-500" : "bg-red-500";
  const width = Math.min(100, (ms / 400) * 100);
  return (
    <div className="flex items-center gap-2 flex-1">
      <div className="flex-1 h-1 bg-slate-800 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${width}%` }} />
      </div>
      <span className={`text-xs tabular-nums font-medium w-14 text-right ${
        ms < 80 ? "text-emerald-400" : ms < 250 ? "text-yellow-400" : "text-red-400"
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
  // ── Login events (card — last 10) ────────────────────────────────────────
  const [loginEvents,        setLoginEvents]        = useState([]);
  const [loginEventsLoading, setLoginEventsLoading] = useState(true);

  useEffect(() => {
    authApi.loginEvents(3)
      .then(r => setLoginEvents(r.data.events ?? []))
      .catch(() => {})
      .finally(() => setLoginEventsLoading(false));
  }, []);

  // ── Resource status (polled every 30 s) ──────────────────────────────────
  const RESOURCE_POLL_MS = 30_000;
  const [resources,        setResources]        = useState(null);  // { flask, polygon, alpaca, yahoo }
  const [resourcesLoading, setResourcesLoading] = useState(true);
  const [resourcesLastAt,  setResourcesLastAt]  = useState(null);

  const fetchResources = useCallback(() => {
    resourcesApi.status()
      .then(r => { setResources(r.data.resources); setResourcesLastAt(Date.now()); })
      .catch(() => {})
      .finally(() => setResourcesLoading(false));
  }, []);

  useEffect(() => {
    fetchResources();
    const id = setInterval(fetchResources, RESOURCE_POLL_MS);
    return () => clearInterval(id);
  }, [fetchResources]);

  // ── Login history modal ───────────────────────────────────────────────────
  const LOGIN_HISTORY_PAGE_SIZE = 15;
  const [loginHistoryOpen,    setLoginHistoryOpen]    = useState(false);
  const [loginHistoryAll,     setLoginHistoryAll]     = useState([]);
  const [loginHistoryLoading, setLoginHistoryLoading] = useState(false);
  const [loginHistoryPage,    setLoginHistoryPage]    = useState(0);

  const openLoginHistory = useCallback(async () => {
    setLoginHistoryOpen(true);
    setLoginHistoryPage(0);
    if (loginHistoryAll.length) return; // already fetched
    setLoginHistoryLoading(true);
    try {
      const r = await authApi.loginEvents(100);
      setLoginHistoryAll(r.data.events ?? []);
    } catch { /* non-fatal */ }
    finally { setLoginHistoryLoading(false); }
  }, [loginHistoryAll.length]);

  // ── Order detail modal ────────────────────────────────────────────────────
  const [detailOrder,   setDetailOrder]   = useState(null);  // { dbOrder, alpacaData } | null
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError,   setDetailError]   = useState(null);

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
  const [reviewOrder,   setReviewOrder]   = useState(null);
  const [snapshotPrices, setSnapshotPrices] = useState({}); // ticker → { price, change_pct } from /api/snapshots/prices
  const ORDERS_PER_PAGE = 10;
  const OPEN_TRADES_SNAPSHOT_POLL_MS = 60_000;

  const openOrderTickers = useMemo(() => {
    const s = new Set(
      orders.filter(o => o.is_open).map(o => String(o.ticker || "").trim().toUpperCase()).filter(Boolean)
    );
    return [...s].sort();
  }, [orders]);

  const pollOpenTradeSnapshots = useCallback(() => {
    if (!openOrderTickers.length) {
      setSnapshotPrices({});
      return;
    }
    snapshotsApi
      .prices(openOrderTickers.join(","))
      .then((r) => setSnapshotPrices(r.data.prices || {}))
      .catch(() => {});
  }, [openOrderTickers]);

  useEffect(() => {
    pollOpenTradeSnapshots();
    const id = setInterval(pollOpenTradeSnapshots, OPEN_TRADES_SNAPSHOT_POLL_MS);
    return () => clearInterval(id);
  }, [pollOpenTradeSnapshots]);

  const syncOrders = useCallback((isBackground = false) => {
    if (!isBackground) setOrdersLoading(true);
    alpacaApi.syncOrders()
      .then(r => {
        setOrders(r.data.orders ?? []);
        setSyncedCount(r.data.synced ?? 0);
        setLastSyncedAt(Date.now());
        setOrdersError(null);
      })
      .catch(() => setOrdersError("Could not load or sync orders."))
      .finally(() => setOrdersLoading(false));
  }, []);

  useEffect(() => {
    syncOrders();
    const id = setInterval(() => syncOrders(true), 60_000);
    return () => clearInterval(id);
  }, [syncOrders]);

  const sparkWeek   = [42, 49, 38, 55, 62, 58, 71];
  const sparkCalls  = [120, 145, 130, 160, 175, 155, 190];
  const sparkUptime = [99.9, 100, 99.8, 100, 100, 99.9, 100];
  const sparkData   = [3.1, 3.4, 3.2, 3.8, 4.1, 3.9, 4.3];

  const { wins, losses, winRate, dollarWin, dollarLoss, avgHoldTime } = useMemo(() => {
    const closed = orders.filter(o => !o.is_open && o.unrealized_pl != null);
    const w = closed.filter(o => o.unrealized_pl > 0).length;
    const l = closed.filter(o => o.unrealized_pl < 0).length;
    const dw = closed.reduce((sum, o) => o.unrealized_pl > 0 ? sum + Number(o.unrealized_pl) : sum, 0);
    const dl = closed.reduce((sum, o) => o.unrealized_pl < 0 ? sum + Number(o.unrealized_pl) : sum, 0);

    const withDuration = orders.filter(o => o.created_at && o.synced_at);
    let avgHold = null;
    if (withDuration.length) {
      const totalMs = withDuration.reduce((sum, o) => {
        const ms = new Date(o.synced_at) - new Date(o.created_at + (o.created_at.endsWith("Z") ? "" : "Z"));
        return sum + (ms > 0 ? ms : 0);
      }, 0);
      const avgMs = totalMs / withDuration.length;
      const totalMins = Math.round(avgMs / 60000);
      if (totalMins < 60)        avgHold = `${totalMins}m`;
      else if (totalMins < 1440) avgHold = `${Math.floor(totalMins / 60)}h ${totalMins % 60}m`;
      else                       avgHold = `${Math.floor(totalMins / 1440)}d ${Math.floor((totalMins % 1440) / 60)}h`;
    }

    return {
      wins: w, losses: l,
      winRate: closed.length ? Math.round((w / closed.length) * 100) : null,
      dollarWin: dw,
      dollarLoss: dl,
      avgHoldTime: avgHold,
    };
  }, [orders]);

  return (
    <div className="h-full overflow-y-auto bg-slate-900">
      <div className="max-w-6xl mx-auto px-6 py-8 flex flex-col gap-6">

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

        {/* ── Row 1: server heartbeat ── */}
        <ServerHeartbeatCard />

        {/* ── Row 2: stat cards (4 equal) ── */}
        <div className="grid grid-cols-4 gap-4">
          <StatCard
            icon={TrendingUp}
            label="Total trades"
            value={ordersLoading ? "…" : orders.length.toLocaleString()}
            change={ordersLoading ? "" : `${orders.filter(o => o.is_open).length} open · ${orders.filter(o => !o.is_open).length} closed`}
            changeDir="up"
            sparkData={sparkWeek}
            accent="blue"
          />
          <StatCard
            icon={Activity}
            label="Wins / Losses"
            value={ordersLoading ? "…" : `${wins} / ${losses}`}
            change={ordersLoading ? "" : winRate != null ? `${winRate}% win rate` : "No closed trades"}
            changeDir={winRate != null && winRate >= 50 ? "up" : "down"}
            sparkData={sparkCalls}
            accent="purple"
          />
          <StatCard
            icon={Zap}
            label="$ Win / $ Loss"
            value={ordersLoading ? "…" : `$${dollarWin.toFixed(2)} / $${Math.abs(dollarLoss).toFixed(2)}`}
            change={ordersLoading ? "" : `Net $${(dollarWin + dollarLoss).toFixed(2)}`}
            changeDir={dollarWin + dollarLoss >= 0 ? "up" : "down"}
            sparkData={sparkUptime}
            accent="green"
          />
          <StatCard
            icon={Clock}
            label="Avg Hold Time"
            value={ordersLoading ? "…" : avgHoldTime ?? "—"}
            change={ordersLoading ? "" : `across ${orders.filter(o => o.synced_at).length} trades`}
            changeDir="up"
            sparkData={sparkData}
            accent="orange"
          />
        </div>

        {/* ── Analytics Dashboard ── */}
        <AnalyticsPanel orders={orders} loading={ordersLoading} />

        {/* ── My Orders ── */}
        <Card>
          <CardHeader
            icon={ShoppingBag}
            title="My Trades"
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
                {!ordersLoading && orders.length > 0 && (
                  <span className="whitespace-nowrap">
                    {orders.filter(o => o.is_open).length} open
                    {orders.filter(o => !o.is_open).length > 0 && (
                      <> · {orders.filter(o => !o.is_open).length} closed</>
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

          {ordersLoading ? (
            <div className="flex items-center justify-center gap-2 py-10 text-slate-400 text-sm">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading orders…
            </div>
          ) : ordersError ? (
            <div className="px-5 py-6 text-sm text-red-400">{ordersError}</div>
          ) : orders.length === 0 ? (
            <div className="px-5 py-10 text-center text-slate-500 text-sm">
              No orders placed yet. Open the chart and place a bracket order to get started.
            </div>
          ) : orders.filter(o => ordersFilter === "open" ? o.is_open : !o.is_open).length === 0 ? (
            <div className="px-5 py-10 text-center text-slate-500 text-sm">
              No {ordersFilter} trades.
            </div>
          ) : (
            <>
              {ordersFilter === "open" ? (
                <>
                  {/* ── Open trades headers ── */}
                  <div className="grid grid-cols-[1.5fr_0.7fr_0.7fr_0.4fr_1fr_1fr_1fr_0.6fr_1fr_0.8fr_1.1fr_0.9fr] gap-3 px-5 py-2 border-b border-slate-800/40">
                    {["Ticker", "Detail", "Chart", "Dir", "Fill / Entry", "Stop", "Target", "Qty", "Open P/L", "State", "Status", "Placed"].map(h => (
                      <span key={h} className={`text-[10px] font-semibold text-slate-500 uppercase tracking-wider${h === "Dir" ? " text-center" : ""}`}>{h}</span>
                    ))}
                  </div>

                  {/* ── Open trades rows ── */}
                  <div className="divide-y divide-slate-800/40">
                    {orders
                      .filter(o => o.is_open)
                      .slice(ordersPage * ORDERS_PER_PAGE, (ordersPage + 1) * ORDERS_PER_PAGE)
                      .map(o => {
                        const isLong    = o.direction === "long";
                        const isPaper   = o.paper_mode;
                        const placed    = o.created_at ? new Date(o.created_at).toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "—";
                        const statusColor = o.status === "filled" ? "text-emerald-400" : o.status === "accepted" || o.status === "new" || o.status === "held" ? "text-yellow-400" : "text-slate-400";
                        const pl        = o.unrealized_pl;
                        const plPos     = pl != null && pl > 0;
                        const plNeg     = pl != null && pl < 0;
                        const plColor   = plPos ? "text-emerald-400" : plNeg ? "text-red-400" : "text-slate-400";
                        const fillPrice = o.filled_avg_price ?? o.entry_price;
                        const tUpper = o.ticker ? String(o.ticker).trim().toUpperCase() : "";
                        const snap = tUpper ? snapshotPrices[tUpper] : null;
                        const livePx = snap?.price != null ? Number(snap.price) : null;
                        const targetPx = o.target_price != null ? Number(o.target_price) : null;
                        const beyondTakeProfit =
                          livePx != null &&
                          targetPx != null &&
                          (isLong ? livePx >= targetPx : livePx <= targetPx);
                        return (
                          <div
                            key={o.id}
                            className={`grid grid-cols-[1.5fr_0.7fr_0.7fr_0.4fr_1fr_1fr_1fr_0.6fr_1fr_0.8fr_1.1fr_0.9fr] gap-3 px-5 py-3 transition items-center ${
                              beyondTakeProfit ? "tp-row-flash" : "hover:bg-slate-800/30"
                            }`}
                          >
                            <div className="flex items-center gap-2 min-w-0">
                              <span className="font-bold text-slate-100 text-sm truncate">{o.ticker}</span>
                              {isPaper && <span className="text-[9px] font-semibold text-blue-400 bg-blue-900/30 border border-blue-700/40 rounded px-1 py-0.5 shrink-0">PAPER</span>}
                            </div>
                            <button onClick={() => openDetail(o)} disabled={!o.alpaca_order_id} title="View bracket details from Alpaca" className="text-slate-500 hover:text-brand-400 disabled:opacity-20 disabled:cursor-not-allowed transition"><Search className="w-3.5 h-3.5" /></button>
                            <button title="View trade chart" onClick={() => setReviewOrder(o)} className="text-slate-500 hover:text-emerald-400 transition"><BarChart2 className="w-3.5 h-3.5" /></button>
                            <div className="flex items-center justify-center gap-1">
                              {isLong ? <TrendingUp className="w-3.5 h-3.5 text-emerald-400" /> : <TrendingDown className="w-3.5 h-3.5 text-red-400" />}
                              <span className={`text-xs font-semibold ${isLong ? "text-emerald-400" : "text-red-400"}`}>{isLong ? "Long" : "Short"}</span>
                            </div>
                            <span className="font-mono text-xs text-slate-200">{fillPrice != null ? `$${Number(fillPrice).toFixed(2)}` : "—"}</span>
                            <span className="font-mono text-xs text-red-400">{o.stop_price != null ? `$${Number(o.stop_price).toFixed(2)}` : "—"}</span>
                            <span className="font-mono text-xs text-emerald-400">{o.target_price != null ? `$${Number(o.target_price).toFixed(2)}` : "—"}</span>
                            <span className="font-mono text-xs text-slate-300">{o.qty}</span>
                            <span className={`font-mono text-xs font-semibold ${plColor}`}>{pl != null ? `${plPos ? "+" : ""}$${Math.abs(pl).toFixed(2)}` : "—"}</span>
                            {o.is_open ? <span className="text-[10px] font-semibold text-emerald-400 bg-emerald-900/30 border border-emerald-700/40 rounded px-1 py-0.5 w-fit">Open</span> : <span className="text-[10px] font-semibold text-slate-400 bg-slate-800/60 border border-slate-700/40 rounded px-1 py-0.5 w-fit">Closed</span>}
                            <span className={`text-xs font-medium capitalize ${statusColor}`}>{o.status ?? "—"}</span>
                            <span className="text-[11px] text-slate-400">{placed}</span>
                          </div>
                        );
                      })}
                  </div>
                </>
              ) : (
                <>
                  {/* ── Closed trades headers ── */}
                  <div className="grid grid-cols-[1.4fr_0.5fr_0.5fr_0.4fr_0.9fr_1fr_0.5fr_0.8fr_0.7fr_1fr_0.8fr_0.9fr_1fr] gap-3 px-5 py-2 border-b border-slate-800/40">
                    {[
                      { h: "Ticker" }, { h: "Detail" }, { h: "Chart" }, { h: "Dir", center: true },
                      { h: "Entry Limit" }, { h: "Fill / Slippage" }, { h: "Qty" },
                      { h: "Risk $" }, { h: "R/R" }, { h: "Final P/L" }, { h: "R Result" },
                      { h: "Status" }, { h: "Date" },
                    ].map(({ h, center }) => (
                      <span key={h} className={`text-[10px] font-semibold text-slate-500 uppercase tracking-wider${center ? " text-center" : ""}`}>{h}</span>
                    ))}
                  </div>

                  {/* ── Closed trades rows ── */}
                  <div className="divide-y divide-slate-800/40">
                    {orders
                      .filter(o => !o.is_open)
                      .slice(ordersPage * ORDERS_PER_PAGE, (ordersPage + 1) * ORDERS_PER_PAGE)
                      .map(o => {
                        const isLong    = o.direction === "long";
                        const isPaper   = o.paper_mode;
                        const entryLim  = o.entry_price  != null ? Number(o.entry_price)      : null;
                        const fillPx    = o.filled_avg_price != null ? Number(o.filled_avg_price) : null;
                        const stopPx    = o.stop_price   != null ? Number(o.stop_price)       : null;
                        const riskAmt   = o.risk_amt     != null ? Number(o.risk_amt)         : (entryLim != null && stopPx != null ? Math.abs(entryLim - stopPx) * (o.qty ?? 1) : null);
                        const rrEff     = o.rr_ratio_effective ?? o.rr_ratio;
                        const pl        = o.unrealized_pl != null ? Number(o.unrealized_pl) : null;
                        const plPos     = pl != null && pl > 0;
                        const plNeg     = pl != null && pl < 0;
                        const plColor   = plPos ? "text-emerald-400" : plNeg ? "text-red-400" : "text-slate-400";

                        // Slippage: positive = paid more than limit (bad for long, good for short)
                        const slip      = entryLim != null && fillPx != null ? fillPx - entryLim : null;
                        const slipBad   = slip != null && (isLong ? slip > 0.005 : slip < -0.005);
                        const slipGood  = slip != null && (isLong ? slip < -0.005 : slip > 0.005);
                        const slipColor = slipBad ? "text-red-400" : slipGood ? "text-emerald-400" : "text-slate-500";

                        // R result: actual P/L divided by planned risk per share × qty
                        const rResult   = pl != null && riskAmt != null && riskAmt > 0 ? (pl / riskAmt) : null;
                        const rColor    = rResult == null ? "text-slate-500" : rResult > 0 ? "text-emerald-400" : "text-red-400";

                        const statusColor = o.status === "filled" ? "text-emerald-400" : o.status === "canceled" || o.status === "expired" ? "text-slate-400" : "text-yellow-400";
                        const date = o.synced_at ?? o.created_at;
                        const dateStr = date ? new Date(date).toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "—";

                        return (
                          <div key={o.id} className="grid grid-cols-[1.4fr_0.5fr_0.5fr_0.4fr_0.9fr_1fr_0.5fr_0.8fr_0.7fr_1fr_0.8fr_0.9fr_1fr] gap-3 px-5 py-3 hover:bg-slate-800/30 transition items-center">
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
                            <span className="font-mono text-xs text-slate-300">{entryLim != null ? `$${entryLim.toFixed(2)}` : "—"}</span>

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
              {orders.filter(o => ordersFilter === "open" ? o.is_open : !o.is_open).length > ORDERS_PER_PAGE && (() => {
                const filteredCount = orders.filter(o => ordersFilter === "open" ? o.is_open : !o.is_open).length;
                const totalPages = Math.ceil(filteredCount / ORDERS_PER_PAGE);
                return (
                  <div className="flex items-center justify-between px-5 py-3 border-t border-slate-800/40">
                    <span className="text-[11px] text-slate-400">
                      {ordersPage * ORDERS_PER_PAGE + 1}–{Math.min((ordersPage + 1) * ORDERS_PER_PAGE, filteredCount)} of {filteredCount}
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

        {/* ── Login History + Security (50/50) ── */}
        <div className="grid grid-cols-2 gap-4">
          {/* Login history */}
          <Card>
            <CardHeader
              icon={Clock}
              title="Login History"
              action={
                <button
                  onClick={openLoginHistory}
                  className="text-xs text-brand-400 hover:text-brand-300 flex items-center gap-1 transition"
                >
                  Show all <ChevronRight className="w-3.5 h-3.5" />
                </button>
              }
            />
            {loginEventsLoading ? (
              <div className="flex items-center justify-center py-10">
                <Loader2 className="w-5 h-5 animate-spin text-slate-500" />
              </div>
            ) : loginEvents.length === 0 ? (
              <p className="px-5 py-6 text-xs text-slate-500 text-center">No login history yet.</p>
            ) : (
              <div className="flex flex-col divide-y divide-slate-800/60">
                {loginEvents.map((ev, i) => {
                  const dt    = new Date(ev.logged_in_at + (ev.logged_in_at.endsWith("Z") ? "" : "Z"));
                  const date  = dt.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
                  const time  = dt.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
                  const isLatest = i === 0;
                  const netPl = ev.net_pl ?? 0;
                  return (
                    <div key={ev.id} className={`px-5 py-3 hover:bg-slate-800/30 transition ${isLatest ? "bg-slate-800/20" : ""}`}>
                      {/* Row 1: date/time + platform + "current" badge */}
                      <div className="flex items-center justify-between mb-1.5">
                        <div className="flex items-center gap-2">
                          <span className="text-xs font-medium text-slate-200">{date}</span>
                          <span className="text-[11px] text-slate-400">{time}</span>
                          {isLatest && (
                            <span className="text-[9px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                              Current
                            </span>
                          )}
                        </div>
                        <span className="text-[11px] text-slate-400 truncate max-w-[130px]">{ev.platform ?? "Unknown"}</span>
                      </div>
                      {/* Row 2: account snapshot pills */}
                      <div className="flex items-center gap-3 flex-wrap">
                        <span className="text-[10px] text-slate-400">
                          Open <span className="font-semibold text-slate-300">{ev.open_trades ?? 0}</span>
                        </span>
                        <span className="text-[10px] text-slate-400">
                          Total <span className="font-semibold text-slate-300">{ev.total_trades ?? 0}</span>
                        </span>
                        <span className="text-[10px] text-slate-400">
                          W/L <span className="font-semibold text-emerald-400">{ev.win_count ?? 0}</span>
                          <span className="text-slate-500"> / </span>
                          <span className="font-semibold text-red-400">{ev.loss_count ?? 0}</span>
                        </span>
                        <span className={`text-[10px] font-semibold font-mono ml-auto ${netPl >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                          {netPl >= 0 ? "+" : ""}${netPl.toFixed(2)}
                        </span>
                      </div>
                      {/* Row 3: IP address */}
                      {ev.ip_address && (
                        <p className="text-[10px] text-slate-500 mt-1 font-mono">{ev.ip_address}</p>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </Card>

          {/* Resources */}
          <Card>
            <CardHeader
              icon={Globe}
              title="Resources"
              action={
                <div className="flex items-center gap-2">
                  {resourcesLastAt && (
                    <span className="text-[10px] text-slate-500 tabular-nums">
                      {new Date(resourcesLastAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                    </span>
                  )}
                  <button
                    onClick={fetchResources}
                    title="Refresh now"
                    className="text-slate-500 hover:text-slate-300 transition"
                  >
                    <RefreshCw className="w-3.5 h-3.5" />
                  </button>
                </div>
              }
            />
            <div className="flex flex-col divide-y divide-slate-800/60">
              {resourcesLoading && !resources ? (
                <div className="flex items-center justify-center gap-2 py-8 text-slate-500 text-xs">
                  <Loader2 className="w-3.5 h-3.5 animate-spin" /> Probing…
                </div>
              ) : (
                [
                  { key: "flask",   label: "Tradefinder Data Center", icon: Server  },
                  { key: "polygon", label: "Polygon.io",           icon: BarChart2 },
                  { key: "alpaca",  label: "Alpaca Markets",        icon: TrendingUp },
                  { key: "yahoo",   label: "Yahoo Finance",         icon: Globe   },
                ].map(({ key, label, icon: Icon }) => {
                  const r      = resources?.[key];
                  const ok     = r?.ok;
                  const status = ok === undefined ? "unknown" : ok ? "online" : "offline";
                  const dotColor = status === "online"  ? "bg-emerald-400"
                                 : status === "offline" ? "bg-red-400"
                                 : "bg-slate-600";
                  const ringColor = status === "online"  ? "ring-emerald-400/30"
                                  : status === "offline" ? "ring-red-400/30"
                                  : "ring-slate-600/30";
                  return (
                    <div key={key} className="flex items-center gap-3 px-5 py-3.5 hover:bg-slate-800/30 transition">
                      {/* Animated dot */}
                      <span className="relative flex h-2.5 w-2.5 shrink-0">
                        {status === "online" && (
                          <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${dotColor} opacity-60`} />
                        )}
                        <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${dotColor} ring-2 ${ringColor}`} />
                      </span>
                      <Icon className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm text-slate-300 font-medium">{label}</p>
                        <p className="text-[11px] text-slate-500 truncate">{r?.detail ?? "—"}</p>
                      </div>
                      {r?.latency_ms != null && (
                        <span className={`text-[11px] font-mono tabular-nums shrink-0 ${
                          r.latency_ms < 300  ? "text-emerald-500"
                          : r.latency_ms < 800 ? "text-yellow-500"
                          : "text-red-500"
                        }`}>
                          {r.latency_ms}ms
                        </span>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          </Card>
        </div>

      </div>

      {/* ── Order detail modal ── */}
      {detailOrder && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-[2px] p-4">
          <div className="bg-slate-800 border border-slate-700 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col max-h-[90vh]">

            {/* Header */}
            <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700 shrink-0">
              <div>
                <p className="text-white font-bold text-sm">
                  Bracket Details — {detailOrder.dbOrder.ticker}
                </p>
                <p className="text-slate-400 text-[11px] mt-0.5 font-mono truncate">
                  {detailOrder.dbOrder.alpaca_order_id}
                </p>
              </div>
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

                const checks = [
                  { label: "Symbol",      db: db.ticker,                                              alpaca: a.symbol ?? "—",                                                  match: db.ticker === a.symbol },
                  { label: "Qty",         db: String(db.qty),                                         alpaca: a.filled_qty != null ? String(parseInt(a.filled_qty, 10)) : "—", match: a.filled_qty != null ? db.qty === parseInt(a.filled_qty, 10) : null },
                  { label: "Fill Price",  db: db.filled_avg_price != null ? `$${Number(db.filled_avg_price).toFixed(2)}` : "—", alpaca: a.filled_avg_price != null ? `$${Number(a.filled_avg_price).toFixed(2)}` : "—", match: priceMatch(db.filled_avg_price, a.filled_avg_price) },
                  { label: "Status",      db: db.status ?? "—",                                       alpaca: a.status ?? "—",                                                  match: (db.status ?? "") === (a.status ?? "") },
                  { label: "Entry Limit", db: db.entry_price != null ? `$${Number(db.entry_price).toFixed(2)}` : "—",       alpaca: a.limit_price != null ? `$${Number(a.limit_price).toFixed(2)}` : "—",     match: priceMatch(db.entry_price, a.limit_price) },
                  { label: "Stop",        db: db.stop_price != null ? `$${Number(db.stop_price).toFixed(2)}` : "—",         alpaca: stopLeg?.stop_price != null ? `$${Number(stopLeg.stop_price).toFixed(2)}` : "—",   match: priceMatch(db.stop_price, stopLeg?.stop_price) },
                  { label: "Target",      db: db.target_price != null ? `$${Number(db.target_price).toFixed(2)}` : "—",     alpaca: profitLeg?.limit_price != null ? `$${Number(profitLeg.limit_price).toFixed(2)}` : "—", match: priceMatch(db.target_price, profitLeg?.limit_price) },
                ];

                const mismatches = checks.filter(c => c.match === false);
                const unknowns   = checks.filter(c => c.match === null);
                const allGood    = mismatches.length === 0;

                return (
                  <div className={`rounded-lg border px-3 py-2.5 ${allGood ? "border-emerald-800/50 bg-emerald-900/10" : "border-amber-700/50 bg-amber-900/10"}`}>
                    <div className="flex items-center gap-1.5 mb-2">
                      {allGood
                        ? <ShieldCheck className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
                        : <ShieldAlert  className="w-3.5 h-3.5 text-amber-400  shrink-0" />}
                      <span className={`text-[11px] font-semibold ${allGood ? "text-emerald-400" : "text-amber-400"}`}>
                        {allGood ? "DB matches Alpaca" : `${mismatches.length} mismatch${mismatches.length !== 1 ? "es" : ""} detected`}
                      </span>
                      {unknowns.length > 0 && (
                        <span className="text-[10px] text-slate-500 italic ml-1">({unknowns.length} unavailable)</span>
                      )}
                    </div>
                    <div className="flex flex-wrap gap-x-3 gap-y-1.5">
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
                  </div>
                );
              })()}

              {detailOrder.alpacaData && (() => {
                const d    = detailOrder.alpacaData;
                const legs = d.legs ?? [];

                const fmt     = (v) => v != null ? `$${parseFloat(v).toFixed(2)}` : "—";
                const fmtDate = (v) => v ? new Date(v).toLocaleString() : "—";

                const OrderBlock = ({ label, data: o, accent }) => (
                  <div className={`rounded-lg border ${accent} bg-slate-900/50 overflow-hidden`}>
                    <div className={`px-3 py-2 border-b ${accent} flex items-center justify-between`}>
                      <span className="font-semibold text-slate-300">{label}</span>
                      <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded ${
                        o.status === "filled"   ? "bg-emerald-900/40 text-emerald-400" :
                        o.status === "canceled" || o.status === "expired" ? "bg-slate-700 text-slate-400" :
                        o.status === "held"     ? "bg-yellow-900/40 text-yellow-400" :
                        "bg-slate-700/60 text-slate-400"
                      }`}>{o.status}</span>
                    </div>
                    <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 px-3 py-3">
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
                      <div className="col-span-2 mt-1 border-t border-slate-700/50 pt-1.5">
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

      {/* ── Login History Modal ── */}
      {loginHistoryOpen && (() => {
        const totalPages = Math.ceil(loginHistoryAll.length / LOGIN_HISTORY_PAGE_SIZE);
        const pageItems  = loginHistoryAll.slice(
          loginHistoryPage * LOGIN_HISTORY_PAGE_SIZE,
          (loginHistoryPage + 1) * LOGIN_HISTORY_PAGE_SIZE,
        );
        return (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-[2px] p-4">
            <div className="bg-slate-800 border border-slate-700 rounded-xl shadow-2xl w-full max-w-2xl flex flex-col max-h-[85vh]">

              {/* Header */}
              <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700 shrink-0">
                <div className="flex items-center gap-2.5">
                  <Clock className="w-4 h-4 text-slate-400" />
                  <h3 className="text-sm font-semibold text-slate-200">Login History</h3>
                  {!loginHistoryLoading && (
                    <span className="text-xs text-slate-400">{loginHistoryAll.length} event{loginHistoryAll.length !== 1 ? "s" : ""}</span>
                  )}
                </div>
                <button
                  onClick={() => setLoginHistoryOpen(false)}
                  className="text-slate-400 hover:text-slate-300 transition"
                >
                  <XCircle className="w-5 h-5" />
                </button>
              </div>

              {/* Column headers */}
              {!loginHistoryLoading && loginHistoryAll.length > 0 && (
                <div className="grid grid-cols-[1fr_1fr_auto_auto_auto_auto] gap-x-4 px-5 py-2 border-b border-slate-800/60 shrink-0">
                  {["Date / Time", "Platform", "Open", "W / L", "Net P&L", "IP"].map(h => (
                    <span key={h} className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{h}</span>
                  ))}
                </div>
              )}

              {/* Body */}
              <div className="overflow-y-auto flex-1">
                {loginHistoryLoading ? (
                  <div className="flex items-center justify-center gap-2 py-12 text-slate-400 text-sm">
                    <Loader2 className="w-4 h-4 animate-spin" /> Loading…
                  </div>
                ) : loginHistoryAll.length === 0 ? (
                  <p className="text-xs text-slate-500 text-center py-10">No login history found.</p>
                ) : (
                  pageItems.map((ev, i) => {
                    const globalIdx = loginHistoryPage * LOGIN_HISTORY_PAGE_SIZE + i;
                    const dt     = new Date(ev.logged_in_at + (ev.logged_in_at.endsWith("Z") ? "" : "Z"));
                    const date   = dt.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
                    const time   = dt.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
                    const netPl  = ev.net_pl ?? 0;
                    const isLatest = globalIdx === 0;
                    return (
                      <div
                        key={ev.id}
                        className={`grid grid-cols-[1fr_1fr_auto_auto_auto_auto] gap-x-4 items-center px-5 py-3 border-b border-slate-800/40 last:border-0 hover:bg-slate-800/30 transition ${isLatest ? "bg-slate-800/20" : ""}`}
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="text-xs text-slate-200 whitespace-nowrap">{date}</span>
                          <span className="text-[11px] text-slate-400 whitespace-nowrap">{time}</span>
                          {isLatest && (
                            <span className="text-[9px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 whitespace-nowrap">
                              Current
                            </span>
                          )}
                        </div>
                        <span className="text-[11px] text-slate-400 truncate">{ev.platform ?? "Unknown"}</span>
                        <span className="text-xs text-center font-semibold text-slate-300 tabular-nums">{ev.open_trades ?? 0}</span>
                        <span className="text-xs text-center tabular-nums whitespace-nowrap">
                          <span className="text-emerald-400 font-semibold">{ev.win_count ?? 0}</span>
                          <span className="text-slate-500"> / </span>
                          <span className="text-red-400 font-semibold">{ev.loss_count ?? 0}</span>
                        </span>
                        <span className={`text-xs font-semibold font-mono tabular-nums whitespace-nowrap ${netPl >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                          {netPl >= 0 ? "+" : ""}${netPl.toFixed(2)}
                        </span>
                        <span className="text-[10px] text-slate-500 font-mono whitespace-nowrap">{ev.ip_address ?? "—"}</span>
                      </div>
                    );
                  })
                )}
              </div>

              {/* Pagination footer */}
              {!loginHistoryLoading && totalPages > 1 && (
                <div className="flex items-center justify-between px-5 py-3 border-t border-slate-700/60 shrink-0">
                  <span className="text-xs text-slate-400">
                    Page {loginHistoryPage + 1} of {totalPages}
                    <span className="ml-2 text-slate-500">
                      ({loginHistoryPage * LOGIN_HISTORY_PAGE_SIZE + 1}–{Math.min((loginHistoryPage + 1) * LOGIN_HISTORY_PAGE_SIZE, loginHistoryAll.length)} of {loginHistoryAll.length})
                    </span>
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setLoginHistoryPage(0)}
                      disabled={loginHistoryPage === 0}
                      className="px-2 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >«</button>
                    <button
                      onClick={() => setLoginHistoryPage(p => p - 1)}
                      disabled={loginHistoryPage === 0}
                      className="px-2.5 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >‹ Prev</button>
                    {Array.from({ length: totalPages }, (_, idx) => (
                      <button
                        key={idx}
                        onClick={() => setLoginHistoryPage(idx)}
                        className={`w-7 h-7 rounded text-xs font-medium transition ${loginHistoryPage === idx ? "bg-brand-500/20 text-brand-400 border border-brand-500/40" : "text-slate-400 hover:text-slate-300 hover:bg-slate-700"}`}
                      >{idx + 1}</button>
                    ))}
                    <button
                      onClick={() => setLoginHistoryPage(p => p + 1)}
                      disabled={loginHistoryPage >= totalPages - 1}
                      className="px-2.5 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >Next ›</button>
                    <button
                      onClick={() => setLoginHistoryPage(totalPages - 1)}
                      disabled={loginHistoryPage >= totalPages - 1}
                      className="px-2 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >»</button>
                  </div>
                </div>
              )}
            </div>
          </div>
        );
      })()}
    </div>
  );
}
