import { useState, useEffect, useCallback, useRef } from "react";
import api, { authApi, resourcesApi } from "../api/client";
import {
  Globe, RefreshCw, Clock, ChevronRight, ChevronDown,
  Loader2, Server, BarChart2, TrendingUp, X, XCircle,
} from "lucide-react";

const PING_INTERVAL_MS   = 10_000;
const HISTORY_MINUTES    = 60;
const LATENCY_GOOD_MS    = 70;
const LATENCY_WARN_MS    = 200;
const RESOURCE_POLL_MS   = 30_000;
const LOGIN_HISTORY_PAGE_SIZE = 15;

const QUALITY_RANK = { green: 1, yellow: 2, red: 3, offline: 4 };

function msToQuality(ms) {
  if (ms == null) return "offline";
  if (ms < LATENCY_GOOD_MS) return "green";
  if (ms < LATENCY_WARN_MS) return "yellow";
  return "red";
}

function tickColor(entry) {
  if (!entry)               return "#1e293b";
  if (entry.q === "red")    return "#f87171";
  if (entry.q === "yellow") return "#fbbf24";
  if (entry.q === "offline") return "#475569";
  return "#22c55e";
}

function worstQuality(qualities) {
  return qualities.reduce(
    (worst, q) => (QUALITY_RANK[q] > QUALITY_RANK[worst] ? q : worst),
    "green",
  );
}

/** Roll raw pings into one bucket per minute for the last 60 minutes. */
function buildMinuteBuckets(pings) {
  const currentMinute = Math.floor(Date.now() / 60_000);
  return Array.from({ length: HISTORY_MINUTES }, (_, i) => {
    const minuteKey = currentMinute - (HISTORY_MINUTES - 1 - i);
    const inBucket = pings.filter((p) => p.minute === minuteKey);
    if (!inBucket.length) return null;
    const qualities = inBucket.map((p) => p.q);
    const msValues = inBucket.map((p) => p.ms).filter((ms) => ms != null);
    const avgMs = msValues.length
      ? Math.round(msValues.reduce((s, ms) => s + ms, 0) / msValues.length)
      : null;
    return {
      q: worstQuality(qualities),
      ms: avgMs,
      count: inBucket.length,
      minuteKey,
    };
  });
}

function ConnectionCard() {
  const [pings, setPings]       = useState([]);
  const [latency, setLatency]   = useState(null);
  const [quality, setQuality]   = useState("connecting");
  const [nextIn, setNextIn]     = useState(PING_INTERVAL_MS / 1000);
  const [, setMinuteTick]       = useState(0);
  const lastPingRef = useRef(null);

  // Re-render timeline when the clock minute rolls over
  useEffect(() => {
    const id = setInterval(() => setMinuteTick((t) => t + 1), 30_000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const id = setInterval(() => {
      if (lastPingRef.current == null) return;
      const secs = Math.max(
        0,
        Math.ceil((PING_INTERVAL_MS - (Date.now() - lastPingRef.current)) / 1000),
      );
      setNextIn(secs);
    }, 500);
    return () => clearInterval(id);
  }, []);

  const doPing = useCallback(async () => {
    const t0 = Date.now();
    const minute = Math.floor(Date.now() / 60_000);
    const cutoffMinute = minute - HISTORY_MINUTES + 1;
    const append = (entry) => {
      setPings((prev) => [...prev.filter((p) => p.minute >= cutoffMinute), entry]);
    };
    try {
      await api.get("/api/health", { timeout: 3000 });
      const ms = Date.now() - t0;
      const q = msToQuality(ms);
      setLatency(ms);
      setQuality(q);
      append({ ms, q, t: Date.now(), minute });
    } catch {
      setLatency(null);
      setQuality("offline");
      append({ ms: null, q: "offline", t: Date.now(), minute });
    }
    lastPingRef.current = Date.now();
    setNextIn(PING_INTERVAL_MS / 1000);
  }, []);

  useEffect(() => {
    doPing();
    const id = setInterval(doPing, PING_INTERVAL_MS);
    return () => clearInterval(id);
  }, [doPing]);

  const buckets = buildMinuteBuckets(pings);
  const bucketsWithData = buckets.filter(Boolean);
  const uptimePct = bucketsWithData.length
    ? Math.round(
        (bucketsWithData.filter((b) => b.q !== "offline").length / bucketsWithData.length) * 100,
      )
    : null;

  const labelCls = {
    green: "text-emerald-400",
    yellow: "text-yellow-400",
    red: "text-red-400",
    offline: "text-red-400",
    connecting: "text-slate-400",
  }[quality] ?? "text-slate-400";

  const statusLabel = {
    green: "Healthy",
    yellow: "Degraded",
    red: "Poor",
    offline: "Offline",
    connecting: "Connecting…",
  }[quality] ?? "—";

  return (
    <div className="bg-slate-800/60 border border-slate-700/60 rounded-2xl overflow-hidden">
      {/* Header — compact for narrow flyout */}
      <div className="flex items-start justify-between gap-2 px-3 pt-3 pb-2 border-b border-slate-700/40">
        <div className="min-w-0">
          <p className="text-xs font-semibold text-slate-200">Connection</p>
          <p className="text-[10px] text-slate-500 mt-0.5">Last 60 minutes</p>
        </div>
        <div className="text-right shrink-0">
          <p className={`text-sm font-bold font-mono tabular-nums leading-none ${labelCls}`}>
            {quality === "connecting" ? "…" : latency != null ? `${latency}ms` : "—"}
          </p>
          <p className={`text-[10px] font-medium mt-1 ${labelCls}`}>{statusLabel}</p>
        </div>
      </div>

      {/* 60-minute timeline — one bar per minute */}
      <div className="px-3 py-2.5">
        <div className="flex items-end gap-px h-9" role="img" aria-label="Connection quality over the last 60 minutes">
          {buckets.map((bucket, i) => (
            <div
              key={i}
              className="flex-1 min-w-0 rounded-[1px]"
              style={{
                height: bucket ? "100%" : "35%",
                backgroundColor: tickColor(bucket),
                opacity: bucket ? 1 : 0.35,
              }}
              title={
                bucket
                  ? `${new Date(bucket.minuteKey * 60_000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} · ${
                      bucket.ms != null ? `${bucket.ms}ms avg` : "offline"
                    } · ${bucket.count} echo${bucket.count !== 1 ? "s" : ""}`
                  : "No data this minute"
              }
            />
          ))}
        </div>
        <div className="flex items-center justify-between mt-1.5 text-[9px] text-slate-600 font-mono tabular-nums">
          <span>−60m</span>
          <span>now</span>
        </div>
      </div>

      {/* Footer stats */}
      <div className="flex items-center justify-between gap-2 px-3 pb-3 text-[10px] text-slate-500">
        <span className="truncate">
          {uptimePct != null ? (
            <span className={uptimePct >= 95 ? "text-emerald-500/90" : uptimePct >= 80 ? "text-yellow-500/90" : "text-red-400/90"}>
              {uptimePct}% uptime
            </span>
          ) : (
            "Collecting…"
          )}
        </span>
        <span className="shrink-0 font-mono tabular-nums">echo in {nextIn}s</span>
      </div>
    </div>
  );
}

function formatPlatform(platform) {
  if (!platform || platform === "Electron app") return "TradeFinder";
  return platform;
}

function Card({ children, className = "" }) {
  return (
    <div className={`bg-slate-800/60 border border-slate-700/60 rounded-2xl ${className}`}>
      {children}
    </div>
  );
}

function AccordionCard({ icon: Icon, title, action, defaultOpen = false, children }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Card>
      <div className={`flex items-center justify-between px-4 py-3 ${open ? "border-b border-slate-700/50" : ""}`}>
        <button
          type="button"
          onClick={() => setOpen(v => !v)}
          className="flex items-center gap-2 flex-1 min-w-0 text-left hover:opacity-80 transition"
        >
          {Icon && <Icon className="w-4 h-4 text-slate-400 shrink-0" />}
          <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
          <ChevronDown className={`w-4 h-4 text-slate-500 shrink-0 transition-transform ${open ? "rotate-180" : ""}`} />
        </button>
        {action && (
          <div className="shrink-0 ml-2" onClick={e => e.stopPropagation()}>
            {action}
          </div>
        )}
      </div>
      {open && children}
    </Card>
  );
}

export default function SystemPanel({ onClose }) {
  const [loginEvents,        setLoginEvents]        = useState([]);
  const [loginEventsLoading, setLoginEventsLoading] = useState(true);

  const [resources,        setResources]        = useState(null);
  const [resourcesLoading, setResourcesLoading] = useState(true);
  const [resourcesLastAt,  setResourcesLastAt]  = useState(null);

  const [loginHistoryOpen,    setLoginHistoryOpen]    = useState(false);
  const [loginHistoryAll,     setLoginHistoryAll]     = useState([]);
  const [loginHistoryLoading, setLoginHistoryLoading] = useState(false);
  const [loginHistoryPage,    setLoginHistoryPage]    = useState(0);

  useEffect(() => {
    authApi.loginEvents(3)
      .then(r => setLoginEvents(r.data.events ?? []))
      .catch(() => {})
      .finally(() => setLoginEventsLoading(false));
  }, []);

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

  const openLoginHistory = useCallback(async () => {
    setLoginHistoryOpen(true);
    setLoginHistoryPage(0);
    if (loginHistoryAll.length) return;
    setLoginHistoryLoading(true);
    try {
      const r = await authApi.loginEvents(100);
      setLoginHistoryAll(r.data.events ?? []);
    } catch { /* non-fatal */ }
    finally { setLoginHistoryLoading(false); }
  }, [loginHistoryAll.length]);

  return (
    <>
      <div className="flex flex-col h-full">
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700 shrink-0">
          <h2 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <Server className="w-4 h-4 text-brand-400" />
            System
          </h2>
          {onClose && (
            <button type="button" onClick={onClose} className="text-slate-500 hover:text-slate-300 transition">
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        <div className="flex-1 overflow-y-auto p-4 flex flex-col gap-4">
          <ConnectionCard />

          <AccordionCard
            icon={Clock}
            title="Login History"
            action={
              <button
                type="button"
                onClick={openLoginHistory}
                className="text-xs text-brand-400 hover:text-brand-300 flex items-center gap-1 transition"
              >
                Show all <ChevronRight className="w-3.5 h-3.5" />
              </button>
            }
          >
            {loginEventsLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="w-5 h-5 animate-spin text-slate-500" />
              </div>
            ) : loginEvents.length === 0 ? (
              <p className="px-4 py-6 text-xs text-slate-500 text-center">No login history yet.</p>
            ) : (
              <div className="flex flex-col divide-y divide-slate-800/60">
                {loginEvents.map((ev, i) => {
                  const dt    = new Date(ev.logged_in_at + (ev.logged_in_at.endsWith("Z") ? "" : "Z"));
                  const date  = dt.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
                  const time  = dt.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
                  const isLatest = i === 0;
                  const netPl = ev.net_pl ?? 0;
                  return (
                    <div key={ev.id} className={`px-4 py-3 hover:bg-slate-800/30 transition ${isLatest ? "bg-slate-800/20" : ""}`}>
                      <div className="flex items-center justify-between mb-1.5 gap-2">
                        <div className="flex items-center gap-2 min-w-0 flex-wrap">
                          <span className="text-xs font-medium text-slate-200">{date}</span>
                          <span className="text-[11px] text-slate-400">{time}</span>
                          {isLatest && (
                            <span className="text-[9px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                              Current
                            </span>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-[10px] text-slate-400">
                          Open <span className="font-semibold text-slate-300">{ev.open_trades ?? 0}</span>
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
                      {ev.ip_address && (
                        <p className="text-[10px] text-slate-500 mt-1 font-mono truncate">{ev.ip_address}</p>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </AccordionCard>

          <AccordionCard
            icon={Globe}
            title="Resources"
            action={
              <div className="flex items-center gap-2">
                {resourcesLastAt && (
                  <span className="text-[10px] text-slate-500 tabular-nums hidden sm:inline">
                    {new Date(resourcesLastAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                  </span>
                )}
                <button
                  type="button"
                  onClick={fetchResources}
                  title="Refresh now"
                  className="text-slate-500 hover:text-slate-300 transition"
                >
                  <RefreshCw className="w-3.5 h-3.5" />
                </button>
              </div>
            }
          >
            <div className="flex flex-col divide-y divide-slate-800/60">
              {resourcesLoading && !resources ? (
                <div className="flex items-center justify-center gap-2 py-8 text-slate-500 text-xs">
                  <Loader2 className="w-3.5 h-3.5 animate-spin" /> Probing…
                </div>
              ) : (
                [
                  { key: "flask",   label: "Tradefinder Data Center", icon: Server     },
                  { key: "polygon", label: "Tick Data Farm",          icon: BarChart2  },
                  { key: "alpaca",  label: "Exchange Connection",     icon: TrendingUp },
                  { key: "yahoo",   label: "Yahoo Finance",           icon: Globe      },
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
                    <div key={key} className="flex items-center gap-3 px-4 py-3 hover:bg-slate-800/30 transition">
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
                          r.latency_ms < 70  ? "text-emerald-500"
                          : r.latency_ms < 200 ? "text-yellow-500"
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
          </AccordionCard>
        </div>
      </div>

      {loginHistoryOpen && (() => {
        const totalPages = Math.ceil(loginHistoryAll.length / LOGIN_HISTORY_PAGE_SIZE);
        const pageItems  = loginHistoryAll.slice(
          loginHistoryPage * LOGIN_HISTORY_PAGE_SIZE,
          (loginHistoryPage + 1) * LOGIN_HISTORY_PAGE_SIZE,
        );
        return (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-[2px] p-4">
            <div className="bg-slate-800 border border-slate-700 rounded-xl shadow-2xl w-full max-w-2xl flex flex-col max-h-[85vh]">
              <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700 shrink-0">
                <div className="flex items-center gap-2.5">
                  <Clock className="w-4 h-4 text-slate-400" />
                  <h3 className="text-sm font-semibold text-slate-200">Login History</h3>
                  {!loginHistoryLoading && (
                    <span className="text-xs text-slate-400">{loginHistoryAll.length} event{loginHistoryAll.length !== 1 ? "s" : ""}</span>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => setLoginHistoryOpen(false)}
                  className="text-slate-400 hover:text-slate-300 transition"
                >
                  <XCircle className="w-5 h-5" />
                </button>
              </div>

              {!loginHistoryLoading && loginHistoryAll.length > 0 && (
                <div className="grid grid-cols-[1fr_1fr_auto_auto_auto_auto] gap-x-4 px-5 py-2 border-b border-slate-800/60 shrink-0">
                  {["Date / Time", "Platform", "Open", "W / L", "Net P&L", "IP"].map(h => (
                    <span key={h} className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{h}</span>
                  ))}
                </div>
              )}

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
                        <span className="text-[11px] text-slate-400 truncate">{formatPlatform(ev.platform)}</span>
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

              {!loginHistoryLoading && totalPages > 1 && (
                <div className="flex items-center justify-between px-5 py-3 border-t border-slate-700/60 shrink-0">
                  <span className="text-xs text-slate-400">
                    Page {loginHistoryPage + 1} of {totalPages}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      onClick={() => setLoginHistoryPage(p => p - 1)}
                      disabled={loginHistoryPage === 0}
                      className="px-2.5 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >‹ Prev</button>
                    <button
                      type="button"
                      onClick={() => setLoginHistoryPage(p => p + 1)}
                      disabled={loginHistoryPage >= totalPages - 1}
                      className="px-2.5 py-1 rounded text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition"
                    >Next ›</button>
                  </div>
                </div>
              )}
            </div>
          </div>
        );
      })()}
    </>
  );
}
