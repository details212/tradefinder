import { useState, useRef, useEffect, useCallback } from "react";
import { stockApi, dataDownloadApi } from "../api/client";
import {
  Download,
  Search,
  X,
  Loader2,
  CheckCircle2,
  AlertCircle,
  Clock,
  FileDown,
  ChevronRight,
  BarChart2,
  RefreshCw,
  Pause,
  Circle,
} from "lucide-react";

// ── Timeframe catalogue (must mirror backend TIMEFRAMES) ─────────────────────

const TIMEFRAME_GROUPS = [
  {
    label: "Minutes",
    items: ["1min", "5min", "10min", "15min", "30min", "45min"],
  },
  {
    label: "Hours",
    items: ["1h", "2h", "3h", "4h"],
  },
  {
    label: "Days / Week / Month",
    items: ["1d", "2d", "3d", "1w", "1m"],
  },
];

// ── Time-window options per timeframe group ───────────────────────────────────

const MINUTE_TFS  = new Set(["1min", "5min", "10min", "15min", "30min", "45min"]);
const HOUR_TFS    = new Set(["1h", "2h", "3h", "4h"]);

function getMaxMonths(tf) {
  if (MINUTE_TFS.has(tf)) return 12;
  if (HOUR_TFS.has(tf))   return 36;
  return 60;
}

function getWindowOptions(tf) {
  if (MINUTE_TFS.has(tf)) {
    return Array.from({ length: 12 }, (_, i) => ({
      months: i + 1,
      label:  `${i + 1}mo`,
    }));
  }
  if (HOUR_TFS.has(tf)) {
    return [12, 24, 36].map((m) => ({
      months: m,
      label:  `${m / 12}yr`,
    }));
  }
  // Day / week / month — up to 5 years
  return [12, 24, 36, 48, 60].map((m) => ({
    months: m,
    label:  `${m / 12}yr`,
  }));
}

const ALL_TIMEFRAMES = TIMEFRAME_GROUPS.flatMap((g) => g.items);

// ── Log entry icon / colour ───────────────────────────────────────────────────

function LogIcon({ type }) {
  switch (type) {
    case "success":
      return <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0 mt-0.5" />;
    case "error":
      return <AlertCircle className="w-3.5 h-3.5 text-red-400 shrink-0 mt-0.5" />;
    case "wait":
      return <Pause className="w-3.5 h-3.5 text-amber-400 shrink-0 mt-0.5" />;
    case "fetch":
      return <RefreshCw className="w-3.5 h-3.5 text-sky-400 shrink-0 mt-0.5 animate-spin" />;
    case "page":
      return <CheckCircle2 className="w-3.5 h-3.5 text-sky-400 shrink-0 mt-0.5" />;
    case "retry":
      return <RefreshCw className="w-3.5 h-3.5 text-amber-400 shrink-0 mt-0.5" />;
    default:
      return <Circle className="w-3 h-3 text-slate-500 shrink-0 mt-0.5" />;
  }
}

// ── Phase badge ───────────────────────────────────────────────────────────────

function PhaseBadge({ phase }) {
  const map = {
    idle:       { label: "Ready",         cls: "bg-slate-700 text-slate-400" },
    connecting: { label: "Connecting…",   cls: "bg-sky-900/50 text-sky-400 animate-pulse" },
    fetching:   { label: "Fetching data", cls: "bg-blue-900/50 text-blue-400 animate-pulse" },
    building:   { label: "Building CSV",  cls: "bg-violet-900/50 text-violet-400 animate-pulse" },
    complete:   { label: "Complete",      cls: "bg-emerald-900/50 text-emerald-400" },
    error:      { label: "Error",         cls: "bg-red-900/50 text-red-400" },
  };
  const { label, cls } = map[phase] ?? map.idle;
  return (
    <span className={`text-[11px] font-semibold px-2.5 py-1 rounded-full ${cls}`}>
      {label}
    </span>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function DataDownload() {
  // ── Step 1: symbol ──────────────────────────────────────────────────────────
  const [query,          setQuery]          = useState("");
  const [searchResults,  setSearchResults]  = useState([]);
  const [searchLoading,  setSearchLoading]  = useState(false);
  const [showDropdown,   setShowDropdown]   = useState(false);
  const [ticker,         setTicker]         = useState(null);   // selected ticker string
  const [tickerName,     setTickerName]     = useState("");

  // ── Step 2: timeframe ───────────────────────────────────────────────────────
  const [timeframe, setTimeframe] = useState("5min");

  // ── Step 3: months ──────────────────────────────────────────────────────────
  const [months, setMonths] = useState(3);

  // Clamp months when timeframe changes so the selection stays valid
  useEffect(() => {
    const opts = getWindowOptions(timeframe);
    const valid = opts.map((o) => o.months);
    if (!valid.includes(months)) {
      // Pick the closest valid option that doesn't exceed the new max
      const max = getMaxMonths(timeframe);
      const clamped = valid.filter((m) => m <= max).at(-1) ?? valid[0];
      setMonths(clamped);
    }
  }, [timeframe]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Download state ──────────────────────────────────────────────────────────
  const [phase,        setPhase]        = useState("idle");
  const [log,          setLog]          = useState([]);
  const [barsTotal,    setBarsTotal]    = useState(0);
  const [pageNum,      setPageNum]      = useState(0);
  const [countdown,    setCountdown]    = useState(null);
  const [completeInfo, setCompleteInfo] = useState(null);
  const [errorMsg,     setErrorMsg]     = useState("");
  const [saving,       setSaving]       = useState(false);

  const esRef            = useRef(null);
  const countdownRef     = useRef(null);
  const completedRef     = useRef(false);   // prevents onerror from overriding "complete"
  const logEndRef        = useRef(null);
  const searchDebounce   = useRef(null);
  const searchInputRef   = useRef(null);

  // ── Auto-scroll log ─────────────────────────────────────────────────────────
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [log]);

  // ── Symbol search ────────────────────────────────────────────────────────────
  useEffect(() => {
    clearTimeout(searchDebounce.current);
    if (!query.trim()) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }
    // Query matches the already-selected ticker — don't re-search or reopen dropdown
    if (ticker && query.toUpperCase() === ticker) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }
    setSearchLoading(true);
    searchDebounce.current = setTimeout(() => {
      stockApi
        .search(query, { params: { limit: 8 } })
        .then((r) => {
          setSearchResults(r.data.results || []);
          setShowDropdown(true);
        })
        .catch(() => setSearchResults([]))
        .finally(() => setSearchLoading(false));
    }, 280);
    return () => clearTimeout(searchDebounce.current);
  }, [query, ticker]);

  const selectTicker = useCallback((result) => {
    setTicker(result.ticker);
    setTickerName(result.name || "");
    setQuery(result.ticker);
    setShowDropdown(false);
    setSearchResults([]);
  }, []);

  const clearTicker = useCallback(() => {
    setTicker(null);
    setTickerName("");
    setQuery("");
    setShowDropdown(false);
    searchInputRef.current?.focus();
  }, []);

  // ── Log helper ───────────────────────────────────────────────────────────────
  const addLog = useCallback((type, message, detail = "") => {
    const ts = new Date().toLocaleTimeString("en-US", {
      hour12: false,
      hour:   "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    setLog((prev) => [...prev, { type, message, detail, ts, id: Date.now() + Math.random() }]);
  }, []);

  // ── Countdown ticker ─────────────────────────────────────────────────────────
  const startCountdown = useCallback((seconds) => {
    clearInterval(countdownRef.current);
    setCountdown(parseFloat(seconds.toFixed(1)));
    const step = 100; // ms
    let remaining = seconds * 1000;
    countdownRef.current = setInterval(() => {
      remaining -= step;
      if (remaining <= 0) {
        clearInterval(countdownRef.current);
        setCountdown(null);
      } else {
        setCountdown(parseFloat((remaining / 1000).toFixed(1)));
      }
    }, step);
  }, []);

  // ── Stop / cleanup ───────────────────────────────────────────────────────────
  const stopStream = useCallback(() => {
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
    clearInterval(countdownRef.current);
    setCountdown(null);
  }, []);

  useEffect(() => () => stopStream(), [stopStream]);

  // ── Start download ───────────────────────────────────────────────────────────
  const handleStart = useCallback(() => {
    if (!ticker) return;
    stopStream();
    completedRef.current = false;
    setPhase("connecting");
    setLog([]);
    setBarsTotal(0);
    setPageNum(0);
    setCompleteInfo(null);
    setErrorMsg("");

    const token   = localStorage.getItem("tf_token") || "";
    const BASE    = window.TRADEFINDER_API_URL || "http://localhost:5000";
    const qs      = new URLSearchParams({ ticker, timeframe, months, token }).toString();
    const url     = `${BASE}/api/download/ohlcv/stream?${qs}`;

    addLog("info", "Connecting to server…", `${ticker} · ${timeframe} · ${months} month${months !== 1 ? "s" : ""}`);

    const es = new EventSource(url);
    esRef.current = es;

    es.addEventListener("start", (e) => {
      const d = JSON.parse(e.data);
      setPhase("fetching");
      addLog("success", "Stream connected", `${d.ticker}  ${d.from} → ${d.to}`);
    });

    es.addEventListener("page_start", (e) => {
      const d = JSON.parse(e.data);
      setPageNum(d.page);
      addLog("fetch", `Requesting Query ${d.page} from Tradefinder Data…`, `${d.bars_so_far.toLocaleString()} bars collected so far`);
    });

    es.addEventListener("page_done", (e) => {
      const d = JSON.parse(e.data);
      setBarsTotal(d.bars_total);
      addLog(
        "page",
        `Page ${d.page} received — ${d.bars_this_page.toLocaleString()} bars`,
        `Running total: ${d.bars_total.toLocaleString()} bars  ·  Polygon status: ${d.polygon_status}`,
      );
    });

    es.addEventListener("retry", (e) => {
      const d = JSON.parse(e.data);
      addLog("retry", `Retrying page ${d.page} (attempt ${d.attempt})…`);
    });

    es.addEventListener("waiting", (e) => {
      const d = JSON.parse(e.data);
      addLog("wait", `Pausing ${d.seconds}s between queries (rate-limit buffer)…`);
      startCountdown(d.seconds);
    });

    es.addEventListener("building_csv", (e) => {
      const d = JSON.parse(e.data);
      setPhase("building");
      addLog("info", "Assembling CSV on server…", `${d.total_bars.toLocaleString()} total bars`);
    });

    es.addEventListener("complete", (e) => {
      const d = JSON.parse(e.data);
      completedRef.current = true;
      setPhase("complete");
      setCompleteInfo(d);
      setBarsTotal(d.total_bars);
      addLog(
        "success",
        `Ready to download!`,
        `${d.total_bars.toLocaleString()} bars  ·  ${d.pages} page${d.pages !== 1 ? "s" : ""}  ·  ${d.filename}`,
      );
      es.close();
      esRef.current = null;
    });

    // Named SSE events with `event: error` sent by the server
    es.addEventListener("error", (e) => {
      // Native EventSource connection errors have no e.data — skip them here
      // and let onerror handle them with a clearer message.
      if (!e.data) return;
      let msg = "Server error";
      try {
        const d = JSON.parse(e.data);
        msg = d.message || msg;
      } catch { /* unparseable — keep generic */ }
      completedRef.current = true;
      setPhase("error");
      setErrorMsg(msg);
      addLog("error", msg);
      es.close();
      esRef.current = null;
    });

    // Native EventSource connection errors (404, 401, network failure, server not restarted, etc.)
    es.onerror = () => {
      if (completedRef.current) return;
      completedRef.current = true;
      const msg =
        "Could not connect to the download endpoint. " +
        "If you just added this feature, restart the backend server and try again.";
      setPhase("error");
      setErrorMsg(msg);
      addLog("error", "EventSource connection failed — server may need a restart");
      es.close();
      esRef.current = null;
    };
  }, [ticker, timeframe, months, stopStream, addLog, startCountdown]);

  // ── Save CSV to disk ─────────────────────────────────────────────────────────
  const handleSave = useCallback(async () => {
    if (!completeInfo?.job_id) return;
    setSaving(true);
    try {
      const r = await dataDownloadApi.result(completeInfo.job_id);
      const blob = new Blob([r.data], { type: "text/csv;charset=utf-8;" });
      const objUrl = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href     = objUrl;
      a.download = completeInfo.filename || `${ticker}_${timeframe}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(objUrl);
      addLog("success", `Saved: ${completeInfo.filename}`);
      // Job is consumed — clear so the button can't be clicked again
      setCompleteInfo(null);
    } catch (err) {
      addLog("error", `Save failed: ${err?.response?.data?.error || err.message}`);
    } finally {
      setSaving(false);
    }
  }, [completeInfo, ticker, timeframe, addLog]);

  // ── Derived ──────────────────────────────────────────────────────────────────
  const isRunning  = phase === "connecting" || phase === "fetching" || phase === "building";
  const canStart   = !!ticker && !isRunning;
  const canSave    = phase === "complete" && !!completeInfo?.job_id && !saving;

  // ── Estimated date range label ────────────────────────────────────────────────
  const fromLabel = (() => {
    const d = new Date();
    d.setMonth(d.getMonth() - months);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  })();
  const toLabel = new Date().toLocaleDateString("en-US", {
    month: "short", day: "numeric", year: "numeric",
  });

  // ── Render ────────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full overflow-hidden bg-slate-900">

      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div className="shrink-0 px-8 py-6 border-b border-slate-800">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-slate-100 flex items-center gap-2.5">
              <FileDown className="w-5 h-5 text-brand-400" />
              Data Download
            </h1>
            <p className="text-sm text-slate-500 mt-0.5">
              Export OHLCV history to CSV via Polygon — pages slowly to stay within rate limits.
            </p>
          </div>
          <PhaseBadge phase={phase} />
        </div>
      </div>

      {/* ── Body (wizard + log) ─────────────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">

        {/* ── Left: wizard ───────────────────────────────────────────────────── */}
        <div className="w-96 shrink-0 flex flex-col gap-6 px-8 py-6 border-r border-slate-800 overflow-y-auto">

          {/* Step 1 ─────────────────────────────────────────────────────────── */}
          <section>
            <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <span className="w-5 h-5 rounded-full bg-brand-600/30 text-brand-400 text-[10px] font-bold flex items-center justify-center">1</span>
              Select Symbol
            </h2>

            <div className="relative">
              {/* Input */}
              <div className="relative">
                {searchLoading
                  ? <Loader2 className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 animate-spin" />
                  : <Search  className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                }
                <input
                  ref={searchInputRef}
                  value={query}
                  onChange={(e) => {
                    setQuery(e.target.value);
                    if (ticker && e.target.value !== ticker) setTicker(null);
                  }}
                  onFocus={() => searchResults.length && setShowDropdown(true)}
                  onBlur={() => setTimeout(() => setShowDropdown(false), 150)}
                  disabled={isRunning}
                  placeholder="Search ticker or company name…"
                  className="w-full bg-slate-800 border border-slate-700 rounded-xl pl-9 pr-9 py-2.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand-500 disabled:opacity-50 transition"
                />
                {query && (
                  <button
                    onClick={clearTicker}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
                  >
                    <X className="w-4 h-4" />
                  </button>
                )}
              </div>

              {/* Dropdown */}
              {showDropdown && searchResults.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-slate-800 border border-slate-700 rounded-xl shadow-2xl z-20 overflow-hidden">
                  {searchResults.map((r) => (
                    <button
                      key={r.ticker}
                      onMouseDown={() => selectTicker(r)}
                      className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-slate-700 transition text-left border-b border-slate-700/50 last:border-0"
                    >
                      <div className="flex items-center gap-3 min-w-0">
                        <span className="text-sm font-bold text-brand-400 w-14 shrink-0">{r.ticker}</span>
                        <span className="text-xs text-slate-400 truncate">{r.name || "—"}</span>
                      </div>
                      <span className="text-[10px] text-slate-600 shrink-0 ml-2">{r.primary_exchange || r.market || ""}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Selected ticker pill */}
            {ticker && (
              <div className="mt-2.5 flex items-center gap-2 px-3 py-2 bg-brand-900/20 border border-brand-700/40 rounded-lg">
                <BarChart2 className="w-4 h-4 text-brand-400 shrink-0" />
                <div className="flex-1 min-w-0">
                  <span className="text-sm font-bold text-brand-300">{ticker}</span>
                  {tickerName && (
                    <span className="text-xs text-slate-500 ml-2 truncate">{tickerName}</span>
                  )}
                </div>
                <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
              </div>
            )}
          </section>

          {/* Step 2 ─────────────────────────────────────────────────────────── */}
          <section>
            <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <span className="w-5 h-5 rounded-full bg-brand-600/30 text-brand-400 text-[10px] font-bold flex items-center justify-center">2</span>
              Timeframe
            </h2>
            <div className="flex flex-col gap-3">
              {TIMEFRAME_GROUPS.map((group) => (
                <div key={group.label}>
                  <p className="text-[10px] font-semibold text-slate-600 uppercase tracking-wider mb-1.5">{group.label}</p>
                  <div className="flex flex-wrap gap-1.5">
                    {group.items.map((tf) => (
                      <button
                        key={tf}
                        onClick={() => setTimeframe(tf)}
                        disabled={isRunning}
                        className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition disabled:opacity-50 ${
                          timeframe === tf
                            ? "bg-brand-600 text-white shadow"
                            : "bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 border border-slate-700"
                        }`}
                      >
                        {tf}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* Step 3 ─────────────────────────────────────────────────────────── */}
          <section>
            <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <span className="w-5 h-5 rounded-full bg-brand-600/30 text-brand-400 text-[10px] font-bold flex items-center justify-center">3</span>
              Time Window
            </h2>
            <div className="flex flex-wrap gap-1.5">
              {getWindowOptions(timeframe).map((opt) => (
                <button
                  key={opt.months}
                  onClick={() => setMonths(opt.months)}
                  disabled={isRunning}
                  className={`px-3 h-10 rounded-lg text-sm font-semibold transition disabled:opacity-50 ${
                    months === opt.months
                      ? "bg-brand-600 text-white shadow"
                      : "bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 border border-slate-700"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
            <p className="text-xs text-slate-500 mt-2.5 flex items-center gap-1.5">
              <Clock className="w-3.5 h-3.5" />
              {fromLabel} → {toLabel}
            </p>
          </section>

          {/* ── Action buttons ─────────────────────────────────────────────── */}
          <div className="flex flex-col gap-2 pt-1">
            <button
              onClick={handleStart}
              disabled={!canStart}
              className={`flex items-center justify-center gap-2 w-full py-3 rounded-xl text-sm font-semibold transition ${
                canStart
                  ? "bg-brand-600 hover:bg-brand-500 text-white shadow-lg shadow-brand-900/40"
                  : "bg-slate-800 text-slate-600 cursor-not-allowed"
              }`}
            >
              {isRunning ? (
                <><Loader2 className="w-4 h-4 animate-spin" /> Downloading…</>
              ) : (
                <><Download className="w-4 h-4" /> Start Download</>
              )}
            </button>

            {canSave && (
              <button
                onClick={handleSave}
                disabled={saving}
                className="flex items-center justify-center gap-2 w-full py-3 rounded-xl text-sm font-semibold transition bg-emerald-700 hover:bg-emerald-600 text-white shadow-lg shadow-emerald-900/40"
              >
                {saving
                  ? <><Loader2 className="w-4 h-4 animate-spin" /> Saving…</>
                  : <><FileDown className="w-4 h-4" /> Save CSV</>
                }
              </button>
            )}

            {isRunning && (
              <button
                onClick={() => {
                  stopStream();
                  setPhase("idle");
                  addLog("info", "Download cancelled by user");
                }}
                className="flex items-center justify-center gap-2 w-full py-2.5 rounded-xl text-sm font-medium text-red-400 hover:text-red-300 hover:bg-red-900/20 border border-red-900/40 transition"
              >
                <X className="w-4 h-4" /> Cancel
              </button>
            )}
          </div>
        </div>

        {/* ── Right: progress log ─────────────────────────────────────────────── */}
        <div className="flex-1 flex flex-col overflow-hidden">

          {/* Stats bar */}
          {(isRunning || phase === "complete" || phase === "error") && (
            <div className="shrink-0 grid grid-cols-3 gap-px bg-slate-800 border-b border-slate-800">
              {[
                {
                  label: "Bars Collected",
                  value: barsTotal.toLocaleString(),
                  accent: "text-slate-200",
                },
                {
                  label: "Queries Fetched",
                  value: pageNum.toString(),
                  accent: "text-sky-300",
                },
                {
                  label: countdown != null ? `Next page in ${countdown}s` : "Rate-limit pause",
                  value: countdown != null ? "Waiting…" : phase === "fetching" ? "Active" : "—",
                  accent: countdown != null ? "text-amber-300" : "text-slate-500",
                },
              ].map((stat) => (
                <div key={stat.label} className="flex flex-col items-center py-3 bg-slate-900/80">
                  <span className={`text-lg font-bold tabular-nums ${stat.accent}`}>{stat.value}</span>
                  <span className="text-[10px] text-slate-600 mt-0.5">{stat.label}</span>
                </div>
              ))}
            </div>
          )}

          {/* Error banner */}
          {phase === "error" && errorMsg && (
            <div className="shrink-0 mx-6 mt-4 flex items-start gap-3 px-4 py-3 bg-red-900/20 border border-red-800/40 rounded-xl">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
              <p className="text-sm text-red-300">{errorMsg}</p>
            </div>
          )}

          {/* Complete banner */}
          {phase === "complete" && completeInfo && (
            <div className="shrink-0 mx-6 mt-4 flex items-center gap-3 px-4 py-3 bg-emerald-900/20 border border-emerald-700/40 rounded-xl">
              <CheckCircle2 className="w-5 h-5 text-emerald-400 shrink-0" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-semibold text-emerald-300">
                  {completeInfo.total_bars.toLocaleString()} bars ready
                </p>
                <p className="text-xs text-slate-500 truncate">{completeInfo.filename}</p>
              </div>
              <span className="text-xs text-slate-500 shrink-0">
                5 min to save
              </span>
            </div>
          )}

          {/* Log list */}
          <div className="flex-1 overflow-y-auto px-6 py-4">
            {log.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <Download className="w-10 h-10 text-slate-800 mb-3" />
                <p className="text-sm text-slate-600 font-medium">No download in progress</p>
                <p className="text-xs text-slate-700 mt-1">
                  Select a symbol, timeframe and window, then click Start Download.
                </p>
              </div>
            ) : (
              <div className="space-y-1.5">
                {log.map((entry) => (
                  <div
                    key={entry.id}
                    className={`flex items-start gap-2.5 py-2 px-3 rounded-lg ${
                      entry.type === "error"   ? "bg-red-950/30" :
                      entry.type === "success" ? "bg-emerald-950/20" :
                      entry.type === "wait"    ? "bg-amber-950/20" :
                      "bg-slate-800/40"
                    }`}
                  >
                    <LogIcon type={entry.type} />
                    <div className="flex-1 min-w-0">
                      <p className="text-xs text-slate-300 leading-snug">{entry.message}</p>
                      {entry.detail && (
                        <p className="text-[11px] text-slate-600 mt-0.5 truncate">{entry.detail}</p>
                      )}
                    </div>
                    <span className="text-[10px] text-slate-700 tabular-nums shrink-0 pt-0.5">{entry.ts}</span>
                  </div>
                ))}

                {/* Live countdown row */}
                {countdown != null && (
                  <div className="flex items-center gap-2.5 py-2 px-3 rounded-lg bg-amber-950/20 border border-amber-900/30">
                    <Clock className="w-3.5 h-3.5 text-amber-400 shrink-0 animate-pulse" />
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <p className="text-xs text-amber-300">
                          Next page request in&nbsp;
                          <span className="font-bold tabular-nums">{countdown}s</span>
                        </p>
                      </div>
                      <div className="mt-1.5 h-1 bg-slate-800 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-amber-500 rounded-full transition-all duration-100"
                          style={{ width: `${Math.max(0, (countdown / 1.5) * 100)}%` }}
                        />
                      </div>
                    </div>
                  </div>
                )}

                <div ref={logEndRef} />
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
