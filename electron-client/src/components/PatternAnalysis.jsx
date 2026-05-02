import { useState, useEffect, useRef, useCallback, cloneElement } from "react";
import { scannerApi, stockApi, alpacaApi } from "../api/client";
import {
  Play,
  RotateCcw,
  TrendingUp,
  ChevronUp,
  ChevronDown,
  Loader2,
  AlertCircle,
  SlidersHorizontal,
  X,
  Search,
} from "lucide-react";
import PatternAnalysisChart from "./PatternAnalysisChart";

// ── Sidebar helper components (must be outside main component — defined inside
//    causes React to see a new type each render, remounting inputs and losing focus) ──
function SidebarSection({ title, children }) {
  return (
    <div className="mb-4">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap">
          {title}
        </span>
        <div className="flex-1 h-px bg-slate-800" />
      </div>
      {children}
    </div>
  );
}

function CbRow({ label, checked, onChange }) {
  return (
    <label className="flex items-center gap-2 mb-1.5 text-xs text-slate-400 cursor-pointer select-none hover:text-slate-300 transition-colors">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="w-3.5 h-3.5 rounded accent-brand-500 flex-shrink-0 cursor-pointer"
      />
      {label}
    </label>
  );
}

function NumInput({ value, placeholder, onChange }) {
  return (
    <input
      type="number"
      value={value}
      placeholder={placeholder}
      onChange={(e) => onChange(e.target.value)}
      className="w-full bg-slate-800 border border-slate-700 text-slate-200 placeholder-slate-600 rounded-lg px-2 py-1.5 text-xs focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/30 transition"
    />
  );
}

function FilterRow({ label, children }) {
  return (
    <div className="flex items-center gap-2 mb-1.5">
      <span className="text-xs text-slate-500 whitespace-nowrap min-w-[72px]">{label}</span>
      {children}
    </div>
  );
}

// ── Vol label colours (matches Leo) ──────────────────────────────────────────
const VOL_COLOR = {
  XHigh: "#ff0000",
  High:  "#ff7800",
  Med:   "#ffcf03",
  Norm:  "#a0d6dc",
  Low:   "#1f9cac",
};
const VOL_HEIGHT = { XHigh: 22, High: 17, Med: 13, Norm: 9, Low: 5 };
const VOL_RANK   = { XHigh: 4, High: 3, Med: 2, Norm: 1, Low: 0 };

const VOL_BADGE_CLS = {
  XHigh: "bg-red-500/20 text-red-400",
  High:  "bg-orange-500/15 text-orange-400",
  Med:   "bg-yellow-500/15 text-yellow-400",
  Norm:  "bg-cyan-500/10 text-cyan-300",
  Low:   "bg-teal-600/10 text-teal-400",
};

function fmtVol(v) {
  if (v == null) return "—";
  v = parseInt(v);
  if (v >= 1e9) return (v / 1e9).toFixed(1) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(0) + "K";
  return String(v);
}

function MaAlign({ row }) {
  const c = parseFloat(row.close);
  const mas = [
    { label: "E10",  val: row.ema_10,  color: "#3fb950" },
    { label: "E20",  val: row.ema_20,  color: "#bc8cff" },
    { sep: true },
    { label: "S50",  val: row.sma_50,  color: "#58a6ff" },
    { label: "S150", val: row.sma_150, color: "#d29922" },
    { label: "S200", val: row.sma_200, color: "#f85149" },
  ];
  return (
    <div className="flex items-end gap-0.5">
      {mas.map((m, i) => {
        if (m.sep) return <div key={i} className="w-4" />;
        if (m.val == null || isNaN(parseFloat(m.val))) return null;
        const above = c >= parseFloat(m.val);
        return (
          <div key={i} className="flex flex-col items-center leading-none mx-px"
               title={`${m.label}: $${parseFloat(m.val).toFixed(2)}`}>
            <span style={{ color: above ? "#3fb950" : "#f85149", fontSize: 10 }}>
              {above ? "▲" : "▼"}
            </span>
            <span style={{ color: m.color, fontSize: 7, fontWeight: 700 }}>
              {m.label}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function VolBars({ history }) {
  if (!history) return null;
  const bars = history.split(",").map((lbl) => lbl.trim());
  return (
    <div className="flex items-end gap-px" style={{ height: 22 }}>
      {bars.map((lbl, i) => (
        <div
          key={i}
          style={{
            width: 5,
            height: VOL_HEIGHT[lbl] || 5,
            background: VOL_COLOR[lbl] || "#30363d",
            borderRadius: "1px 1px 0 0",
          }}
        />
      ))}
    </div>
  );
}

// ── Default filter state ──────────────────────────────────────────────────────
const DEFAULT_FILTERS = {
  days:             1,
  priceMin:         "",
  priceMax:         "",
  aboveSma50:       false,
  belowSma50:       false,
  aboveSma150:      true,
  aboveSma200:      true,
  rsiMin:           "",
  rsiMax:           "",
  rsiAbove50:       false,
  rsiBelow50:       false,
  volLabels:        ["XHigh", "High", "Med", "Norm", "Low"],
  volRatioMin:      "",
  pctFromHighMax:   "",
  pctFromLowMin:    "",
  atrSqueeze:       true,
  atrSqueezeBreak:  false,
  atrDeclBarsMin:   "",
  rmv15Compressed:  false,
  rmv15Max:         "",
  as1mMin:          "70",
  as1mMax:          "",
  rsAboveSma50:     false,
  rsBelowSma50:     false,
};

/** When symbol history mode starts — relax defaults that hide most past bars */
const SYMBOL_SEARCH_FILTER_OVERRIDES = {
  aboveSma150: false,
  aboveSma200: false,
  as1mMin:     "",
};

const VOL_LABEL_ORDER = ["XHigh", "High", "Med", "Norm", "Low"];

function mergeVolLabelsForSymbolSearch(currentLabels) {
  const set = new Set([...(currentLabels || []), "Norm", "Low"]);
  return VOL_LABEL_ORDER.filter((l) => set.has(l));
}

const SORT_OPTIONS = [
  { value: "default",      label: "Default" },
  { value: "ticker",       label: "Symbol" },
  { value: "close",        label: "Close" },
  { value: "rsi_14",       label: "RSI" },
  { value: "vol_ratio",    label: "Vol Ratio" },
  { value: "pct_from_high",label: "% from High" },
  { value: "pct_from_low", label: "% from Low" },
  { value: "as_1m",        label: "AS 1M" },
  { value: "rmv_15",       label: "RMV 15" },
  { value: "sma50_dist",   label: "% vs SMA 50" },
  { value: "sma200_dist",  label: "% vs SMA 200" },
  { value: "bar_date",     label: "Date" },
];

// ── Component ─────────────────────────────────────────────────────────────────
export default function PatternAnalysis({ onSelectTicker, openChartRequest, onConsumedOpenChartRequest }) {
  const [filters, setFilters]     = useState(DEFAULT_FILTERS);
  const [sortBy,  setSortBy]      = useState("default");
  const [sortDir, setSortDir]     = useState("asc");
  const [clientSort, setClientSort] = useState({ col: null, dir: 1 });
  const [rows,    setRows]        = useState(null);
  const [loading, setLoading]     = useState(false);
  const [error,   setError]       = useState(null);
  const [tradingDates, setTradingDates] = useState([]);
  const debounceRef    = useRef(null);
  const hasAutoScanned = useRef(false);
  const [chartModal,   setChartModal]   = useState(null); // null | { ticker }
  const [chartHeight,  setChartHeight]  = useState(null);
  const [openTickers,  setOpenTickers]  = useState(new Set());

  const refreshOpenTickers = useCallback(() => {
    alpacaApi.openTickers()
      .then(r => setOpenTickers(new Set(r.data.tickers ?? [])))
      .catch(() => {});
  }, []);

  useEffect(() => { refreshOpenTickers(); }, [refreshOpenTickers]);
  /** Draft in the symbol box vs last-applied value sent to the API */
  const [symbolDraft,    setSymbolDraft]    = useState("");
  const [symbolApplied,  setSymbolApplied]  = useState("");
  const [symbolHits,     setSymbolHits]     = useState([]);
  /** Debounce timer for /stocks/search — kept out of useEffect so typing isn’t interrupted by effect re-runs */
  const symbolSearchTimerRef = useRef(null);

  // ── Load trading dates on mount ─────────────────────────────────────────────
  useEffect(() => {
    scannerApi.dates()
      .then((res) => setTradingDates(res.data))
      .catch(() => {});
  }, []);

  function handleSymbolInputChange(e) {
    const v = e.target.value;
    setSymbolDraft(v);

    if (symbolSearchTimerRef.current) {
      clearTimeout(symbolSearchTimerRef.current);
      symbolSearchTimerRef.current = null;
    }

    const q = v.trim();
    if (q.length < 2) {
      setSymbolHits([]);
      return;
    }

    symbolSearchTimerRef.current = setTimeout(() => {
      symbolSearchTimerRef.current = null;
      stockApi
        .search(q, { params: { limit: 12 } })
        .then((r) => setSymbolHits(r.data?.results ?? []))
        .catch(() => setSymbolHits([]));
    }, 300);
  }

  function clearPendingSymbolSearch() {
    if (symbolSearchTimerRef.current) {
      clearTimeout(symbolSearchTimerRef.current);
      symbolSearchTimerRef.current = null;
    }
  }

  // Sidebar watchlist → open Pattern Analysis chart modal (source patternanalysis)
  useEffect(() => {
    if (!openChartRequest?.ticker || openChartRequest.key == null) return;
    setChartModal({ ticker: openChartRequest.ticker });
    onConsumedOpenChartRequest?.();
  }, [openChartRequest?.key, openChartRequest?.ticker, onConsumedOpenChartRequest]);

  // Compute chart area height when modal opens: modal is 95vh, header ~50px
  useEffect(() => {
    if (!chartModal) { setChartHeight(null); return; }
    setChartHeight(Math.floor(window.innerHeight * 0.95) - 50);
  }, [chartModal]);

  // ── Run scan ────────────────────────────────────────────────────────────────
  /** @param symbolOverride pass "" to force date-window mode; omit to use symbolApplied */
  const runScan = useCallback(async (overrideFilters, overrideSortBy, overrideSortDir, symbolOverride) => {
    const f  = overrideFilters  ?? filters;
    const sb = overrideSortBy   ?? sortBy;
    const sd = overrideSortDir  ?? sortDir;
    const sym  = symbolOverride !== undefined
      ? String(symbolOverride || "").trim().toUpperCase()
      : symbolApplied.trim().toUpperCase();

    const params = {};
    if (sym) {
      params.ticker = sym;
      params.sort_by = "bar_date";
      params.sort_dir = "desc";
    } else {
      params.days = f.days;
      params.sort_by = sb === "default" ? "ticker" : sb;
      params.sort_dir = sd;
    }

    if (f.priceMin)       params.price_min       = f.priceMin;
    if (f.priceMax)       params.price_max       = f.priceMax;
    if (f.aboveSma50)     params.above_sma50     = "1";
    if (f.belowSma50)     params.below_sma50     = "1";
    if (f.aboveSma150)    params.above_sma150    = "1";
    if (f.aboveSma200)    params.above_sma200    = "1";
    if (f.rsiMin)         params.rsi_min         = f.rsiMin;
    if (f.rsiMax)         params.rsi_max         = f.rsiMax;
    if (f.rsiAbove50)     params.rsi_above50     = "1";
    if (f.rsiBelow50)     params.rsi_below50     = "1";
    if (f.volLabels.length) params.vol_labels    = f.volLabels.join(",");
    if (f.volRatioMin)    params.vol_ratio_min   = f.volRatioMin;
    if (f.pctFromHighMax) params.pct_from_high_max = f.pctFromHighMax;
    if (f.pctFromLowMin)  params.pct_from_low_min  = f.pctFromLowMin;
    if (f.atrSqueeze)       params.atr_squeeze       = "1";
    if (f.atrSqueezeBreak)  params.atr_squeeze_break = "1";
    if (f.atrDeclBarsMin)   params.atr_decl_bars_min = f.atrDeclBarsMin;
    if (f.rmv15Compressed)params.rmv_15_compressed = "1";
    if (f.rmv15Max)       params.rmv_15_max      = f.rmv15Max;
    if (f.as1mMin)        params.as_1m_min       = f.as1mMin;
    if (f.as1mMax)        params.as_1m_max       = f.as1mMax;
    if (f.rsAboveSma50)   params.rs_above_sma50  = "1";
    if (f.rsBelowSma50)   params.rs_below_sma50  = "1";

    setLoading(true);
    setError(null);
    setClientSort({ col: null, dir: 1 });

    try {
      const res = await scannerApi.scan(params);
      setRows(res.data);
    } catch (e) {
      setError(e?.response?.data?.error || e.message || "Scan failed");
      setRows(null);
    } finally {
      setLoading(false);
    }
  }, [filters, sortBy, sortDir, symbolApplied]);

  function handleRunScan() {
    clearPendingSymbolSearch();
    const sym = symbolDraft.trim().toUpperCase();
    setSymbolApplied(sym);
    if (sym) {
      setClientSort({ col: null, dir: 1 });
      const nextFilters = {
        ...filters,
        ...SYMBOL_SEARCH_FILTER_OVERRIDES,
        volLabels: mergeVolLabelsForSymbolSearch(filters.volLabels),
      };
      setFilters(nextFilters);
      runScan(nextFilters, "bar_date", "desc", sym);
    } else {
      runScan(undefined, undefined, undefined, "");
    }
  }

  function pickSymbolHit(ticker) {
    const t = String(ticker || "").trim().toUpperCase();
    if (!t) return;
    clearPendingSymbolSearch();
    const nextFilters = {
      ...filters,
      ...SYMBOL_SEARCH_FILTER_OVERRIDES,
      volLabels: mergeVolLabelsForSymbolSearch(filters.volLabels),
    };
    setFilters(nextFilters);
    setSymbolDraft(t);
    setSymbolApplied(t);
    setSymbolHits([]);
    setClientSort({ col: null, dir: 1 });
    runScan(nextFilters, "bar_date", "desc", t);
  }

  // ── Auto-scan once trading dates have loaded ─────────────────────────────────
  useEffect(() => {
    if (tradingDates.length > 0 && !hasAutoScanned.current) {
      hasAutoScanned.current = true;
      runScan();
    }
  }, [tradingDates, runScan]);

  // ── Debounced scan on filter change ─────────────────────────────────────────
  function scheduleReScan(newFilters) {
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => runScan(newFilters), 450);
  }

  function setFilter(key, val) {
    const next = { ...filters, [key]: val };
    setFilters(next);
    scheduleReScan(next);
  }

  function toggleVolLabel(label) {
    const next = filters.volLabels.includes(label)
      ? filters.volLabels.filter((l) => l !== label)
      : [...filters.volLabels, label];
    const nextFilters = { ...filters, volLabels: next };
    setFilters(nextFilters);
    scheduleReScan(nextFilters);
  }

  function setDays(d) {
    clearPendingSymbolSearch();
    setSymbolDraft("");
    setSymbolApplied("");
    setSymbolHits([]);
    const next = { ...filters, days: d };
    setFilters(next);
    runScan(next, undefined, undefined, "");
  }

  function handleSortByChange(val) {
    setSortBy(val);
    runScan(undefined, val, sortDir);
  }

  function handleSortDirChange(val) {
    setSortDir(val);
    runScan(undefined, sortBy, val);
  }

  function resetFilters() {
    clearPendingSymbolSearch();
    setFilters(DEFAULT_FILTERS);
    setSortBy("default");
    setSortDir("asc");
    setSymbolDraft("");
    setSymbolApplied("");
    setSymbolHits([]);
    setRows(null);
    setError(null);
    setClientSort({ col: null, dir: 1 });
  }

  // ── Client-side column sort ─────────────────────────────────────────────────
  function handleColSort(col) {
    if (symbolApplied || !rows?.length) return;
    let dir = 1;
    if (clientSort.col === col) dir = clientSort.dir * -1;
    setClientSort({ col, dir });
  }

  function getSortedRows() {
    if (!rows) return [];
    let r = [...rows];

    if (symbolApplied) {
      r.sort((a, b) => {
        const ad = a.bar_date != null ? String(a.bar_date) : "";
        const bd = b.bar_date != null ? String(b.bar_date) : "";
        return bd.localeCompare(ad);
      });
      return r;
    }

    if (clientSort.col === null) {
      r.forEach((row) => {
        row._vol_rank  = VOL_RANK[row.vol_label] ?? -1;
        const c = parseFloat(row.close);
        row._ma_score  = [row.ema_10, row.ema_20, row.sma_50, row.sma_150, row.sma_200]
          .filter((v) => v != null && !isNaN(parseFloat(v)) && c >= parseFloat(v)).length;
      });
      r.sort((a, b) => {
        if (a._vol_rank !== b._vol_rank) return b._vol_rank - a._vol_rank;
        const pa = a.pct_from_high != null ? parseFloat(a.pct_from_high) : 9999;
        const pb = b.pct_from_high != null ? parseFloat(b.pct_from_high) : 9999;
        if (pa !== pb) return pa - pb;
        return b._ma_score - a._ma_score;
      });
    } else {
      const col = clientSort.col;
      const d   = clientSort.dir;
      r.sort((a, b) => {
        const av = a[col], bv = b[col];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (!isNaN(parseFloat(av))) return (parseFloat(av) - parseFloat(bv)) * d;
        return String(av).localeCompare(String(bv)) * d;
      });
    }
    return r;
  }

  // ── Active filter count ─────────────────────────────────────────────────────
  function activeFilterCount() {
    let n = 0;
    if (filters.priceMin)       n++;
    if (filters.priceMax)       n++;
    if (filters.rsiMin)         n++;
    if (filters.rsiMax)         n++;
    if (filters.pctFromHighMax) n++;
    if (filters.pctFromLowMin)  n++;
    if (filters.volRatioMin)    n++;
    if (filters.rmv15Max)       n++;
    if (filters.as1mMax)        n++;
    if (filters.as1mMin && filters.as1mMin !== "70") n++;
    if (filters.aboveSma50)     n++;
    if (filters.belowSma50)     n++;
    if (!filters.aboveSma150)   n++;
    if (!filters.aboveSma200)   n++;
    if (filters.rsiAbove50)     n++;
    if (filters.rsiBelow50)     n++;
    if (!filters.atrSqueeze)    n++;
    if (filters.atrSqueezeBreak)n++;
    if (filters.atrDeclBarsMin) n++;
    if (filters.rmv15Compressed)n++;
    if (filters.rsAboveSma50)   n++;
    if (filters.rsBelowSma50)   n++;
    const defVol = ["XHigh", "High", "Med", "Norm", "Low"];
    defVol.forEach((l) => { if (!filters.volLabels.includes(l)) n++; });
    filters.volLabels.forEach((l) => { if (!defVol.includes(l)) n++; });
    return n;
  }

  const sortedRows = getSortedRows();
  const badgeCount = activeFilterCount();

  // ── Date formatting helpers ─────────────────────────────────────────────────
  const DAY_NAMES = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
  const MON_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  function fmtDateFull(iso) {
    const [y, m, d] = iso.split("-").map(Number);
    const dt = new Date(y, m - 1, d);
    return `${DAY_NAMES[dt.getDay()]} ${MON_NAMES[dt.getMonth()]} ${dt.getDate()}`;
  }
  function fmtDateShort(iso) {
    const [y, m, d] = iso.split("-").map(Number);
    const dt = new Date(y, m - 1, d);
    return `${MON_NAMES[dt.getMonth()]} ${dt.getDate()}`;
  }

  // ── Column definitions ──────────────────────────────────────────────────────
  const cols = [
    {
      key: "ticker", label: "Symbol", sortable: true,
      cell: (r) => {
        const inOpenTrade = openTickers.has(r.ticker);
        return (
          <td className="px-3 py-2.5" title={inOpenTrade ? "Trade Currently Open" : undefined}>
            <button
              onClick={() => setChartModal({ ticker: r.ticker })}
              className={`font-bold text-left transition-colors hover:underline ${inOpenTrade ? "opacity-50 text-slate-400 line-through decoration-red-400 decoration-2" : "text-yellow-400 hover:text-yellow-300"}`}
            >
              {r.ticker}
            </button>
          </td>
        );
      },
    },
    {
      key: "bar_date", label: "Date", sortable: true,
      cell: (r) => (
        <td className="px-3 py-2.5 text-slate-500">{r.bar_date || "—"}</td>
      ),
    },
    {
      key: "close", label: "Close", sortable: true,
      cell: (r) => (
        <td className="px-3 py-2.5 tabular-nums text-slate-200">
          {r.close != null ? `$${parseFloat(r.close).toFixed(2)}` : "—"}
        </td>
      ),
    },
    {
      key: "ma_align", label: "MA Align", sortable: false,
      cell: (r) => (
        <td className="px-3 py-2.5">
          <MaAlign row={r} />
        </td>
      ),
    },
    {
      key: "rsi_14", label: "RSI", sortable: true,
      cell: (r) => {
        const v = r.rsi_14;
        if (v == null) return <td className="px-3 py-2.5 text-slate-500">—</td>;
        const color = v >= 70 ? "#3fb950" : v <= 30 ? "#f85149" : undefined;
        return (
          <td className="px-3 py-2.5 tabular-nums">
            <span style={color ? { color, fontWeight: 600 } : { color: "#cbd5e1" }}>
              {parseFloat(v).toFixed(1)}
            </span>
          </td>
        );
      },
    },
    {
      key: "volume", label: "Volume", sortable: true,
      cell: (r) => <td className="px-3 py-2.5 tabular-nums text-slate-300">{fmtVol(r.volume)}</td>,
    },
    {
      key: "vol_ratio", label: "Vol Ratio", sortable: true,
      cell: (r) => {
        const v = parseFloat(r.vol_ratio);
        if (isNaN(v)) return <td className="px-3 py-2.5 text-slate-500">—</td>;
        const cls = v >= 2 ? "text-emerald-400" : v >= 1 ? "text-slate-400" : "text-red-400";
        return <td className={`px-3 py-2.5 tabular-nums ${cls}`}>{v.toFixed(2)}x</td>;
      },
    },
    {
      key: "vol_label", label: "Vol", sortable: true,
      cell: (r) => (
        <td className="px-3 py-2.5">
          {r.vol_label ? (
            <span className={`inline-block rounded-md px-1.5 py-0.5 text-[10px] font-semibold ${VOL_BADGE_CLS[r.vol_label] || "text-slate-400"}`}>
              {r.vol_label}
            </span>
          ) : "—"}
        </td>
      ),
    },
    {
      key: "vol_history", label: "Vol Bars", sortable: false,
      cell: (r) => (
        <td className="px-3 py-2.5">
          <VolBars history={r.vol_history} />
        </td>
      ),
    },
    {
      key: "pct_from_high", label: "% from High", sortable: true,
      cell: (r) => {
        const v = r.pct_from_high;
        if (v == null) return <td className="px-3 py-2.5 text-slate-500">—</td>;
        const color = v <= 10 ? "#3fb950" : v <= 20 ? "#d29922" : v <= 30 ? "#f85149" : "#64748b";
        return (
          <td className="px-3 py-2.5 tabular-nums">
            <span style={{ color }}>{parseFloat(v).toFixed(1)}%</span>
          </td>
        );
      },
    },
    {
      key: "atr_squeeze", label: "ATR Sq", sortable: false,
      cell: (r) => (
        <td className="px-3 py-2.5 text-center">
          {r.atr_squeeze ? (
            <span
              title="ATR squeeze active"
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ background: "#3fb950", boxShadow: "0 0 5px #3fb950" }}
            />
          ) : null}
        </td>
      ),
    },
    {
      key: "rmv_15", label: "RMV 15", sortable: true,
      cell: (r) => {
        if (r.rmv_15 == null) return <td className="px-3 py-2.5" />;
        const v = parseFloat(r.rmv_15);
        if (v < 20)
          return (
            <td className="px-3 py-2.5 text-center">
              <span
                title={`RMV 15: ${v.toFixed(1)}`}
                className="inline-block w-2.5 h-2.5 rounded-full"
                style={{ background: "#2ba5ff", boxShadow: "0 0 5px #2ba5ff" }}
              />
            </td>
          );
        return <td className="px-3 py-2.5" />;
      },
    },
    {
      key: "_rs_vs_spy", label: "RS v SPY", sortable: false,
      cell: (r) => {
        if (r.rs_line == null || r.rs_sma_50 == null)
          return <td className="px-3 py-2.5 text-slate-500">—</td>;
        const above = parseFloat(r.rs_line) > parseFloat(r.rs_sma_50);
        return (
          <td className="px-3 py-2.5">
            <span className={`inline-block rounded-md px-1.5 py-0.5 text-[10px] font-semibold ${
              above
                ? "bg-brand-600/15 text-brand-300 border border-brand-600/25"
                : "bg-amber-900/20 text-amber-400 border border-amber-800/30"
            }`}>
              {above ? "Above" : "Below"}
            </span>
          </td>
        );
      },
    },
    {
      key: "as_1m", label: "AS 1M", sortable: true,
      cell: (r) => {
        const v = r.as_1m;
        if (v == null) return <td className="px-3 py-2.5 text-slate-500">—</td>;
        const color = v >= 80 ? "#3fb950" : v >= 50 ? "#d29922" : "#f85149";
        return (
          <td className="px-3 py-2.5 tabular-nums">
            <span style={{ color, fontWeight: 700 }}>{v}</span>
          </td>
        );
      },
    },
  ];

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div className="flex h-full overflow-hidden">

      {/* ── Sidebar ─────────────────────────────────────────────────────────── */}
      <aside className="w-72 shrink-0 border-r border-slate-800 flex flex-col overflow-hidden bg-slate-900">
        <div className="flex items-center gap-2 px-4 py-3.5 border-b border-slate-800 shrink-0">
          <SlidersHorizontal className="w-4 h-4 text-brand-400" />
          <h2 className="text-sm font-semibold text-slate-200">Filters</h2>
          {badgeCount > 0 && (
            <span className="ml-auto text-[10px] bg-brand-600/20 text-brand-400 border border-brand-600/30 px-1.5 py-0.5 rounded-full font-semibold">
              {badgeCount} active
            </span>
          )}
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4">

          {/* Symbol — full history for one ticker */}
          <SidebarSection title="Symbol search">
            <p className="text-[10px] text-slate-500 mb-2 leading-relaxed">
              Enter a ticker and run the scanner to load that symbol&apos;s historical rows (date window ignored).
              Other filters still apply.
            </p>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />
              <input
                type="text"
                value={symbolDraft}
                onChange={handleSymbolInputChange}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    handleRunScan();
                  }
                }}
                placeholder="e.g. AAPL"
                name="pattern-symbol-search"
                autoComplete="off"
                autoCorrect="off"
                autoCapitalize="characters"
                spellCheck={false}
                className="w-full bg-slate-800 border border-slate-700 text-slate-200 placeholder-slate-600 rounded-lg pl-8 pr-2 py-2 text-xs font-mono uppercase focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/30"
              />
              {symbolHits.length > 0 && (
                <ul className="absolute z-20 left-0 right-0 top-full mt-1 max-h-40 overflow-auto rounded-lg border border-slate-700 bg-slate-900 shadow-xl">
                  {symbolHits.map((h) => (
                    <li key={h.ticker}>
                      <button
                        type="button"
                        onClick={() => pickSymbolHit(h.ticker)}
                        className="w-full text-left px-2.5 py-1.5 text-xs hover:bg-slate-800 flex flex-col gap-0.5"
                      >
                        <span className="font-mono font-semibold text-yellow-400">{h.ticker}</span>
                        {h.name && <span className="text-[10px] text-slate-500 truncate">{h.name}</span>}
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            {symbolApplied && (
              <p className="text-[10px] text-amber-500/90 mt-2">
                Active: <span className="font-mono font-semibold">{symbolApplied}</span>
                {" · "}
                <button
                  type="button"
                  onClick={() => {
                    clearPendingSymbolSearch();
                    setSymbolDraft("");
                    setSymbolApplied("");
                    setSymbolHits([]);
                    runScan(undefined, undefined, undefined, "");
                  }}
                  className="text-amber-400/90 underline hover:text-amber-300"
                >
                  Clear symbol
                </button>
              </p>
            )}
          </SidebarSection>

          {/* Date Window */}
          <SidebarSection title="Date Window">
            {symbolApplied && (
              <p className="text-[10px] text-slate-500 mb-2 leading-relaxed">
                Not used while a symbol search is active. Use &quot;Clear symbol&quot; above or pick a date tab in the main bar.
              </p>
            )}
            <FilterRow label="Look back">
              <select
                value={filters.days}
                disabled={!!symbolApplied}
                onChange={(e) => setDays(parseInt(e.target.value))}
                className="w-full bg-slate-800 border border-slate-700 text-slate-200 rounded-lg px-2 py-1.5 text-xs focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/30 transition appearance-none disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {tradingDates.length === 0 ? (
                  <option value={1}>Loading…</option>
                ) : (
                  tradingDates.map((item) => (
                    <option key={item.n} value={item.n}>
                      {fmtDateFull(item.date)}
                    </option>
                  ))
                )}
              </select>
            </FilterRow>
          </SidebarSection>

          {/* Price */}
          <SidebarSection title="Price">
            <FilterRow label="Min close">
              <NumInput value={filters.priceMin} placeholder="e.g. 10"
                onChange={(v) => setFilter("priceMin", v)} />
            </FilterRow>
            <FilterRow label="Max close">
              <NumInput value={filters.priceMax} placeholder="e.g. 500"
                onChange={(v) => setFilter("priceMax", v)} />
            </FilterRow>
          </SidebarSection>

          {/* Moving Averages */}
          <SidebarSection title="Moving Averages">
            <CbRow label="Close above SMA 50"  checked={filters.aboveSma50}  onChange={(v) => setFilter("aboveSma50", v)} />
            <CbRow label="Close below SMA 50"  checked={filters.belowSma50}  onChange={(v) => setFilter("belowSma50", v)} />
            <CbRow label="Close above SMA 150" checked={filters.aboveSma150} onChange={(v) => setFilter("aboveSma150", v)} />
            <CbRow label="Close above SMA 200" checked={filters.aboveSma200} onChange={(v) => setFilter("aboveSma200", v)} />
          </SidebarSection>

          {/* RSI 14 */}
          <SidebarSection title="RSI 14">
            <FilterRow label="Min RSI">
              <NumInput value={filters.rsiMin} placeholder="0"
                onChange={(v) => setFilter("rsiMin", v)} />
            </FilterRow>
            <FilterRow label="Max RSI">
              <NumInput value={filters.rsiMax} placeholder="100"
                onChange={(v) => setFilter("rsiMax", v)} />
            </FilterRow>
            <CbRow label="RSI above 50" checked={filters.rsiAbove50} onChange={(v) => setFilter("rsiAbove50", v)} />
            <CbRow label="RSI below 50" checked={filters.rsiBelow50} onChange={(v) => setFilter("rsiBelow50", v)} />
          </SidebarSection>

          {/* Volume */}
          <SidebarSection title="Volume">
            <p className="text-[10px] text-slate-500 mb-1.5">Volume label</p>
            <div className="flex flex-wrap gap-1 mb-2">
              {["XHigh","High","Med","Norm","Low"].map((lbl) => {
                const on = filters.volLabels.includes(lbl);
                const colors = {
                  XHigh: on ? "border-red-500/60 text-red-400 bg-red-500/10" : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-400",
                  High:  on ? "border-orange-500/60 text-orange-400 bg-orange-500/10" : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-400",
                  Med:   on ? "border-yellow-500/60 text-yellow-400 bg-yellow-500/10" : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-400",
                  Norm:  on ? "border-cyan-500/60 text-cyan-400 bg-cyan-500/10" : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-400",
                  Low:   on ? "border-teal-500/60 text-teal-400 bg-teal-500/10" : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-400",
                };
                return (
                  <button
                    key={lbl}
                    onClick={() => toggleVolLabel(lbl)}
                    className={`border rounded-md px-2 py-0.5 text-[10px] font-semibold transition-all select-none ${colors[lbl]}`}
                  >
                    {lbl}
                  </button>
                );
              })}
            </div>
            <FilterRow label="Vol/MA ≥">
              <NumInput value={filters.volRatioMin} placeholder="e.g. 2.0"
                onChange={(v) => setFilter("volRatioMin", v)} />
            </FilterRow>
          </SidebarSection>

          {/* 52-Week Range */}
          <SidebarSection title="52-Week Range">
            <FilterRow label="% from high ≤">
              <NumInput value={filters.pctFromHighMax} placeholder="e.g. 10"
                onChange={(v) => setFilter("pctFromHighMax", v)} />
            </FilterRow>
            <FilterRow label="% from low ≥">
              <NumInput value={filters.pctFromLowMin} placeholder="e.g. 50"
                onChange={(v) => setFilter("pctFromLowMin", v)} />
            </FilterRow>
          </SidebarSection>

          {/* ATR Compression */}
          <SidebarSection title="ATR Compression">
            <CbRow label="ATR squeeze active" checked={filters.atrSqueeze}
              onChange={(v) => setFilter("atrSqueeze", v)} />
            <CbRow label="Squeeze just released" checked={filters.atrSqueezeBreak}
              onChange={(v) => setFilter("atrSqueezeBreak", v)} />
            <FilterRow label="Consec. bars ≥">
              <NumInput value={filters.atrDeclBarsMin} placeholder="e.g. 5"
                onChange={(v) => setFilter("atrDeclBarsMin", v)} />
            </FilterRow>
          </SidebarSection>

          {/* RMV 15 */}
          <SidebarSection title="RMV 15">
            <CbRow label="Compressed (RMV 15 < 20)" checked={filters.rmv15Compressed}
              onChange={(v) => setFilter("rmv15Compressed", v)} />
            <FilterRow label="Max RMV">
              <NumInput value={filters.rmv15Max} placeholder="e.g. 30"
                onChange={(v) => setFilter("rmv15Max", v)} />
            </FilterRow>
          </SidebarSection>

          {/* Accumulation Score */}
          <SidebarSection title="Accum. Score (1M)">
            <FilterRow label="Min AS 1M">
              <NumInput value={filters.as1mMin} placeholder="e.g. 70"
                onChange={(v) => setFilter("as1mMin", v)} />
            </FilterRow>
            <FilterRow label="Max AS 1M">
              <NumInput value={filters.as1mMax} placeholder="e.g. 99"
                onChange={(v) => setFilter("as1mMax", v)} />
            </FilterRow>
          </SidebarSection>

          {/* RS Line vs SPY */}
          <SidebarSection title="RS Line vs SPY">
            <CbRow label="RS Line above RS SMA 50" checked={filters.rsAboveSma50}
              onChange={(v) => setFilter("rsAboveSma50", v)} />
            <CbRow label="RS Line below RS SMA 50" checked={filters.rsBelowSma50}
              onChange={(v) => setFilter("rsBelowSma50", v)} />
          </SidebarSection>

        </div>

        {/* Action buttons */}
        <div className="px-4 py-3 border-t border-slate-800 flex flex-col gap-2 shrink-0">
          <button
            onClick={handleRunScan}
            disabled={loading}
            className="flex items-center justify-center gap-2 w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold text-sm py-2.5 rounded-lg transition"
          >
            {loading
              ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
              : <Play className="w-3.5 h-3.5" />}
            Run Scanner
          </button>
          <button
            onClick={resetFilters}
            className="flex items-center justify-center gap-1.5 w-full text-slate-500 hover:text-slate-300 text-xs font-medium py-1.5 rounded-lg hover:bg-slate-800 transition"
          >
            <RotateCcw className="w-3 h-3" />
            Reset Filters
          </button>
        </div>
      </aside>

      {/* ── Main content ─────────────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col overflow-hidden">

        {/* Top bar: date tabs + count + sort */}
        <div className="flex flex-col border-b border-slate-800 bg-slate-900 shrink-0">
          {symbolApplied && (
            <div className="flex items-center gap-2 px-5 py-2 bg-amber-950/40 border-b border-amber-900/50 text-[11px] text-amber-200/95">
              <Search className="w-3.5 h-3.5 text-amber-400 shrink-0" />
              <span>
                Showing history for <span className="font-mono font-semibold text-amber-300">{symbolApplied}</span>
                <span className="text-amber-200/70"> — date range ignored</span>
              </span>
              <button
                type="button"
                onClick={() => {
                  clearPendingSymbolSearch();
                  setSymbolDraft("");
                  setSymbolApplied("");
                  setSymbolHits([]);
                  runScan(undefined, undefined, undefined, "");
                }}
                className="ml-auto text-amber-400/90 hover:text-amber-300 underline text-[11px] font-medium"
              >
                Clear symbol
              </button>
            </div>
          )}
          <div className="flex items-center gap-2 px-5 py-3 flex-wrap">
          <div className="flex gap-1 flex-wrap">
            {tradingDates.slice(0, 10).map((item) => (
              <button
                key={item.n}
                onClick={() => setDays(item.n)}
                className={`rounded-md px-2.5 py-1 text-xs font-medium border transition ${
                  filters.days === item.n && !symbolApplied
                    ? "border-brand-600/40 text-brand-400 bg-brand-600/10"
                    : "border-slate-700 text-slate-500 hover:border-slate-600 hover:text-slate-300 hover:bg-slate-800"
                }`}
              >
                {fmtDateShort(item.date)}
              </button>
            ))}
          </div>

          <div className="ml-auto flex items-center gap-2">
            {rows != null && (
              <span className="text-xs bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full border border-slate-700">
                {sortedRows.length} result{sortedRows.length !== 1 ? "s" : ""}
                {sortedRows.length === 500 && (
                  <span className="text-yellow-400 ml-1">(capped)</span>
                )}
              </span>
            )}
            {symbolApplied ? (
              <span className="text-xs text-slate-400 tabular-nums flex items-center gap-1.5">
                <span className="text-slate-500">Sort</span>
                <span className="text-slate-200 font-medium">Date</span>
                <ChevronDown className="w-3.5 h-3.5 text-brand-400" title="Newest first" />
                <span className="text-slate-500">(newest first)</span>
              </span>
            ) : (
              <>
                <span className="text-xs text-slate-500">Sort by</span>
                <select
                  value={sortBy}
                  onChange={(e) => handleSortByChange(e.target.value)}
                  className="bg-slate-800 border border-slate-700 text-slate-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:border-brand-500 transition"
                >
                  {SORT_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </select>
                <select
                  value={sortDir}
                  onChange={(e) => handleSortDirChange(e.target.value)}
                  className="bg-slate-800 border border-slate-700 text-slate-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:border-brand-500 transition"
                >
                  <option value="asc">Asc</option>
                  <option value="desc">Desc</option>
                </select>
              </>
            )}
          </div>
          </div>
        </div>

        {/* Results area */}
        <div className="flex-1 overflow-auto">

          {loading && (
            <div className="h-full flex flex-col items-center justify-center gap-3">
              <Loader2 className="w-6 h-6 text-brand-400 animate-spin" />
              <p className="text-sm text-slate-500">Running scan…</p>
            </div>
          )}

          {!loading && error && (
            <div className="m-6 flex items-start gap-3 bg-red-900/20 border border-red-800 rounded-xl p-4">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-red-300">Scan error</p>
                <p className="text-xs text-red-400 mt-1 font-mono">{error}</p>
              </div>
            </div>
          )}

          {!loading && !error && rows === null && (
            <div className="h-full flex flex-col items-center justify-center text-center px-4">
              <div className="w-16 h-16 bg-slate-800 rounded-2xl flex items-center justify-center mb-3 border border-slate-700">
                <TrendingUp className="w-8 h-8 text-slate-600" />
              </div>
              <p className="text-slate-400 text-sm font-medium">No scan run yet</p>
              <p className="text-slate-600 text-xs mt-1">Adjust filters and click Run Scanner</p>
            </div>
          )}

          {!loading && !error && rows !== null && sortedRows.length === 0 && (
            <div className="h-full flex flex-col items-center justify-center text-center px-4">
              <div className="w-16 h-16 bg-slate-800 rounded-2xl flex items-center justify-center mb-3 border border-slate-700">
                <TrendingUp className="w-8 h-8 text-slate-600" />
              </div>
              <p className="text-slate-400 text-sm font-medium">No results matched the filters</p>
              <p className="text-slate-600 text-xs mt-1">Try relaxing a filter or selecting a different date</p>
            </div>
          )}

          {!loading && !error && sortedRows.length > 0 && (
            <table className="w-full text-left border-collapse whitespace-nowrap">
              <thead className="sticky top-0 z-10">
                <tr className="bg-slate-900 border-b border-slate-800">
                  {cols.map((col) =>
                    col.sortable && !symbolApplied ? (
                      <th
                        key={col.key}
                        onClick={() => handleColSort(col.key)}
                        className="px-3 py-2.5 text-[10px] font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap cursor-pointer hover:text-brand-400 select-none transition-colors"
                      >
                        <span className="flex items-center gap-1">
                          {col.label}
                          {clientSort.col === col.key ? (
                            clientSort.dir === 1
                              ? <ChevronUp className="w-3 h-3 text-brand-400" />
                              : <ChevronDown className="w-3 h-3 text-brand-400" />
                          ) : null}
                        </span>
                      </th>
                    ) : (
                      <th
                        key={col.key}
                        className="px-3 py-2.5 text-[10px] font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap"
                      >
                        {col.label}
                      </th>
                    )
                  )}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => (
                  <tr
                    key={row.ticker + "_" + row.bar_date + "_" + i}
                    className="border-b border-slate-800 hover:bg-slate-800/40 transition-colors"
                  >
                    {cols.map((col) => cloneElement(col.cell(row), { key: col.key }))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* ── Chart modal ──────────────────────────────────────────────────────── */}
      {chartModal && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
          onClick={() => { setChartModal(null); refreshOpenTickers(); }}
        >
          <div
            className="relative bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl flex flex-col overflow-hidden"
            style={{ width: "95vw", height: "95vh" }}
            onClick={e => e.stopPropagation()}
          >
            {/* Modal header */}
            <div className="flex items-center gap-3 px-5 py-3 border-b border-slate-800 shrink-0">
              <span className="text-lg font-bold text-yellow-400">{chartModal.ticker}</span>
              <button
                onClick={() => { setChartModal(null); refreshOpenTickers(); }}
                className="ml-auto text-slate-500 hover:text-slate-200 transition"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            {/* Chart fills remaining height — explicit px height passed like Trade Ideas */}
            <div className="flex-1 min-h-0 overflow-hidden">
              {chartHeight != null && (
                <PatternAnalysisChart
                  ticker={chartModal.ticker}
                  height={chartHeight}
                  onClose={() => { setChartModal(null); refreshOpenTickers(); }}
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
