import { useState, useEffect, useRef, useCallback } from "react";
import { stockApi } from "../api/client";
import {
  Search, Loader2, TrendingUp, ArrowRight,
  ChevronLeft, ChevronRight, ChevronDown, RefreshCw,
} from "lucide-react";

const PAGE_SIZE = 25;

function fmtVol(v) {
  if (v == null) return null;
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return String(v);
}

// Dark tint → saturated color as |pct| grows; saturates at ±3%
function heatColor(pct) {
  const t = Math.min(Math.abs(pct) / 3, 1);
  if (pct >= 0) {
    // green-950 (5,46,22) → green-500 (34,197,94)
    return `rgb(${lerp(5,34,t)},${lerp(46,197,t)},${lerp(22,94,t)})`;
  } else {
    // red-950 (69,10,10) → red-500 (239,68,68)
    return `rgb(${lerp(69,239,t)},${lerp(10,68,t)},${lerp(10,68,t)})`;
  }
}

function lerp(a, b, t) { return Math.round(a + (b - a) * t); }

function SectorHeatmap({ onSectorClick }) {
  const [data,    setData]    = useState([]);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState("");
  const [updated, setUpdated] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await stockApi.sectorHeatmap();
      const sectors = res.data.sectors || [];
      setData(sectors);
      if (sectors.length && sectors[0].last_updated) {
        setUpdated(new Date(sectors[0].last_updated));
      }
    } catch (err) {
      setError(err.response?.data?.error || err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8 text-slate-500 gap-2 text-sm">
        <Loader2 className="w-4 h-4 animate-spin" /> Loading sector data…
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-sm text-red-400 text-center py-4">{error}</div>
    );
  }

  if (!data.length) {
    return (
      <div className="text-sm text-slate-500 text-center py-4">
        No snapshot data yet — prices refresh every 60 s.
      </div>
    );
  }

      return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
          Sector Performance
        </p>
        <div className="flex items-center gap-3">
          {updated && (
            <span className="text-[11px] text-slate-600">
              Updated {updated.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
            </span>
          )}
          <button
            onClick={load}
            className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 hover:bg-slate-800 transition"
            title="Refresh"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Heatmap grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
        {data.map((s) => {
          const pct   = s.avg_change ?? 0;
          const bgCol = heatColor(pct);
          const sign  = pct > 0 ? "+" : "";

          return (
            <button
              key={s.sector}
              onClick={() => onSectorClick(s.sector)}
              style={{ backgroundColor: bgCol }}
              className="relative rounded-xl p-3 text-left hover:ring-2 hover:ring-white/30 transition-all group"
            >
              <p className="text-xs font-semibold text-white/80 leading-tight mb-1.5 truncate">
                {s.sector}
              </p>
              <p className={`text-lg font-bold leading-none ${pct >= 0 ? "text-green-100" : "text-red-100"}`}>
                {sign}{pct.toFixed(2)}%
              </p>
              <div className="flex items-center gap-2 mt-1.5">
                <span className="text-[10px] text-white/40">{s.total} stocks</span>
                <span className="text-[10px] text-green-200/70">▲{s.up_count}</span>
                <span className="text-[10px] text-red-200/70">▼{s.down_count}</span>
              </div>
            </button>
          );
        })}
      </div>

      {/* Legend */}
      <div className="flex items-center justify-center gap-3 mt-3">
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm bg-red-700" />
          <span className="text-[10px] text-slate-500">≤ −5%</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm bg-slate-800 border border-slate-700" />
          <span className="text-[10px] text-slate-500">Flat</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm bg-green-700" />
          <span className="text-[10px] text-slate-500">≥ +5%</span>
        </div>
      </div>
    </div>
  );
}

// options: array of { value, count } OR plain strings (backwards-compat)
function Select({ value, onChange, options, placeholder, disabled }) {
  const normalized = options.map((o) =>
    typeof o === "string" ? { value: o, count: null } : o
  );
  return (
    <div className="relative flex-1">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        className={`w-full appearance-none bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 pr-10 text-sm
          text-slate-100 focus:outline-none focus:border-brand-500 focus:ring-2 focus:ring-brand-500/30
          transition disabled:opacity-40 disabled:cursor-not-allowed ${!value ? "text-slate-400" : ""}`}
      >
        <option value="">{placeholder}</option>
        {normalized.map((o) => (
          <option key={o.value} value={o.value}>
            {o.value}{o.count != null ? ` (${o.count.toLocaleString()})` : ""}
          </option>
        ))}
      </select>
      <ChevronDown className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
    </div>
  );
}

export default function NewTrade({ onSelectTicker }) {
  // ── text search ─────────────────────────────────────────────────────────────
  const [query,      setQuery]      = useState("");
  const [results,    setResults]    = useState([]);
  const [searching,  setSearching]  = useState(false);
  const [searchErr,  setSearchErr]  = useState("");
  const debounceRef = useRef(null);
  const inputRef    = useRef(null);

  // ── dropdowns ───────────────────────────────────────────────────────────────
  const [sectors,    setSectors]    = useState([]);
  const [industries, setIndustries] = useState([]);
  const [selSector,  setSelSector]  = useState("");
  const [selIndustry, setSelIndustry] = useState("");

  // ── filtered table ──────────────────────────────────────────────────────────
  const [filterRows,    setFilterRows]    = useState([]);
  const [filterTotal,   setFilterTotal]   = useState(0);
  const [filterPage,    setFilterPage]    = useState(1);
  const [filterPages,   setFilterPages]   = useState(1);
  const [filterLoading, setFilterLoading] = useState(false);
  const [filterErr,     setFilterErr]     = useState("");

  // ── on mount: autofocus + load sectors ──────────────────────────────────────
  useEffect(() => {
    inputRef.current?.focus();
    stockApi.sectors()
      .then((r) => setSectors(r.data.sectors || []))   // [{value, count}]
      .catch(() => {});
  }, []);

  // ── when sector changes, reload industries & reset industry ─────────────────
  useEffect(() => {
    setSelIndustry("");
    setIndustries([]);
    if (!selSector) return;
    stockApi.industries(selSector)
      .then((r) => setIndustries(r.data.industries || []))  // [{value, count}]
      .catch(() => {});
  }, [selSector]);

  // ── fetch filtered results whenever sector / industry / page changes ─────────
  const fetchFiltered = useCallback(async (sector, industry, page) => {
    if (!sector && !industry) {
      setFilterRows([]);
      setFilterTotal(0);
      setFilterPages(1);
      return;
    }
    setFilterLoading(true);
    setFilterErr("");
    try {
      const res = await stockApi.search("", {
        params: { sector, industry, page, page_size: PAGE_SIZE },
      });
      setFilterRows(res.data.results  || []);
      setFilterTotal(res.data.total   ?? 0);
      setFilterPages(res.data.pages   ?? 1);
      setFilterPage(res.data.page     ?? page);
    } catch (err) {
      setFilterErr(err.response?.data?.error || err.message);
      setFilterRows([]);
    } finally {
      setFilterLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchFiltered(selSector, selIndustry, 1);
    setFilterPage(1);
  }, [selSector, selIndustry, fetchFiltered]);

  const goToPage = (p) => {
    const clamped = Math.max(1, Math.min(p, filterPages));
    setFilterPage(clamped);
    fetchFiltered(selSector, selIndustry, clamped);
  };

  // ── debounced text search ────────────────────────────────────────────────────
  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setSearchErr("");
      return;
    }
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      setSearching(true);
      setSearchErr("");
      try {
        const res = await stockApi.search(query);
        setResults(res.data.results || []);
      } catch (err) {
        setSearchErr(err.response?.data?.error || err.message);
        setResults([]);
      } finally {
        setSearching(false);
      }
    }, 350);
    return () => clearTimeout(debounceRef.current);
  }, [query]);

  const handleSelect = (ticker) => onSelectTicker(ticker);

  const hasFilter = selSector || selIndustry;

  return (
    <div className="flex flex-col items-center justify-start w-full h-full overflow-y-auto bg-slate-950/40 pt-16 pb-16 px-6">

      {/* Hero */}
      <div className="flex flex-col items-center mb-8 text-center">
        <div className="w-14 h-14 rounded-2xl bg-brand-600/20 border border-brand-500/40 flex items-center justify-center mb-5">
          <TrendingUp className="w-7 h-7 text-brand-400" />
        </div>
        <h1 className="text-3xl font-bold text-white mb-2 tracking-tight">New Trade</h1>
        <p className="text-slate-400 text-sm max-w-sm">
          Search by ticker, or browse by sector and industry.
        </p>
      </div>

      <div className="w-full max-w-3xl space-y-4">

        {/* Text search */}
        <div className="relative">
          {searching ? (
            <Loader2 className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400 animate-spin" />
          ) : (
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" />
          )}
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search ticker or company name…"
            className="w-full bg-slate-800 border border-slate-700 rounded-2xl pl-12 pr-5 py-4 text-slate-100 placeholder-slate-500
              focus:outline-none focus:border-brand-500 focus:ring-2 focus:ring-brand-500/30 transition text-base shadow-xl"
          />
        </div>

        {/* Sector / Industry dropdowns */}
        <div className="flex gap-3">
          <Select
            value={selSector}
            onChange={setSelSector}
            options={sectors}
            placeholder="All Sectors"
          />
          <Select
            value={selIndustry}
            onChange={setSelIndustry}
            options={industries}
            placeholder={selSector ? "All Industries" : "Select a sector first"}
            disabled={!selSector}
          />
          {(selSector || selIndustry) && (
            <button
              onClick={() => { setSelSector(""); setSelIndustry(""); }}
              className="px-4 py-2 text-xs text-slate-400 hover:text-slate-200 border border-slate-700 rounded-xl hover:border-slate-500 transition shrink-0"
            >
              Clear
            </button>
          )}
        </div>

        {/* ── Text search results ── */}
        {searchErr && (
          <p className="text-sm text-red-400 text-center">{searchErr}</p>
        )}

        {results.length > 0 && (
          <div className="bg-slate-800 border border-slate-700 rounded-2xl shadow-2xl overflow-hidden">
            {results.map((r, i) => (
              <button
                key={r.ticker}
                onClick={() => handleSelect(r.ticker)}
                className={`w-full flex items-center gap-4 px-5 py-3.5 hover:bg-slate-700 transition group text-left ${
                  i > 0 ? "border-t border-slate-700/60" : ""
                }`}
              >
                <span className="text-base font-bold text-brand-400 w-20 shrink-0 font-mono">
                  {r.ticker}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-slate-200 truncate group-hover:text-white">{r.name}</p>
                  {(r.sector || r.industry) && (
                    <p className="text-xs text-slate-500 truncate mt-0.5">
                      {[r.sector, r.industry].filter(Boolean).join(" · ")}
                    </p>
                  )}
                </div>
                <div className="text-right shrink-0 space-y-0.5">
                  {r.last_day_close != null && (
                    <p className="text-sm font-semibold text-slate-200 font-mono">
                      ${Number(r.last_day_close).toFixed(2)}
                    </p>
                  )}
                  {r.last_day_volume != null && (
                    <p className="text-xs text-slate-500">Vol {fmtVol(r.last_day_volume)}</p>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1 shrink-0 w-16">
                  {r.primary_exchange && (
                    <span className="text-[10px] text-slate-600 font-mono uppercase">
                      {r.primary_exchange.replace("XNAS", "NASDAQ").replace("XNYS", "NYSE")}
                    </span>
                  )}
                  <ArrowRight className="w-4 h-4 text-slate-600 group-hover:text-brand-400 transition" />
                </div>
              </button>
            ))}
          </div>
        )}

        {query.trim() && !searching && !searchErr && results.length === 0 && (
          <div className="bg-slate-800 border border-slate-700 rounded-2xl px-5 py-6 text-center">
            <p className="text-slate-500 text-sm">
              No results for <span className="text-slate-300 font-medium">"{query}"</span>
            </p>
          </div>
        )}

        {!query.trim() && !hasFilter && (
          <p className="text-xs text-slate-600 text-center">
            Try <span className="text-slate-500">AAPL</span>, <span className="text-slate-500">MSFT</span>, or use the dropdowns to browse by sector.
          </p>
        )}

        {/* ── Sector heatmap (shown when no text query and no filter active) ── */}
        {!query.trim() && !hasFilter && (
          <div className="mt-2 bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
            <SectorHeatmap
              onSectorClick={(sector) => {
                setSelSector(sector);
                setSelIndustry("");
              }}
            />
          </div>
        )}

        {/* ── Sector / Industry filtered table ── */}
        {hasFilter && (
          <div className="mt-2">
            {/* Table header */}
            <div className="flex items-center justify-between mb-3">
              <p className="text-sm text-slate-400">
                {filterLoading ? (
                  <span className="flex items-center gap-1.5">
                    <Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading…
                  </span>
                ) : (
                  <>
                    <span className="text-slate-200 font-medium">{filterTotal.toLocaleString()}</span> symbols
                    {selSector && <> in <span className="text-brand-400">{selSector}</span></>}
                    {selIndustry && <> · <span className="text-brand-300">{selIndustry}</span></>}
                  </>
                )}
              </p>
              {filterPages > 1 && (
                <p className="text-xs text-slate-500">
                  Page {filterPage} of {filterPages}
                </p>
              )}
            </div>

            {filterErr && (
              <p className="text-sm text-red-400 text-center mb-3">{filterErr}</p>
            )}

            {/* Table */}
            {filterRows.length > 0 && (
              <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
                {/* Column headings */}
                <div className="grid grid-cols-[5rem_1fr_7rem_6rem_7rem] gap-x-4 px-5 py-2.5 border-b border-slate-800 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
                  <span>Ticker</span>
                  <span>Name</span>
                  <span className="text-right">Price</span>
                  <span className="text-right">Volume</span>
                  <span className="text-right">Exchange</span>
                </div>

                {filterRows.map((r, i) => (
                  <button
                    key={r.ticker}
                    onClick={() => handleSelect(r.ticker)}
                    className={`w-full grid grid-cols-[5rem_1fr_7rem_6rem_7rem] gap-x-4 items-center px-5 py-3 hover:bg-slate-800 transition group text-left ${
                      i > 0 ? "border-t border-slate-800/60" : ""
                    }`}
                  >
                    <span className="text-sm font-bold text-brand-400 font-mono truncate">
                      {r.ticker}
                    </span>
                    <div className="min-w-0">
                      <p className="text-sm text-slate-200 truncate group-hover:text-white">{r.name}</p>
                      {r.industry && (
                        <p className="text-xs text-slate-600 truncate mt-0.5">{r.industry}</p>
                      )}
                    </div>
                    <span className="text-sm font-mono text-slate-300 text-right">
                      {r.last_day_close != null ? `$${Number(r.last_day_close).toFixed(2)}` : "—"}
                    </span>
                    <span className="text-xs text-slate-500 text-right">
                      {r.last_day_volume != null ? fmtVol(r.last_day_volume) : "—"}
                    </span>
                    <div className="flex items-center justify-end gap-1.5">
                      <span className="text-[10px] text-slate-600 font-mono uppercase truncate">
                        {(r.primary_exchange || "").replace("XNAS", "NASDAQ").replace("XNYS", "NYSE")}
                      </span>
                      <ArrowRight className="w-3.5 h-3.5 text-slate-700 group-hover:text-brand-400 transition shrink-0" />
                    </div>
                  </button>
                ))}
              </div>
            )}

            {/* Pagination controls */}
            {filterPages > 1 && (
              <div className="flex items-center justify-center gap-2 mt-4">
                <button
                  onClick={() => goToPage(filterPage - 1)}
                  disabled={filterPage <= 1 || filterLoading}
                  className="p-2 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500
                    disabled:opacity-30 disabled:cursor-not-allowed transition"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>

                {/* Page number buttons — show a window of up to 7 */}
                {(() => {
                  const half  = 3;
                  let start   = Math.max(1, filterPage - half);
                  let end     = Math.min(filterPages, start + 6);
                  start       = Math.max(1, end - 6);
                  return Array.from({ length: end - start + 1 }, (_, idx) => start + idx).map((p) => (
                    <button
                      key={p}
                      onClick={() => goToPage(p)}
                      disabled={filterLoading}
                      className={`w-8 h-8 rounded-lg text-sm font-medium transition ${
                        p === filterPage
                          ? "bg-brand-600 text-white border border-brand-500"
                          : "border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500"
                      } disabled:opacity-30`}
                    >
                      {p}
                    </button>
                  ));
                })()}

                <button
                  onClick={() => goToPage(filterPage + 1)}
                  disabled={filterPage >= filterPages || filterLoading}
                  className="p-2 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500
                    disabled:opacity-30 disabled:cursor-not-allowed transition"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
