import { useState, useEffect, useRef } from "react";
import { stockApi } from "../api/client";
import { Search, Loader2, TrendingUp, ArrowRight } from "lucide-react";

export default function NewTrade({ onSelectTicker }) {
  const [query,   setQuery]   = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState("");
  const debounceRef = useRef(null);
  const inputRef    = useRef(null);

  // Auto-focus the input on mount
  useEffect(() => { inputRef.current?.focus(); }, []);

  // Debounced search
  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setError("");
      return;
    }

    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      setLoading(true);
      setError("");
      try {
        const res = await stockApi.search(query);
        setResults(res.data.results || []);
      } catch (err) {
        const msg = err.response?.data?.error || err.message;
        setError(msg);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 350);

    return () => clearTimeout(debounceRef.current);
  }, [query]);

  const handleSelect = (ticker) => {
    onSelectTicker(ticker);
  };

  const fmtVol = (v) => {
    if (v == null) return null;
    if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
    return String(v);
  };

  return (
    <div className="flex flex-col items-center justify-start w-full h-full overflow-y-auto bg-slate-950/40 pt-24 pb-16 px-6">

      {/* Hero */}
      <div className="flex flex-col items-center mb-10 text-center">
        <div className="w-14 h-14 rounded-2xl bg-brand-600/20 border border-brand-500/40 flex items-center justify-center mb-5">
          <TrendingUp className="w-7 h-7 text-brand-400" />
        </div>
        <h1 className="text-3xl font-bold text-white mb-2 tracking-tight">New Trade</h1>
        <p className="text-slate-400 text-sm max-w-sm">
          Search for a ticker or company name to open a chart and plan your entry.
        </p>
      </div>

      {/* Search input */}
      <div className="w-full max-w-xl">
        <div className="relative">
          {loading ? (
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
            className="w-full bg-slate-800 border border-slate-700 rounded-2xl pl-12 pr-5 py-4 text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-2 focus:ring-brand-500/30 transition text-base shadow-xl"
          />
        </div>

        {/* Error */}
        {error && (
          <p className="mt-3 text-sm text-red-400 text-center">{error}</p>
        )}

        {/* Results */}
        {results.length > 0 && (
          <div className="mt-3 bg-slate-800 border border-slate-700 rounded-2xl shadow-2xl overflow-hidden">
            {results.map((r, i) => (
              <button
                key={r.ticker}
                onClick={() => handleSelect(r.ticker)}
                className={`w-full flex items-center gap-4 px-5 py-3.5 hover:bg-slate-700 transition group text-left ${
                  i > 0 ? "border-t border-slate-700/60" : ""
                }`}
              >
                {/* Ticker */}
                <span className="text-base font-bold text-brand-400 w-20 shrink-0 font-mono">
                  {r.ticker}
                </span>

                {/* Name + sector */}
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-slate-200 truncate group-hover:text-white">
                    {r.name}
                  </p>
                  {(r.sector || r.industry) && (
                    <p className="text-xs text-slate-500 truncate mt-0.5">
                      {[r.sector, r.industry].filter(Boolean).join(" · ")}
                    </p>
                  )}
                </div>

                {/* Price + volume */}
                <div className="text-right shrink-0 space-y-0.5">
                  {r.last_day_close != null && (
                    <p className="text-sm font-semibold text-slate-200 font-mono">
                      ${Number(r.last_day_close).toFixed(2)}
                    </p>
                  )}
                  {r.last_day_volume != null && (
                    <p className="text-xs text-slate-500">
                      Vol {fmtVol(r.last_day_volume)}
                    </p>
                  )}
                </div>

                {/* Exchange + arrow */}
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

        {/* Empty state */}
        {query.trim() && !loading && !error && results.length === 0 && (
          <div className="mt-3 bg-slate-800 border border-slate-700 rounded-2xl px-5 py-6 text-center">
            <p className="text-slate-500 text-sm">No results for <span className="text-slate-300 font-medium">"{query}"</span></p>
          </div>
        )}

        {/* Hint when empty */}
        {!query.trim() && (
          <p className="mt-4 text-xs text-slate-600 text-center">
            Try <span className="text-slate-500">AAPL</span>, <span className="text-slate-500">MSFT</span>, or <span className="text-slate-500">Apple</span>
          </p>
        )}
      </div>
    </div>
  );
}
