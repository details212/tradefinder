import { useState, useEffect, useRef } from "react";
import { stockApi } from "../api/client";
import { Search, X, Loader2 } from "lucide-react";

export default function StockSearch({ onSelect }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [searchError, setSearchError] = useState("");
  const debounceRef = useRef(null);
  const wrapperRef = useRef(null);

  useEffect(() => {
    const handleClick = (e) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setSearchError("");
      setOpen(false);
      return;
    }

    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      setLoading(true);
      setSearchError("");
      try {
        const res = await stockApi.search(query);
        setResults(res.data.results || []);
        setOpen(true);
      } catch (err) {
        const status = err.response?.status;
        const msg = err.response?.data?.error || err.message;
        setSearchError(`${status ? `[${status}] ` : ""}${msg}`);
        setResults([]);
        setOpen(true);
      } finally {
        setLoading(false);
      }
    }, 400);

    return () => clearTimeout(debounceRef.current);
  }, [query]);

  const handleSelect = (ticker) => {
    setQuery("");
    setOpen(false);
    onSelect(ticker);
  };

  return (
    <div ref={wrapperRef} className="relative w-full max-w-lg">
      <div className="relative">
        {loading ? (
          <Loader2 className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 animate-spin" />
        ) : (
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
        )}
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search ticker or company name…"
          className="w-full bg-slate-800 border border-slate-700 rounded-xl pl-9 pr-9 py-2.5 text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 transition text-sm"
        />
        {query && (
          <button
            onClick={() => {
              setQuery("");
              setOpen(false);
            }}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>

      {open && results.length > 0 && (
        <div className="absolute top-full mt-1.5 w-full bg-slate-800 border border-slate-700 rounded-xl shadow-2xl z-50 overflow-hidden">
          <ul className="max-h-72 overflow-y-auto py-1">
            {            results.map((r) => (
              <li key={r.ticker}>
                <button
                  onClick={() => handleSelect(r.ticker)}
                  className="w-full text-left flex items-center gap-3 px-4 py-2.5 hover:bg-slate-700 transition"
                >
                  <span className="text-sm font-bold text-brand-400 w-16 shrink-0">
                    {r.ticker}
                  </span>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-slate-300 truncate">{r.name}</p>
                    {r.sector && (
                      <p className="text-xs text-slate-500 truncate">{r.sector}</p>
                    )}
                  </div>
                  <div className="ml-auto text-right shrink-0">
                    {r.last_day_close != null && (
                      <p className="text-sm text-slate-300">
                        ${Number(r.last_day_close).toFixed(2)}
                      </p>
                    )}
                    <p className="text-xs text-slate-500">{r.primary_exchange}</p>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {open && searchError && (
        <div className="absolute top-full mt-1.5 w-full bg-slate-800 border border-red-700 rounded-xl shadow-2xl z-50 px-4 py-3 text-sm text-red-400">
          Error: {searchError}
        </div>
      )}

      {open && results.length === 0 && !loading && !searchError && query && (
        <div className="absolute top-full mt-1.5 w-full bg-slate-800 border border-slate-700 rounded-xl shadow-2xl z-50 px-4 py-3 text-sm text-slate-400">
          No results for "{query}"
        </div>
      )}
    </div>
  );
}
