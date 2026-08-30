import { useEffect, useRef, useState } from "react";
import { ChevronDown } from "lucide-react";

function looksLikeCrypto(ticker) {
  const raw = String(ticker || "");
  const compact = raw.replace(/[-/]/g, "").toUpperCase();
  return /^X:/i.test(raw) || /^(?:[A-Z]{2,})(USD|USDT|USDC|EUR|GBP)$/.test(compact);
}

/**
 * Toolbar control: pick a bracket limit or a simple market fill.
 * Crypto has no Alpaca brackets — market only.
 * onSelect("limit" | "market")
 */
export default function OpenOrderMenu({ onSelect, ticker }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const crypto = looksLikeCrypto(ticker);

  useEffect(() => {
    if (!open) return undefined;
    const onDoc = (e) => {
      if (!ref.current?.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  const pick = (type) => {
    setOpen(false);
    onSelect(type);
  };

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1 px-3 py-1.5 rounded text-xs font-semibold bg-yellow-500/20 hover:bg-yellow-500/30 text-yellow-300 border border-yellow-500/50 transition shadow-sm animate-pulse"
      >
        Open Order
        <ChevronDown className={`w-3 h-3 transition ${open ? "rotate-180" : ""}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 w-56 rounded-lg border border-slate-600 bg-slate-800 shadow-xl overflow-hidden">
          {!crypto && (
            <button
              type="button"
              onClick={() => pick("limit")}
              className="w-full text-left px-3 py-2.5 text-xs hover:bg-slate-700 transition"
            >
              <span className="block text-yellow-300 font-semibold">Bracket limit</span>
              <span className="block text-slate-500 mt-0.5 leading-snug">
                Limit entry with take-profit and stop
              </span>
            </button>
          )}
          <button
            type="button"
            onClick={() => pick("market")}
            className={`w-full text-left px-3 py-2.5 text-xs hover:bg-slate-700 transition ${crypto ? "" : "border-t border-slate-700"}`}
          >
            <span className="block text-blue-300 font-semibold">Market order</span>
            <span className="block text-slate-500 mt-0.5 leading-snug">
              {crypto
                ? "Fill now — stop and target are placed on Alpaca after the fill"
                : "Fill now, long or short from the R/R"}
            </span>
          </button>
        </div>
      )}
    </div>
  );
}
