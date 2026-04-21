import { useRef, useLayoutEffect, useState } from "react";
import { TrendingUp, TrendingDown, Radio } from "lucide-react";
import { fmtEtString } from "../utils/timeUtils";

function Chip({ item, isNew, onClick }) {
  const isLong = item.direction?.toLowerCase() === "long";
  return (
    <button
      onClick={() => onClick(item)}
      title={`${item.strategy_name} · ${item.direction} · Click to open chart${isNew ? " · New signal!" : ""}`}
      className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded border shrink-0 text-[11px] font-medium transition-all duration-700 hover:opacity-80 cursor-pointer ${
        isNew
          ? isLong
            ? "bg-green-800/70 border-green-500 shadow-[0_0_10px_rgba(74,222,128,0.4)]"
            : "bg-red-800/70   border-red-500   shadow-[0_0_10px_rgba(248,113,113,0.4)]"
          : isLong
            ? "bg-green-900/30 border-green-800/50"
            : "bg-red-900/30   border-red-800/50"
      }`}
    >
      {/* Pulsing amber dot shown only on new signals */}
      {isNew && (
        <span className="relative flex shrink-0 w-1.5 h-1.5">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-amber-400" />
        </span>
      )}
      <span className="text-slate-500 text-[10px] font-mono">S{item.strategy_id}</span>
      <span className={`font-bold tracking-wide ${isLong ? "text-green-300" : "text-red-300"}`}>
        {item.ticker}
      </span>
      {isLong
        ? <TrendingUp  className="w-2.5 h-2.5 text-green-400 shrink-0" />
        : <TrendingDown className="w-2.5 h-2.5 text-red-400  shrink-0" />}
      {item.close != null && (
        <span className="text-slate-400 tabular-nums">${Number(item.close).toFixed(2)}</span>
      )}
      <span className="text-slate-600 text-[10px]">{fmtEtString(item.bar_time)}</span>
    </button>
  );
}

export default function LiveStreamBar({ items, newCount, lastPoll, newKeys = new Set(), onClearNew, onClickItem }) {
  const hasItems = items.length > 0;

  // ── Overflow-only scroll ─────────────────────────────────────────────────
  // Render items ONCE. If they overflow the container we add a second copy so
  // the seamless loop works (translateX(-50%) = exactly one copy's width).
  // When items fit, they simply sit left-aligned with blank space to the right —
  // no duplication, no padding, no repetition.
  const outerRef = useRef(null);   // overflow-hidden container
  const innerRef = useRef(null);   // the chip row
  const [overflows, setOverflows] = useState(false);

  useLayoutEffect(() => {
    setOverflows(false); // reset on every items change; re-measure after paint
  }, [items]);

  useLayoutEffect(() => {
    if (!hasItems || overflows) return; // skip if already confirmed overflow
    const outer = outerRef.current;
    const inner = innerRef.current;
    if (!outer || !inner) return;
    if (inner.scrollWidth > outer.offsetWidth) setOverflows(true);
  }, [items, hasItems, overflows]);

  // When overflowing, duplicate for seamless loop; otherwise show once.
  const displayItems = hasItems && overflows ? [...items, ...items] : items;
  const duration     = Math.max(30, items.length * 8);

  return (
    <div className="flex items-center h-9 bg-slate-950 border-b border-slate-800/80 shrink-0 overflow-hidden no-drag">

      {/* ── Label ─────────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-1.5 px-4 shrink-0 border-r border-slate-800 h-full">
        <Radio className={`w-3 h-3 shrink-0 ${hasItems ? "text-red-500 animate-pulse" : "text-slate-700"}`} />
        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest whitespace-nowrap">
          Live Stream
        </span>
      </div>

      {/* ── Scroll area ───────────────────────────────────────────────────── */}
      <div ref={outerRef} className="flex-1 overflow-hidden relative h-full flex items-center">
        {!hasItems ? (
          <span className="text-[11px] text-slate-700 px-4 italic">
            No signals in the last 15 min
          </span>
        ) : (
          <div
            ref={innerRef}
            className={`flex items-center gap-2.5 px-3 ${overflows ? "live-stream-scroll" : ""}`}
            style={overflows ? { animationDuration: `${duration}s` } : undefined}
          >
            {displayItems.map((item, i) => {
              const itemKey = `${item.strategy_id}:${item.ticker}:${item.bar_time}`;
              return (
                <Chip key={i} item={item} isNew={newKeys.has(itemKey)} onClick={onClickItem} />
              );
            })}
          </div>
        )}
      </div>

      {/* ── "N new" badge ─────────────────────────────────────────────────── */}
      {newCount > 0 && (
        <button
          onClick={onClearNew}
          title="New signals since last check — click to dismiss"
          className="ml-1 mr-2 shrink-0 flex items-center gap-1 bg-yellow-500/20 border border-yellow-500/50 text-yellow-300 text-[10px] font-bold px-2 py-0.5 rounded-full hover:bg-yellow-500/30 transition whitespace-nowrap"
        >
          +{newCount} new
        </button>
      )}

      {/* ── Last poll timestamp ────────────────────────────────────────────── */}
      {lastPoll && (
        <span className="pr-4 shrink-0 text-[10px] text-slate-700 tabular-nums whitespace-nowrap">
          {lastPoll.toLocaleTimeString("en-US", {
            hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
          })}
        </span>
      )}
    </div>
  );
}
