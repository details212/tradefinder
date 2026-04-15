import { TrendingUp, TrendingDown, Radio } from "lucide-react";
import { fmtEtString } from "../utils/timeUtils";

function Chip({ item, onClick }) {
  const isLong = item.direction?.toLowerCase() === "long";
  return (
    <button
      onClick={() => onClick(item)}
      title={`${item.strategy_name} · ${item.direction} · Click to open chart`}
      className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded border shrink-0 text-[11px] font-medium transition-opacity hover:opacity-80 cursor-pointer ${
        isLong
          ? "bg-green-900/30 border-green-800/50"
          : "bg-red-900/30 border-red-800/50"
      }`}
    >
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

export default function LiveStreamBar({ items, newCount, lastPoll, onClearNew, onClickItem }) {
  const hasItems = items.length > 0;
  // Scroll speed: ~80px per second; each chip ≈ 160px wide → 2s each, min 10s total
  const duration = Math.max(10, items.length * 3);

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
      <div className="flex-1 overflow-hidden relative h-full flex items-center">
        {!hasItems ? (
          <span className="text-[11px] text-slate-700 px-4 italic">
            No signals in the last 15 min
          </span>
        ) : (
          <div
            className="flex items-center gap-2.5 px-3 live-stream-scroll"
            style={{ animationDuration: `${duration}s` }}
          >
            {/* Duplicate list for seamless loop: animation moves -50% */}
            {[...items, ...items].map((item, i) => (
              <Chip key={i} item={item} onClick={onClickItem} />
            ))}
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
