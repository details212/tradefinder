import { useState, useEffect, useRef, useCallback } from "react";
import { createChart, ColorType, CrosshairMode, CandlestickSeries, HistogramSeries, createSeriesMarkers, LineStyle } from "lightweight-charts";
import { stockApi } from "../api/client";
import { Loader2, Radio } from "lucide-react";

// ── Intervals ─────────────────────────────────────────────────────────────────
// limit is set above the theoretical max bars for each range so Polygon
// always returns the full date window (6.5 trading hrs/day):
//   5m/15d  ≈ 10 trading days × 78 bars  = ~780
//   10m/30d ≈ 21 trading days × 39 bars  = ~820
//   15m/45d ≈ 31 trading days × 26 bars  = ~810
//   30m/60d ≈ 43 trading days × 13 bars  = ~560
//   1h/120d ≈ 85 trading days × 7  bars  = ~595
//   2h–4h   ≈ 85 trading days × 3–4 bars = ~340
//   1D/360d ≈ 252 bars   1W/4yr ≈ 208 bars
const INTERVALS = [
  { label: "5m",  multiplier: "5",  timespan: "minute", days: 15,   limit: 1500,  pollMs: 10000  },
  { label: "10m", multiplier: "10", timespan: "minute", days: 30,   limit: 1500,  pollMs: 15000  },
  { label: "15m", multiplier: "15", timespan: "minute", days: 45,   limit: 1500,  pollMs: 20000  },
  { label: "30m", multiplier: "30", timespan: "minute", days: 60,   limit: 1000,  pollMs: 30000  },
  { label: "1h",  multiplier: "1",  timespan: "hour",   days: 120,  limit: 1000,  pollMs: 60000  },
  { label: "2h",  multiplier: "2",  timespan: "hour",   days: 120,  limit: 500,   pollMs: 60000  },
  { label: "3h",  multiplier: "3",  timespan: "hour",   days: 120,  limit: 500,   pollMs: 60000  },
  { label: "4h",  multiplier: "4",  timespan: "hour",   days: 120,  limit: 500,   pollMs: 60000  },
  { label: "1D",  multiplier: "1",  timespan: "day",    days: 360,  limit: 500,   pollMs: 60000  },
  { label: "1W",  multiplier: "1",  timespan: "week",   days: 1460, limit: 500,   pollMs: 300000 },
];

function offsetDate(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().split("T")[0];
}

function fmtET(unixSec) {
  return new Date(unixSec * 1000).toLocaleString("en-US", {
    timeZone: "America/New_York",
    weekday: "short", month: "short", day: "numeric",
    year: "numeric",  hour: "2-digit", minute: "2-digit",
    hour12: false,
  }) + " ET";
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function StockChart({ ticker, defaultInterval, barTime, threshold, height = 480 }) {
  const [range, setRange] = useState(
    () => INTERVALS.find((i) => i.label === defaultInterval) ?? INTERVALS[2]
  );
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState("");
  const [live, setLive]         = useState(false);
  const [liveFlash, setLiveFlash] = useState(false);
  const [tooltip, setTooltip]   = useState(null);

  const containerRef    = useRef(null);
  const chartRef        = useRef(null);
  const candleSeriesRef = useRef(null);
  const volSeriesRef    = useRef(null);
  const pollRef         = useRef(null);
  const rangeRef        = useRef(range);
  const tickerRef       = useRef(ticker);

  useEffect(() => { rangeRef.current  = range;  }, [range]);
  useEffect(() => { tickerRef.current = ticker; }, [ticker]);

  // ── Build & tear down chart whenever ticker or range changes ────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    setError("");
    setLoading(true);
    setTooltip(null);

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#64748b",
      },
      grid: {
        vertLines: { color: "#1e293b" },
        horzLines: { color: "#1e293b" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#334155" },
      leftPriceScale:  { visible: false },
      timeScale: {
        borderColor: "#334155",
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: (time, tickMarkType) => {
          const d = new Date(time * 1000);
          const toET = (opts) =>
            new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", ...opts }).format(d);
          if (tickMarkType === 0) return toET({ year: "numeric" });
          if (tickMarkType === 1) return toET({ month: "short", year: "2-digit" });
          if (tickMarkType === 2) return toET({ month: "short", day: "numeric" });
          return toET({ hour: "2-digit", minute: "2-digit", hour12: false });
        },
      },
      localization: {
        timeFormatter: fmtET,
        priceFormatter: (p) => "$" + p.toFixed(2),
      },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor:      "#22c55e",
      downColor:    "#ef4444",
      borderVisible: false,
      wickUpColor:   "#22c55e",
      wickDownColor: "#ef4444",
    });

    // Threshold / resistance horizontal ray
    if (threshold != null) {
      candleSeries.createPriceLine({
        price:            Number(threshold),
        color:            "#ff00ff",
        lineWidth:        1,
        lineStyle:        LineStyle.Dashed,
        axisLabelVisible: true,
        title:            `Threshold $${Number(threshold).toFixed(2)}`,
      });
    }

    // Volume as a histogram in its own price scale, pinned to the bottom 20%
    const volSeries = chart.addSeries(HistogramSeries, {
      color:       "rgba(100,116,139,0.5)",
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    chart.priceScale("vol").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    // Crosshair → tooltip state
    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time ||
          param.point.x < 0 || param.point.y < 0) {
        setTooltip(null);
        return;
      }
      const c = param.seriesData.get(candleSeries);
      if (!c) { setTooltip(null); return; }
      const v = param.seriesData.get(volSeries);
      setTooltip({ time: param.time, ...c, vol: v?.value ?? null });
    });

    chartRef.current        = chart;
    candleSeriesRef.current = candleSeries;
    volSeriesRef.current    = volSeries;

    // Fetch bars
    const today = new Date().toISOString().split("T")[0];
    stockApi
      .history(ticker, {
        multiplier: range.multiplier,
        timespan:   range.timespan,
        from:       offsetDate(range.days),
        to:         today,
        limit:      range.limit,
      })
      .then((res) => {
        const bars = (res.data.bars || []).sort((a, b) => a.t - b.t);

        candleSeries.setData(
          bars.map((b) => ({ time: b.t / 1000, open: b.o, high: b.h, low: b.l, close: b.c }))
        );
        volSeries.setData(
          bars.map((b) => ({
            time:  b.t / 1000,
            value: b.v,
            color: b.c >= b.o ? "rgba(34,197,94,0.6)" : "rgba(239,68,68,0.6)",
          }))
        );
        // Watchlist signal marker — find closest bar to barTime
        if (barTime && bars.length > 0) {
          const targetMs = new Date(barTime).getTime();
          const closest  = bars.reduce((prev, curr) =>
            Math.abs(curr.t - targetMs) < Math.abs(prev.t - targetMs) ? curr : prev
          );
          createSeriesMarkers(candleSeries, [{
            time:     closest.t / 1000,
            position: "aboveBar",
            color:    "#ff00ff",
            shape:    "arrowDown",
            size:     2,
            text:     "⚑ Signal",
          }]);
        }

        // Default zoom: last 3 calendar days ending at the most recent bar
        if (bars.length > 0) {
          const to   = bars[bars.length - 1].t / 1000;
          const from = to - 3 * 24 * 60 * 60;
          chart.timeScale().setVisibleRange({ from, to });
        } else {
          chart.timeScale().fitContent();
        }
      })
      .catch(() => setError("Failed to load chart data"))
      .finally(() => setLoading(false));

    return () => {
      chart.remove();
      chartRef.current        = null;
      candleSeriesRef.current = null;
      volSeriesRef.current    = null;
    };
  }, [ticker, range]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Live poll ────────────────────────────────────────────────────────────────
  const poll = useCallback(async () => {
    const tkr = tickerRef.current;
    const rng = rangeRef.current;
    if (!tkr || !candleSeriesRef.current) return;
    try {
      const res = await stockApi.history(tkr, {
        multiplier: rng.multiplier,
        timespan:   rng.timespan,
        from:       offsetDate(1),   // yesterday → covers today's session
        to:         new Date().toISOString().split("T")[0],
        limit:      200,             // enough to reach the current forming bar
      });

      // Sort ascending so we feed bars in chronological order.
      // series.update() appends a new bar when time > last bar, or updates
      // the last bar in-place when time === last bar.
      // We wrap each call so "cannot update bars in the past" errors for
      // already-plotted bars are silently swallowed.
      const bars = (res.data.bars || []).sort((a, b) => a.t - b.t);
      bars.forEach((b) => {
        const vColor = b.c >= b.o ? "rgba(34,197,94,0.6)" : "rgba(239,68,68,0.6)";
        try {
          candleSeriesRef.current?.update({ time: b.t / 1000, open: b.o, high: b.h, low: b.l, close: b.c });
        } catch { /* past bar — ignore */ }
        try {
          volSeriesRef.current?.update({ time: b.t / 1000, value: b.v, color: vColor });
        } catch { /* past bar — ignore */ }
      });

      setLiveFlash((f) => !f);
    } catch { /* skip failed polls */ }
  }, []);

  useEffect(() => {
    clearInterval(pollRef.current);
    if (live) {
      poll();
      pollRef.current = setInterval(poll, range.pollMs);
    }
    return () => clearInterval(pollRef.current);
  }, [live, range, poll]);

  useEffect(() => { setLive(false); }, [ticker, range]);

  if (!ticker) return null;

  const fillParent = height === "100%";

  return (
    <div className={`bg-slate-800 rounded-2xl border border-slate-700 p-4${fillParent ? " flex flex-col h-full" : ""}`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-3 gap-3 flex-wrap">
        <div className="flex items-center gap-3 shrink-0">
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
            Price History
          </h3>
          <button
            onClick={() => setLive((v) => !v)}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium transition border ${
              live
                ? "bg-green-900/40 border-green-700 text-green-400"
                : "bg-slate-900 border-slate-700 text-slate-500 hover:text-slate-300"
            }`}
          >
            {live ? (
              <>
                <span className={`w-1.5 h-1.5 rounded-full bg-green-400 transition-opacity ${liveFlash ? "opacity-100" : "opacity-30"}`} />
                LIVE
              </>
            ) : (
              <><Radio className="w-3 h-3" /> Live</>
            )}
          </button>
          {live && (
            <span className="text-xs text-slate-600">
              updates every {range.pollMs / 1000}s
            </span>
          )}
        </div>

        {/* Interval selector */}
        <div className="flex bg-slate-900 rounded-lg p-0.5 gap-0.5 flex-wrap">
          {INTERVALS.map((r) => (
            <button
              key={r.label}
              onClick={() => setRange(r)}
              className={`px-2.5 py-1 text-xs font-medium rounded-md transition ${
                range.label === r.label
                  ? "bg-brand-600 text-white"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* Chart area */}
      <div className={fillParent ? "relative flex-1 min-h-0" : "relative"} style={fillParent ? undefined : { height }}>
        {/* OHLC + Volume tooltip — fixed to top-left */}
        {tooltip && (() => {
          const chg = tooltip.close - tooltip.open;
          const pct = ((chg / tooltip.open) * 100).toFixed(2);
          const col = chg >= 0 ? "#22c55e" : "#ef4444";
          const vol = tooltip.vol;
          const volFmt = vol == null ? "—"
            : vol >= 1e9 ? (vol / 1e9).toFixed(2) + "B"
            : vol >= 1e6 ? (vol / 1e6).toFixed(2) + "M"
            : vol >= 1e3 ? (vol / 1e3).toFixed(0) + "K"
            : String(vol);
          return (
            <div
              className="absolute z-10 pointer-events-none bg-slate-900/95 border border-slate-700 rounded-lg px-3 py-2 text-xs whitespace-nowrap"
              style={{ top: 8, left: 8 }}
            >
              <div className="text-slate-400 mb-1">{fmtET(tooltip.time)}</div>
              <span className="text-slate-500">O </span>
              <span style={{ color: col }}>${tooltip.open?.toFixed(2)}</span>
              <span className="text-slate-500"> H </span>
              <span className="text-slate-200">${tooltip.high?.toFixed(2)}</span>
              <span className="text-slate-500"> L </span>
              <span className="text-slate-200">${tooltip.low?.toFixed(2)}</span>
              <span className="text-slate-500"> C </span>
              <span style={{ color: col }}>${tooltip.close?.toFixed(2)}</span>
              <span style={{ color: col }} className="ml-2">
                {chg >= 0 ? "▲" : "▼"} {Math.abs(chg).toFixed(2)} ({pct}%)
              </span>
              <span className="text-slate-500 ml-3">Vol </span>
              <span className="text-slate-300">{volFmt}</span>
            </div>
          );
        })()}

        {/* Loading / error overlays */}
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-slate-800/60 z-20 rounded-xl">
            <Loader2 className="w-6 h-6 text-slate-500 animate-spin" />
          </div>
        )}
        {error && !loading && (
          <div className="absolute inset-0 flex items-center justify-center text-sm text-slate-500 z-20">
            {error}
          </div>
        )}

        {/* Chart mounts here — always in DOM so the ref is available */}
        <div ref={containerRef} className="w-full h-full" />
      </div>
    </div>
  );
}
