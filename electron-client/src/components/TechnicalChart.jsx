import { useState, useEffect } from "react";
import {
  ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { stockApi } from "../api/client";
import { format, fromUnixTime } from "date-fns";
import { Loader2 } from "lucide-react";

const INDICATORS = [
  { key: "sma50",  label: "SMA 50",  indicator: "sma", params: { window: 50 },  color: "#f59e0b" },
  { key: "sma200", label: "SMA 200", indicator: "sma", params: { window: 200 }, color: "#a78bfa" },
  { key: "ema20",  label: "EMA 20",  indicator: "ema", params: { window: 20 },  color: "#34d399" },
];

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-900 border border-slate-700 rounded-xl px-3 py-2 text-xs shadow-xl space-y-1">
      <p className="text-slate-400">{label}</p>
      {payload.map((p) => (
        <p key={p.dataKey} style={{ color: p.color }}>
          {p.name}: {p.value != null ? `$${Number(p.value).toFixed(2)}` : "—"}
        </p>
      ))}
    </div>
  );
};

export default function TechnicalChart({ ticker }) {
  const [priceData, setPriceData] = useState([]);
  const [overlays, setOverlays] = useState({});
  const [rsi, setRsi] = useState([]);
  const [macd, setMacd] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!ticker) return;
    setLoading(true);
    setError("");

    const today = new Date().toISOString().split("T")[0];
    const from = new Date();
    from.setFullYear(from.getFullYear() - 1);
    const fromStr = from.toISOString().split("T")[0];

    const requests = [
      stockApi.history(ticker, { from: fromStr, to: today, limit: 365 }),
      ...INDICATORS.map((ind) =>
        stockApi.indicator(ticker, ind.indicator, { ...ind.params, limit: 365 })
      ),
      stockApi.indicator(ticker, "rsi", { window: 14, limit: 365 }),
      stockApi.indicator(ticker, "macd", { short_window: 12, long_window: 26, signal_window: 9, limit: 365 }),
    ];

    Promise.allSettled(requests).then((results) => {
      const [histRes, ...rest] = results;

      // Price bars
      if (histRes.status === "fulfilled") {
        const bars = (histRes.value.data.bars || []).map((b) => ({
          date: format(fromUnixTime(b.t / 1000), "MMM d"),
          timestamp: b.t,
          price: b.c,
          volume: b.v,
        }));
        setPriceData(bars);
      }

      // Overlay indicators (SMA50, SMA200, EMA20)
      const newOverlays = {};
      INDICATORS.forEach((ind, i) => {
        const res = rest[i];
        if (res.status === "fulfilled") {
          const vals = res.value.data.values || [];
          newOverlays[ind.key] = Object.fromEntries(
            vals.map((v) => [format(new Date(v.timestamp), "MMM d"), v.value])
          );
        }
      });
      setOverlays(newOverlays);

      // RSI
      const rsiRes = rest[INDICATORS.length];
      if (rsiRes.status === "fulfilled") {
        setRsi(
          (rsiRes.value.data.values || []).map((v) => ({
            date: format(new Date(v.timestamp), "MMM d"),
            rsi: v.value,
          }))
        );
      }

      // MACD
      const macdRes = rest[INDICATORS.length + 1];
      if (macdRes.status === "fulfilled") {
        setMacd(
          (macdRes.value.data.values || []).map((v) => ({
            date: format(new Date(v.timestamp), "MMM d"),
            macd: v.value,
            signal: v.signal,
            histogram: v.histogram,
          }))
        );
      }
    }).catch(() => setError("Failed to load technical data"))
      .finally(() => setLoading(false));
  }, [ticker]);

  // Merge overlay data into price bars
  const chartData = priceData.map((bar) => ({
    ...bar,
    sma50: overlays.sma50?.[bar.date],
    sma200: overlays.sma200?.[bar.date],
    ema20: overlays.ema20?.[bar.date],
  }));

  if (loading) return (
    <div className="flex justify-center py-12">
      <Loader2 className="w-6 h-6 animate-spin text-slate-500" />
    </div>
  );

  if (error) return <p className="text-center text-slate-500 py-12">{error}</p>;

  const rsiReversed = [...rsi].reverse();
  const macdReversed = [...macd].reverse();

  return (
    <div className="space-y-5">
      {/* Price + Moving Averages */}
      <div className="bg-slate-800 rounded-2xl border border-slate-700 p-4">
        <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
          Price + Moving Averages
        </h3>
        <ResponsiveContainer width="100%" height={240}>
          <ComposedChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
            <YAxis tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={(v) => `$${v.toFixed(0)}`} width={55} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#94a3b8" }} />
            <Bar dataKey="volume" name="Volume" fill="#1e40af" opacity={0.3} yAxisId={0} />
            <Line type="monotone" dataKey="price" name="Price" stroke="#3b82f6" strokeWidth={1.5} dot={false} />
            <Line type="monotone" dataKey="sma50" name="SMA 50" stroke="#f59e0b" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line type="monotone" dataKey="sma200" name="SMA 200" stroke="#a78bfa" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line type="monotone" dataKey="ema20" name="EMA 20" stroke="#34d399" strokeWidth={1.5} dot={false} strokeDasharray="2 2" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* RSI */}
      {rsiReversed.length > 0 && (
        <div className="bg-slate-800 rounded-2xl border border-slate-700 p-4">
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1">
            RSI (14)
          </h3>
          <p className="text-xs text-slate-600 mb-3">Overbought &gt;70 · Oversold &lt;30</p>
          <ResponsiveContainer width="100%" height={120}>
            <ComposedChart data={rsiReversed} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis domain={[0, 100]} tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} width={35} />
              <Tooltip formatter={(v) => [v?.toFixed(2), "RSI"]} />
              <Line type="monotone" dataKey="rsi" stroke="#f472b6" strokeWidth={1.5} dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* MACD */}
      {macdReversed.length > 0 && (
        <div className="bg-slate-800 rounded-2xl border border-slate-700 p-4">
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
            MACD (12, 26, 9)
          </h3>
          <ResponsiveContainer width="100%" height={140}>
            <ComposedChart data={macdReversed} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} width={45} />
              <Tooltip formatter={(v) => [v?.toFixed(4)]} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#94a3b8" }} />
              <Bar dataKey="histogram" name="Histogram" fill="#64748b" opacity={0.6} />
              <Line type="monotone" dataKey="macd" name="MACD" stroke="#3b82f6" strokeWidth={1.5} dot={false} />
              <Line type="monotone" dataKey="signal" name="Signal" stroke="#f97316" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
