import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  BarChart2,
  ChevronDown,
  ChevronUp,
  Loader2,
  RefreshCw,
  Scale,
  Search,
  TrendingDown,
  TrendingUp,
  X,
} from "lucide-react";
import { alpacaApi } from "../api/client";
import TradeReviewModal from "./TradeReviewModal";
import {
  analyzeClosedTrades,
  compareAlpacaDetail,
  EXIT_LABELS,
  summarizeAudits,
} from "../utils/closedTradeAudit";

const FILTERS = [
  { id: "all",     label: "All" },
  { id: "flagged", label: "Flagged" },
  { id: "entry",   label: "Entry slip" },
  { id: "exit",    label: "Exit slip" },
  { id: "data",    label: "Data errors" },
];

function signed$(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v).toFixed(digits);
  if (Math.abs(v) < 0.005) return `$${abs}`;
  return v > 0 ? `+$${abs}` : `-$${abs}`;
}

function signedN(v, digits = 3) {
  if (v == null || Number.isNaN(v)) return "—";
  const n = Number(v).toFixed(digits);
  if (Math.abs(v) < 0.0005) return n;
  return v > 0 ? `+${n}` : n;
}

function slipCls(v, loose = 0.005) {
  if (v == null || Number.isNaN(v)) return "text-slate-500";
  if (v > loose) return "text-red-400";
  if (v < -loose) return "text-emerald-400";
  return "text-slate-400";
}

function plCls(v) {
  if (v == null || Number.isNaN(v)) return "text-slate-500";
  if (v > 0.005) return "text-emerald-400";
  if (v < -0.005) return "text-red-400";
  return "text-slate-400";
}

function fmtWhen(ms) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function Kpi({ label, value, sub, color = "text-slate-100" }) {
  return (
    <div className="bg-slate-800/60 border border-slate-700/60 rounded-xl px-3 py-2.5 min-w-0">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</p>
      <p className={`text-sm font-bold font-mono tabular-nums leading-tight mt-0.5 truncate ${color}`}>{value}</p>
      {sub && <p className="text-[10px] text-slate-500 mt-0.5 truncate">{sub}</p>}
    </div>
  );
}

function SortTh({ id, label, sort, onSort, className = "", align = "left" }) {
  const active = sort.key === id;
  return (
    <button
      type="button"
      onClick={() => onSort(id)}
      className={`text-[10px] font-semibold uppercase tracking-wider transition ${
        active ? "text-brand-400" : "text-slate-500 hover:text-slate-300"
      } ${align === "right" ? "text-right" : align === "center" ? "text-center" : "text-left"} ${className}`}
    >
      {label}
      {active && (sort.dir === "desc" ? " ↓" : " ↑")}
    </button>
  );
}

function FlagPills({ flags, compact = false }) {
  if (!flags.length) return <span className="text-[10px] text-slate-600">Clean</span>;
  const shown = compact ? flags.slice(0, 2) : flags;
  return (
    <div className="flex flex-wrap gap-1">
      {shown.map((f) => (
        <span
          key={f.key}
          title={f.detail}
          className={`text-[9px] font-semibold px-1.5 py-0.5 rounded border leading-none ${
            f.severity === "error"
              ? "bg-red-950/60 text-red-300 border-red-800/60"
              : f.severity === "warn"
                ? "bg-amber-950/50 text-amber-300 border-amber-800/50"
                : "bg-slate-800 text-slate-400 border-slate-700"
          }`}
        >
          {f.label}
        </span>
      ))}
      {compact && flags.length > 2 && (
        <span className="text-[9px] text-slate-500">+{flags.length - 2}</span>
      )}
    </div>
  );
}

function DigestRow({ label, value, color = "text-slate-200" }) {
  return (
    <div className="flex items-center justify-between gap-3 py-0.5">
      <span className="text-[11px] text-slate-500">{label}</span>
      <span className={`text-[11px] font-mono tabular-nums ${color}`}>{value}</span>
    </div>
  );
}

function AlpacaVerify({ row }) {
  const [state, setState] = useState(null);

  const run = useCallback(() => {
    if (!row.order.alpaca_order_id) return;
    setState({ loading: true });
    alpacaApi.getOrderDetail(row.order.alpaca_order_id)
      .then((res) => setState({ data: compareAlpacaDetail(row, res.data) }))
      .catch((err) => {
        setState({ error: err?.response?.data?.error || "Could not fetch Alpaca order." });
      });
  }, [row]);

  useEffect(() => { run(); }, [run]);

  if (state?.loading) {
    return (
      <div className="flex items-center gap-2 text-[11px] text-slate-500 py-2">
        <Loader2 className="w-3.5 h-3.5 animate-spin" /> Checking Alpaca legs…
      </div>
    );
  }
  if (state?.error) {
    return <p className="text-[11px] text-red-400 py-2">{state.error}</p>;
  }
  const d = state?.data;
  if (!d) return null;

  const matchFill = d.fillDelta == null || Math.abs(d.fillDelta) <= 0.015;
  const matchPl = d.plDelta == null || Math.abs(d.plDelta) <= 1;
  const matchMethod = !d.inferredMethod || d.inferredMethod === row.order.exit_method
    || (d.inferredMethod === "manual" && (row.order.exit_method === "manual"
      || row.order.exit_method === "auto_close_tp"
      || row.order.exit_method === "auto_close_sl"));

  return (
    <div className="mt-2 rounded-lg border border-slate-700/70 bg-slate-950/40 px-3 py-2">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-1.5">
        Alpaca reconcile
      </p>
      <DigestRow
        label="Entry fill"
        value={d.alpacaFill != null ? `$${d.alpacaFill.toFixed(4)}` : "—"}
        color={matchFill ? "text-emerald-400" : "text-red-400"}
      />
      <DigestRow
        label="DB vs Alpaca fill"
        value={d.fillDelta != null ? `${signedN(d.fillDelta, 4)}` : "—"}
        color={matchFill ? "text-slate-400" : "text-red-400"}
      />
      <DigestRow
        label="Filled exit leg"
        value={d.alpacaExit != null
          ? `$${d.alpacaExit.toFixed(4)} (${d.filledLeg?.type ?? "?"})`
          : "No filled TP/SL leg"}
        color={d.alpacaExit != null ? "text-slate-200" : "text-amber-400"}
      />
      <DigestRow
        label="P/L from Alpaca fills"
        value={signed$(d.impliedPl)}
        color={plCls(d.impliedPl)}
      />
      <DigestRow
        label="Stored P/L − Alpaca P/L"
        value={signed$(d.plDelta)}
        color={matchPl ? "text-slate-400" : "text-red-400"}
      />
      <DigestRow
        label="Alpaca exit inference"
        value={d.inferredMethod ? (EXIT_LABELS[d.inferredMethod] ?? d.inferredMethod) : "—"}
        color={matchMethod ? "text-slate-300" : "text-red-400"}
      />
      {!matchPl && d.plDelta != null && (
        <p className="text-[10px] text-red-300/90 mt-1.5">
          Stored P/L does not match a close at the filled Alpaca leg. The last open mark was probably kept as the final P/L.
        </p>
      )}
      {!d.alpacaExit && (
        <p className="text-[10px] text-amber-300/90 mt-1.5">
          No bracket leg filled — this was a market/manual close. Exit price in the table is inferred from stored P/L only.
        </p>
      )}
    </div>
  );
}

function ExpandedRow({ row, onReview }) {
  return (
    <div className="col-span-full bg-slate-950/50 border-t border-slate-800 px-4 py-3">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-1">Levels</p>
          <DigestRow
            label="Account"
            value={row.order.paper_mode ? "Paper" : "Live"}
            color={row.order.paper_mode ? "text-blue-400" : "text-slate-200"}
          />
          <DigestRow label="Limit" value={row.limit != null ? `$${row.limit.toFixed(2)}` : "—"} />
          <DigestRow label="Fill" value={row.fill != null ? `$${row.fill.toFixed(2)}` : "—"} color={slipCls(row.entrySlipPerShare)} />
          <DigestRow label="Stop" value={row.stop != null ? `$${row.stop.toFixed(2)}` : "—"} />
          <DigestRow label="Target" value={row.target != null ? `$${row.target.toFixed(2)}` : "—"} />
          <DigestRow
            label="Inferred exit"
            value={row.exitPrice != null ? `$${row.exitPrice.toFixed(2)}` : "—"}
            color={slipCls(row.exitSlipPerShare)}
          />
          <DigestRow
            label="Reference exit"
            value={row.refExit != null ? `$${row.refExit.toFixed(2)}` : row.exitType === "manual" ? "n/a (manual)" : "—"}
          />
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-1">Slippage & P/L</p>
          <DigestRow label="Entry slip /sh" value={`${signedN(row.entrySlipPerShare)}`} color={slipCls(row.entrySlipPerShare)} />
          <DigestRow label="Entry slip $" value={signed$(row.entrySlipDollar)} color={slipCls(row.entrySlipDollar)} />
          <DigestRow
            label="Exit slip /sh"
            value={row.exitSlipPerShare != null ? signedN(row.exitSlipPerShare) : "n/a"}
            color={row.exitSlipPerShare != null ? slipCls(row.exitSlipPerShare) : "text-slate-600"}
          />
          <DigestRow label="Exit slip $" value={signed$(row.exitSlipDollar)} color={slipCls(row.exitSlipDollar)} />
          <DigestRow label="Round-trip $" value={signed$(row.roundTripDollar)} color={slipCls(row.roundTripDollar)} />
          <DigestRow
            label="Expected P/L at method"
            value={signed$(row.expectedPl)}
            color={plCls(row.expectedPl)}
          />
          <DigestRow label="Stored P/L" value={signed$(row.pl)} color={plCls(row.pl)} />
          <DigestRow
            label="R result"
            value={row.rResult != null ? `${signedN(row.rResult, 2)}R` : "—"}
            color={plCls(row.rResult)}
          />
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-1">Flags</p>
          {row.flags.length === 0 ? (
            <p className="text-[11px] text-slate-500">No integrity or slippage flags.</p>
          ) : (
            <ul className="space-y-1.5">
              {row.flags.map((f) => (
                <li key={f.key} className="text-[11px] leading-snug">
                  <span className={f.severity === "error" ? "text-red-300" : f.severity === "warn" ? "text-amber-300" : "text-slate-400"}>
                    {f.label}.
                  </span>{" "}
                  <span className="text-slate-500">{f.detail}</span>
                </li>
              ))}
            </ul>
          )}
          <button
            type="button"
            onClick={() => onReview(row.order)}
            className="mt-3 inline-flex items-center gap-1.5 text-[11px] font-medium text-brand-400 hover:text-brand-300 transition"
          >
            <BarChart2 className="w-3.5 h-3.5" /> Open trade review
          </button>
          <AlpacaVerify row={row} />
        </div>
      </div>
    </div>
  );
}

export default function ClosedTradesPanel({ onClose }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastAt, setLastAt] = useState(null);
  const [filter, setFilter] = useState("all");
  const [query, setQuery] = useState("");
  const [modeFilter, setModeFilter] = useState("all");
  const [methodFilter, setMethodFilter] = useState("all");
  const [sort, setSort] = useState({ key: "severity", dir: "desc" });
  const [expandedId, setExpandedId] = useState(null);
  const [reviewOrder, setReviewOrder] = useState(null);

  const load = useCallback((useSync = false) => {
    setLoading(true);
    const req = useSync ? alpacaApi.syncOrders() : alpacaApi.getOrders();
    req
      .then((r) => {
        const list = Array.isArray(r.data) ? r.data : (r.data?.orders ?? []);
        setOrders(list);
        setLastAt(Date.now());
        setError(null);
      })
      .catch(() => setError("Could not load closed trades."))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(false); }, [load]);

  const rows = useMemo(() => analyzeClosedTrades(orders), [orders]);

  const methods = useMemo(() => {
    const set = new Set(rows.map((r) => r.order.exit_method).filter(Boolean));
    return [...set].sort();
  }, [rows]);

  const filtered = useMemo(() => {
    const needle = query.trim().toUpperCase();
    return rows.filter((r) => {
      if (filter === "flagged" && !r.flags.length) return false;
      if (filter === "entry" && !r.hasEntryIssue) return false;
      if (filter === "exit" && !r.hasExitIssue) return false;
      if (filter === "data" && !r.hasDataIssue) return false;
      if (modeFilter === "paper" && !r.order.paper_mode) return false;
      if (modeFilter === "live" && r.order.paper_mode) return false;
      if (methodFilter === "none" && r.order.exit_method) return false;
      if (methodFilter !== "all" && methodFilter !== "none" && r.order.exit_method !== methodFilter) return false;
      if (needle && !String(r.order.ticker || "").toUpperCase().includes(needle)) return false;
      return true;
    });
  }, [rows, filter, query, modeFilter, methodFilter]);

  const sorted = useMemo(() => {
    const list = filtered.slice();
    const { key, dir } = sort;
    const mul = dir === "asc" ? 1 : -1;
    const val = (r) => {
      switch (key) {
        case "ticker": return r.order.ticker || "";
        case "fill": return r.fill ?? -Infinity;
        case "entry": return r.entrySlipDollar ?? -Infinity;
        case "exit": return r.exitSlipDollar ?? -Infinity;
        case "pl": return r.pl ?? -Infinity;
        case "date": return r.closedAtMs ?? 0;
        case "severity": return r.severity;
        default: return 0;
      }
    };
    list.sort((a, b) => {
      const av = val(a);
      const bv = val(b);
      if (typeof av === "string") return av.localeCompare(bv) * mul;
      if (av === bv) return (b.closedAtMs ?? 0) - (a.closedAtMs ?? 0);
      return (av < bv ? -1 : 1) * mul;
    });
    return list;
  }, [filtered, sort]);

  const stats = useMemo(() => summarizeAudits(filtered), [filtered]);
  const allStats = useMemo(() => summarizeAudits(rows), [rows]);

  const onSort = (key) => {
    setSort((prev) => (
      prev.key === key
        ? { key, dir: prev.dir === "desc" ? "asc" : "desc" }
        : { key, dir: key === "ticker" ? "asc" : "desc" }
    ));
  };

  return (
    <div className="flex flex-col h-full min-w-0">
      <div className="flex items-start justify-between gap-3 px-4 py-3 border-b border-slate-700 shrink-0">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <Scale className="w-4 h-4 text-brand-400" />
            Closed Trades
          </h2>
          <p className="text-[11px] text-slate-500 mt-0.5">
            Fill & close audit · {allStats.count} closed
            {lastAt ? ` · ${new Date(lastAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : ""}
          </p>
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          <button
            type="button"
            onClick={() => load(true)}
            disabled={loading}
            title="Sync with Alpaca and re-audit"
            className="text-slate-500 hover:text-slate-200 disabled:opacity-40 transition p-1"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />
          </button>
          {onClose && (
            <button type="button" onClick={onClose} className="text-slate-500 hover:text-slate-300 transition p-1">
              <X className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {error && (
          <div className="mx-4 mt-4 flex items-center gap-2 text-[12px] text-red-300 bg-red-950/40 border border-red-900/50 rounded-lg px-3 py-2">
            <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
            {error}
          </div>
        )}

        <div className="grid grid-cols-3 xl:grid-cols-6 gap-2 p-4">
          <Kpi label="Closed" value={stats.count} sub={`${stats.wins}W / ${stats.losses}L`} />
          <Kpi label="Net P/L" value={signed$(stats.netPl)} color={plCls(stats.netPl)} />
          <Kpi
            label="Entry slip"
            value={signed$(stats.entrySlipDollar)}
            color={slipCls(stats.entrySlipDollar)}
            sub={stats.avgEntrySlipPs != null ? `${signedN(stats.avgEntrySlipPs)}/sh avg` : undefined}
          />
          <Kpi
            label="Exit slip"
            value={signed$(stats.exitSlipDollar)}
            color={slipCls(stats.exitSlipDollar)}
            sub={stats.avgExitSlipPs != null ? `${signedN(stats.avgExitSlipPs)}/sh avg` : "vs TP/SL only"}
          />
          <Kpi
            label="Flagged"
            value={String(stats.flagged)}
            color={stats.errors ? "text-red-400" : stats.flagged ? "text-amber-300" : "text-emerald-400"}
            sub={`${stats.errors} errors`}
          />
          <Kpi
            label="Stale P/L?"
            value={String(stats.stalePl)}
            color={stats.stalePl ? "text-red-400" : "text-slate-200"}
            sub="close ≠ last mark"
          />
        </div>

        <div className="px-4 pb-3 flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-1.5">
            {FILTERS.map((f) => (
              <button
                key={f.id}
                type="button"
                onClick={() => setFilter(f.id)}
                className={`text-[11px] px-2.5 py-1 rounded-full border transition ${
                  filter === f.id
                    ? "bg-brand-600/20 text-brand-300 border-brand-600/40"
                    : "bg-slate-800/40 text-slate-400 border-slate-700 hover:text-slate-200"
                }`}
              >
                {f.label}
                {f.id === "flagged" && ` ${allStats.flagged}`}
                {f.id === "entry" && ` ${allStats.entryIssues}`}
                {f.id === "exit" && ` ${allStats.exitIssues}`}
                {f.id === "data" && ` ${allStats.dataIssues}`}
              </button>
            ))}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="relative flex-1 min-w-[8rem]">
              <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Ticker"
                className="w-full bg-slate-800/70 border border-slate-700 rounded-lg pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none focus:border-brand-600/50"
              />
            </div>
            <select
              value={modeFilter}
              onChange={(e) => setModeFilter(e.target.value)}
              className="bg-slate-800/70 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-slate-300"
            >
              <option value="all">Paper + live</option>
              <option value="paper">Paper</option>
              <option value="live">Live</option>
            </select>
            <select
              value={methodFilter}
              onChange={(e) => setMethodFilter(e.target.value)}
              className="bg-slate-800/70 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-slate-300"
            >
              <option value="all">Any exit</option>
              <option value="none">Unknown exit</option>
              {methods.map((m) => (
                <option key={m} value={m}>{EXIT_LABELS[m] ?? m}</option>
              ))}
            </select>
          </div>
        </div>

        {loading && !rows.length ? (
          <div className="flex items-center justify-center py-16 text-slate-500">
            <Loader2 className="w-5 h-5 animate-spin" />
          </div>
        ) : sorted.length === 0 ? (
          <p className="px-4 py-10 text-center text-xs text-slate-500">
            No closed trades match this filter.
          </p>
        ) : (
          <div className="min-w-0 overflow-x-auto">
            <div className="grid grid-cols-[5.2rem_1.6rem_4.2rem_4.4rem_4.4rem_4.4rem_5rem_4.8rem_minmax(7rem,1fr)_5.4rem] gap-x-2 px-4 py-2 border-y border-slate-800/80">
              <SortTh id="ticker" label="Ticker" sort={sort} onSort={onSort} />
              <span />
              <SortTh id="fill" label="Fill" sort={sort} onSort={onSort} />
              <SortTh id="entry" label="E.Slip" sort={sort} onSort={onSort} />
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Exit</span>
              <SortTh id="exit" label="X.Slip" sort={sort} onSort={onSort} />
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Method</span>
              <SortTh id="pl" label="P/L" sort={sort} onSort={onSort} />
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Flags</span>
              <SortTh id="date" label="Closed" sort={sort} onSort={onSort} align="right" />
            </div>

            <div className="divide-y divide-slate-800/50">
              {sorted.map((r) => {
                const open = expandedId === r.order.id;
                return (
                  <div key={r.order.id}>
                    <button
                      type="button"
                      onClick={() => setExpandedId(open ? null : r.order.id)}
                      className={`w-full grid grid-cols-[5.2rem_1.6rem_4.2rem_4.4rem_4.4rem_4.4rem_5rem_4.8rem_minmax(7rem,1fr)_5.4rem] gap-x-2 px-4 py-2 items-center text-left hover:bg-slate-800/40 transition ${
                        r.severity === 3 ? "bg-red-950/20" : r.severity === 2 ? "bg-amber-950/10" : ""
                      }`}
                    >
                      <div className="flex items-center gap-1 min-w-0">
                        {open ? <ChevronUp className="w-3 h-3 text-slate-500 shrink-0" /> : <ChevronDown className="w-3 h-3 text-slate-500 shrink-0" />}
                        <span className="text-xs font-bold text-slate-100 truncate">{r.order.ticker}</span>
                        {r.order.paper_mode && (
                          <span className="text-[8px] font-semibold text-blue-400/80 shrink-0">P</span>
                        )}
                      </div>
                      <span className="flex justify-center">
                        {r.isLong
                          ? <TrendingUp className="w-3.5 h-3.5 text-emerald-400" />
                          : <TrendingDown className="w-3.5 h-3.5 text-red-400" />}
                      </span>
                      <span className="text-[11px] font-mono text-slate-200 tabular-nums">
                        {r.fill != null ? `$${r.fill.toFixed(2)}` : "—"}
                      </span>
                      <span className={`text-[11px] font-mono tabular-nums ${slipCls(r.entrySlipPerShare)}`}>
                        {r.entrySlipPerShare != null ? `${signedN(r.entrySlipPerShare, 2)}` : "—"}
                      </span>
                      <span className="text-[11px] font-mono text-slate-300 tabular-nums">
                        {r.exitPrice != null ? `$${r.exitPrice.toFixed(2)}` : "—"}
                      </span>
                      <span className={`text-[11px] font-mono tabular-nums ${slipCls(r.exitSlipPerShare)}`}>
                        {r.exitSlipPerShare != null ? `${signedN(r.exitSlipPerShare, 2)}` : "—"}
                      </span>
                      <span className="text-[10px] text-slate-400 truncate">
                        {r.order.exit_method ? (EXIT_LABELS[r.order.exit_method] ?? r.order.exit_method) : "—"}
                      </span>
                      <span className={`text-[11px] font-mono font-semibold tabular-nums ${plCls(r.pl)}`}>
                        {signed$(r.pl)}
                      </span>
                      <FlagPills flags={r.flags} compact />
                      <span className="text-[10px] text-slate-500 text-right tabular-nums">{fmtWhen(r.closedAtMs)}</span>
                    </button>
                    {open && <ExpandedRow row={r} onReview={setReviewOrder} />}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {reviewOrder && (
        <TradeReviewModal
          order={reviewOrder}
          onClose={() => setReviewOrder(null)}
          onTradeClosed={() => { setReviewOrder(null); load(true); }}
        />
      )}
    </div>
  );
}
