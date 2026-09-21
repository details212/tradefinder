import { useCallback, useEffect, useMemo, useState } from "react";
import Highcharts from "highcharts";
import HighchartsReact from "highcharts-react-official";
import { alpacaApi } from "../api/client";
import { alpacaTsMs, historyToAccountSeries } from "../utils/alpacaPortfolio";
import {
  BarChart2,
  RefreshCw,
  Loader2,
  AlertCircle,
  ExternalLink,
  Building2,
} from "lucide-react";

const EQUITY_PERIODS = [
  { id: "day", label: "Day" },
  { id: "week", label: "Week" },
  { id: "month", label: "Month" },
  { id: "all", label: "All" },
];

const PAPER_DASHBOARD = "https://app.alpaca.markets/paper/dashboard/overview";
const LIVE_DASHBOARD = "https://app.alpaca.markets/live/dashboard/overview";

const MONEY_KEYS = new Set([
  "buying_power", "regt_buying_power", "daytrading_buying_power",
  "effective_buying_power", "non_marginable_buying_power", "options_buying_power",
  "cash", "cash_withdrawable", "cash_transferable", "accrued_fees",
  "pending_transfer_out", "pending_transfer_in", "portfolio_value", "equity",
  "last_equity", "last_buying_power", "last_cash", "last_initial_margin",
  "last_long_market_value", "last_maintenance_margin", "last_options_buying_power",
  "last_regt_buying_power", "last_short_market_value", "long_market_value",
  "short_market_value", "position_market_value", "initial_margin",
  "maintenance_margin", "sma", "intraday_adjustments", "pending_reg_taf_fees",
  "bod_dtbp",
]);

const PCT_KEYS = new Set([
  "unrealized_plpc", "unrealized_intraday_plpc", "change_today",
]);

const DATE_KEYS = new Set([
  "created_at", "balance_asof", "previous_close", "timestamp",
  "next_open", "next_close",
]);

const SKIP_KEYS = new Set(["admin_configurations", "user_configurations"]);

const GROUPS = [
  {
    id: "identity",
    title: "Account",
    keys: [
      "account_number", "id", "status", "crypto_status", "currency",
      "created_at", "clearing_broker", "crypto_tier",
    ],
  },
  {
    id: "balances",
    title: "Balances",
    keys: [
      "equity", "last_equity", "portfolio_value", "cash", "last_cash",
      "cash_withdrawable", "cash_transferable",
      "long_market_value", "last_long_market_value",
      "short_market_value", "last_short_market_value",
      "position_market_value",
      "accrued_fees", "pending_transfer_in", "pending_transfer_out",
      "pending_reg_taf_fees", "intraday_adjustments",
      "balance_asof", "previous_close",
    ],
  },
  {
    id: "buying_power",
    title: "Buying power & margin",
    keys: [
      "buying_power", "last_buying_power", "effective_buying_power",
      "regt_buying_power", "last_regt_buying_power",
      "daytrading_buying_power", "bod_dtbp",
      "non_marginable_buying_power",
      "options_buying_power", "last_options_buying_power",
      "multiplier", "initial_margin", "last_initial_margin",
      "maintenance_margin", "last_maintenance_margin", "sma",
    ],
  },
  {
    id: "permissions",
    title: "Permissions & restrictions",
    keys: [
      "pattern_day_trader", "daytrade_count", "shorting_enabled",
      "trading_blocked", "account_blocked", "transfers_blocked",
      "trade_suspended_by_user",
      "options_approved_level", "options_trading_level",
    ],
  },
];

const CONFIG_LABELS = {
  dtbp_check: "Day-trade buying-power check",
  trade_confirm_email: "Trade confirmation emails",
  suspend_trade: "Trading suspended",
  max_margin_multiplier: "Max margin multiplier",
  max_options_trading_level: "Max options trading level",
  pdt_check: "Pattern-day-trader check",
  nip_disable: "NIP disabled",
  no_shorting: "Shorting disabled",
  fractional_trading: "Fractional trading",
  closing_transactions_only: "Closing transactions only",
  disable_overnight_trading: "Overnight trading disabled",
  ptp_no_exception_entry: "PTP no-exception entry",
};

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function fmtMoney(v, currency = "USD") {
  const n = num(v);
  if (n == null) return "—";
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: currency || "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function fmtPct(v) {
  const n = num(v);
  if (n == null) return "—";
  const pct = Math.abs(n) <= 1.5 ? n * 100 : n;
  return `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
}

function fmtDate(v) {
  if (v == null || v === "") return "—";
  const s = String(v);
  const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T00:00:00` : s);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function prettyKey(key) {
  return String(key || "")
    .replace(/_/g, " ")
    .replace(/\bdtbp\b/i, "DTBP")
    .replace(/\bsma\b/i, "SMA")
    .replace(/\bpdt\b/i, "PDT")
    .replace(/\bnip\b/i, "NIP")
    .replace(/\bid\b/i, "ID")
    .replace(/\breg taf\b/i, "Reg TAF")
    .replace(/\bregt\b/i, "Reg T")
    .replace(/\bbod\b/i, "BOD");
}

function formatValue(key, value, currency = "USD") {
  if (value == null || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "object") {
    try { return JSON.stringify(value); } catch { return String(value); }
  }
  if (MONEY_KEYS.has(key)) return fmtMoney(value, currency);
  if (PCT_KEYS.has(key)) return fmtPct(value);
  if (DATE_KEYS.has(key)) return fmtDate(value);
  return String(value);
}

function moneyColor(v) {
  const n = num(v);
  if (n == null || n === 0) return "text-slate-200";
  return n > 0 ? "text-emerald-400" : "text-red-400";
}

function boolBadge(v) {
  if (v === true) {
    return "bg-emerald-900/40 text-emerald-400 border-emerald-700/50";
  }
  if (v === false) {
    return "bg-slate-800 text-slate-400 border-slate-700/60";
  }
  return "bg-slate-800 text-slate-300 border-slate-700/60";
}

function statusBadge(status) {
  const s = String(status || "").toUpperCase();
  if (s === "ACTIVE") return "bg-emerald-900/50 text-emerald-400 border-emerald-700/50";
  if (s === "ACCOUNT_UPDATED" || s === "APPROVAL_PENDING" || s === "SUBMITTED") {
    return "bg-amber-900/50 text-amber-400 border-amber-700/50";
  }
  if (s.includes("REJECT") || s.includes("SUSPEND") || s === "INACTIVE") {
    return "bg-red-900/50 text-red-400 border-red-700/50";
  }
  return "bg-slate-800 text-slate-300 border-slate-700/60";
}

function openExternal(url) {
  if (!url) return;
  if (window.electronAPI?.openExternal) window.electronAPI.openExternal(url);
  else window.open(url, "_blank", "noopener,noreferrer");
}

function StatCard({ label, value, sub, color }) {
  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl px-4 py-3">
      <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</p>
      <p className={`text-lg font-bold font-mono tabular-nums mt-1 ${color ?? "text-slate-100"}`}>{value}</p>
      {sub && <p className="text-[11px] text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
}

function FieldRow({ label, value, valueClass, badge }) {
  return (
    <div className="flex items-start justify-between gap-4 py-2 border-b border-slate-800/80 last:border-0">
      <span className="text-sm text-slate-400 shrink-0">{label}</span>
      {badge ? (
        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold border ${badge}`}>
          {value}
        </span>
      ) : (
        <span className={`text-sm font-mono tabular-nums text-right break-all ${valueClass ?? "text-slate-200"}`}>
          {value}
        </span>
      )}
    </div>
  );
}

function Section({ title, children, extra }) {
  return (
    <section className="bg-slate-900/60 border border-slate-800 rounded-xl p-4">
      <div className="flex items-center justify-between gap-3 mb-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-500">{title}</h2>
        {extra}
      </div>
      {children}
    </section>
  );
}

function periodNoun(period) {
  if (period === "day") return "session";
  if (period === "week") return "week";
  if (period === "month") return "month";
  return "account";
}

function EquityChart({ currency }) {
  const [period, setPeriod] = useState("all");
  const [hist, setHist] = useState(null);
  const [err, setErr] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErr(null);
    alpacaApi.portfolioHistory(period)
      .then((r) => {
        if (cancelled) return;
        if (r.data?.ok) {
          setHist(r.data);
          setErr(null);
        } else {
          setHist(null);
          setErr(r.data?.error || "Alpaca history unavailable.");
        }
      })
      .catch(() => {
        if (!cancelled) {
          setHist(null);
          setErr("Could not load account equity history.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [period]);

  const series = useMemo(() => (hist ? historyToAccountSeries(hist) : null), [hist]);

  const chartOpts = useMemo(() => {
    const points = series?.equityPoints || [];
    if (!points.length) return null;
    const data = points
      .map((p) => {
        const ms = alpacaTsMs(p.timeMs);
        if (ms == null || p.equity == null) return null;
        return [ms, Number(p.equity)];
      })
      .filter(Boolean);
    if (!data.length) return null;
    const first = data[0][1];
    const last = data[data.length - 1][1];
    const up = last >= first;
    const color = up ? "#34d399" : "#f87171";
    return {
      time: { timezone: "America/New_York" },
      chart: {
        type: "area",
        height: 260,
        backgroundColor: "transparent",
        style: { fontFamily: "inherit" },
        marginTop: 12,
        spacing: [8, 8, 8, 8],
      },
      title: { text: null },
      credits: { enabled: false },
      legend: { enabled: false },
      xAxis: {
        type: "datetime",
        lineColor: "#334155",
        tickColor: "#334155",
        labels: { style: { color: "#64748b", fontSize: "10px" } },
        gridLineColor: "transparent",
      },
      yAxis: {
        title: { text: null },
        gridLineColor: "#1e293b",
        labels: {
          style: { color: "#64748b", fontSize: "10px" },
          formatter() { return fmtMoney(this.value, currency); },
        },
      },
      tooltip: {
        backgroundColor: "#0f172a",
        borderColor: "#334155",
        borderRadius: 6,
        style: { color: "#e2e8f0", fontSize: "11px" },
        formatter() {
          const when = new Date(this.x).toLocaleString("en-US", { timeZone: "America/New_York" });
          return `<span style="color:#94a3b8">${when}</span><br/><b>Equity</b> ${fmtMoney(this.y, currency)}`;
        },
      },
      plotOptions: {
        area: {
          threshold: null,
          animation: false,
          marker: { enabled: false, radius: 2, states: { hover: { enabled: true } } },
          lineWidth: 2,
          fillOpacity: 0.18,
        },
      },
      series: [{
        name: "Equity",
        data,
        color,
        fillColor: {
          linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
          stops: [
            [0, up ? "rgba(52, 211, 153, 0.35)" : "rgba(248, 113, 113, 0.35)"],
            [1, "rgba(15, 23, 42, 0)"],
          ],
        },
      }],
    };
  }, [series, currency]);

  const periodChange = useMemo(() => {
    const pts = series?.equityPoints || [];
    if (pts.length < 2) return null;
    const first = pts[0].equity;
    const last = pts[pts.length - 1].equity;
    if (first == null || last == null) return null;
    return { delta: last - first, pct: first !== 0 ? (last - first) / first : null };
  }, [series]);

  const periodToggle = (
    <span className="flex items-center rounded border border-slate-700/70 overflow-hidden shrink-0">
      {EQUITY_PERIODS.map((p) => (
        <button
          key={p.id}
          type="button"
          onClick={() => setPeriod(p.id)}
          className={`px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide transition ${
            period === p.id
              ? "bg-brand-500/20 text-brand-400"
              : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
        >
          {p.label}
        </button>
      ))}
    </span>
  );

  return (
    <Section title="Account equity" extra={periodToggle}>
      <div className="flex items-baseline justify-between gap-3 mb-1">
        <p className="text-[11px] text-slate-500">
          Alpaca portfolio history for this {periodNoun(period)} — not TradeFinder ticket P/L.
        </p>
        {periodChange && (
          <p className={`text-sm font-mono tabular-nums font-semibold ${moneyColor(periodChange.delta)}`}>
            {periodChange.delta >= 0 ? "+" : ""}{fmtMoney(periodChange.delta, currency)}
            {periodChange.pct != null && (
              <span className="text-slate-500 font-normal ml-1.5">{fmtPct(periodChange.pct)}</span>
            )}
          </p>
        )}
      </div>
      {loading ? (
        <div className="h-[260px] flex items-center justify-center text-slate-500 text-sm gap-2">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading equity…
        </div>
      ) : err ? (
        <div className="h-[120px] flex items-center text-sm text-amber-400">{err}</div>
      ) : !chartOpts ? (
        <div className="h-[120px] flex items-center text-sm text-slate-500">
          No equity history for this range.
        </div>
      ) : (
        <HighchartsReact highcharts={Highcharts} options={chartOpts} />
      )}
    </Section>
  );
}

export default function ExchangePanel({ onOpenBrokerage }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [payload, setPayload] = useState(null);
  const [fetchedAt, setFetchedAt] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await alpacaApi.account();
      const d = r.data;
      if (!d?.ok) {
        setPayload(d || null);
        setError(d?.error || "Could not load Alpaca account.");
      } else {
        setPayload(d);
        setFetchedAt(new Date());
      }
    } catch (err) {
      const msg = err?.response?.data?.error || err?.message || "Could not load Alpaca account.";
      setError(msg);
      setPayload(err?.response?.data || null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const account = payload?.account || {};
  const config = payload?.configurations || {};
  const clock = payload?.clock || {};
  const positions = payload?.positions || [];
  const paper = payload?.paper === true;
  const currency = account.currency || "USD";

  const dayPl = useMemo(() => {
    const eq = num(account.equity);
    const last = num(account.last_equity);
    if (eq == null || last == null) return null;
    return eq - last;
  }, [account.equity, account.last_equity]);

  const leftover = useMemo(() => {
    const known = new Set(GROUPS.flatMap((g) => g.keys));
    SKIP_KEYS.forEach((k) => known.add(k));
    return Object.keys(account).filter((k) => !known.has(k) && account[k] != null && account[k] !== "");
  }, [account]);

  const posUnrealized = useMemo(
    () => positions.reduce((s, p) => s + (num(p.unrealized_pl) ?? 0), 0),
    [positions],
  );

  if (loading && !payload) {
    return (
      <div className="h-full flex items-center justify-center text-slate-500 text-sm gap-2">
        <Loader2 className="w-4 h-4 animate-spin" /> Connecting to Alpaca…
      </div>
    );
  }

  const notConfigured = /not configured/i.test(error || "");

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-5xl mx-auto px-6 py-8 flex flex-col gap-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-2.5">
              <BarChart2 className="w-5 h-5 text-brand-400" />
              <h1 className="text-lg font-semibold text-slate-100">Exchange</h1>
            </div>
            <p className="text-sm text-slate-500 mt-0.5">
              Live snapshot from Alpaca&apos;s trading API for the currently selected paper or live account.
            </p>
          </div>
          <button
            type="button"
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium text-slate-300 bg-slate-800 hover:bg-slate-700 border border-slate-700 disabled:opacity-50 transition"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
        </div>

        {error && (
          <div className="flex items-start gap-2 px-3 py-2.5 rounded-lg text-sm bg-red-900/30 text-red-400 border border-red-800/50">
            <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
            <div>
              <p>{error}</p>
              {notConfigured && onOpenBrokerage && (
                <button
                  type="button"
                  onClick={onOpenBrokerage}
                  className="mt-1.5 inline-flex items-center gap-1 text-brand-400 hover:text-brand-300 font-medium"
                >
                  <Building2 className="w-3.5 h-3.5" />
                  Open Brokerage settings
                </button>
              )}
            </div>
          </div>
        )}

        {payload?.ok && (
          <>
            <div className={`rounded-xl border px-4 py-3 flex flex-wrap items-center justify-between gap-3 ${
              paper
                ? "bg-sky-950/40 border-sky-800/50"
                : "bg-amber-950/30 border-amber-800/50"
            }`}>
              <div>
                <div className="flex items-center gap-2">
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold uppercase tracking-wider border ${
                    paper
                      ? "bg-sky-900/50 text-sky-300 border-sky-700/50"
                      : "bg-amber-900/50 text-amber-300 border-amber-700/50"
                  }`}>
                    {paper ? "Paper" : "Live"}
                  </span>
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold border ${statusBadge(account.status)}`}>
                    {account.status || "Unknown"}
                  </span>
                  {clock.is_open != null && (
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold border ${
                      clock.is_open
                        ? "bg-emerald-900/50 text-emerald-400 border-emerald-700/50"
                        : "bg-slate-800 text-slate-400 border-slate-700/60"
                    }`}>
                      Market {clock.is_open ? "open" : "closed"}
                    </span>
                  )}
                </div>
                <p className="text-xs text-slate-400 mt-1.5 font-mono break-all">
                  {payload.base_url}
                </p>
                <p className="text-xs text-slate-500 mt-1 max-w-2xl">
                  {paper
                    ? "Simulated paper account. Fills, balances, and P/L are not real money. Transfers, borrow fees, and fill emails do not apply."
                    : "Live Alpaca account. Balances and orders are real money."}
                </p>
              </div>
              <button
                type="button"
                onClick={() => openExternal(paper ? PAPER_DASHBOARD : LIVE_DASHBOARD)}
                className="inline-flex items-center gap-1.5 text-xs font-medium text-brand-400 hover:text-brand-300"
              >
                Open Alpaca dashboard
                <ExternalLink className="w-3 h-3" />
              </button>
            </div>

            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <StatCard
                label="Equity"
                value={fmtMoney(account.equity, currency)}
                sub={fetchedAt ? `as of ${fmtDate(fetchedAt.toISOString())}` : null}
              />
              <StatCard
                label="Cash"
                value={fmtMoney(account.cash, currency)}
                sub={account.cash_withdrawable != null ? `Withdrawable ${fmtMoney(account.cash_withdrawable, currency)}` : null}
              />
              <StatCard
                label="Buying power"
                value={fmtMoney(account.buying_power, currency)}
                sub={account.multiplier ? `${account.multiplier}× margin` : null}
              />
              <StatCard
                label="Day P/L"
                value={dayPl == null ? "—" : fmtMoney(dayPl, currency)}
                sub={account.last_equity != null ? `vs last equity ${fmtMoney(account.last_equity, currency)}` : "equity − last equity"}
                color={moneyColor(dayPl)}
              />
            </div>

            <EquityChart currency={currency} />

            {(clock.next_open || clock.next_close || clock.timestamp) && (
              <Section title="Market clock">
                <FieldRow label="Session" value={clock.is_open ? "Open" : "Closed"} badge={boolBadge(!!clock.is_open)} />
                <FieldRow label="As of" value={fmtDate(clock.timestamp)} />
                <FieldRow label="Next open" value={fmtDate(clock.next_open)} />
                <FieldRow label="Next close" value={fmtDate(clock.next_close)} />
              </Section>
            )}

            {GROUPS.map((group) => {
              const rows = group.keys.filter((k) => account[k] != null && account[k] !== "");
              if (!rows.length) return null;
              return (
                <Section key={group.id} title={group.title}>
                  {rows.map((key) => {
                    const raw = account[key];
                    const isBool = typeof raw === "boolean";
                    const isStatus = key === "status" || key === "crypto_status";
                    const isMoney = MONEY_KEYS.has(key);
                    const signed = isMoney && (key.includes("short") || key.includes("fee") || key.includes("pending") || key === "intraday_adjustments");
                    return (
                      <FieldRow
                        key={key}
                        label={prettyKey(key)}
                        value={formatValue(key, raw, currency)}
                        valueClass={signed ? moneyColor(raw) : undefined}
                        badge={isBool ? boolBadge(raw) : isStatus ? statusBadge(raw) : undefined}
                      />
                    );
                  })}
                </Section>
              );
            })}

            {Object.keys(config).length > 0 && (
              <Section title="Account configuration">
                {Object.entries(config).map(([key, raw]) => (
                  <FieldRow
                    key={key}
                    label={CONFIG_LABELS[key] || prettyKey(key)}
                    value={formatValue(key, raw, currency)}
                    badge={typeof raw === "boolean" ? boolBadge(raw) : undefined}
                  />
                ))}
              </Section>
            )}

            <Section title={`Positions${positions.length ? ` (${positions.length})` : ""}`}>
              {payload.positions_error && (
                <p className="text-xs text-amber-400 mb-2">{payload.positions_error}</p>
              )}
              {positions.length === 0 ? (
                <p className="text-sm text-slate-500">No open positions on this Alpaca account.</p>
              ) : (
                <>
                  <p className="text-xs text-slate-500 mb-2">
                    Unrealized P/L {fmtMoney(posUnrealized, currency)} across {positions.length} position{positions.length === 1 ? "" : "s"}.
                  </p>
                  <div className="overflow-x-auto -mx-1">
                    <table className="w-full text-left text-xs">
                      <thead>
                        <tr className="text-slate-500 uppercase tracking-wider">
                          <th className="py-1.5 pr-3 font-semibold">Symbol</th>
                          <th className="py-1.5 pr-3 font-semibold">Side</th>
                          <th className="py-1.5 pr-3 font-semibold text-right">Qty</th>
                          <th className="py-1.5 pr-3 font-semibold text-right">Avg entry</th>
                          <th className="py-1.5 pr-3 font-semibold text-right">Mark</th>
                          <th className="py-1.5 pr-3 font-semibold text-right">Mkt value</th>
                          <th className="py-1.5 pr-3 font-semibold text-right">Unreal. P/L</th>
                          <th className="py-1.5 font-semibold text-right">Today</th>
                        </tr>
                      </thead>
                      <tbody>
                        {positions.map((p) => (
                          <tr key={p.symbol} className="border-t border-slate-800/80">
                            <td className="py-1.5 pr-3 font-semibold text-slate-200">{p.symbol}</td>
                            <td className="py-1.5 pr-3 capitalize text-slate-400">{p.side || "—"}</td>
                            <td className="py-1.5 pr-3 text-right font-mono tabular-nums text-slate-300">
                              {p.qty == null ? "—" : Number(p.qty).toLocaleString()}
                            </td>
                            <td className="py-1.5 pr-3 text-right font-mono tabular-nums text-slate-300">
                              {fmtMoney(p.avg_entry_price, currency)}
                            </td>
                            <td className="py-1.5 pr-3 text-right font-mono tabular-nums text-slate-300">
                              {fmtMoney(p.current_price, currency)}
                            </td>
                            <td className="py-1.5 pr-3 text-right font-mono tabular-nums text-slate-300">
                              {fmtMoney(p.market_value, currency)}
                            </td>
                            <td className={`py-1.5 pr-3 text-right font-mono tabular-nums ${moneyColor(p.unrealized_pl)}`}>
                              {fmtMoney(p.unrealized_pl, currency)}
                              {p.unrealized_plpc != null && (
                                <span className="text-slate-500 ml-1">{fmtPct(p.unrealized_plpc)}</span>
                              )}
                            </td>
                            <td className={`py-1.5 text-right font-mono tabular-nums ${moneyColor(p.unrealized_intraday_pl)}`}>
                              {fmtMoney(p.unrealized_intraday_pl, currency)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </Section>

            {leftover.length > 0 && (
              <Section title="Other fields from Alpaca">
                {leftover.map((key) => (
                  <FieldRow
                    key={key}
                    label={prettyKey(key)}
                    value={formatValue(key, account[key], currency)}
                    badge={typeof account[key] === "boolean" ? boolBadge(account[key]) : undefined}
                  />
                ))}
              </Section>
            )}
          </>
        )}
      </div>
    </div>
  );
}
