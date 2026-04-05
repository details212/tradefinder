import { useState, useEffect } from "react";
import { stockApi } from "../api/client";
import StockDetailChart from "./StockDetailChart";
import TechnicalChart from "./TechnicalChart";
import {
  TrendingUp, TrendingDown, Star, StarOff, ExternalLink,
  Loader2, X, Newspaper, BarChart2, DollarSign, Activity,
  Building2, MapPin, Users, Calendar,
} from "lucide-react";

const TABS = ["Overview", "Financials", "Dividends", "Technical"];

function StatCard({ label, value, sub }) {
  return (
    <div className="bg-slate-900 rounded-xl p-3">
      <p className="text-xs text-slate-500 mb-0.5">{label}</p>
      <p className="text-sm font-semibold text-slate-100 truncate">{value ?? "—"}</p>
      {sub && <p className="text-xs text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
}

function fmt(n, decimals = 2) {
  if (n == null) return "—";
  return Number(n).toFixed(decimals);
}

function fmtLarge(n) {
  if (n == null) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  return `$${Number(n).toLocaleString()}`;
}

function fmtPct(n) {
  if (n == null) return "—";
  return `${Number(n).toFixed(2)}%`;
}

// ── Overview Tab ──────────────────────────────────────────────────────────────
function OverviewTab({ ticker, quote, details, news, watchlist, defaultInterval, barTime, threshold }) {
  const price = quote?.last_trade_price ?? quote?.close;
  const change = quote?.change;
  const changePct = quote?.change_pct;
  const isUp = change == null ? null : change >= 0;

  const toggleWatchlist = async () => {
    try {
      if (watchlist.includes(ticker)) {
        await stockApi.removeFromWatchlist(ticker);
      } else {
        await stockApi.addToWatchlist(ticker, { source: "stocks" });
      }
      // Watchlist sidebar refreshes via `tf:watchlist-changed` (axios interceptor)
    } catch {}
  };

  return (
    <div className="space-y-5">
      {/* Price */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2 mb-1">
            {details?.icon_url && (
              <img src={details.icon_url} alt="" className="w-8 h-8 rounded-lg bg-white object-contain p-1"
                onError={(e) => (e.currentTarget.style.display = "none")} />
            )}
            <h2 className="text-xl font-bold text-white">{ticker}</h2>
            <button onClick={toggleWatchlist}
              className={`transition ${watchlist.includes(ticker) ? "text-yellow-400 hover:text-yellow-300" : "text-slate-500 hover:text-yellow-400"}`}>
              {watchlist.includes(ticker)
                ? <Star className="w-4 h-4 fill-current" />
                : <StarOff className="w-4 h-4" />}
            </button>
          </div>
          <p className="text-sm text-slate-400">{details?.name}</p>
        </div>
        {price != null && (
          <div className="text-right">
            <p className="text-3xl font-bold text-white">${fmt(price)}</p>
            {change != null && (
              <span className={`flex items-center justify-end gap-1 text-sm font-medium ${isUp ? "text-green-400" : "text-red-400"}`}>
                {isUp ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                {isUp ? "+" : ""}{fmt(change)} ({isUp ? "+" : ""}{fmt(changePct)}%)
              </span>
            )}
          </div>
        )}
      </div>

      {/* Chart */}
      <StockDetailChart ticker={ticker} barTime={barTime} threshold={threshold} />

      {/* Company info */}
      {details && (
        <div className="bg-slate-800 rounded-2xl border border-slate-700 p-4 space-y-3">
          <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
            <Building2 className="w-3.5 h-3.5" /> Company
          </h4>
          <div className="grid grid-cols-2 gap-2 text-sm">
            <div>
              <p className="text-slate-500 text-xs">Market Cap</p>
              <p className="text-slate-200">{fmtLarge(details.market_cap)}</p>
            </div>
            <div>
              <p className="text-slate-500 text-xs">Employees</p>
              <p className="text-slate-200">{details.employees ? Number(details.employees).toLocaleString() : "—"}</p>
            </div>
            <div>
              <p className="text-slate-500 text-xs">Exchange</p>
              <p className="text-slate-200">{details.primary_exchange ?? "—"}</p>
            </div>
            <div>
              <p className="text-slate-500 text-xs">Listed</p>
              <p className="text-slate-200">{details.list_date ?? "—"}</p>
            </div>
            <div>
              <p className="text-slate-500 text-xs">SIC</p>
              <p className="text-slate-200 text-xs">{details.sic_description ?? "—"}</p>
            </div>
            <div>
              <p className="text-slate-500 text-xs">Shares Outstanding</p>
              <p className="text-slate-200">{details.weighted_shares_outstanding ? (details.weighted_shares_outstanding / 1e6).toFixed(1) + "M" : "—"}</p>
            </div>
          </div>
          {details.address?.city && (
            <p className="text-xs text-slate-500 flex items-center gap-1">
              <MapPin className="w-3 h-3" />
              {[details.address.address1, details.address.city, details.address.state, details.address.postal_code].filter(Boolean).join(", ")}
            </p>
          )}
          {details.description && (
            <p className="text-sm text-slate-300 leading-relaxed line-clamp-4">{details.description}</p>
          )}
          {details.homepage_url && (
            <a href={details.homepage_url} target="_blank" rel="noreferrer"
              className="inline-flex items-center gap-1 text-xs text-brand-400 hover:text-brand-300 transition">
              <ExternalLink className="w-3 h-3" />{details.homepage_url}
            </a>
          )}
        </div>
      )}


    </div>
  );
}

// ── Financials Tab ────────────────────────────────────────────────────────────
function FinancialsTab({ ticker }) {
  const [timeframe, setTimeframe] = useState("quarterly");
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    stockApi.financials(ticker, timeframe)
      .then((r) => setData(r.data.periods || []))
      .catch(() => setError("Failed to load financials"))
      .finally(() => setLoading(false));
  }, [ticker, timeframe]);

  const Row = ({ label, values, formatter = fmtLarge, highlight = false }) => (
    <tr className={highlight ? "bg-slate-800/50" : ""}>
      <td className="py-2 pr-4 text-xs text-slate-400 whitespace-nowrap">{label}</td>
      {values.map((v, i) => (
        <td key={i} className={`py-2 text-right text-xs font-mono ${v < 0 ? "text-red-400" : "text-slate-200"}`}>
          {v != null ? formatter(v) : "—"}
        </td>
      ))}
    </tr>
  );

  return (
    <div>
      <div className="flex items-center gap-2 mb-4">
        {["quarterly", "annual", "ttm"].map((t) => (
          <button key={t} onClick={() => setTimeframe(t)}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg transition capitalize ${timeframe === t ? "bg-brand-600 text-white" : "text-slate-400 hover:text-slate-200 bg-slate-800"}`}>
            {t}
          </button>
        ))}
      </div>

      {loading && <div className="flex justify-center py-12"><Loader2 className="w-6 h-6 animate-spin text-slate-500" /></div>}
      {error && <p className="text-center text-slate-500 py-12">{error}</p>}

      {!loading && !error && data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr>
                <th className="text-left text-xs text-slate-500 pb-3 pr-4 font-medium">Metric</th>
                {data.map((p) => (
                  <th key={p.period} className="text-right text-xs text-slate-500 pb-3 font-medium whitespace-nowrap">
                    {p.fiscal_period} {p.fiscal_year}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              <tr><td colSpan={data.length + 1} className="py-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Income Statement</td></tr>
              <Row label="Revenue" values={data.map((p) => p.income_statement.revenues)} highlight />
              <Row label="Gross Profit" values={data.map((p) => p.income_statement.gross_profit)} />
              <Row label="Operating Income" values={data.map((p) => p.income_statement.operating_income)} highlight />
              <Row label="Net Income" values={data.map((p) => p.income_statement.net_income)} />
              <Row label="EPS (Basic)" values={data.map((p) => p.income_statement.eps_basic)} formatter={(v) => `$${fmt(v, 2)}`} highlight />
              <Row label="EPS (Diluted)" values={data.map((p) => p.income_statement.eps_diluted)} formatter={(v) => `$${fmt(v, 2)}`} />
              <Row label="R&D Expense" values={data.map((p) => p.income_statement.rd_expense)} highlight />
              <Row label="Income Tax" values={data.map((p) => p.income_statement.income_tax)} />

              <tr><td colSpan={data.length + 1} className="pt-4 pb-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Balance Sheet</td></tr>
              <Row label="Total Assets" values={data.map((p) => p.balance_sheet.assets)} highlight />
              <Row label="Current Assets" values={data.map((p) => p.balance_sheet.current_assets)} />
              <Row label="Total Liabilities" values={data.map((p) => p.balance_sheet.liabilities)} highlight />
              <Row label="Current Liabilities" values={data.map((p) => p.balance_sheet.current_liabilities)} />
              <Row label="Equity" values={data.map((p) => p.balance_sheet.equity)} highlight />
              <Row label="Cash" values={data.map((p) => p.balance_sheet.cash)} />
              <Row label="Long-term Debt" values={data.map((p) => p.balance_sheet.long_term_debt)} highlight />

              <tr><td colSpan={data.length + 1} className="pt-4 pb-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Cash Flow</td></tr>
              <Row label="Operating CF" values={data.map((p) => p.cash_flow.operating)} highlight />
              <Row label="Investing CF" values={data.map((p) => p.cash_flow.investing)} />
              <Row label="Financing CF" values={data.map((p) => p.cash_flow.financing)} highlight />
              <Row label="CapEx" values={data.map((p) => p.cash_flow.capex)} />
            </tbody>
          </table>
        </div>
      )}
      {!loading && !error && data.length === 0 && (
        <p className="text-center text-slate-500 py-12">No financial data available</p>
      )}
    </div>
  );
}

// ── Dividends Tab ─────────────────────────────────────────────────────────────
function DividendsTab({ ticker }) {
  const [dividends, setDividends] = useState([]);
  const [splits, setSplits] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([stockApi.dividends(ticker), stockApi.splits(ticker)])
      .then(([dRes, sRes]) => {
        setDividends(dRes.data.dividends || []);
        setSplits(sRes.data.splits || []);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [ticker]);

  const FREQ = { 1: "Annual", 2: "Bi-Annual", 4: "Quarterly", 12: "Monthly" };

  if (loading) return <div className="flex justify-center py-12"><Loader2 className="w-6 h-6 animate-spin text-slate-500" /></div>;

  return (
    <div className="space-y-6">
      {/* Dividends */}
      <div>
        <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
          <DollarSign className="w-3.5 h-3.5" /> Dividend History
        </h4>
        {dividends.length === 0 ? (
          <p className="text-slate-500 text-sm">No dividend history</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-500 border-b border-slate-800">
                  <th className="text-left pb-2 font-medium">Ex-Date</th>
                  <th className="text-right pb-2 font-medium">Amount</th>
                  <th className="text-right pb-2 font-medium">Frequency</th>
                  <th className="text-right pb-2 font-medium">Pay Date</th>
                  <th className="text-right pb-2 font-medium">Type</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {dividends.map((d, i) => (
                  <tr key={i} className="text-xs">
                    <td className="py-2 text-slate-300">{d.ex_dividend_date ?? "—"}</td>
                    <td className="py-2 text-right text-green-400 font-mono">${fmt(d.cash_amount, 4)}</td>
                    <td className="py-2 text-right text-slate-400">{FREQ[d.frequency] ?? d.frequency ?? "—"}</td>
                    <td className="py-2 text-right text-slate-400">{d.pay_date ?? "—"}</td>
                    <td className="py-2 text-right text-slate-500 capitalize">{d.dividend_type ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Splits */}
      <div>
        <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
          <BarChart2 className="w-3.5 h-3.5" /> Stock Splits
        </h4>
        {splits.length === 0 ? (
          <p className="text-slate-500 text-sm">No split history</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-slate-500 border-b border-slate-800">
                  <th className="text-left pb-2 font-medium">Date</th>
                  <th className="text-right pb-2 font-medium">Ratio</th>
                  <th className="text-right pb-2 font-medium">From</th>
                  <th className="text-right pb-2 font-medium">To</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {splits.map((s, i) => (
                  <tr key={i} className="text-xs">
                    <td className="py-2 text-slate-300">{s.execution_date}</td>
                    <td className="py-2 text-right text-brand-400 font-mono">{s.ratio ? `${fmt(s.ratio, 2)}:1` : "—"}</td>
                    <td className="py-2 text-right text-slate-400">{s.split_from}</td>
                    <td className="py-2 text-right text-slate-400">{s.split_to}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Technical Tab ─────────────────────────────────────────────────────────────
function TechnicalTab({ ticker }) {
  return (
    <div className="space-y-5">
      <TechnicalChart ticker={ticker} />
    </div>
  );
}

// ── Main StockDetail ──────────────────────────────────────────────────────────
export default function StockDetail({ ticker, onClose, watchlist, defaultInterval, barTime, threshold }) {
  const [activeTab, setActiveTab] = useState("Overview");
  const [quote, setQuote] = useState(null);
  const [details, setDetails] = useState(null);
  const [news, setNews] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!ticker) return;
    setLoading(true);
    setError("");
    setActiveTab("Overview");

    Promise.all([
      stockApi.quote(ticker),
      stockApi.details(ticker),
      stockApi.news(ticker),
    ])
      .then(([qRes, dRes, nRes]) => {
        setQuote(qRes.data);
        setDetails(dRes.data);
        setNews(nRes.data.articles || []);
      })
      .catch(() => setError("Failed to load stock data"))
      .finally(() => setLoading(false));
  }, [ticker]);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-5 pt-4 pb-0 border-b border-slate-700 shrink-0">
        <div className="flex gap-1">
          {TABS.map((tab) => (
            <button key={tab} onClick={() => setActiveTab(tab)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition -mb-px ${
                activeTab === tab
                  ? "border-brand-500 text-white"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}>
              {tab}
            </button>
          ))}
        </div>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-200 p-1 mb-1 transition">
          <X className="w-5 h-5" />
        </button>
      </div>

      {loading && (
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="w-7 h-7 text-slate-500 animate-spin" />
        </div>
      )}
      {error && !loading && (
        <div className="flex-1 flex items-center justify-center text-slate-500">{error}</div>
      )}

      {!loading && !error && (
        <div className="flex-1 overflow-y-auto p-5">
          {activeTab === "Overview" && (
            <OverviewTab ticker={ticker} quote={quote} details={details} news={news}
              watchlist={watchlist}
              defaultInterval={defaultInterval} barTime={barTime} threshold={threshold} />
          )}
          {activeTab === "Financials" && <FinancialsTab ticker={ticker} />}
          {activeTab === "Dividends" && <DividendsTab ticker={ticker} />}
          {activeTab === "Technical" && <TechnicalTab ticker={ticker} />}
        </div>
      )}
    </div>
  );
}
