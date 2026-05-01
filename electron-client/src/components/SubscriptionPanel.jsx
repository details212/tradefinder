import { useState, useEffect, useCallback } from "react";
import { stripeApi } from "../api/client";
import {
  CreditCard,
  RefreshCw,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Clock,
  ExternalLink,
  FileText,
  Loader2,
} from "lucide-react";

function statusBadge(status) {
  switch (status) {
    case "active":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-900/50 text-emerald-400 border border-emerald-700/50">
          <CheckCircle2 className="w-3 h-3" />
          Active
        </span>
      );
    case "past_due":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-amber-900/50 text-amber-400 border border-amber-700/50">
          <AlertTriangle className="w-3 h-3" />
          Past Due
        </span>
      );
    case "canceled":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-red-900/50 text-red-400 border border-red-700/50">
          <XCircle className="w-3 h-3" />
          Canceled
        </span>
      );
    default:
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-slate-700/60 text-slate-400 border border-slate-600/50">
          <Clock className="w-3 h-3" />
          {status ?? "Inactive"}
        </span>
      );
  }
}

function invoiceStatusBadge(status) {
  if (status === "paid") {
    return (
      <span className="px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-900/50 text-emerald-400">
        Paid
      </span>
    );
  }
  if (status === "open") {
    return (
      <span className="px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-900/50 text-amber-400">
        Open
      </span>
    );
  }
  return (
    <span className="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-700 text-slate-400">
      {status}
    </span>
  );
}

function fmt(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

export default function SubscriptionPanel() {
  const [subStatus,  setSubStatus]  = useState(null);
  const [invoices,   setInvoices]   = useState([]);
  const [loading,    setLoading]    = useState(true);
  const [subError,   setSubError]   = useState(null);
  const [invError,   setInvError]   = useState(null);
  const [portalLoading, setPortalLoading] = useState(false);
  const [portalError,   setPortalError]   = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setSubError(null);
    setInvError(null);

    const [statusRes, invRes] = await Promise.allSettled([
      stripeApi.getSubscriptionStatus(),
      stripeApi.getInvoices(),
    ]);

    if (statusRes.status === "fulfilled") {
      setSubStatus(statusRes.value.data);
    } else {
      setSubError("Could not load subscription status.");
    }

    if (invRes.status === "fulfilled") {
      setInvoices(invRes.value.data.invoices || []);
    } else {
      setInvError("Could not load billing history.");
    }

    setLoading(false);
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const openPortal = async () => {
    setPortalLoading(true);
    setPortalError(null);
    try {
      const res = await stripeApi.getBillingPortal();
      const url = res.data?.url;
      if (url) {
        // Open in system browser via Electron shell or window.open
        if (window.electronAPI?.openExternal) {
          window.electronAPI.openExternal(url);
        } else {
          window.open(url, "_blank", "noopener,noreferrer");
        }
      } else {
        setPortalError("No portal URL returned.");
      }
    } catch (err) {
      setPortalError(
        err?.response?.data?.error || "Failed to open billing portal."
      );
    } finally {
      setPortalLoading(false);
    }
  };

  const openLink = (url) => {
    if (!url) return;
    if (window.electronAPI?.openExternal) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, "_blank", "noopener,noreferrer");
    }
  };

  return (
    <div className="flex flex-col h-full overflow-y-auto bg-slate-950 p-6 gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-brand-600/20 border border-brand-600/30">
            <CreditCard className="w-5 h-5 text-brand-400" />
          </div>
          <div>
            <h1 className="text-lg font-semibold text-slate-100">Subscription</h1>
            <p className="text-xs text-slate-500">Billing &amp; payment history</p>
          </div>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200 transition px-3 py-1.5 rounded-lg hover:bg-slate-800 border border-slate-700 disabled:opacity-50"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Subscription status card */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5 flex flex-col gap-4">
        <h2 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
          <CheckCircle2 className="w-4 h-4 text-brand-400" />
          Current Plan
        </h2>

        {loading ? (
          <div className="flex items-center gap-2 text-slate-500 text-sm">
            <Loader2 className="w-4 h-4 animate-spin" />
            Loading…
          </div>
        ) : subError ? (
          <p className="text-sm text-red-400">{subError}</p>
        ) : (
          <div className="flex flex-col gap-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-400">Status</span>
              {statusBadge(subStatus?.status)}
            </div>
            {subStatus?.period_end && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">
                  {subStatus?.status === "canceled" ? "Ends" : "Renews"}
                </span>
                <span className="text-sm font-medium text-slate-200">
                  {fmtDate(subStatus.period_end)}
                </span>
              </div>
            )}
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-400">Plan</span>
              <span className="text-sm font-medium text-slate-200">TradeFinder Pro</span>
            </div>
          </div>
        )}

        {/* Manage billing button */}
        {!loading && !subError && (
          <div className="pt-1 border-t border-slate-800">
            {portalError && (
              <p className="text-xs text-red-400 mb-2">{portalError}</p>
            )}
            <button
              onClick={openPortal}
              disabled={portalLoading}
              className="flex items-center gap-2 text-sm font-medium text-brand-400 hover:text-brand-300 transition disabled:opacity-50"
            >
              {portalLoading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <ExternalLink className="w-4 h-4" />
              )}
              Manage billing &amp; payment method
            </button>
          </div>
        )}
      </div>

      {/* Billing history */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5 flex flex-col gap-4">
        <h2 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
          <FileText className="w-4 h-4 text-brand-400" />
          Billing History
        </h2>

        {loading ? (
          <div className="flex items-center gap-2 text-slate-500 text-sm">
            <Loader2 className="w-4 h-4 animate-spin" />
            Loading…
          </div>
        ) : invError ? (
          <p className="text-sm text-red-400">{invError}</p>
        ) : invoices.length === 0 ? (
          <p className="text-sm text-slate-500 py-4 text-center">No invoices yet.</p>
        ) : (
          <div className="flex flex-col divide-y divide-slate-800">
            {/* Column headers */}
            <div className="grid grid-cols-[1fr_auto_auto_auto] gap-3 pb-2 text-[10px] font-semibold text-slate-500 uppercase tracking-wider">
              <span>Period</span>
              <span className="text-right">Amount</span>
              <span className="text-right">Status</span>
              <span className="text-right">Invoice</span>
            </div>
            {invoices.map((inv) => (
              <div
                key={inv.id}
                className="grid grid-cols-[1fr_auto_auto_auto] gap-3 items-center py-3"
              >
                <div>
                  <p className="text-sm text-slate-200">
                    {fmt(inv.period_start)} – {fmt(inv.period_end)}
                  </p>
                  {inv.number && (
                    <p className="text-[10px] text-slate-600 mt-0.5">{inv.number}</p>
                  )}
                </div>
                <span className="text-sm font-semibold text-slate-200 tabular-nums text-right">
                  ${Number(inv.amount_paid).toFixed(2)}{" "}
                  <span className="text-[10px] font-normal text-slate-500">
                    {inv.currency}
                  </span>
                </span>
                <span className="text-right">{invoiceStatusBadge(inv.status)}</span>
                <div className="flex items-center justify-end gap-2">
                  {inv.hosted_invoice_url && (
                    <button
                      onClick={() => openLink(inv.hosted_invoice_url)}
                      title="View invoice"
                      className="text-slate-500 hover:text-brand-400 transition"
                    >
                      <ExternalLink className="w-3.5 h-3.5" />
                    </button>
                  )}
                  {inv.invoice_pdf && (
                    <button
                      onClick={() => openLink(inv.invoice_pdf)}
                      title="Download PDF"
                      className="text-slate-500 hover:text-brand-400 transition"
                    >
                      <FileText className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
