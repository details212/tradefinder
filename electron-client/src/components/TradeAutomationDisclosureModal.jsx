import { useState, useRef, useCallback } from "react";
import {
  Zap, AlertTriangle, Clock, TrendingDown, Activity,
  ShieldAlert, CheckCircle, ChevronRight, ChevronsDown,
} from "lucide-react";

/**
 * Displayed on every login when the user has Trade Automation
 * (auto_market_close_beyond_tp) enabled.  The user must explicitly
 * acknowledge before proceeding to the dashboard.
 *
 * Props:
 *   onAcknowledge — called when the user clicks "I Understand"
 */
export default function TradeAutomationDisclosureModal({ onAcknowledge }) {
  const [canAcknowledge, setCanAcknowledge] = useState(false);
  const scrollRef = useRef(null);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    // Allow a 12px threshold so they don't have to pixel-perfect scroll
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 12) {
      setCanAcknowledge(true);
    }
  }, []);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/85 backdrop-blur-sm p-4">
      <div className="bg-slate-800 border border-slate-700 rounded-2xl shadow-2xl w-full max-w-2xl max-h-[92vh] flex flex-col">

        {/* ── Header ──────────────────────────────────────────────────────── */}
        <div className="px-8 pt-8 pb-6 border-b border-slate-700/60">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 rounded-xl bg-amber-500/15 border border-amber-500/30 flex items-center justify-center shrink-0">
              <Zap className="w-6 h-6 text-amber-400" />
            </div>
            <div>
              <h2 className="text-lg font-bold text-white leading-tight">
                Trade Automation Is Active
              </h2>
              <p className="text-sm text-amber-400/90 mt-0.5 font-medium">
                Important disclosure — please read before trading
              </p>
            </div>
          </div>
          <p className="text-sm text-slate-300 leading-relaxed">
            Your account is configured to automatically close open positions when the last
            traded price moves past your designated take-profit or stop-loss level. Review
            the information below before you begin your session.
          </p>
        </div>

        {/* ── Scrollable body ─────────────────────────────────────────────── */}
        <div
          ref={scrollRef}
          onScroll={handleScroll}
          className="overflow-y-auto flex-1 px-8 py-6 flex flex-col gap-5"
        >

          <DisclosureCard
            icon={Clock}
            iconColor="text-sky-400"
            accent="border-sky-500/50"
            title="How the automation works"
            items={[
              "The TradeFinder server polls Alpaca for the last traded price of every open position approximately once per minute during market hours.",
              "When a price breach is detected, the system cancels all open bracket legs (take-profit and stop-loss orders) for that position.",
              "A market order is then submitted immediately to close the full remaining share quantity — identical to clicking \"Close Trade\" manually from the My Trades panel.",
            ]}
          />

          <DisclosureCard
            icon={AlertTriangle}
            iconColor="text-amber-400"
            accent="border-amber-500/50"
            title="Market orders — no price guarantee"
            items={[
              "Market orders fill at the best available price at the moment the order reaches the exchange. Your actual fill price may differ from your take-profit or stop-loss target.",
              "During volatile conditions, wide bid-ask spreads, or thin liquidity, the gap between your intended exit and your fill price (slippage) can be significant.",
              "Pre-market gaps, earnings surprises, or trading halts can produce fills that are materially worse than your stop-loss — including situations where the price skips past the stop entirely.",
              "Unlike a limit order (which locks in price but not execution), market orders prioritize a fill. You gain certainty of execution at the cost of price certainty.",
            ]}
          />

          <DisclosureCard
            icon={Activity}
            iconColor="text-purple-400"
            accent="border-purple-500/50"
            title="Polling latency & snapshot accuracy"
            items={[
              "Price checks are not tick-perfect. Alpaca snapshots are queried roughly every 60 seconds, so a breach that occurs between polls may not trigger a close until the next cycle.",
              "Price can continue to move against your position during that window. This latency is inherent to server-side polling and cannot be eliminated.",
              "Snapshot data reflects the last trade price, which can lag real-time quotes by several seconds in fast-moving markets.",
            ]}
          />

          <DisclosureCard
            icon={ShieldAlert}
            iconColor="text-red-400"
            accent="border-red-500/50"
            title="Not a substitute for active monitoring"
            items={[
              "This feature is a convenience layer — it is not a substitute for a professional stop-loss strategy, a DMA system, or broker-native conditional orders.",
              "Network interruptions, server downtime, or Alpaca API outages may prevent orders from being placed. TradeFinder makes no guarantee that automation orders will be submitted or filled.",
              "You remain solely responsible for all trading outcomes. Regularly verify your open position status directly in your Alpaca account.",
              "Paper and live trading use the same automation logic, but live trading involves real capital. Validate behavior thoroughly in paper mode before relying on this feature with real money.",
            ]}
          />

          {/* Bracket cancellation note */}
          <div className="flex items-start gap-3 px-4 py-4 rounded-xl bg-slate-900/60 border border-slate-700/50">
            <TrendingDown className="w-4 h-4 text-slate-400 shrink-0 mt-0.5" />
            <p className="text-sm text-slate-400 leading-relaxed">
              <span className="text-slate-200 font-semibold">Bracket cancellation is best-effort.</span>{" "}
              If Alpaca has already triggered a native take-profit or stop-loss fill before the
              automation cycle runs, the system detects the position as closed and takes no additional
              action. Occasional duplicate close attempts may appear in your Alpaca order history —
              these are harmless.
            </p>
          </div>
        </div>

        {/* ── Footer / CTA ────────────────────────────────────────────────── */}
        <div className="px-8 py-6 border-t border-slate-700/60 flex flex-col gap-3">
          {!canAcknowledge ? (
            <div className="flex items-center justify-center gap-2 text-slate-500 text-xs animate-pulse">
              <ChevronsDown className="w-4 h-4" />
              Scroll down to read the full disclosure before continuing
            </div>
          ) : (
            <p className="text-xs text-slate-500 leading-relaxed text-center">
              This disclosure appears at every login while Trade Automation is enabled.
              To disable it, visit{" "}
              <span className="text-slate-400 font-medium">Account Settings → Trade Automation</span>.
            </p>
          )}
          <button
            onClick={onAcknowledge}
            disabled={!canAcknowledge}
            className="w-full flex items-center justify-center gap-2 px-5 py-3 rounded-xl bg-brand-600 hover:bg-brand-500 active:bg-brand-700 disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-brand-600 text-white text-sm font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-brand-500/50"
          >
            <CheckCircle className="w-4 h-4" />
            I Understand — Continue to Dashboard
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>

      </div>
    </div>
  );
}

// ── Internal card component ───────────────────────────────────────────────────
function DisclosureCard({ icon: Icon, iconColor, accent, title, items }) {
  return (
    <div className={`rounded-xl bg-slate-900/50 border-l-4 border border-slate-700/50 ${accent} pl-5 pr-5 py-4`}>
      <div className="flex items-center gap-2.5 mb-3">
        <Icon className={`w-4 h-4 shrink-0 ${iconColor}`} />
        <h3 className="text-sm font-semibold text-slate-100">{title}</h3>
      </div>
      <ul className="flex flex-col gap-3">
        {items.map((item, i) => (
          <li key={i} className="flex items-start gap-3">
            <span className="mt-[7px] w-1.5 h-1.5 rounded-full bg-slate-500 shrink-0" />
            <p className="text-sm text-slate-300 leading-relaxed">{item}</p>
          </li>
        ))}
      </ul>
    </div>
  );
}
