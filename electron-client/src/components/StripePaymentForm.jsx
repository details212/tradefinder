import { useState } from "react";
import { PaymentElement, useStripe, useElements } from "@stripe/react-stripe-js";
import { AlertCircle } from "lucide-react";
import { stripeApi } from "../api/client";

const SpinnerSVG = () => (
  <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
  </svg>
);

/**
 * Renders the Stripe Payment Element (SetupIntent flow) and handles confirmation.
 *
 * Flow:
 *   1. User enters card → stripe.confirmSetup() saves the card to Stripe
 *   2. Frontend calls /api/stripe/confirm-setup with the payment method ID
 *   3. Backend creates the subscription using the saved card
 *   4. onSuccess() is called
 *
 * Props:
 *   onSuccess()  — called when subscription is fully activated
 *   onCancel()   — optional back/cancel link
 *   priceLabel   — text on the submit button
 */
export default function StripePaymentForm({ onSuccess, onCancel, priceLabel = "Subscribe" }) {
  const stripe   = useStripe();
  const elements = useElements();
  const [loading, setLoading] = useState(false);
  const [status,  setStatus]  = useState("");   // "" | "saving" | "subscribing"
  const [error,   setError]   = useState("");

  async function handleSubmit(e) {
    e.preventDefault();
    if (!stripe || !elements) return;

    setLoading(true);
    setError("");

    // Step 1 — validate the form
    const { error: submitError } = await elements.submit();
    if (submitError) {
      setError(submitError.message);
      setLoading(false);
      return;
    }

    // Step 2 — save the card via SetupIntent (no redirect needed for standard cards)
    setStatus("saving");
    const { error: setupError, setupIntent } = await stripe.confirmSetup({
      elements,
      confirmParams: {
        return_url: `${window.TRADEFINDER_API_URL || "http://localhost:5000"}/api/stripe/checkout-return?status=success`,
      },
      redirect: "if_required",
    });

    if (setupError) {
      setError(setupError.message);
      setLoading(false);
      setStatus("");
      return;
    }

    // Step 3 — create the subscription server-side using the saved payment method
    setStatus("subscribing");
    try {
      await stripeApi.confirmSetup(setupIntent.id, setupIntent.payment_method);
      onSuccess();
    } catch (err) {
      setError(err.response?.data?.error || "Failed to activate subscription. Please try again.");
    } finally {
      setLoading(false);
      setStatus("");
    }
  }

  const buttonLabel = () => {
    if (status === "saving")      return <><SpinnerSVG /> Saving card…</>;
    if (status === "subscribing") return <><SpinnerSVG /> Activating…</>;
    if (loading)                  return <><SpinnerSVG /> Processing…</>;
    return priceLabel;
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="rounded-xl overflow-hidden border border-slate-600 p-4 bg-slate-900">
        <PaymentElement
          options={{
            layout: "tabs",
            wallets: { applePay: "never", googlePay: "never" },
          }}
        />
      </div>

      {error && (
        <div className="flex items-start gap-2 bg-red-900/40 border border-red-700 text-red-300 rounded-xl px-4 py-3 text-sm">
          <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <button
        type="submit"
        disabled={!stripe || !elements || loading}
        className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm"
      >
        {buttonLabel()}
      </button>

      {onCancel && !loading && (
        <button
          type="button"
          onClick={onCancel}
          className="w-full text-slate-400 hover:text-slate-200 text-xs transition"
        >
          Cancel
        </button>
      )}
    </form>
  );
}
