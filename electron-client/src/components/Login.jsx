import { useState, useEffect, useRef } from "react";
import { loadStripe } from "@stripe/stripe-js";
import { Elements } from "@stripe/react-stripe-js";
import { authApi, stripeApi } from "../api/client";
import StripePaymentForm from "./StripePaymentForm";
import { Eye, EyeOff, AlertCircle, CheckCircle, ArrowLeft, Mail, Phone, MapPin } from "lucide-react";
import logo from "../assets/logo.png";

const US_STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

const inputCls =
  "w-full bg-slate-900 border border-slate-600 rounded-xl px-4 py-2.5 text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 transition text-sm";

const SpinnerSVG = () => (
  <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
  </svg>
);

// ── Register — 3-step flow ────────────────────────────────────────────────────
// Step 1: email → send PIN
// Step 2: enter PIN → verify
// Step 3: username + password + contact info → create account

function RegisterFlow({ onSuccess, onBack }) {
  const [step,        setStep]        = useState(1);
  const [email,       setEmail]       = useState("");
  const [code,        setCode]        = useState("");
  const [loading,     setLoading]     = useState(false);
  const [error,       setError]       = useState("");
  const [success,     setSuccess]     = useState("");
  const [stripePromise, setStripePromise] = useState(null);
  const [clientSecret,  setClientSecret]  = useState(null);

  const [form, setForm] = useState({
    username: "", password: "", confirm: "",
    first_name: "", last_name: "",
    address: "", city: "", state: "", zipcode: "", mobile_phone: "",
  });
  const [showPw, setShowPw] = useState(false);

  const setF = (k) => (e) => { setForm(f => ({ ...f, [k]: e.target.value })); setError(""); };

  async function handlePaymentSuccess() {
    try {
      const meRes = await authApi.me();
      const storedToken = localStorage.getItem("tf_token");
      onSuccess(storedToken, meRes.data.user, null, null);
    } catch {
      onSuccess(localStorage.getItem("tf_token"), null, null, null);
    }
  }

  // ── Step 1: send code ─────────────────────────────────────────────────────
  async function handleSendCode(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    setLoading(true);
    try {
      await authApi.sendRegistrationCode(email.trim().toLowerCase());
      setSuccess(`Verification code sent to ${email.trim()}`);
      setStep(2);
    } catch (err) {
      setError(err.response?.data?.error ?? "Failed to send code. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  // ── Step 2: confirm code ──────────────────────────────────────────────────
  async function handleConfirmCode(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    setLoading(true);
    try {
      await authApi.confirmRegistrationCode(email.trim().toLowerCase(), code.trim());
      setSuccess("Email verified! Complete your account details below.");
      setStep(3);
    } catch (err) {
      setError(err.response?.data?.error ?? "Verification failed.");
    } finally {
      setLoading(false);
    }
  }

  // ── Step 3: create account ────────────────────────────────────────────────
  async function handleCreate(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    if (form.password !== form.confirm) { setError("Passwords do not match."); return; }
    if (form.password.length < 8)       { setError("Password must be at least 8 characters."); return; }
    setLoading(true);
    try {
      const res = await authApi.register({
        email:        email.trim().toLowerCase(),
        username:     form.username.trim(),
        password:     form.password,
        first_name:   form.first_name.trim(),
        last_name:    form.last_name.trim(),
        address:      form.address.trim(),
        city:         form.city.trim(),
        state:        form.state,
        zipcode:      form.zipcode.trim(),
        mobile_phone: form.mobile_phone.trim(),
      });

      // Store token so the payment step can call authenticated endpoints
      localStorage.setItem("tf_token", res.data.token);
      localStorage.setItem("tf_user", JSON.stringify(res.data.user));

      if (res.data.client_secret && res.data.publishable_key) {
        setStripePromise(loadStripe(res.data.publishable_key));
        setClientSecret(res.data.client_secret);
        setStep(4);
      } else {
        // Stripe not configured; proceed directly to dashboard
        onSuccess(res.data.token, res.data.user, res.data.required_version, res.data.download_url);
      }
    } catch (err) {
      setError(err.response?.data?.error ?? "Failed to create account.");
    } finally {
      setLoading(false);
    }
  }

  const stepLabels = ["Verify Email", "Enter Code", "Account Details", "Activate Plan"];

  return (
    <>
      {/* Back link */}
      <button onClick={onBack}
        className="flex items-center gap-1.5 text-slate-400 hover:text-slate-200 text-xs mb-5 transition">
        <ArrowLeft className="w-3.5 h-3.5" /> Back to sign in
      </button>

      <h2 className="text-lg font-semibold text-white mb-1">Create account</h2>
      <p className="text-slate-400 text-sm mb-5">
        {step === 1 && "Enter your email address to get started."}
        {step === 2 && "Enter the 6-digit code sent to your email."}
        {step === 3 && "Complete your profile to finish signing up."}
      </p>

      {/* Step indicator */}
      <div className="flex items-center gap-2 mb-6">
        {stepLabels.map((label, i) => {
          const n = i + 1;
          return (
            <div key={label} className="flex items-center gap-2">
              <div className={`flex items-center justify-center w-6 h-6 rounded-full text-[11px] font-bold transition-all ${
                n < step  ? "bg-emerald-500 text-white" :
                n === step? "bg-brand-600 text-white ring-2 ring-brand-500/40" :
                            "bg-slate-700 text-slate-500"
              }`}>
                {n < step ? <CheckCircle className="w-3.5 h-3.5" /> : n}
              </div>
              <span className={`text-xs ${n === step ? "text-slate-200" : "text-slate-500"}`}>{label}</span>
              {i < stepLabels.length - 1 && <div className="w-6 h-px bg-slate-700 mx-1" />}
            </div>
          );
        })}
      </div>

      {/* Banners */}
      {error && (
        <div className="flex items-start gap-2 bg-red-900/40 border border-red-700 text-red-300 rounded-xl px-4 py-3 mb-4 text-sm">
          <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" /><span>{error}</span>
        </div>
      )}
      {success && (
        <div className="flex items-start gap-2 bg-emerald-900/40 border border-emerald-700 text-emerald-300 rounded-xl px-4 py-3 mb-4 text-sm">
          <CheckCircle className="w-4 h-4 mt-0.5 shrink-0" /><span>{success}</span>
        </div>
      )}

      {/* ── Step 1 ── */}
      {step === 1 && (
        <form onSubmit={handleSendCode} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Email address</label>
            <input type="email" value={email} onChange={e => { setEmail(e.target.value); setError(""); }}
              placeholder="you@example.com" required autoFocus className={inputCls} />
          </div>
          <button type="submit" disabled={loading || !email.trim()}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm">
            {loading ? <><SpinnerSVG /> Sending…</> : <><Mail className="w-4 h-4" /> Send Verification Code</>}
          </button>
        </form>
      )}

      {/* ── Step 2 ── */}
      {step === 2 && (
        <form onSubmit={handleConfirmCode} className="space-y-4">
          <div className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-slate-900/60 border border-slate-700">
            <Mail className="w-4 h-4 text-brand-400 shrink-0" />
            <p className="text-sm text-slate-300">{email}</p>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">6-digit code</label>
            <input type="text" inputMode="numeric" pattern="\d{6}" maxLength={6}
              value={code} onChange={e => { setCode(e.target.value.replace(/\D/g, "")); setError(""); }}
              placeholder="000000" required autoFocus
              className={`${inputCls} tracking-[0.5em] text-center text-xl font-bold`} />
            <p className="text-xs text-slate-500 mt-1.5">Code expires in 30 minutes.</p>
          </div>
          <button type="submit" disabled={loading || code.length !== 6}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm">
            {loading ? <><SpinnerSVG /> Verifying…</> : "Verify Code"}
          </button>
          <button type="button" onClick={() => { setStep(1); setCode(""); setError(""); setSuccess(""); }}
            className="w-full text-slate-400 hover:text-slate-200 text-xs transition">
            Use a different email
          </button>
        </form>
      )}

      {/* ── Step 3 ── */}
      {step === 3 && (
        <form onSubmit={handleCreate} className="space-y-4">

          {/* Username */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Username</label>
            <input type="text" value={form.username} onChange={setF("username")}
              placeholder="Choose a username" required autoFocus className={inputCls} />
          </div>

          {/* Password */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Password</label>
              <div className="relative">
                <input type={showPw ? "text" : "password"} value={form.password} onChange={setF("password")}
                  placeholder="Min. 8 characters" required className={`${inputCls} pr-10`} />
                <button type="button" onClick={() => setShowPw(v => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200 transition">
                  {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Confirm password</label>
              <input type={showPw ? "text" : "password"} value={form.confirm} onChange={setF("confirm")}
                placeholder="••••••••" required className={inputCls} />
            </div>
          </div>

          {/* Divider */}
          <div className="flex items-center gap-3 py-1">
            <div className="flex-1 h-px bg-slate-700" />
            <span className="text-xs text-slate-500 flex items-center gap-1.5"><MapPin className="w-3 h-3" />Contact Information</span>
            <div className="flex-1 h-px bg-slate-700" />
          </div>

          {/* First / Last */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">First name</label>
              <input type="text" value={form.first_name} onChange={setF("first_name")}
                placeholder="John" className={inputCls} />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Last name</label>
              <input type="text" value={form.last_name} onChange={setF("last_name")}
                placeholder="Smith" className={inputCls} />
            </div>
          </div>

          {/* Address */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Street address</label>
            <input type="text" value={form.address} onChange={setF("address")}
              placeholder="123 Main St" className={inputCls} />
          </div>

          {/* City / State / Zip */}
          <div className="grid grid-cols-[1fr_80px_100px] gap-3">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">City</label>
              <input type="text" value={form.city} onChange={setF("city")}
                placeholder="New York" className={inputCls} />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">State</label>
              <select value={form.state} onChange={setF("state")}
                className={`${inputCls} appearance-none cursor-pointer`}>
                <option value="">—</option>
                {US_STATES.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Zipcode</label>
              <input type="text" value={form.zipcode} onChange={setF("zipcode")}
                placeholder="10001" maxLength={10} className={inputCls} />
            </div>
          </div>

          {/* Mobile */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">
              <span className="flex items-center gap-1.5">
                <Phone className="w-3.5 h-3.5 text-slate-400" />
                Mobile phone
                <span className="text-slate-500 font-normal text-xs">(optional)</span>
              </span>
            </label>
            <input type="text" inputMode="numeric" maxLength={10}
              value={form.mobile_phone}
              onChange={e => { setForm(f => ({ ...f, mobile_phone: e.target.value.replace(/\D/g, "") })); setError(""); }}
              placeholder="6195551234" className={inputCls} />
          </div>

          <button type="submit" disabled={loading || !form.username.trim() || !form.password}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm mt-2">
            {loading ? <><SpinnerSVG /> Creating account…</> : "Create Account"}
          </button>
        </form>
      )}

      {/* ── Step 4: embedded Stripe payment form ── */}
      {step === 4 && stripePromise && clientSecret && (
        <div className="space-y-4">
          <p className="text-slate-400 text-sm">
            Enter your card details to activate your subscription.
          </p>
          <Elements
            stripe={stripePromise}
            options={{
              clientSecret,
              appearance: {
                theme: "night",
                variables: {
                  colorPrimary: "#6366f1",
                  colorBackground: "#0f172a",
                  colorText: "#f1f5f9",
                  colorDanger: "#ef4444",
                  fontFamily: "Segoe UI, Arial, sans-serif",
                  borderRadius: "12px",
                  spacingUnit: "4px",
                },
              },
            }}
          >
            <StripePaymentForm
              onSuccess={handlePaymentSuccess}
              onCancel={() => { setStep(3); setClientSecret(null); setStripePromise(null); }}
              priceLabel="Subscribe — $5.00/mo"
            />
          </Elements>
        </div>
      )}
    </>
  );
}

// ── Forgot password — 3-phase sub-flow ───────────────────────────────────────
// Phase 1: enter email/username  → send code
// Phase 2: enter 6-digit code   → verify
// Phase 3: enter new password   → reset

function ForgotPasswordFlow({ onBack }) {
  const [phase,      setPhase]      = useState("request");  // request | verify | reset
  const [identifier, setIdentifier] = useState("");
  const [code,       setCode]       = useState("");
  const [newPw,      setNewPw]      = useState("");
  const [confirmPw,  setConfirmPw]  = useState("");
  const [showPw,     setShowPw]     = useState(false);
  const [loading,    setLoading]    = useState(false);
  const [error,      setError]      = useState("");
  const [success,    setSuccess]    = useState("");

  // ── Phase 1: send code ────────────────────────────────────────────────────
  async function handleSendCode(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    setLoading(true);
    try {
      const r = await authApi.forgotPassword(identifier.trim().toLowerCase());
      setSuccess(r.data.message);
      setPhase("verify");
    } catch (err) {
      setError(err.response?.data?.error ?? "Failed to send code. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  // ── Phase 2: verify code ──────────────────────────────────────────────────
  async function handleVerifyCode(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    setLoading(true);
    try {
      await authApi.verifyResetCode(identifier.trim().toLowerCase(), code.trim());
      setSuccess("Code verified. Set your new password below.");
      setPhase("reset");
    } catch (err) {
      setError(err.response?.data?.error ?? "Verification failed.");
    } finally {
      setLoading(false);
    }
  }

  // ── Phase 3: set new password ─────────────────────────────────────────────
  async function handleReset(e) {
    e.preventDefault();
    setError(""); setSuccess("");
    if (newPw !== confirmPw) { setError("Passwords do not match."); return; }
    if (newPw.length < 8)    { setError("Password must be at least 8 characters."); return; }
    setLoading(true);
    try {
      const r = await authApi.resetPassword(identifier.trim().toLowerCase(), code.trim(), newPw);
      setSuccess(r.data.message);
      // Brief pause then return to login
      setTimeout(onBack, 2000);
    } catch (err) {
      setError(err.response?.data?.error ?? "Reset failed. Please start over.");
    } finally {
      setLoading(false);
    }
  }

  const stepLabels = ["Send Code", "Verify", "New Password"];
  const stepIdx    = { request: 0, verify: 1, reset: 2 }[phase];

  return (
    <>
      {/* Back link */}
      <button
        onClick={onBack}
        className="flex items-center gap-1.5 text-slate-400 hover:text-slate-200 text-xs mb-5 transition"
      >
        <ArrowLeft className="w-3.5 h-3.5" /> Back to sign in
      </button>

      <h2 className="text-lg font-semibold text-white mb-1">Reset your password</h2>
      <p className="text-slate-400 text-sm mb-5">
        {phase === "request" && "Enter your email or username and we'll send a verification code."}
        {phase === "verify"  && "Enter the 6-digit code sent to your email."}
        {phase === "reset"   && "Choose a new password for your account."}
      </p>

      {/* Step indicators */}
      <div className="flex items-center gap-2 mb-6">
        {stepLabels.map((label, i) => (
          <div key={label} className="flex items-center gap-2">
            <div className={`flex items-center justify-center w-6 h-6 rounded-full text-[11px] font-bold transition-all ${
              i < stepIdx  ? "bg-emerald-500 text-white" :
              i === stepIdx? "bg-brand-600 text-white ring-2 ring-brand-500/40" :
                             "bg-slate-700 text-slate-500"
            }`}>
              {i < stepIdx ? <CheckCircle className="w-3.5 h-3.5" /> : i + 1}
            </div>
            <span className={`text-xs ${i === stepIdx ? "text-slate-200" : "text-slate-500"}`}>{label}</span>
            {i < stepLabels.length - 1 && <div className="w-6 h-px bg-slate-700 mx-1" />}
          </div>
        ))}
      </div>

      {/* Error / success banners */}
      {error && (
        <div className="flex items-start gap-2 bg-red-900/40 border border-red-700 text-red-300 rounded-xl px-4 py-3 mb-4 text-sm">
          <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" /><span>{error}</span>
        </div>
      )}
      {success && (
        <div className="flex items-start gap-2 bg-emerald-900/40 border border-emerald-700 text-emerald-300 rounded-xl px-4 py-3 mb-4 text-sm">
          <CheckCircle className="w-4 h-4 mt-0.5 shrink-0" /><span>{success}</span>
        </div>
      )}

      {/* ── Phase 1 ── */}
      {phase === "request" && (
        <form onSubmit={handleSendCode} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Email or username</label>
            <input
              type="text"
              value={identifier}
              onChange={e => { setIdentifier(e.target.value); setError(""); }}
              placeholder="you@example.com"
              required
              autoFocus
              className={inputCls}
            />
          </div>
          <button
            type="submit"
            disabled={loading || !identifier.trim()}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm"
          >
            {loading ? <><SpinnerSVG /> Sending…</> : <><Mail className="w-4 h-4" /> Send Reset Code</>}
          </button>
        </form>
      )}

      {/* ── Phase 2 ── */}
      {phase === "verify" && (
        <form onSubmit={handleVerifyCode} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">6-digit verification code</label>
            <input
              type="text"
              inputMode="numeric"
              pattern="\d{6}"
              maxLength={6}
              value={code}
              onChange={e => { setCode(e.target.value.replace(/\D/g, "")); setError(""); }}
              placeholder="000000"
              required
              autoFocus
              className={`${inputCls} tracking-[0.5em] text-center text-xl font-bold`}
            />
            <p className="text-xs text-slate-500 mt-1.5">Code expires in 15 minutes.</p>
          </div>
          <button
            type="submit"
            disabled={loading || code.length !== 6}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm"
          >
            {loading ? <><SpinnerSVG /> Verifying…</> : "Verify Code"}
          </button>
          <button type="button" onClick={() => { setPhase("request"); setCode(""); setError(""); setSuccess(""); }}
            className="w-full text-slate-400 hover:text-slate-200 text-xs transition">
            Resend code
          </button>
        </form>
      )}

      {/* ── Phase 3 ── */}
      {phase === "reset" && (
        <form onSubmit={handleReset} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">New password</label>
            <div className="relative">
              <input
                type={showPw ? "text" : "password"}
                value={newPw}
                onChange={e => { setNewPw(e.target.value); setError(""); }}
                placeholder="Min. 8 characters"
                required
                autoFocus
                className={`${inputCls} pr-11`}
              />
              <button type="button" onClick={() => setShowPw(v => !v)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200 transition">
                {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Confirm new password</label>
            <input
              type={showPw ? "text" : "password"}
              value={confirmPw}
              onChange={e => { setConfirmPw(e.target.value); setError(""); }}
              placeholder="••••••••"
              required
              className={inputCls}
            />
          </div>
          <button
            type="submit"
            disabled={loading || !newPw || !confirmPw}
            className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition flex items-center justify-center gap-2 text-sm"
          >
            {loading ? <><SpinnerSVG /> Resetting…</> : "Reset Password"}
          </button>
        </form>
      )}
    </>
  );
}

// ── Main Login component ──────────────────────────────────────────────────────
export default function Login({ onLogin }) {
  const [mode,          setMode]          = useState("login"); // "login" | "register" | "forgot"
  const [username,      setUsername]      = useState("");
  const [password,      setPassword]      = useState("");
  const [showPassword,  setShowPassword]  = useState(false);
  const [loading,       setLoading]       = useState(false);
  const [error,         setError]         = useState("");
  // Subscription payment state (shown when login returns 402)
  const [subStripePromise, setSubStripePromise] = useState(null);
  const [subClientSecret,  setSubClientSecret]  = useState(null);

  const handleLogin = async (e) => {
    if (e) e.preventDefault();
    setError(""); setSubStripePromise(null); setSubClientSecret(null);
    setLoading(true);
    try {
      const res = await authApi.login(username, password);
      onLogin(res.data.token, res.data.user, res.data.required_version, res.data.download_url);
    } catch (err) {
      if (err.response?.status === 402) {
        const { client_secret, publishable_key, message } = err.response.data || {};
        setError(message || "An active subscription is required.");
        if (client_secret && publishable_key) {
          setSubStripePromise(loadStripe(publishable_key));
          setSubClientSecret(client_secret);
        }
      } else {
        setError(err.response?.data?.error || "Connection failed. Check that the server is running.");
      }
    } finally {
      setLoading(false);
    }
  };

  async function handleSubscriptionSuccess() {
    setSubStripePromise(null);
    setSubClientSecret(null);
    setError("");
    // Re-attempt login now that subscription is active
    await handleLogin(null);
  }

  // Expand card for register / forgot flows
  const isExpanded = mode === "register" || mode === "forgot";

  return (
    <div className="min-h-screen bg-slate-900 flex items-center justify-center p-4">
      {/* Background grid */}
      <div className="absolute inset-0 opacity-5" style={{
        backgroundImage: "linear-gradient(#3b82f6 1px, transparent 1px), linear-gradient(90deg, #3b82f6 1px, transparent 1px)",
        backgroundSize: "40px 40px",
      }} />

      <div className={`relative w-full transition-all duration-300 ${isExpanded ? "max-w-lg" : "max-w-md"}`}>
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl mb-4 shadow-lg overflow-hidden">
            <img src={logo} alt="TradeFinder" className="w-full h-full object-cover" />
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">TradeFinder</h1>
          <p className="mt-1 text-slate-400 text-sm">Real-time stock market trade ideas</p>
        </div>

        {/* Card */}
        <div className="bg-slate-800 rounded-2xl border border-slate-700 p-8 shadow-2xl">

          {/* ── Register flow ── */}
          {mode === "register" && (
            <RegisterFlow
              onSuccess={onLogin}
              onBack={() => { setMode("login"); setError(""); }}
            />
          )}

          {/* ── Forgot password flow ── */}
          {mode === "forgot" && (
            <ForgotPasswordFlow onBack={() => { setMode("login"); setError(""); }} />
          )}

          {/* ── Login form ── */}
          {mode === "login" && (
            <>
              {/* Tabs */}
              <div className="flex bg-slate-900 rounded-xl p-1 mb-6">
                <button onClick={() => { setMode("login"); setError(""); }}
                  className="flex-1 py-2 text-sm font-medium rounded-lg transition-all bg-brand-600 text-white shadow">
                  Sign In
                </button>
                <button onClick={() => { setMode("register"); setError(""); }}
                  className="flex-1 py-2 text-sm font-medium rounded-lg transition-all text-slate-400 hover:text-slate-200">
                  Register
                </button>
              </div>

              {error && (
                <div className="mb-5 space-y-3">
                  <div className="flex items-start gap-2 bg-red-900/40 border border-red-700 text-red-300 rounded-xl px-4 py-3 text-sm">
                    <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" /><span>{error}</span>
                  </div>
                  {subStripePromise && subClientSecret && (
                    <div className="space-y-2">
                      <p className="text-slate-400 text-xs">Enter your card to activate your subscription:</p>
                      <Elements
                        stripe={subStripePromise}
                        options={{
                          clientSecret: subClientSecret,
                          appearance: {
                            theme: "night",
                            variables: {
                              colorPrimary: "#6366f1",
                              colorBackground: "#0f172a",
                              colorText: "#f1f5f9",
                              colorDanger: "#ef4444",
                              fontFamily: "Segoe UI, Arial, sans-serif",
                              borderRadius: "12px",
                            },
                          },
                        }}
                      >
                        <StripePaymentForm
                          onSuccess={handleSubscriptionSuccess}
                          priceLabel="Subscribe — $5.00/mo"
                        />
                      </Elements>
                    </div>
                  )}
                </div>
              )}

              <form onSubmit={handleLogin} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1.5">Username</label>
                  <input type="text" autoComplete="username" value={username}
                    onChange={e => setUsername(e.target.value)} placeholder="Enter your username"
                    required className={inputCls} />
                </div>

                <div>
                  <div className="flex items-center justify-between mb-1.5">
                    <label className="block text-sm font-medium text-slate-300">Password</label>
                    <button type="button" onClick={() => { setMode("forgot"); setError(""); }}
                      className="text-xs text-brand-400 hover:text-brand-300 transition">
                      Forgot password?
                    </button>
                  </div>
                  <div className="relative">
                    <input type={showPassword ? "text" : "password"} autoComplete="current-password"
                      value={password} onChange={e => setPassword(e.target.value)}
                      placeholder="Enter your password" required className={`${inputCls} pr-11`} />
                    <button type="button" onClick={() => setShowPassword(v => !v)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200 transition">
                      {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                    </button>
                  </div>
                </div>

                <button type="submit" disabled={loading}
                  className="w-full bg-brand-600 hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-2.5 rounded-xl transition-colors mt-2 flex items-center justify-center gap-2 text-sm">
                  {loading ? <><SpinnerSVG /> Signing in…</> : "Sign In"}
                </button>
              </form>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
