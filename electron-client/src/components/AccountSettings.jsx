import { useState, useEffect, useCallback, useRef } from "react";
import { authApi, alpacaApi, preferencesApi } from "../api/client";
import {
  User, Lock, TrendingUp, ShieldAlert, Zap,
  CheckCircle, AlertCircle, Loader2, Save, RefreshCw, DollarSign, Wallet,
  AtSign, XCircle, Mail, Send, MapPin, Phone,
} from "lucide-react";

// ── Small reusable status banner ──────────────────────────────────────────────
function StatusBanner({ status }) {
  if (!status) return null;
  const isError = status.type === "error";
  return (
    <div className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm ${
      isError ? "bg-red-900/30 text-red-400 border border-red-800/50"
              : "bg-emerald-900/30 text-emerald-400 border border-emerald-800/50"
    }`}>
      {isError
        ? <AlertCircle className="w-4 h-4 shrink-0" />
        : <CheckCircle className="w-4 h-4 shrink-0" />}
      {status.message}
    </div>
  );
}

// ── Section card wrapper ──────────────────────────────────────────────────────
function Section({ icon: Icon, title, children }) {
  return (
    <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl p-6">
      <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200 mb-5">
        <Icon className="w-4 h-4 text-brand-400" />
        {title}
      </h2>
      {children}
    </div>
  );
}

// ── Profile section ───────────────────────────────────────────────────────────
function ProfileSection({ user, currentUsername, currentEmail }) {
  const joined = user?.created_at
    ? new Date(user.created_at).toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" })
    : "—";

  return (
    <Section icon={User} title="Profile">
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-xs text-slate-500 mb-1">Username</p>
          <p className="text-sm text-slate-200 font-medium">{currentUsername ?? user?.username ?? "—"}</p>
        </div>
        <div>
          <p className="text-xs text-slate-500 mb-1">Email</p>
          <p className="text-sm text-slate-200 font-medium">{currentEmail ?? user?.email ?? "—"}</p>
        </div>
        <div>
          <p className="text-xs text-slate-500 mb-1">Member since</p>
          <p className="text-sm text-slate-200">{joined}</p>
        </div>
        <div>
          <p className="text-xs text-slate-500 mb-1">Status</p>
          <span className="inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded-full bg-emerald-900/40 text-emerald-400 border border-emerald-800/50">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
            Active
          </span>
        </div>
      </div>
    </Section>
  );
}

// ── Change username section ───────────────────────────────────────────────────
function ChangeUsernameSection({ currentUsername, onSuccess }) {
  const [value,      setValue]      = useState("");
  const [checkState, setCheckState] = useState("idle"); // idle | checking | available | same | taken | error
  const [saving,     setSaving]     = useState(false);
  const [status,     setStatus]     = useState(null);
  const debounceRef = useRef(null);

  const inputCls = "w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 pr-28 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition";

  function handleChange(e) {
    const v = e.target.value;
    setValue(v);
    setStatus(null);

    if (debounceRef.current) clearTimeout(debounceRef.current);

    const trimmed = v.trim();
    if (!trimmed) { setCheckState("idle"); return; }
    if (trimmed === currentUsername) { setCheckState("same"); return; }
    if (trimmed.length < 3) { setCheckState("idle"); return; }

    setCheckState("checking");
    debounceRef.current = setTimeout(async () => {
      try {
        const r = await authApi.checkUsername(trimmed);
        if (r.data.same)            setCheckState("same");
        else if (r.data.available)  setCheckState("available");
        else                        setCheckState("taken");
      } catch {
        setCheckState("error");
      }
    }, 400);
  }

  async function handleSubmit(e) {
    e.preventDefault();
    if (checkState !== "available") return;
    setSaving(true);
    setStatus(null);
    try {
      await authApi.changeUsername(value.trim());
      setStatus({ type: "success", message: "Username updated successfully." });
      onSuccess(value.trim());
      setValue("");
      setCheckState("idle");
    } catch (err) {
      setStatus({ type: "error", message: err.response?.data?.error ?? "Failed to update username." });
    } finally {
      setSaving(false);
    }
  }

  const indicator = (() => {
    if (checkState === "checking")
      return <span className="flex items-center gap-1 text-slate-400 text-xs"><Loader2 className="w-3.5 h-3.5 animate-spin" /> Checking…</span>;
    if (checkState === "available")
      return <span className="flex items-center gap-1 text-emerald-400 text-xs"><CheckCircle className="w-3.5 h-3.5" /> Available</span>;
    if (checkState === "taken")
      return <span className="flex items-center gap-1 text-red-400 text-xs"><XCircle className="w-3.5 h-3.5" /> Already taken</span>;
    if (checkState === "same")
      return <span className="flex items-center gap-1 text-slate-400 text-xs"><AlertCircle className="w-3.5 h-3.5" /> Same as current</span>;
    if (checkState === "error")
      return <span className="flex items-center gap-1 text-yellow-400 text-xs"><AlertCircle className="w-3.5 h-3.5" /> Check failed</span>;
    return null;
  })();

  return (
    <Section icon={AtSign} title="Change Username">
      <form onSubmit={handleSubmit} className="flex flex-col gap-4">
        <p className="text-xs text-slate-500">
          Current username: <span className="font-medium text-slate-300">{currentUsername}</span>
        </p>
        <div>
          <label className="block text-xs text-slate-500 mb-1.5">New username</label>
          <div className="relative">
            <input
              type="text"
              value={value}
              onChange={handleChange}
              placeholder="Enter new username"
              minLength={3}
              maxLength={64}
              autoComplete="off"
              className={inputCls}
            />
            {indicator && (
              <span className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none">
                {indicator}
              </span>
            )}
          </div>
          <p className="text-[11px] text-slate-500 mt-1">3–64 characters. Must be unique across all accounts.</p>
        </div>
        <StatusBanner status={status} />
        <div>
          <button
            type="submit"
            disabled={saving || checkState !== "available"}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium transition"
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            {saving ? "Saving…" : "Update Username"}
          </button>
        </div>
      </form>
    </Section>
  );
}

// ── Change email section ──────────────────────────────────────────────────────
function ChangeEmailSection({ currentEmail, onSuccess }) {
  // phase: "request" → user enters new email
  //        "verify"  → user enters the 6-digit code
  const [phase,      setPhase]      = useState("request");
  const [email,      setEmail]      = useState("");
  const [code,       setCode]       = useState("");
  const [sending,    setSending]    = useState(false);
  const [verifying,  setVerifying]  = useState(false);
  const [status,     setStatus]     = useState(null);
  const [pendingEmail, setPendingEmail] = useState("");

  const inputCls = "w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition";

  async function handleSendCode(e) {
    e.preventDefault();
    setStatus(null);
    setSending(true);
    try {
      await authApi.requestEmailChange(email.trim());
      setPendingEmail(email.trim());
      setEmail("");
      setCode("");
      setPhase("verify");
      setStatus({ type: "success", message: `Verification code sent to ${email.trim()}` });
    } catch (err) {
      setStatus({ type: "error", message: err.response?.data?.error ?? "Failed to send verification code." });
    } finally {
      setSending(false);
    }
  }

  async function handleVerify(e) {
    e.preventDefault();
    setStatus(null);
    setVerifying(true);
    try {
      const r = await authApi.verifyEmailChange(code.trim());
      setStatus({ type: "success", message: "Email updated successfully." });
      setCode("");
      setPendingEmail("");
      setPhase("request");
      onSuccess(r.data.user?.email ?? pendingEmail);
    } catch (err) {
      setStatus({ type: "error", message: err.response?.data?.error ?? "Verification failed." });
    } finally {
      setVerifying(false);
    }
  }

  function handleRestart() {
    setPhase("request");
    setCode("");
    setStatus(null);
    setPendingEmail("");
  }

  return (
    <Section icon={Mail} title="Change Email">
      {phase === "request" ? (
        <form onSubmit={handleSendCode} className="flex flex-col gap-4">
          <p className="text-xs text-slate-500">
            Current email: <span className="font-medium text-slate-300">{currentEmail}</span>
          </p>
          <div>
            <label className="block text-xs text-slate-500 mb-1.5">New email address</label>
            <input
              type="email"
              value={email}
              onChange={e => { setEmail(e.target.value); setStatus(null); }}
              placeholder="you@example.com"
              required
              className={inputCls}
            />
            <p className="text-[11px] text-slate-500 mt-1">
              A 6-digit verification code will be sent to this address via Postmark.
            </p>
          </div>
          <StatusBanner status={status} />
          <div>
            <button
              type="submit"
              disabled={sending || !email.trim()}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium transition"
            >
              {sending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              {sending ? "Sending…" : "Send Verification Code"}
            </button>
          </div>
        </form>
      ) : (
        <form onSubmit={handleVerify} className="flex flex-col gap-4">
          <div className="flex items-start gap-3 px-3 py-2.5 rounded-lg bg-slate-900/60 border border-slate-700/50">
            <Mail className="w-4 h-4 text-brand-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-xs text-slate-400">Code sent to</p>
              <p className="text-sm font-medium text-slate-200">{pendingEmail}</p>
            </div>
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1.5">Enter 6-digit code</label>
            <input
              type="text"
              inputMode="numeric"
              pattern="\d{6}"
              maxLength={6}
              value={code}
              onChange={e => { setCode(e.target.value.replace(/\D/g, "")); setStatus(null); }}
              placeholder="000000"
              required
              className={`${inputCls} tracking-[0.4em] text-center text-lg font-bold`}
            />
            <p className="text-[11px] text-slate-500 mt-1">Code expires in 15 minutes.</p>
          </div>
          <StatusBanner status={status} />
          <div className="flex items-center gap-3">
            <button
              type="submit"
              disabled={verifying || code.length !== 6}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium transition"
            >
              {verifying ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle className="w-4 h-4" />}
              {verifying ? "Verifying…" : "Confirm Change"}
            </button>
            <button
              type="button"
              onClick={handleRestart}
              className="text-xs text-slate-400 hover:text-slate-200 transition"
            >
              Use a different email
            </button>
          </div>
        </form>
      )}
    </Section>
  );
}

// ── Change password section ───────────────────────────────────────────────────
function ChangePasswordSection({ userEmail }) {
  // phase: "request" → enter current + new passwords
  //        "verify"  → enter 6-digit code
  const [phase,     setPhase]     = useState("request");
  const [form,      setForm]      = useState({ next: "", confirm: "" });
  const [code,      setCode]      = useState("");
  const [sending,   setSending]   = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [status,    setStatus]    = useState(null);

  const set = (k) => (e) => { setForm(f => ({ ...f, [k]: e.target.value })); setStatus(null); };
  const inputCls = "w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition";

  async function handleSendCode(e) {
    e.preventDefault();
    setStatus(null);
    if (form.next !== form.confirm) {
      setStatus({ type: "error", message: "New passwords do not match." });
      return;
    }
    if (form.next.length < 8) {
      setStatus({ type: "error", message: "New password must be at least 8 characters." });
      return;
    }
    setSending(true);
    try {
      const r = await authApi.requestPasswordChange(null, form.next);
      setStatus({ type: "success", message: r.data.message });
      setCode("");
      setPhase("verify");
    } catch (err) {
      setStatus({ type: "error", message: err.response?.data?.error ?? "Failed to send verification code." });
    } finally {
      setSending(false);
    }
  }

  async function handleVerify(e) {
    e.preventDefault();
    setStatus(null);
    setVerifying(true);
    try {
      await authApi.verifyPasswordChange(code.trim());
      setStatus({ type: "success", message: "Password updated successfully." });
      setForm({ next: "", confirm: "" });
      setCode("");
      setPhase("request");
    } catch (err) {
      setStatus({ type: "error", message: err.response?.data?.error ?? "Verification failed." });
    } finally {
      setVerifying(false);
    }
  }

  function handleRestart() {
    setPhase("request");
    setCode("");
    setStatus(null);
  }

  return (
    <Section icon={Lock} title="Change Password">
      {phase === "request" ? (
        <form onSubmit={handleSendCode} className="flex flex-col gap-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-slate-500 mb-1.5">New password</label>
              <input type="password" value={form.next} onChange={set("next")} placeholder="••••••••" required className={inputCls} />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1.5">Confirm new password</label>
              <input type="password" value={form.confirm} onChange={set("confirm")} placeholder="••••••••" required className={inputCls} />
            </div>
          </div>
          <p className="text-[11px] text-slate-500">
            A 6-digit verification code will be sent to your email before the password is changed.
          </p>
          <StatusBanner status={status} />
          <div>
            <button
              type="submit"
              disabled={sending}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white text-sm font-medium transition"
            >
              {sending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              {sending ? "Sending…" : "Send Verification Code"}
            </button>
          </div>
        </form>
      ) : (
        <form onSubmit={handleVerify} className="flex flex-col gap-4">
          <div className="flex items-start gap-3 px-3 py-2.5 rounded-lg bg-slate-900/60 border border-slate-700/50">
            <Mail className="w-4 h-4 text-brand-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-xs text-slate-400">Verification code sent to your email</p>
              {userEmail && <p className="text-sm font-medium text-slate-200">{userEmail}</p>}
            </div>
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1.5">Enter 6-digit code</label>
            <input
              type="text"
              inputMode="numeric"
              pattern="\d{6}"
              maxLength={6}
              value={code}
              onChange={e => { setCode(e.target.value.replace(/\D/g, "")); setStatus(null); }}
              placeholder="000000"
              required
              className={`${inputCls} tracking-[0.4em] text-center text-lg font-bold`}
            />
            <p className="text-[11px] text-slate-500 mt-1">Code expires in 15 minutes.</p>
          </div>
          <StatusBanner status={status} />
          <div className="flex items-center gap-3">
            <button
              type="submit"
              disabled={verifying || code.length !== 6}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium transition"
            >
              {verifying ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle className="w-4 h-4" />}
              {verifying ? "Verifying…" : "Confirm Change"}
            </button>
            <button type="button" onClick={handleRestart} className="text-xs text-slate-400 hover:text-slate-200 transition">
              Start over
            </button>
          </div>
        </form>
      )}
    </Section>
  );
}

// ── Stat tile ─────────────────────────────────────────────────────────────────
function StatTile({ icon: Icon, label, value, highlight }) {
  return (
    <div className="bg-slate-900/60 border border-slate-700/50 rounded-lg p-4 flex flex-col gap-1.5">
      <div className="flex items-center gap-1.5 text-xs text-slate-500">
        <Icon className="w-3.5 h-3.5" />
        {label}
      </div>
      <p className={`text-lg font-semibold ${highlight ? "text-emerald-400" : "text-slate-100"}`}>
        {value}
      </p>
    </div>
  );
}

// ── Trade settings section ────────────────────────────────────────────────────
function TradeSettingsSection({ account, loading, error, onRefresh }) {
  const fmt = (n) =>
    n == null ? "—" : `$${parseFloat(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  return (
    <Section icon={TrendingUp} title="Trade Settings">
      <div className="flex items-center justify-between mb-4">
        <p className="text-xs text-slate-500">Alpaca brokerage account summary</p>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-slate-700 hover:bg-slate-600 disabled:opacity-50 text-slate-300 text-xs font-medium transition"
        >
          <RefreshCw className={`w-3 h-3 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-slate-500 text-sm py-4">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading account data…
        </div>
      )}

      {!loading && error && (
        <div className="flex items-center gap-2 px-3 py-2.5 rounded-lg bg-red-900/30 text-red-400 border border-red-800/50 text-sm">
          <AlertCircle className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {!loading && account && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2">
            <span className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full font-medium border ${
              account.paper
                ? "bg-amber-900/30 text-amber-400 border-amber-800/50"
                : "bg-emerald-900/30 text-emerald-400 border-emerald-800/50"
            }`}>
              <span className={`w-1.5 h-1.5 rounded-full ${account.paper ? "bg-amber-400" : "bg-emerald-400"}`} />
              {account.paper ? "Paper Trading" : "Live Trading"}
            </span>
            <span className="text-xs text-slate-500 capitalize">{account.status}</span>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <StatTile icon={Wallet}     label="Portfolio Value" value={fmt(account.portfolio_value)} highlight />
            <StatTile icon={DollarSign} label="Buying Power"    value={fmt(account.buying_power)} />
          </div>

          <p className="text-xs text-slate-600">
            Account ID: <span className="font-mono text-slate-500">{account.account_id}</span>
          </p>
        </div>
      )}
    </Section>
  );
}

// ── Contact info section ──────────────────────────────────────────────────────
const US_STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

function ContactInfoSection() {
  const [form,    setForm]    = useState({ first_name: "", last_name: "", address: "", city: "", state: "", zipcode: "", mobile_phone: "" });
  const [loading, setLoading] = useState(true);
  const [saving,  setSaving]  = useState(false);
  const [status,  setStatus]  = useState(null);

  useEffect(() => {
    authApi.getProfile()
      .then(r => {
        const u = r.data.user ?? {};
        setForm({
          first_name:   u.first_name   ?? "",
          last_name:    u.last_name    ?? "",
          address:      u.address      ?? "",
          city:         u.city         ?? "",
          state:        u.state        ?? "",
          zipcode:      u.zipcode      ?? "",
          mobile_phone: u.mobile_phone ?? "",
        });
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const set = (k) => (e) => { setForm(f => ({ ...f, [k]: e.target.value })); setStatus(null); };

  async function handleSave(e) {
    e.preventDefault();
    setStatus(null);
    if (!form.address.trim() || !form.city.trim() || !form.state || !form.zipcode.trim()) {
      setStatus({ type: "error", message: "Address, City, State, and Zipcode are required." });
      return;
    }
    setSaving(true);
    try {
      await authApi.updateProfile({
        first_name:   form.first_name.trim(),
        last_name:    form.last_name.trim(),
        address:      form.address.trim(),
        city:         form.city.trim(),
        state:        form.state,
        zipcode:      form.zipcode.trim(),
        mobile_phone: form.mobile_phone.trim(),
      });
      setStatus({ type: "success", message: "Contact information saved." });
    } catch {
      setStatus({ type: "error", message: "Failed to save. Please try again." });
    } finally {
      setSaving(false);
    }
  }

  const inputCls = "w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition";

  return (
    <Section icon={MapPin} title="Contact Information">
      {loading ? (
        <div className="flex items-center gap-2 text-slate-500 text-sm">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading…
        </div>
      ) : (
        <form onSubmit={handleSave} className="flex flex-col gap-4">

          {/* First / Last name */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-slate-500 mb-1.5">First name</label>
              <input
                type="text"
                value={form.first_name}
                onChange={set("first_name")}
                placeholder="John"
                className={inputCls}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1.5">Last name</label>
              <input
                type="text"
                value={form.last_name}
                onChange={set("last_name")}
                placeholder="Smith"
                className={inputCls}
              />
            </div>
          </div>

          {/* Street address */}
          <div>
            <label className="block text-xs text-slate-500 mb-1.5">Street address</label>
            <input
              type="text"
              value={form.address}
              onChange={set("address")}
              placeholder="123 Main St"
              className={inputCls}
            />
          </div>

          {/* City / State / Zip on one row */}
          <div className="grid grid-cols-[1fr_auto_auto] gap-3">
            <div>
              <label className="block text-xs text-slate-500 mb-1.5">City</label>
              <input
                type="text"
                value={form.city}
                onChange={set("city")}
                placeholder="New York"
                className={inputCls}
              />
            </div>
            <div className="w-24">
              <label className="block text-xs text-slate-500 mb-1.5">State</label>
              <select
                value={form.state}
                onChange={set("state")}
                className={`${inputCls} appearance-none cursor-pointer`}
              >
                <option value="">—</option>
                {US_STATES.map(s => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>
            <div className="w-28">
              <label className="block text-xs text-slate-500 mb-1.5">Zipcode</label>
              <input
                type="text"
                value={form.zipcode}
                onChange={set("zipcode")}
                placeholder="10001"
                maxLength={10}
                className={inputCls}
              />
            </div>
          </div>

          {/* Mobile phone (optional) */}
          <div>
            <label className="block text-xs text-slate-500 mb-1.5">
              <span className="flex items-center gap-1.5">
                <Phone className="w-3 h-3" />
                Mobile phone
                <span className="text-slate-600 font-normal">(optional — for text alerts)</span>
              </span>
            </label>
            <input
              type="text"
              inputMode="numeric"
              value={form.mobile_phone}
              onChange={e => { setForm(f => ({ ...f, mobile_phone: e.target.value.replace(/\D/g, "") })); setStatus(null); }}
              placeholder="6195551234"
              maxLength={10}
              className={inputCls}
            />
          </div>

          <StatusBanner status={status} />

          <div>
            <button
              type="submit"
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium transition"
            >
              {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
              {saving ? "Saving…" : "Save Contact Info"}
            </button>
          </div>
        </form>
      )}
    </Section>
  );
}

// ── Auto market close when price is past take profit (server-side) ─────────────
function AutoCloseBeyondTpSection() {
  const [enabled, setEnabled] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState(null);

  useEffect(() => {
    preferencesApi
      .get()
      .then((r) => {
        const v = r.data.preferences?.auto_market_close_beyond_tp;
        setEnabled(v === true || String(v).toLowerCase() === "true" || v === "1");
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  async function handleToggle() {
    const next = !enabled;
    setSaving(true);
    setStatus(null);
    try {
      await preferencesApi.update({ auto_market_close_beyond_tp: next ? "true" : "false" });
      setEnabled(next);
      setStatus({ type: "success", message: "Preference saved." });
    } catch {
      setStatus({ type: "error", message: "Failed to save. Please try again." });
    } finally {
      setSaving(false);
    }
  }

  return (
    <Section icon={Zap} title="Take-profit automation">
      {loading ? (
        <div className="flex items-center gap-2 text-slate-500 text-sm">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading…
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          <p className="text-xs text-slate-500 leading-relaxed">
            When the last traded price (from the app&apos;s snapshot feed) moves past your take-profit
            level while the position is still open, automatically cancel the bracket legs and send a
            <span className="text-slate-400"> market </span>
            order to flatten—same behavior as closing a trade manually from My Trades. The check runs
            about once per minute while the app server is running.
          </p>
          <div className="flex items-center justify-between gap-4 py-1">
            <span className="text-sm text-slate-300">Auto-close with market order past take profit</span>
            <button
              type="button"
              role="switch"
              aria-checked={enabled}
              disabled={saving}
              onClick={handleToggle}
              className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-brand-500/50 disabled:opacity-50 ${
                enabled ? "bg-brand-600" : "bg-slate-600"
              }`}
            >
              <span
                className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition ${
                  enabled ? "translate-x-6" : "translate-x-1"
                }`}
              />
            </button>
          </div>
          <p className="text-[11px] text-slate-600 leading-relaxed">
            Snapshots are not tick-perfect. Review live and paper behavior before relying on this for real
            money. Requires Alpaca credentials and an open trade recorded in TradeFinder.
          </p>
          <StatusBanner status={status} />
        </div>
      )}
    </Section>
  );
}

// ── Risk management section ───────────────────────────────────────────────────
function RiskManagementSection({ portfolioValue }) {
  const [mode,    setMode]    = useState("dollar");   // "dollar" | "percent"
  const [value,   setValue]   = useState("");
  const [saving,  setSaving]  = useState(false);
  const [status,  setStatus]  = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    preferencesApi.get()
      .then(r => {
        const p = r.data.preferences ?? {};
        if (p.risk_mode)  setMode(p.risk_mode);
        if (p.risk_value) setValue(p.risk_value);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const numVal = parseFloat(value) || 0;

  const derivedDollar = (() => {
    if (mode !== "percent" || !numVal) return null;
    const portfolio = parseFloat(portfolioValue) || 0;
    if (!portfolio) return null;
    return (numVal / 100) * portfolio;
  })();

  const fmt = (n) =>
    `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  async function handleSave(e) {
    e.preventDefault();
    setStatus(null);
    if (!numVal || numVal <= 0) {
      setStatus({ type: "error", message: "Enter a value greater than zero." });
      return;
    }
    if (mode === "percent" && numVal > 100) {
      setStatus({ type: "error", message: "Percent cannot exceed 100." });
      return;
    }
    setSaving(true);
    try {
      await preferencesApi.update({ risk_mode: mode, risk_value: String(numVal) });
      setStatus({ type: "success", message: "Risk settings saved." });
    } catch {
      setStatus({ type: "error", message: "Failed to save. Please try again." });
    } finally {
      setSaving(false);
    }
  }

  const inputCls = "bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition w-full";

  return (
    <Section icon={ShieldAlert} title="Risk Management">
      {loading ? (
        <div className="flex items-center gap-2 text-slate-500 text-sm">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading…
        </div>
      ) : (
        <form onSubmit={handleSave} className="flex flex-col gap-5">
          <p className="text-xs text-slate-500">
            Set the maximum amount you are willing to risk per trade.
          </p>

          {/* ── Mode toggle ── */}
          <div>
            <label className="block text-xs text-slate-500 mb-2">Risk type</label>
            <div className="flex rounded-lg overflow-hidden border border-slate-700 w-fit">
              {[
                { id: "dollar",  label: "$ Dollar amount" },
                { id: "percent", label: "% Portfolio percent" },
              ].map(opt => (
                <button
                  key={opt.id}
                  type="button"
                  onClick={() => { setMode(opt.id); setValue(""); setStatus(null); }}
                  className={`px-4 py-2 text-xs font-medium transition ${
                    mode === opt.id
                      ? "bg-brand-600 text-white"
                      : "bg-slate-800 text-slate-400 hover:text-slate-200"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* ── Value input ── */}
          <div>
            {mode === "dollar" ? (
              <>
                <label className="block text-xs text-slate-500 mb-1.5">Amount per trade</label>
                <div className="relative">
                  <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 text-sm">$</span>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={value}
                    onChange={e => { setValue(e.target.value); setStatus(null); }}
                    placeholder="500.00"
                    className={`${inputCls} pl-7`}
                  />
                </div>
              </>
            ) : (
              <>
                <label className="block text-xs text-slate-500 mb-1.5">Percent of portfolio per trade</label>
                <div className="relative">
                  <input
                    type="number"
                    min="0"
                    max="100"
                    step="0.1"
                    value={value}
                    onChange={e => { setValue(e.target.value); setStatus(null); }}
                    placeholder="2.0"
                    className={`${inputCls} pr-7`}
                  />
                  <span className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 text-sm">%</span>
                </div>

                {/* ── Derived dollar value ── */}
                <div className={`mt-3 flex items-center gap-3 px-3 py-2.5 rounded-lg border transition ${
                  derivedDollar != null
                    ? "bg-slate-900/60 border-slate-700/50"
                    : "bg-slate-900/30 border-slate-700/30"
                }`}>
                  <DollarSign className="w-4 h-4 text-slate-500 shrink-0" />
                  <div>
                    <p className="text-xs text-slate-500">Equals approximately</p>
                    <p className={`text-sm font-semibold mt-0.5 ${derivedDollar != null ? "text-emerald-400" : "text-slate-600"}`}>
                      {derivedDollar != null
                        ? fmt(derivedDollar)
                        : portfolioValue
                          ? "Enter a percent above"
                          : "Connect Alpaca to see derived value"}
                    </p>
                  </div>
                  {portfolioValue && (
                    <p className="ml-auto text-xs text-slate-600">
                      of {fmt(parseFloat(portfolioValue))}
                    </p>
                  )}
                </div>
              </>
            )}
          </div>

          <StatusBanner status={status} />

          <div>
            <button
              type="submit"
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white text-sm font-medium transition"
            >
              {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
              {saving ? "Saving…" : "Save Risk Settings"}
            </button>
          </div>
        </form>
      )}
    </Section>
  );
}

// ── Main export ───────────────────────────────────────────────────────────────
export default function AccountSettings({ user, onUserUpdated }) {
  const [currentUsername, setCurrentUsername] = useState(user?.username ?? "");
  const [currentEmail,    setCurrentEmail]    = useState(user?.email    ?? "");
  const [account,  setAccount]  = useState(null);
  const [acctLoad, setAcctLoad] = useState(true);
  const [acctErr,  setAcctErr]  = useState(null);

  const loadAccount = useCallback(() => {
    setAcctLoad(true);
    setAcctErr(null);
    alpacaApi.test()
      .then(r => {
        if (r.data.ok) setAccount(r.data);
        else setAcctErr(r.data.error ?? "Could not load account data.");
      })
      .catch(() => setAcctErr("Failed to reach the server."))
      .finally(() => setAcctLoad(false));
  }, []);

  useEffect(() => { loadAccount(); }, [loadAccount]);

  function handleUsernameChanged(newUsername) {
    setCurrentUsername(newUsername);
    onUserUpdated?.({ ...user, username: newUsername });
  }

  function handleEmailChanged(newEmail) {
    setCurrentEmail(newEmail);
    onUserUpdated?.({ ...user, email: newEmail });
  }

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-2xl mx-auto px-6 py-8 flex flex-col gap-6">
        <div>
          <h1 className="text-lg font-semibold text-slate-100">Account Settings</h1>
          <p className="text-sm text-slate-500 mt-0.5">Manage your profile, password, and trading account.</p>
        </div>
        <ProfileSection user={user} currentUsername={currentUsername} currentEmail={currentEmail} />
        <ChangeUsernameSection currentUsername={currentUsername} onSuccess={handleUsernameChanged} />
        <ChangeEmailSection    currentEmail={currentEmail}        onSuccess={handleEmailChanged} />
        <ChangePasswordSection userEmail={currentEmail} />
        <ContactInfoSection />
        <TradeSettingsSection
          account={account}
          loading={acctLoad}
          error={acctErr}
          onRefresh={loadAccount}
        />
        <AutoCloseBeyondTpSection />
        <RiskManagementSection portfolioValue={account?.portfolio_value ?? null} />
      </div>
    </div>
  );
}
