import { useState, useEffect } from "react";
import { settingsApi, alpacaApi } from "../api/client";
import { CheckCircle, AlertCircle, Loader2, Save, Eye, EyeOff, Info, Wifi } from "lucide-react";

// ── Broker definitions ────────────────────────────────────────────────────────
const BROKERS = [
  {
    id:          "alpaca",
    name:        "Alpaca",
    tagline:     "Commission-free API-first broker",
    color:       "from-yellow-500/20 to-yellow-600/10 border-yellow-600/40",
    activeColor: "from-yellow-500/30 to-yellow-600/20 border-yellow-500/70",
    dot:         "bg-yellow-400",
    // fields not used for Alpaca — managed by AlpacaForm below
    fields: [
      { key: "alpaca_api_key",    secret: true  },
      { key: "alpaca_api_secret", secret: true  },
    ],
    note: "Create keys in your Alpaca dashboard under the Paper or Live account section.",
    docsUrl: "https://alpaca.markets/docs/api-references/trading-api/",
  },
  {
    id:          "schwab",
    name:        "Charles Schwab",
    tagline:     "Full-service broker with API access",
    color:       "from-blue-500/20 to-blue-600/10 border-blue-600/40",
    activeColor: "from-blue-500/30 to-blue-600/20 border-blue-500/70",
    dot:         "bg-blue-400",
    fields: [
      { key: "schwab_app_key",      label: "App Key (Client ID)",        placeholder: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", secret: true  },
      { key: "schwab_app_secret",   label: "App Secret (Client Secret)", placeholder: "xxxxxxxxxxxxxxxx",                secret: true  },
      { key: "schwab_callback_url", label: "Callback URL",               placeholder: "https://127.0.0.1",               secret: false },
    ],
    toggle: null,
    note: "Register your app at the Schwab Developer Portal to obtain credentials. OAuth 2.0 authorization required before trading.",
    docsUrl: "https://developer.schwab.com/",
  },
  {
    id:          "tradestation",
    name:        "TradeStation",
    tagline:     "Professional trading platform & API",
    color:       "from-red-500/20 to-red-600/10 border-red-600/40",
    activeColor: "from-red-500/30 to-red-600/20 border-red-500/70",
    dot:         "bg-red-400",
    fields: [
      { key: "tradestation_api_key",    label: "API Key (Client ID)",        placeholder: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", secret: true  },
      { key: "tradestation_api_secret", label: "API Secret (Client Secret)", placeholder: "xxxxxxxxxxxxxxxx",                secret: true  },
    ],
    toggle: { key: "tradestation_sim_mode", label: "Simulation mode", note: "Routes orders to the TradeStation simulation environment" },
    note: "Register at the TradeStation Developer portal. OAuth 2.0 — you will need to authorize this app before placing orders.",
    docsUrl: "https://api.tradestation.com/docs/",
  },
  {
    id:          "moomoo",
    name:        "moomoo",
    tagline:     "Futu OpenD local gateway",
    color:       "from-orange-500/20 to-orange-600/10 border-orange-600/40",
    activeColor: "from-orange-500/30 to-orange-600/20 border-orange-500/70",
    dot:         "bg-orange-400",
    fields: [
      { key: "moomoo_host",        label: "OpenD Host",               placeholder: "127.0.0.1",          secret: false },
      { key: "moomoo_port",        label: "OpenD Port",               placeholder: "11111",              secret: false },
      { key: "moomoo_trading_pwd", label: "Trading Password",         placeholder: "••••••••",           secret: true  },
      { key: "moomoo_account_id",  label: "Account ID (optional)",    placeholder: "Futu account number",secret: false },
    ],
    toggle: { key: "moomoo_paper_mode", label: "Paper / simulated account", note: "Connect to a simulated account instead of live trading" },
    note: "Requires Futu OpenD running locally. Download and configure OpenD from the moomoo developer site.",
    docsUrl: "https://openapi.moomoo.com/moomoo-api-doc/en/",
  },
  {
    id:          "webull",
    name:        "Webull",
    tagline:     "Commission-free trading platform",
    color:       "from-teal-500/20 to-teal-600/10 border-teal-600/40",
    activeColor: "from-teal-500/30 to-teal-600/20 border-teal-500/70",
    dot:         "bg-teal-400",
    fields: [
      { key: "webull_email",       label: "Email / Username",         placeholder: "you@email.com",              secret: false },
      { key: "webull_password",    label: "Password",                 placeholder: "••••••••",                   secret: true  },
      { key: "webull_trading_pin", label: "Trading PIN",              placeholder: "6-digit PIN",                secret: true  },
      { key: "webull_device_id",   label: "Device ID (optional)",     placeholder: "Auto-generated if blank",    secret: false },
    ],
    toggle: { key: "webull_paper_mode", label: "Paper trading account", note: "Use Webull's built-in paper trading account" },
    note: "Uses the unofficial Webull API. Keep credentials secure and enable 2FA on your Webull account.",
    docsUrl: "https://github.com/tedchou12/webull",
  },
];

// ── Shared UI helpers ─────────────────────────────────────────────────────────
function StatusBanner({ status }) {
  if (!status) return null;
  const isError = status.type === "error";
  return (
    <div className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm ${
      isError ? "bg-red-900/30 text-red-400 border border-red-800/50"
              : "bg-emerald-900/30 text-emerald-400 border border-emerald-800/50"
    }`}>
      {isError ? <AlertCircle className="w-4 h-4 shrink-0" /> : <CheckCircle className="w-4 h-4 shrink-0" />}
      {status.message}
    </div>
  );
}

function Toggle({ on, onToggle, label, note }) {
  return (
    <div className="flex items-center justify-between py-3 border-t border-slate-700/50">
      <div>
        <p className="text-sm text-slate-300 font-medium">{label}</p>
        {note && <p className="text-xs text-slate-500 mt-0.5">{note}</p>}
      </div>
      <button
        type="button"
        onClick={onToggle}
        className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors shrink-0 ${on ? "bg-brand-600" : "bg-slate-700"}`}
      >
        <span className={`inline-block h-4 w-4 rounded-full bg-white shadow transition-transform ${on ? "translate-x-6" : "translate-x-1"}`} />
      </button>
    </div>
  );
}

const inputCls =
  "flex-1 bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 " +
  "placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 " +
  "focus:ring-brand-500/40 transition font-mono";

// ── Alpaca form — dedicated endpoint ─────────────────────────────────────────
function AlpacaForm({ broker, onSaved }) {
  const [apiKey,    setApiKey]    = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [paperMode, setPaperMode] = useState(true);
  const [keyInfo,   setKeyInfo]   = useState({ configured: false, masked: "" });
  const [secInfo,   setSecInfo]   = useState({ configured: false, masked: "" });
  const [visible,   setVisible]   = useState({ key: false, secret: false });
  const [loading,   setLoading]   = useState(true);
  const [saving,    setSaving]    = useState(false);
  const [testing,   setTesting]   = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [status,    setStatus]    = useState(null);

  useEffect(() => {
    alpacaApi.get()
      .then(r => {
        setKeyInfo(r.data.api_key);
        setSecInfo(r.data.api_secret);
        setPaperMode(r.data.paper_mode === "true");
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  async function handleSave(e) {
    e.preventDefault();
    setSaving(true);
    setStatus(null);
    try {
      const payload = { paper_mode: paperMode };
      if (apiKey.trim())    payload.api_key    = apiKey.trim();
      if (apiSecret.trim()) payload.api_secret = apiSecret.trim();

      await alpacaApi.save(payload);

      // Refresh configured/masked state
      const r = await alpacaApi.get();
      setKeyInfo(r.data.api_key);
      setSecInfo(r.data.api_secret);
      setApiKey("");
      setApiSecret("");

      setStatus({ type: "success", message: "Alpaca settings saved." });
      onSaved?.();
    } catch {
      setStatus({ type: "error", message: "Failed to save. Please try again." });
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setTestResult(null);
    try {
      const r = await alpacaApi.test();
      setTestResult(r.data);
    } catch (err) {
      setTestResult({ ok: false, error: err.response?.data?.error ?? "Request failed." });
    } finally {
      setTesting(false);
    }
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-slate-500 text-sm py-4">
        <Loader2 className="w-4 h-4 animate-spin" /> Loading…
      </div>
    );
  }

  const SecretField = ({ id, label, value, onChange, info, visKey }) => (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <label className="text-xs text-slate-500">{label}</label>
        {info.configured && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-900/50 text-emerald-400 border border-emerald-800/50 font-mono">
            {info.masked}
          </span>
        )}
      </div>
      <div className="flex gap-2">
        <input
          type={visible[visKey] ? "text" : "password"}
          value={value}
          onChange={e => onChange(e.target.value)}
          placeholder={info.configured ? "Leave blank to keep existing" : "Enter key…"}
          className={inputCls}
          autoComplete="off"
        />
        <button
          type="button"
          onClick={() => setVisible(p => ({ ...p, [visKey]: !p[visKey] }))}
          className="px-2.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-slate-200 transition shrink-0"
        >
          {visible[visKey] ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
        </button>
      </div>
    </div>
  );

  return (
    <form onSubmit={handleSave} className="flex flex-col gap-4">
      <SecretField
        id="key"
        label="API Key ID"
        value={apiKey}
        onChange={setApiKey}
        info={keyInfo}
        visKey="key"
      />
      <SecretField
        id="secret"
        label="API Secret Key"
        value={apiSecret}
        onChange={setApiSecret}
        info={secInfo}
        visKey="secret"
      />

      <Toggle
        on={paperMode}
        onToggle={() => setPaperMode(p => !p)}
        label="Paper trading mode"
        note="Routes orders to paper-api.alpaca.markets"
      />

      <div className="flex gap-2 px-3 py-2.5 rounded-lg bg-slate-900/60 border border-slate-700/50 text-xs text-slate-500">
        <Info className="w-3.5 h-3.5 shrink-0 mt-0.5 text-slate-600" />
        <span>
          {broker.note}&nbsp;
          <a href={broker.docsUrl} target="_blank" rel="noreferrer"
             className="text-brand-400 hover:text-brand-300 underline underline-offset-2">
            View docs ↗
          </a>
        </span>
      </div>

      <StatusBanner status={status} />

      {/* Test connection result */}
      {testResult && (
        <div className={`rounded-lg border px-4 py-3 text-sm ${
          testResult.ok
            ? "bg-emerald-900/20 border-emerald-800/50 text-emerald-300"
            : "bg-red-900/20 border-red-800/50 text-red-400"
        }`}>
          {testResult.ok ? (
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center gap-2 font-medium">
                <CheckCircle className="w-4 h-4 shrink-0" />
                Connected — {testResult.paper ? "Paper" : "Live"} account
              </div>
              <div className="grid grid-cols-2 gap-x-6 gap-y-1 mt-1 text-xs text-emerald-400/80 font-mono">
                <span className="text-emerald-500/60">Account ID</span>
                <span>{testResult.account_id}</span>
                <span className="text-emerald-500/60">Status</span>
                <span className="capitalize">{testResult.status}</span>
                <span className="text-emerald-500/60">Buying power</span>
                <span>${Number(testResult.buying_power).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span>
                <span className="text-emerald-500/60">Portfolio value</span>
                <span>${Number(testResult.portfolio_value).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <AlertCircle className="w-4 h-4 shrink-0" />
              {testResult.error}
            </div>
          )}
        </div>
      )}

      <div className="flex items-center gap-3">
        <button
          type="submit"
          disabled={saving}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white text-sm font-medium transition"
        >
          {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
          {saving ? "Saving…" : "Save Settings"}
        </button>

        <button
          type="button"
          onClick={handleTest}
          disabled={testing || (!keyInfo.configured || !secInfo.configured)}
          title={!keyInfo.configured || !secInfo.configured ? "Save credentials first" : "Test connection to Alpaca"}
          className="flex items-center gap-2 px-4 py-2 rounded-lg border border-slate-600 bg-slate-800 hover:bg-slate-700 disabled:opacity-40 text-slate-300 text-sm font-medium transition"
        >
          {testing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Wifi className="w-4 h-4" />}
          {testing ? "Testing…" : "Test Connection"}
        </button>
      </div>
    </form>
  );
}

// ── Generic broker form (non-Alpaca) ──────────────────────────────────────────
function BrokerForm({ broker, allSettings, onSaved }) {
  const initValues = () => Object.fromEntries(broker.fields.map(f => [f.key, allSettings[f.key] ?? ""]));
  const initToggle = () => (broker.toggle ? allSettings[broker.toggle.key] === "true" : false);

  const [values,   setValues]  = useState(initValues);
  const [toggleOn, setToggle]  = useState(initToggle);
  const [visible,  setVisible] = useState({});
  const [saving,   setSaving]  = useState(false);
  const [status,   setStatus]  = useState(null);

  useEffect(() => {
    setValues(initValues());
    setToggle(initToggle());
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allSettings, broker.id]);

  async function handleSave(e) {
    e.preventDefault();
    setSaving(true);
    setStatus(null);
    try {
      const saves = broker.fields.map(f => settingsApi.update(f.key, values[f.key]));
      if (broker.toggle) saves.push(settingsApi.update(broker.toggle.key, String(toggleOn)));
      saves.push(settingsApi.update("broker_active", broker.id));
      await Promise.all(saves);
      setStatus({ type: "success", message: `${broker.name} settings saved.` });
      onSaved?.();
    } catch {
      setStatus({ type: "error", message: "Failed to save. Please try again." });
    } finally {
      setSaving(false);
    }
  }

  return (
    <form onSubmit={handleSave} className="flex flex-col gap-4">
      {broker.fields.map(({ key, label, placeholder, secret }) => (
        <div key={key}>
          <label className="block text-xs text-slate-500 mb-1.5">{label}</label>
          <div className="flex gap-2">
            <input
              type={secret && !visible[key] ? "password" : "text"}
              value={values[key]}
              onChange={e => setValues(p => ({ ...p, [key]: e.target.value }))}
              placeholder={placeholder}
              className={inputCls}
              autoComplete="off"
            />
            {secret && (
              <button
                type="button"
                onClick={() => setVisible(p => ({ ...p, [key]: !p[key] }))}
                className="px-2.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-slate-200 transition shrink-0"
              >
                {visible[key] ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            )}
          </div>
        </div>
      ))}

      {broker.toggle && (
        <Toggle
          on={toggleOn}
          onToggle={() => setToggle(p => !p)}
          label={broker.toggle.label}
          note={broker.toggle.note}
        />
      )}

      <div className="flex gap-2 px-3 py-2.5 rounded-lg bg-slate-900/60 border border-slate-700/50 text-xs text-slate-500">
        <Info className="w-3.5 h-3.5 shrink-0 mt-0.5 text-slate-600" />
        <span>
          {broker.note}&nbsp;
          <a href={broker.docsUrl} target="_blank" rel="noreferrer"
             className="text-brand-400 hover:text-brand-300 underline underline-offset-2">
            View docs ↗
          </a>
        </span>
      </div>

      <StatusBanner status={status} />

      <div>
        <button
          type="submit"
          disabled={saving}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white text-sm font-medium transition"
        >
          {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
          {saving ? "Saving…" : `Save ${broker.name} Settings`}
        </button>
      </div>
    </form>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function BrokerageSettings() {
  const [allSettings,  setAllSettings]  = useState({});
  const [alpacaStatus, setAlpacaStatus] = useState({ configured: false });
  const [activeBroker, setActiveBroker] = useState(null);
  const [loading,      setLoading]      = useState(true);

  function reloadSettings() {
    return settingsApi.list().then(r => {
      const map = {};
      (r.data.settings ?? []).forEach(s => { map[s.key] = s.value ?? ""; });
      setAllSettings(map);
      if (map["broker_active"]) {
        const found = BROKERS.find(b => b.id === map["broker_active"]);
        if (found) setActiveBroker(found);
      }
    });
  }

  useEffect(() => {
    Promise.all([
      reloadSettings(),
      alpacaApi.get().then(r => {
        const d = r.data;
        setAlpacaStatus({ configured: d.api_key?.configured || d.api_secret?.configured });
      }).catch(() => {}),
    ]).finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // "Configured" badge for non-Alpaca brokers
  const configuredBrokers = new Set(
    BROKERS
      .filter(b => b.id !== "alpaca" && b.fields.some(f => allSettings[f.key]))
      .map(b => b.id)
  );
  if (alpacaStatus.configured) configuredBrokers.add("alpaca");

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-2xl mx-auto px-6 py-8 flex flex-col gap-6">

        <div>
          <h1 className="text-lg font-semibold text-slate-100">Brokerage</h1>
          <p className="text-sm text-slate-500 mt-0.5">Select a broker and enter your credentials for order execution.</p>
        </div>

        {loading ? (
          <div className="flex items-center gap-2 text-slate-500 text-sm">
            <Loader2 className="w-4 h-4 animate-spin" /> Loading…
          </div>
        ) : (
          <>
            {/* Broker selector grid */}
            <div className="grid grid-cols-1 gap-2">
              {BROKERS.map(broker => {
                const isActive     = activeBroker?.id === broker.id;
                const isConfigured = configuredBrokers.has(broker.id);
                return (
                  <button
                    key={broker.id}
                    type="button"
                    onClick={() => setActiveBroker(isActive ? null : broker)}
                    className={`flex items-center gap-4 w-full px-4 py-3.5 rounded-xl border bg-gradient-to-r text-left transition ${
                      isActive ? broker.activeColor : `${broker.color} hover:brightness-110`
                    }`}
                  >
                    <span className={`w-2.5 h-2.5 rounded-full shrink-0 ${broker.dot}`} />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-semibold text-slate-100">{broker.name}</p>
                      <p className="text-xs text-slate-400 mt-0.5">{broker.tagline}</p>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      {isConfigured && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-900/50 text-emerald-400 border border-emerald-800/50 font-medium">
                          Configured
                        </span>
                      )}
                      <span className={`text-xs font-medium transition ${isActive ? "text-slate-300" : "text-slate-500"}`}>
                        {isActive ? "▲ Collapse" : "▼ Configure"}
                      </span>
                    </div>
                  </button>
                );
              })}
            </div>

            {/* Expanded broker form */}
            {activeBroker && (
              <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl p-6">
                <h2 className="text-sm font-semibold text-slate-200 mb-5">
                  {activeBroker.name} — Credentials
                </h2>
                {activeBroker.id === "alpaca" ? (
                  <AlpacaForm
                    key="alpaca"
                    broker={activeBroker}
                    onSaved={() =>
                      alpacaApi.get().then(r => {
                        const d = r.data;
                        setAlpacaStatus({ configured: d.api_key?.configured || d.api_secret?.configured });
                      }).catch(() => {})
                    }
                  />
                ) : (
                  <BrokerForm
                    key={activeBroker.id}
                    broker={activeBroker}
                    allSettings={allSettings}
                    onSaved={reloadSettings}
                  />
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
