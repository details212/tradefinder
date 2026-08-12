import { useEffect, useRef, useState, useCallback } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
import Login from "./components/Login";
import Dashboard from "./components/Dashboard";
import UpdateModal from "./components/UpdateModal";
import TradeAutomationDisclosureModal from "./components/TradeAutomationDisclosureModal";
import { authApi, preferencesApi } from "./api/client";

/** Compare two semver strings. Returns true if `a` is strictly older than `b`. */
function isOutdated(a, b) {
  if (!a || !b) return false;
  const pa = a.split(".").map(Number);
  const pb = b.split(".").map(Number);
  for (let i = 0; i < 3; i++) {
    const va = pa[i] ?? 0;
    const vb = pb[i] ?? 0;
    if (va < vb) return true;
    if (va > vb) return false;
  }
  return false;
}

const SESSION_MS = 12 * 60 * 60 * 1000; // 12 hours — non-remember sessions only

function App() {
  const [token,    setToken]    = useState(null);
  const [user,     setUser]     = useState(null);
  const [checking, setChecking] = useState(true);
  const logoutTimerRef = useRef(null);

  // Version-gate state
  const [updateNeeded,  setUpdateNeeded]  = useState(false);
  const [clientVersion, setClientVersion] = useState("");
  const [reqVersion,    setReqVersion]    = useState("");
  const [downloadUrl,   setDownloadUrl]   = useState("");

  // Trade-automation disclosure modal (shown every login when feature is on)
  const [showAutomationDisclosure, setShowAutomationDisclosure] = useState(false);

  const navigate = useNavigate();

  // Restore remember-me session on launch; otherwise require fresh login
  useEffect(() => {
    async function restoreSession() {
      const remember = localStorage.getItem("tf_remember_me") === "true";
      const storedToken = localStorage.getItem("tf_token");

      if (!remember) {
        localStorage.removeItem("tf_token");
        localStorage.removeItem("tf_user");
        localStorage.removeItem("tf_remember_me");
        setChecking(false);
        return;
      }

      if (!storedToken) {
        localStorage.removeItem("tf_remember_me");
        setChecking(false);
        return;
      }

      try {
        const res = await authApi.me();
        setToken(storedToken);
        setUser(res.data.user);
      } catch {
        localStorage.removeItem("tf_token");
        localStorage.removeItem("tf_user");
        localStorage.removeItem("tf_remember_me");
      } finally {
        setChecking(false);
      }
    }

    restoreSession();
  }, []);

  /**
   * Called by Login (and RegisterFlow) after a successful auth response.
   * The backend now includes `required_version` and `download_url` in the
   * login/register response so we can gate access before storing the token.
   */
  const handleLogin = async (newToken, newUser, requiredVersion, dlUrl, rememberMe = false) => {
    const client = window.APP_VERSION || "0.0.0";

    if (requiredVersion && isOutdated(client, requiredVersion)) {
      // Block access until the user downloads the new build
      setClientVersion(client);
      setReqVersion(requiredVersion);
      setDownloadUrl(dlUrl || "");
      setUpdateNeeded(true);
      return;
    }

    localStorage.setItem("tf_token", newToken);
    localStorage.setItem("tf_user", JSON.stringify(newUser));
    localStorage.setItem("tf_remember_me", rememberMe ? "true" : "false");
    setToken(newToken);
    setUser(newUser);

    // Check whether Trade Automation is enabled; if so, show the disclosure
    // modal before letting the user into the dashboard.
    try {
      const r = await preferencesApi.get();
      const v = r.data.preferences?.auto_market_close_beyond_tp;
      const isEnabled = v === true || String(v).toLowerCase() === "true" || v === "1";
      if (isEnabled) {
        setShowAutomationDisclosure(true);
        return;
      }
    } catch {
      // If the preference fetch fails, proceed normally — don't block login.
    }

    navigate("/dashboard");
  };

  const handleLogout = useCallback(async () => {
    clearTimeout(logoutTimerRef.current);
    try {
      if (localStorage.getItem("tf_token")) {
        await authApi.logout();
      }
    } catch {
      // Ignore — still clear local session
    }
    localStorage.removeItem("tf_token");
    localStorage.removeItem("tf_user");
    localStorage.removeItem("tf_remember_me");
    setToken(null);
    setUser(null);
    navigate("/login");
  }, [navigate]);

  // Listen for 401 events from the API client
  useEffect(() => {
    const onUnauthorized = () => handleLogout();
    window.addEventListener("tf:unauthorized", onUnauthorized);
    return () => window.removeEventListener("tf:unauthorized", onUnauthorized);
  }, [handleLogout]);

  // Force logout after 12 hours for non-remember sessions
  useEffect(() => {
    if (!token) return;
    if (localStorage.getItem("tf_remember_me") === "true") return;
    logoutTimerRef.current = setTimeout(handleLogout, SESSION_MS);
    return () => clearTimeout(logoutTimerRef.current);
  }, [token, handleLogout]);

  if (checking) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <svg className="animate-spin w-8 h-8 text-brand-500" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
        </svg>
      </div>
    );
  }

  function handleAutomationAcknowledge() {
    setShowAutomationDisclosure(false);
    navigate("/dashboard");
  }

  return (
    <>
      {updateNeeded && (
        <UpdateModal
          clientVersion={clientVersion}
          requiredVersion={reqVersion}
          downloadUrl={downloadUrl}
        />
      )}

      {showAutomationDisclosure && (
        <TradeAutomationDisclosureModal onAcknowledge={handleAutomationAcknowledge} />
      )}

      <Routes>
        <Route
          path="/login"
          element={
            token ? (
              <Navigate to="/dashboard" replace />
            ) : (
              <Login onLogin={handleLogin} />
            )
          }
        />
        <Route
          path="/dashboard/*"
          element={
            token ? (
              <Dashboard user={user} onLogout={handleLogout} />
            ) : (
              <Navigate to="/login" replace />
            )
          }
        />
        <Route path="*" element={<Navigate to={token ? "/dashboard" : "/login"} replace />} />
      </Routes>
    </>
  );
}

export default App;
