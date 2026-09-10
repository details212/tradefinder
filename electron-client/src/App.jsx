import { useEffect, useRef, useState, useCallback } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
import Login from "./components/Login";
import Dashboard from "./components/Dashboard";
import TradeAutomationDisclosureModal from "./components/TradeAutomationDisclosureModal";
import { authApi, preferencesApi } from "./api/client";

const SESSION_MS = 12 * 60 * 60 * 1000; // 12 hours — non-remember sessions only

function App() {
  const [token,    setToken]    = useState(null);
  const [user,     setUser]     = useState(null);
  const [checking, setChecking] = useState(true);
  const logoutTimerRef = useRef(null);

  // Trade-automation disclosure modal (shown every login when feature is on)
  const [showAutomationDisclosure, setShowAutomationDisclosure] = useState(false);

  const navigate = useNavigate();

  useEffect(() => {
    let cancelled = false;

    async function boot() {
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
        if (cancelled) return;
        setToken(storedToken);
        setUser(res.data.user);
      } catch {
        localStorage.removeItem("tf_token");
        localStorage.removeItem("tf_user");
        localStorage.removeItem("tf_remember_me");
      } finally {
        if (!cancelled) setChecking(false);
      }
    }

    boot();
    return () => { cancelled = true; };
  }, []);

  const handleLogin = async (newToken, newUser, rememberMe = false) => {
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
