import { useEffect, useRef, useState, useCallback } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
import Login from "./components/Login";
import Dashboard from "./components/Dashboard";
import UpdateModal from "./components/UpdateModal";

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

const SESSION_MS = 12 * 60 * 60 * 1000; // 12 hours

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

  const navigate = useNavigate();

  // Clear any saved session on every app launch — always require login
  useEffect(() => {
    localStorage.removeItem("tf_token");
    localStorage.removeItem("tf_user");
    setChecking(false);
  }, []);

  /**
   * Called by Login (and RegisterFlow) after a successful auth response.
   * The backend now includes `required_version` and `download_url` in the
   * login/register response so we can gate access before storing the token.
   */
  const handleLogin = (newToken, newUser, requiredVersion, dlUrl) => {
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
    setToken(newToken);
    setUser(newUser);
    navigate("/dashboard");
  };

  const handleLogout = useCallback(() => {
    clearTimeout(logoutTimerRef.current);
    localStorage.removeItem("tf_token");
    localStorage.removeItem("tf_user");
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

  // Force logout after 12 hours — fires even if no API call is made
  useEffect(() => {
    if (!token) return;
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

  return (
    <>
      {updateNeeded && (
        <UpdateModal
          clientVersion={clientVersion}
          requiredVersion={reqVersion}
          downloadUrl={downloadUrl}
        />
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
