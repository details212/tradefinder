import axios from "axios";

const BASE_URL = window.TRADEFINDER_API_URL || "http://localhost:5000";

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
  headers: { "Content-Type": "application/json" },
});

// Attach JWT token from localStorage to every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("tf_token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Successful watchlist mutations → sidebar can refresh without prop drilling
api.interceptors.response.use(
  (response) => {
    try {
      const url = response.config?.url || "";
      const method = (response.config?.method || "").toLowerCase();
      // POST/DELETE …/watchlist/<ticker> — exclude GET /api/stocks/watchlist (list)
      const wlMutate =
        url.includes("/api/stocks/watchlist/") && (method === "post" || method === "delete");
      if (wlMutate) {
        window.dispatchEvent(new CustomEvent("tf:watchlist-changed"));
      }
    } catch (_) { /* ignore */ }
    return response;
  },
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem("tf_token");
      localStorage.removeItem("tf_user");
      window.dispatchEvent(new CustomEvent("tf:unauthorized"));
    }
    return Promise.reject(error);
  }
);

// ── Auth ──────────────────────────────────────────────────────────────────────
export const authApi = {
  login: (username, password) =>
    api.post("/api/auth/login", { username, password }),
  sendRegistrationCode: (email) =>
    api.post("/api/auth/send-registration-code", { email }),
  confirmRegistrationCode: (email, code) =>
    api.post("/api/auth/confirm-registration-code", { email, code }),
  register: (data) =>
    api.post("/api/auth/register", data),
  me: () => api.get("/api/auth/me"),
  requestPasswordChange: (old_password, new_password) =>
    api.post("/api/auth/request-password-change", { old_password, new_password }),
  verifyPasswordChange: (code) =>
    api.post("/api/auth/verify-password-change", { code }),
  forgotPassword: (identifier) =>
    api.post("/api/auth/forgot-password", { identifier }),
  verifyResetCode: (identifier, code) =>
    api.post("/api/auth/verify-reset-code", { identifier, code }),
  resetPassword: (identifier, code, new_password) =>
    api.post("/api/auth/reset-password", { identifier, code, new_password }),
  checkUsername: (username) =>
    api.get("/api/auth/check-username", { params: { username } }),
  changeUsername: (username) =>
    api.post("/api/auth/change-username", { username }),
  getProfile:         ()       => api.get("/api/auth/profile"),
  updateProfile:      (data)   => api.put("/api/auth/profile", data),
  requestEmailChange: (email) =>
    api.post("/api/auth/request-email-change", { email }),
  verifyEmailChange: (code) =>
    api.post("/api/auth/verify-email-change", { code }),
  loginEvents: (limit = 10) =>
    api.get("/api/auth/login-events", { params: { limit } }),
  resetHistory: () =>
    api.post("/api/auth/reset-history"),
};

// ── Stocks ───────────────────────────────────────────────────────────────────
export const stockApi = {
  // Search (local DB)
  search: (q, extra = {}) => api.get("/api/stocks/search", { params: { q, ...extra?.params } }),

  // Quote & price
  quote: (ticker) => api.get(`/api/stocks/${ticker}/quote`),
  prevClose: (ticker) => api.get(`/api/stocks/${ticker}/prev-close`),

  // Company
  details: (ticker) => api.get(`/api/stocks/${ticker}/details`),
  related: (ticker) => api.get(`/api/stocks/${ticker}/related`),
  // Batch name lookup from local DB (no external API call)
  names:   (tickers) => api.get("/api/stocks/names", { params: { tickers: tickers.join(",") } }),

  // History — long timeout: backend waits up to 30 s per Polygon page + retry
  history: (ticker, params = {}) =>
    api.get(`/api/stocks/${ticker}/history`, { params, timeout: 75000 }),

  // Technicals: indicator = sma | ema | rsi | macd
  indicator: (ticker, indicator, params = {}) =>
    api.get(`/api/stocks/${ticker}/indicators/${indicator}`, { params }),

  // Fundamentals
  financials: (ticker, timeframe = "quarterly") =>
    api.get(`/api/stocks/${ticker}/financials`, { params: { timeframe } }),
  dividends: (ticker) => api.get(`/api/stocks/${ticker}/dividends`),
  splits: (ticker) => api.get(`/api/stocks/${ticker}/splits`),

  // News
  news: (ticker) => api.get(`/api/stocks/${ticker}/news`),

  // Market
  marketStatus: () => api.get("/api/stocks/market-status"),

  // Watchlist
  getWatchlist: () => api.get("/api/stocks/watchlist"),
  addToWatchlist: (ticker, data = {}) => api.post(`/api/stocks/watchlist/${ticker}`, data),
  removeFromWatchlist: (ticker) => api.delete(`/api/stocks/watchlist/${ticker}`),

  // Batch live prices — reads from server-side cache (non-blocking)
  snapshots: (tickers) => api.get("/api/snapshots/prices", { params: { tickers } }),
};

// ── Trade Ideas ───────────────────────────────────────────────────────────────
export const tradeIdeasApi = {
  list: () => api.get("/api/tradeideas/"),
  run:  (id) => api.get(`/api/tradeideas/${id}`),
  /** Signals fired in the last `minutes` minutes across all strategies. */
  recent: (minutes = 15) => api.get("/api/tradeideas/recent", { params: { minutes } }),
  /** @param {string[]} tickers @param {{ staleOk?: boolean, queueRefresh?: boolean }} [opts] */
  maCache: (tickers, opts = {}) =>
    api.post("/api/tradeideas/ma-cache", {
      tickers,
      stale_ok: opts.staleOk ?? false,
      queue_refresh: opts.queueRefresh !== false,
    }),
};

// ── Snapshot cache (prices maintained server-side; client reads only) ─────────
export const snapshotsApi = {
  prices: (tickers) => api.get("/api/snapshots/prices", { params: { tickers } }),
};

// ── Leaderboard (system performance settings + per-user closed-trade stats) ───
export const leaderboardApi = {
  get: () => api.get("/api/leaderboard/"),
};

// ── Settings ──────────────────────────────────────────────────────────────────
export const settingsApi = {
  list:   ()           => api.get("/api/settings/"),
  update: (key, value) => api.put(`/api/settings/${key}`, { value }),
};

// ── Broker: Alpaca ────────────────────────────────────────────────────────────
// GET    /api/broker/alpaca         → { api_key: { configured, masked }, api_secret: { configured, masked }, paper_mode }
// PUT    /api/broker/alpaca         → { api_key?, api_secret?, paper_mode? }  (omit a field to leave it unchanged)
// GET    /api/broker/alpaca/test    → { ok, paper, account_id, status, buying_power, portfolio_value, currency }
// POST   /api/broker/alpaca/order   → place bracket order; returns { message, order, alpaca_raw }
// GET    /api/broker/alpaca/orders  → list persisted orders for current user (newest first)
//          optional ?ticker=AAPL to filter
export const alpacaApi = {
  get:        ()            => api.get("/api/broker/alpaca"),
  save:       (data)        => api.put("/api/broker/alpaca", data),
  test:       ()            => api.get("/api/broker/alpaca/test"),
  quote:      (ticker)      => api.get(`/api/broker/alpaca/quote/${ticker}`),
  placeOrder:      (data)        => api.post("/api/broker/alpaca/order", data),
  closeTrade:      (dbOrderId)   => api.post(`/api/broker/alpaca/order/${dbOrderId}/close`),
  patchLevels:     (dbOrderId, data) => api.patch(`/api/broker/alpaca/order/${dbOrderId}/levels`, data),
  getOrderDetail:  (alpacaId)    => api.get(`/api/broker/alpaca/order/${alpacaId}`),
  getOrders:       (ticker = "") => api.get("/api/broker/alpaca/orders", { params: ticker ? { ticker } : {} }),
  syncOrders:      ()            => api.post("/api/broker/alpaca/orders/sync"),
  openTickers:     ()            => api.get("/api/broker/alpaca/orders/open-tickers"),
};

// ── User preferences (per-user key/value store) ───────────────────────────────
// GET /api/preferences/        → { preferences: { key: value, … } }
// PUT /api/preferences/        → body { key: value, … }
export const preferencesApi = {
  get:    ()     => api.get("/api/preferences/"),
  update: (data) => api.put("/api/preferences/", data),
};

// ── Resource status ───────────────────────────────────────────────────────────
export const resourcesApi = {
  status: () => api.get("/api/resources/status"),
};

// ── Stripe / Billing ──────────────────────────────────────────────────────────
export const stripeApi = {
  /** Returns { publishable_key } for initialising Stripe.js. */
  getConfig: () =>
    api.get("/api/stripe/config"),
  /** Create a SetupIntent to collect card details. Returns { client_secret, publishable_key, mode }. */
  createSubscription: () =>
    api.post("/api/stripe/create-subscription"),
  /** After SetupIntent confirmation, create the subscription server-side. */
  confirmSetup: (setup_intent_id, payment_method_id) =>
    api.post("/api/stripe/confirm-setup", { setup_intent_id, payment_method_id }),
  /** Create a Stripe Customer Portal session. Returns { url } — open in system browser. */
  getBillingPortal: () =>
    api.post("/api/stripe/billing-portal"),
  /** Poll subscription status for the authenticated user. Returns { status, period_end, active }. */
  getSubscriptionStatus: () =>
    api.get("/api/stripe/subscription-status"),
  /** List last 24 invoices for the authenticated user. */
  getInvoices: () =>
    api.get("/api/stripe/invoices"),
};

// ── Pattern Analysis Scanner ──────────────────────────────────────────────────
export const scannerApi = {
  dates: ()       => api.get("/api/scanner/dates"),
  scan:  (params) => api.get("/api/scanner/scan", { params, timeout: 60000 }),
};

// ── Enriched chart data (OHLCV + studies + ticker info) ───────────────────────
export const chartApi = {
  get: (ticker) => api.get(`/api/scanner/chart/${encodeURIComponent(ticker)}`, { timeout: 60000 }),
};

// ── Drawn chart boxes (per user + ticker, persisted to DB) ───────────────────
export const boxApi = {
  list:   (ticker) => api.get("/api/scanner/boxes", { params: { ticker } }),
  create: (data)   => api.post("/api/scanner/boxes", data),
  remove: (id)     => api.delete(`/api/scanner/boxes/${id}`),
};

export default api;
