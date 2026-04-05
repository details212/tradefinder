# TradeFinder

A stock market data app with a **Flask REST API backend** and an **Electron + React desktop client** that compiles to a Windows `.exe`.

---

## Project Structure

```
tradefinder/
├── backend/                  Flask API server
│   ├── app.py                App factory
│   ├── config.py             Config from .env
│   ├── extensions.py         Flask extensions
│   ├── auth.py               JWT helpers & decorator
│   ├── models.py             User & WatchlistItem models
│   ├── seed.py               Create initial admin user
│   ├── requirements.txt
│   └── routes/
│       ├── auth_routes.py    /api/auth/*
│       └── stock_routes.py   /api/stocks/*
└── electron-client/          Electron + React app
    ├── electron/
    │   ├── main.js           Electron main process
    │   └── preload.js
    ├── src/
    │   ├── main.jsx
    │   ├── App.jsx
    │   ├── api/client.js     Axios API client
    │   └── components/
    │       ├── Login.jsx
    │       ├── Dashboard.jsx
    │       ├── StockSearch.jsx
    │       ├── StockDetail.jsx
    │       └── StockChart.jsx
    ├── package.json
    └── vite.config.js
```

---

## Backend Setup

### Prerequisites
- Python 3.10+
- A [Polygon.io](https://polygon.io) API key (free tier works for basic data)

### 1. Install dependencies

```bash
cd backend
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 2. Configure environment

```bash
copy .env.example .env
```

Edit `.env`:

```
SECRET_KEY=<random string>
JWT_SECRET_KEY=<another random string>
POLYGON_API_KEY=<your polygon key>
DATABASE_URI=sqlite:///tradefinder.db
```

### 3. Seed the database (creates admin user)

```bash
python seed.py
```

Default credentials: `admin` / `changeme123` — **change this after first login.**

### 4. Run the server

**Development:**
```bash
python app.py
```

**Production (Linux/macOS):**
```bash
gunicorn "app:create_app()" -b 0.0.0.0:5000 -w 4
```

Use nginx as a reverse proxy and add SSL for production deployments.

---

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/api/auth/login` | No | Login → JWT token |
| POST | `/api/auth/register` | No | Create account |
| GET | `/api/auth/me` | Yes | Current user info |
| POST | `/api/auth/change-password` | Yes | Change password |
| GET | `/api/stocks/search?q=AAPL` | Yes | Search tickers |
| GET | `/api/stocks/{ticker}/quote` | Yes | Real-time quote |
| GET | `/api/stocks/{ticker}/details` | Yes | Company info |
| GET | `/api/stocks/{ticker}/history` | Yes | OHLC bars |
| GET | `/api/stocks/{ticker}/news` | Yes | Latest news |
| GET | `/api/stocks/watchlist` | Yes | Get watchlist |
| POST | `/api/stocks/watchlist/{ticker}` | Yes | Add to watchlist |
| DELETE | `/api/stocks/watchlist/{ticker}` | Yes | Remove from watchlist |

---

## Electron Client Setup

### Prerequisites
- Node.js 18+
- npm 9+

### 1. Install dependencies

```bash
cd electron-client
npm install
```

### 2. Run in development

Make sure the Flask backend is running on port 5000, then:

```bash
npm run dev
```

This starts Vite + Electron side by side.

### 3. Build the Windows installer (.exe)

Set your server URL first:

```powershell
$env:TRADEFINDER_API_URL = "https://api.yourdomain.com"
npm run dist:win
```

The installer and portable exe will be in `electron-client/dist-exe/`.

> **Note:** To build for Windows from a non-Windows machine you need Wine installed,
> or use GitHub Actions / a Windows CI runner.

---

## Distributing to Users

1. Deploy the Flask backend to a server (VPS, Railway, Render, etc.)
2. Set `TRADEFINDER_API_URL` to your server's public URL before building
3. Run `npm run dist:win` to produce the NSIS installer
4. Share `dist-exe/TradeFinder Setup x.x.x.exe` with your users
5. Users install it, open it, create an account or use the credentials you provide

---

## User Management

Users register themselves through the app (or you disable the register tab and
create accounts manually via the seed script or a custom admin endpoint).

To disable self-registration, remove the `/register` route in `auth_routes.py`
and hide the "Register" tab in `Login.jsx`.

---

## Customising the App Icon

Replace `electron-client/assets/icon.ico` with your own `.ico` file (256×256 px recommended)
before building. You can convert a PNG to ICO using an online tool.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python, Flask, SQLAlchemy, PyJWT, Flask-Bcrypt |
| Database | SQLite (swap to PostgreSQL for production) |
| Market Data | Polygon.io REST API |
| Frontend | React 18, Vite, Tailwind CSS |
| Desktop | Electron 31, electron-builder |
| Charts | Recharts |
| HTTP client | Axios |
