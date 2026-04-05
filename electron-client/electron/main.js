const { app, BrowserWindow, shell, Menu, ipcMain } = require("electron");
const path = require("path");
const { exec } = require("child_process");

const isDev = !app.isPackaged;

function createWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 900,
    minHeight: 600,
    title: "TradeFinder",
    backgroundColor: "#0f172a",
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, "preload.js"),
    },
    icon: path.join(__dirname, "../assets/icon.ico"),
    show: false,
  });

  if (isDev) {
    win.loadURL("http://localhost:5173");
  } else {
    win.loadFile(path.join(__dirname, "../dist/index.html"));
  }

  win.once("ready-to-show", () => win.show());

  // F12 or Ctrl+Shift+I opens DevTools
  win.webContents.on("before-input-event", (event, input) => {
    if (
      input.key === "F12" ||
      (input.control && input.shift && input.key === "I")
    ) {
      win.webContents.toggleDevTools();
    }
  });

  // Open external links in the default browser, not Electron
  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });
}

// ── IPC: ping a hostname and return latency ───────────────────────────────────
ipcMain.handle("ping", (_event, host) => {
  return new Promise((resolve) => {
    // Strip protocol/path so callers can pass full URLs
    const clean = host.replace(/^https?:\/\//i, "").split("/")[0];

    // -n 1 / -c 1 → one packet; -w 2000 / -W 2 → 2-second timeout
    const cmd = process.platform === "win32"
      ? `ping -n 1 -w 2000 ${clean}`
      : `ping -c 1 -W 2 ${clean}`;

    const start = Date.now();

    exec(cmd, { timeout: 5000 }, (err, stdout) => {
      const elapsed = Date.now() - start;

      if (err) {
        resolve({ ok: false, host: clean, latency: null, error: "Host unreachable" });
        return;
      }

      // Parse "time=12.3ms" (Linux/Mac) or "time=12ms" / "Average = 12ms" (Windows)
      const winMatch  = stdout.match(/Average\s*=\s*(\d+)ms/i);
      const unixMatch = stdout.match(/time[<=]([\d.]+)\s*ms/i);
      const raw = winMatch?.[1] ?? unixMatch?.[1];
      const latency = raw != null ? Math.round(parseFloat(raw)) : elapsed;

      resolve({ ok: true, host: clean, latency, raw: stdout });
    });
  });
});

app.whenReady().then(() => {
  Menu.setApplicationMenu(null);
  createWindow();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
