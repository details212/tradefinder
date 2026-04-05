const { contextBridge, ipcRenderer } = require("electron");

// Expose a safe, narrow API surface to the renderer process.
// Never expose ipcRenderer directly — only wrap specific channels.
contextBridge.exposeInMainWorld("electronAPI", {
  /**
   * Ping a host (or full URL — protocol/path is stripped automatically).
   * Returns: { ok: boolean, host: string, latency: number|null, error?: string }
   */
  ping: (host) => ipcRenderer.invoke("ping", host),
});
