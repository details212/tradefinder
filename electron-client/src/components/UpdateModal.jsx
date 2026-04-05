import { Download, AlertTriangle, RefreshCw } from "lucide-react";

/**
 * Blocking modal shown when the running client version is older than the
 * server's required version.  The user cannot dismiss it — they must
 * download and install the latest build.
 *
 * Props:
 *   clientVersion   — the version embedded in this build, e.g. "0.11.0"
 *   requiredVersion — minimum version the server accepts, e.g. "0.12.0"
 *   downloadUrl     — direct link to the latest TradeFinder.exe (may be "")
 */
export default function UpdateModal({ clientVersion, requiredVersion, downloadUrl }) {
  const displayClient   = `v${clientVersion}`;
  const displayRequired = `v${requiredVersion}`;

  function handleDownload() {
    if (downloadUrl) {
      // shell.openExternal is not available in the renderer; window.open with
      // _blank is intercepted by Electron's setWindowOpenHandler and routed
      // to the user's default browser.
      window.open(downloadUrl, "_blank");
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm">
      <div className="bg-slate-800 border border-slate-700 rounded-2xl p-8 max-w-sm w-full mx-4 shadow-2xl">

        {/* Icon */}
        <div className="flex justify-center mb-6">
          <div className="w-16 h-16 rounded-2xl bg-amber-500/20 border border-amber-500/30 flex items-center justify-center">
            <AlertTriangle className="w-8 h-8 text-amber-400" />
          </div>
        </div>

        {/* Heading */}
        <h2 className="text-xl font-bold text-white text-center mb-2">
          Update Required
        </h2>
        <p className="text-slate-400 text-sm text-center leading-relaxed mb-6">
          Your version of TradeFinder is out of date. Please download and
          install the latest version to continue.
        </p>

        {/* Version comparison */}
        <div className="bg-slate-900 rounded-xl border border-slate-700 p-4 mb-6 space-y-3">
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">Your version</span>
            <span className="font-mono font-semibold text-red-400">
              {displayClient}
            </span>
          </div>
          <div className="h-px bg-slate-700" />
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">Required version</span>
            <span className="font-mono font-semibold text-emerald-400">
              {displayRequired}
            </span>
          </div>
        </div>

        {/* Download CTA */}
        {downloadUrl ? (
          <button
            onClick={handleDownload}
            className="w-full bg-brand-600 hover:bg-brand-500 text-white font-semibold py-3 rounded-xl transition-colors flex items-center justify-center gap-2"
          >
            <Download className="w-4 h-4" />
            Download Latest Version
          </button>
        ) : (
          <div className="w-full bg-slate-700 border border-slate-600 rounded-xl py-3 px-4 flex items-center justify-center gap-2 text-slate-400 text-sm">
            <RefreshCw className="w-4 h-4" />
            Contact your administrator for the download link
          </div>
        )}

        <p className="text-slate-600 text-xs text-center mt-4">
          After installing the update, relaunch TradeFinder and sign in again.
        </p>
      </div>
    </div>
  );
}
