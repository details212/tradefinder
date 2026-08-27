/**
 * Windows pre-build cleanup for electron-builder.
 * Stops running TradeFinder/Electron instances and clears dist-exe so
 * app.asar is not locked from a previous run.
 */

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const clientRoot = path.join(__dirname, "..");
const distExe = path.join(clientRoot, "dist-exe");
const winUnpacked = path.join(distExe, "win-unpacked");

function run(cmd) {
  try {
    execSync(cmd, { stdio: "ignore", windowsHide: true });
  } catch (_) {
    // expected when process is not running
  }
}

function sleep(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* wait */
  }
}

function removeDir(dir) {
  if (!fs.existsSync(dir)) return true;
  try {
    fs.rmSync(dir, { recursive: true, force: true });
    return !fs.existsSync(dir);
  } catch (_) {
    return false;
  }
}

console.log("\n  Pre-build: stopping TradeFinder / Electron if running...");
run('taskkill /F /IM TradeFinder.exe /T');
run('taskkill /F /IM electron.exe /T');

sleep(1500);

console.log("  Pre-build: clearing previous dist-exe output...");
let cleared = false;
for (let attempt = 1; attempt <= 5; attempt++) {
  if (removeDir(winUnpacked) || !fs.existsSync(winUnpacked)) {
    cleared = true;
    break;
  }
  sleep(1000);
}

if (!cleared && fs.existsSync(distExe)) {
  const stale = `${distExe}.stale-${Date.now()}`;
  try {
    fs.renameSync(distExe, stale);
    console.log(`  Pre-build: moved locked dist-exe → ${path.basename(stale)}`);
    cleared = true;
  } catch (err) {
    console.error(
      "\n  ERROR: Cannot clear dist-exe — a file is locked by another process.\n" +
        "  Close TradeFinder, any Electron dev window, and File Explorer\n" +
        "  windows open under electron-client\\dist-exe, then rebuild.\n"
    );
    process.exit(1);
  }
}

console.log("  Pre-build: ready.\n");
