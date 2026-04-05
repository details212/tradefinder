/**
 * Runs automatically at the end of dist:win.
 * Reads the freshly-bumped version from package.json and calls
 * backend/set_version.py so the server immediately requires this version.
 */

const { execSync } = require("child_process");
const path         = require("path");
const fs           = require("fs");

const pkgPath    = path.join(__dirname, "..", "package.json");
const pkg        = JSON.parse(fs.readFileSync(pkgPath, "utf-8"));
const version    = pkg.version;
const backendDir = path.resolve(__dirname, "..", "..", "backend");

console.log(`\n  Updating server  →  app_required_version = ${version}`);

// Try the Windows 'py' launcher first, then 'python', then 'python3' (Mac/Linux)
const cmds = [
  `py set_version.py ${version}`,
  `python set_version.py ${version}`,
  `python3 set_version.py ${version}`,
];

let ok = false;
for (const cmd of cmds) {
  try {
    const out = execSync(cmd, { cwd: backendDir, encoding: "utf-8", stdio: "pipe" });
    process.stdout.write(out);
    ok = true;
    break;
  } catch (_) {
    // try next variant
  }
}

if (!ok) {
  console.warn(
    `\n  WARNING: Could not auto-update the server version.\n` +
    `  Run this manually after deploying the new build:\n\n` +
    `    cd ..\\backend && python set_version.py ${version}\n`
  );
}
