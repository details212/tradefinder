/**
 * Increments the minor version in package.json before each production build.
 * e.g. 0.11.0 → 0.12.0
 *
 * Usage (called automatically by dist:win):
 *   node scripts/bump-version.js
 *
 * To bump the major version instead, pass --major:
 *   node scripts/bump-version.js --major
 */

const fs   = require("fs");
const path = require("path");

const pkgPath = path.join(__dirname, "..", "package.json");
const pkg     = JSON.parse(fs.readFileSync(pkgPath, "utf-8"));

const parts   = pkg.version.split(".").map(Number);
let [major, minor, patch] = parts;
const prev    = pkg.version;

if (process.argv.includes("--major")) {
  major += 1;
  minor  = 0;
  patch  = 0;
} else {
  minor += 1;
  patch  = 0;
}

const next    = `${major}.${minor}.${patch}`;
pkg.version   = next;

fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");

console.log(`\n  Version bumped: ${prev}  →  ${next}\n`);
