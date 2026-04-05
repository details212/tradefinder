import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { readFileSync } from "fs";

const pkg = JSON.parse(readFileSync("./package.json", "utf-8"));

export default defineConfig(({ command }) => {
  const apiUrl =
    process.env.TRADEFINDER_API_URL ||
    (command === "build" ? "https://bt.tradefinderdata.com" : "http://localhost:5000");

  return {
  plugins: [react()],
  base: "./",
  define: {
    // Dev → localhost:5000  |  Build → production URL (or override via env var)
    "window.TRADEFINDER_API_URL": JSON.stringify(apiUrl),
    // Embed the package.json version so the client can report it to the server
    "window.APP_VERSION": JSON.stringify(pkg.version),
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    strictPort: true,
  },
  };
});
