/**
 * Set TRADEFINDER_API_URL before building the app to point to your remote server.
 * e.g. https://api.yourdomain.com
 *
 * This value is injected into the renderer via a <script> tag in index.html at build time.
 * For development it falls back to http://localhost:5000 (see api/client.js).
 */
module.exports = {
  API_URL: process.env.TRADEFINDER_API_URL || "http://localhost:5000",
};
