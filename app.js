'use strict';

/*
 * app.js — entry point for the hosted (Render) deployment of the GTA V web port.
 *
 * Runs the same self-contained streaming server used by the desktop app, but as
 * a normal web service:
 *   - binds to 0.0.0.0:$PORT (Render injects PORT)
 *   - streams the game out of the 20 GB archive.org bundle on demand
 *   - adds the COOP/COEP/CORP headers the engine needs for SharedArrayBuffer
 *   - exposes /__status for Render's health checks
 *
 * No third-party dependencies: Node built-ins only.
 */

const os = require('os');
const path = require('path');
const { createServer } = require('./server');

const PORT = parseInt(process.env.PORT || '10000', 10);
const HOST = process.env.HOST || '0.0.0.0';

// Keep the in-RAM/disk cache modest so we fit comfortably in a small container.
const CACHE_LIMIT = parseInt(process.env.GTA5_CACHE_LIMIT || String(256 * 1024 * 1024), 10);
const CACHE_DIR = process.env.GTA5_CACHE_DIR || path.join(os.tmpdir(), 'gta5-cache');

(async () => {
  const { server, proxy } = await createServer({
    cacheLimit: CACHE_LIMIT,
    cacheDir: CACHE_DIR,
    publicDir: path.join(__dirname, 'public'),
    downloadUrl: process.env.GTA5_DOWNLOAD_URL || null,
  });

  server.listen(PORT, HOST, () => {
    console.log(`[gta5] listening on http://${HOST}:${PORT}`);
    console.log(`[gta5] mode=${proxy.mode} cache_limit=${CACHE_LIMIT} cache_dir=${CACHE_DIR}`);
    console.log(`[gta5] entries indexed: ${Object.keys(proxy.index.entries).length}`);
  });

  // Keep the free instance warm. Render spins a free web service down after
  // ~15 minutes with no traffic, and the wake-up request is exactly what makes a
  // returning visitor see a blank/stuck page. Pinging our own public URL every
  // 4 minutes stays well inside that idle window, so once the service is up it
  // stays up and never expires from inactivity. RENDER_EXTERNAL_URL is injected
  // by Render; GTA5_SELF_URL overrides it.
  const SELF_URL = (process.env.GTA5_SELF_URL || process.env.RENDER_EXTERNAL_URL || 'https://gta5free.onrender.com').replace(/\/+$/, '');
  const keepAlive = async () => {
    try {
      await fetch(SELF_URL + '/__status', { cache: 'no-store' });
      await fetch(SELF_URL + '/', { cache: 'no-store' });
    } catch (e) {
      /* the instance is waking or briefly unreachable; the next tick retries */
    }
  };
  const keepAliveTimer = setInterval(keepAlive, 4 * 60 * 1000);
  keepAliveTimer.unref();
  setTimeout(keepAlive, 20 * 1000).unref();
  console.log(`[gta5] keep-alive pinging ${SELF_URL} every 4 min`);

  const shutdown = () => {
    console.log('[gta5] shutting down');
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
})().catch((err) => {
  console.error('[gta5] failed to start:', err);
  process.exit(1);
});
