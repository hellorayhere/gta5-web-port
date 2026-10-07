'use strict';

/*
 * server.js — self-contained streaming game server for the GTA V web port.
 *
 * It serves the game to the Electron renderer (or any browser) from one of two
 * sources:
 *
 *   remote  (default)  stream members out of the 20 GB playgta5-offline.zip on
 *                      archive.org using HTTP range requests — nothing large is
 *                      ever stored locally.
 *   local              serve a local extracted mirror/playgta5.com/ directory
 *                      (fully offline, fastest).
 *
 * It also reproduces the two things the engine needs from the original
 * serve_local.py:
 *   - cross-origin isolation headers (COOP/COEP/CORP) for SharedArrayBuffer
 *   - a POST /data/batch range-batching endpoint
 *   - HTTP byte-range (206) support for every asset
 *
 * No third-party dependencies: only Node built-ins.
 */

const http = require('http');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const { once } = require('events');

const UA = 'playgta5-desktop/1.0';
const PREFIX = 'playgta5-offline/mirror/playgta5.com/';
const FULL_CACHE_MAX = 64 * 1024 * 1024; // members <= this are cached whole
const READ_CHUNK = 8 * 1024 * 1024; // compressed read granularity

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.bin': 'application/octet-stream',
  '.wgsl': 'text/plain; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
};

function ctype(name) {
  return MIME[path.extname(name).toLowerCase()] || 'application/octet-stream';
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Resolve when a writable stream drains or closes (removes both listeners). */
function drainOrClose(stream) {
  return new Promise((resolve) => {
    const cleanup = () => {
      stream.removeListener('drain', onDrain);
      stream.removeListener('close', onClose);
    };
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onClose = () => {
      cleanup();
      resolve();
    };
    stream.on('drain', onDrain);
    stream.on('close', onClose);
  });
}

class GameServer {
  /**
   * @param {object} opts
   * @param {object} [opts.index]      parsed zip_index.json (remote mode)
   * @param {string} [opts.mirrorDir]  local mirror/playgta5.com dir (local mode)
   * @param {string} [opts.cacheDir]   where decompressed members are cached
   * @param {number} [opts.cacheLimit] decompressed cache ceiling in bytes
   */
  constructor(opts = {}) {
    this.index = opts.index || null;
    this.mirrorDir = opts.mirrorDir ? path.resolve(opts.mirrorDir) : null;
    this.cacheDir = opts.cacheDir || path.join(__dirname, '.cache');
    this.cacheLimit = opts.cacheLimit || 2_000_000_000;
    this.publicDir = opts.publicDir ? path.resolve(opts.publicDir) : null;
    this.downloadUrl = opts.downloadUrl || process.env.GTA5_DOWNLOAD_URL || null;
    this.stats = { requests: 0, archive_bytes: 0, cache_hits: 0, cache_misses: 0 };

    this._resolvedUrl = null;
    this._resolvedPromise = null;
    this._dataOff = new Map();
    this._busy = new Map();

    fs.mkdirSync(this.cacheDir, { recursive: true });
  }

  get mode() {
    return this.mirrorDir ? 'local' : 'remote';
  }

  // ---------------------------------------------------------------- remote I/O

  async _ensureResolved() {
    if (this._resolvedUrl) return this._resolvedUrl;
    if (!this._resolvedPromise) {
      this._resolvedPromise = (async () => {
        const res = await fetch(this.index.url, {
          method: 'HEAD',
          headers: { 'User-Agent': UA },
          redirect: 'follow',
        });
        this._resolvedUrl = res.url || this.index.url;
        return this._resolvedUrl;
      })();
    }
    return this._resolvedPromise;
  }

  /** Fetch bytes [start, end] (inclusive) of the remote zip, with retries. */
  async httpRange(start, end, tries = 6) {
    if (end < start) return Buffer.alloc(0);
    let lastErr;
    for (let i = 0; i < tries; i++) {
      try {
        const url = await this._ensureResolved();
        const res = await fetch(url, {
          headers: { 'User-Agent': UA, Range: `bytes=${start}-${end}` },
          redirect: 'follow',
        });
        if (res.status !== 206 && res.status !== 200) throw new Error('HTTP ' + res.status);
        return Buffer.from(await res.arrayBuffer());
      } catch (e) {
        lastErr = e;
        if (i === tries - 1) break;
        await sleep(1500 * (i + 1));
      }
    }
    throw lastErr;
  }

  /** A Readable of bytes [start, end] of the remote zip (for direct proxying). */
  async httpStream(start, end) {
    const url = await this._ensureResolved();
    const res = await fetch(url, {
      headers: { 'User-Agent': UA, Range: `bytes=${start}-${end}` },
      redirect: 'follow',
    });
    if (res.status !== 206 && res.status !== 200) throw new Error('HTTP ' + res.status);
    return Readable.fromWeb(res.body);
  }

  /** Resolve a member's data offset by reading its local file header (cached). */
  async dataOffset(name) {
    if (this._dataOff.has(name)) return this._dataOff.get(name);
    const e = this.index.entries[name];
    const lho = e.lho;
    const hdr = await this.httpRange(lho, lho + 29);
    if (hdr.toString('latin1', 0, 4) !== 'PK\x03\x04') {
      throw new Error('bad local header for ' + name);
    }
    const nlen = hdr.readUInt16LE(26);
    const elen = hdr.readUInt16LE(28);
    const off = lho + 30 + nlen + elen;
    this._dataOff.set(name, off);
    return off;
  }

  // -------------------------------------------------------------- cache (disk)

  _cachePaths(name) {
    const key = crypto.createHash('sha1').update(name).digest('hex');
    return { data: path.join(this.cacheDir, key), meta: path.join(this.cacheDir, key + '.json') };
  }

  _readMeta(name) {
    try {
      return JSON.parse(fs.readFileSync(this._cachePaths(name).meta, 'utf8'));
    } catch {
      return null;
    }
  }

  cacheTotal() {
    let total = 0;
    let files = [];
    try {
      files = fs.readdirSync(this.cacheDir);
    } catch {
      return 0;
    }
    for (const f of files) {
      if (f.endsWith('.json') || f.endsWith('.tmp')) continue;
      try {
        total += fs.statSync(path.join(this.cacheDir, f)).size;
      } catch {
        /* ignore */
      }
    }
    return total;
  }

  /** Delete least-recently-used cache entries until under the limit. */
  evict(protect, need = 0) {
    let files = [];
    try {
      files = fs.readdirSync(this.cacheDir);
    } catch {
      return;
    }
    const entries = [];
    for (const f of files) {
      if (!f.endsWith('.json')) continue;
      const meta = path.join(this.cacheDir, f);
      let info;
      try {
        info = JSON.parse(fs.readFileSync(meta, 'utf8'));
      } catch {
        continue;
      }
      const data = path.join(this.cacheDir, f.slice(0, -5));
      if (fs.existsSync(data)) {
        entries.push({ at: info.at || 0, data, meta, size: fs.statSync(data).size });
      }
    }
    let total = entries.reduce((a, e) => a + e.size, 0);
    if (total + need <= this.cacheLimit) return;
    entries.sort((a, b) => a.at - b.at);
    for (const e of entries) {
      if (protect && path.basename(e.data) === protect) continue;
      try {
        fs.unlinkSync(e.data);
        fs.unlinkSync(e.meta);
      } catch {
        /* ignore */
      }
      total -= e.size;
      if (total + need <= this.cacheLimit) break;
    }
  }

  /** Fetch a whole member (<= FULL_CACHE_MAX) and cache it decompressed. */
  async buildFullCache(name) {
    const e = this.index.entries[name];
    const { data: dataPath, meta: metaPath } = this._cachePaths(name);
    const tmp = dataPath + '.tmp';
    const off = await this.dataOffset(name);
    const raw = await this.httpRange(off, off + e.csize - 1);
    this.stats.archive_bytes += raw.length;
    const data = e.method === 0 ? raw : zlib.inflateRawSync(raw);
    await fsp.writeFile(tmp, data);
    await fsp.rename(tmp, dataPath);
    await fsp.writeFile(
      metaPath,
      JSON.stringify({ name, have: e.usize, usize: e.usize, at: Date.now() / 1000 })
    );
    return dataPath;
  }

  /**
   * Ensure the decompressed cache for a large DEFLATE member covers `needEnd`.
   * Deflate is a stream, so a range read requires inflating from the start.
   */
  async buildPrefixCache(name, needEnd) {
    const e = this.index.entries[name];
    const { data: dataPath, meta: metaPath } = this._cachePaths(name);
    const meta = this._readMeta(name) || { have: 0 };
    if (meta.have > needEnd) return dataPath;

    const off = await this.dataOffset(name);
    const csize = e.csize;
    const target = Math.min(needEnd + 1, e.usize);
    const tmp = dataPath + '.tmp';

    const inflate = zlib.createInflateRaw();
    const ws = fs.createWriteStream(tmp);
    let got = 0;
    let consumed = 0;

    const feed = (async () => {
      try {
        while (consumed < csize && !inflate.destroyed) {
          const end = Math.min(consumed + READ_CHUNK, csize) - 1;
          const raw = await this.httpRange(off + consumed, off + end);
          if (!raw.length) break;
          consumed += raw.length;
          this.stats.archive_bytes += raw.length;
          if (!inflate.write(raw)) {
            await drainOrClose(inflate);
          }
        }
        if (!inflate.destroyed) inflate.end();
      } catch {
        /* aborted once we have enough */
      }
    })();

    try {
      for await (const chunk of inflate) {
        const room = target - got;
        if (room <= 0) break;
        const take = chunk.length <= room ? chunk : chunk.subarray(0, room);
        got += take.length;
        if (!ws.write(take)) await once(ws, 'drain');
        if (got >= target) break;
      }
    } finally {
      if (!inflate.destroyed) inflate.destroy();
      await new Promise((r) => ws.end(r));
      await feed;
    }

    await fsp.rename(tmp, dataPath);
    await fsp.writeFile(
      metaPath,
      JSON.stringify({ name, have: got, usize: e.usize, at: Date.now() / 1000 })
    );
    return dataPath;
  }

  // ------------------------------------------------------------------- locking

  async _lock(name) {
    while (this._busy.has(name)) {
      await this._busy.get(name);
    }
    let release;
    const gate = new Promise((r) => (release = r));
    this._busy.set(name, gate);
    return {
      release: () => {
        this._busy.delete(name);
        release();
      },
    };
  }

  // ------------------------------------------------------------------ routing

  /** Map a request path to a zip member (remote) or a local file (local mode). */
  resolve(pathname) {
    if (this.mirrorDir) {
      const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
      const file = path.normalize(path.join(this.mirrorDir, rel));
      if (!file.startsWith(this.mirrorDir)) return null;
      try {
        const st = fs.statSync(file);
        if (!st.isFile()) return null;
        return { file, size: st.size };
      } catch {
        return null;
      }
    }
    const name = pathname === '/' ? PREFIX + 'index.html' : PREFIX + pathname.replace(/^\/+/, '');
    const e = this.index.entries[name];
    if (!e) return null;
    return { name, size: e.usize };
  }

  /** Resolve a /data/<x> reference (used by the batch endpoint). */
  resolveData(ref) {
    const clean = String(ref).replace(/^\/+/, '');
    if (this.mirrorDir) {
      const file = path.normalize(path.join(this.mirrorDir, 'data', clean));
      if (!file.startsWith(this.mirrorDir)) return null;
      try {
        const st = fs.statSync(file);
        if (!st.isFile()) return null;
        return { file, size: st.size };
      } catch {
        return null;
      }
    }
    const name = PREFIX + 'data/' + clean;
    const e = this.index.entries[name];
    if (!e) return null;
    return { name, size: e.usize };
  }

  // ------------------------------------------------------------------- serving

  async _readFileSlice(file, start, end) {
    const fh = await fsp.open(file, 'r');
    try {
      const len = end - start + 1;
      const buf = Buffer.alloc(len);
      await fh.read(buf, 0, len, start);
      return buf;
    } finally {
      await fh.close();
    }
  }

  /** Read a range from either a local file or a (possibly cached) zip member. */
  async readRange(target, start, end) {
    if (target.file) return this._readFileSlice(target.file, start, end);

    const name = target.name;
    const e = this.index.entries[name];
    const lock = await this._lock(name);
    try {
      if (e.usize <= FULL_CACHE_MAX) {
        const { data } = this._cachePaths(name);
        if (!fs.existsSync(data)) {
          this.evict(path.basename(data), e.usize);
          await this.buildFullCache(name);
        }
        return this._readFileSlice(data, start, end);
      }
      if (e.method === 0) {
        const off = await this.dataOffset(name);
        const buf = await this.httpRange(off + start, off + end);
        this.stats.archive_bytes += buf.length;
        return buf;
      }
      const { data } = this._cachePaths(name);
      const meta = this._readMeta(name);
      if (!meta || meta.have <= end) {
        this.evict(path.basename(data), e.usize);
        await this.buildPrefixCache(name, end);
      }
      return this._readFileSlice(data, start, end);
    } finally {
      lock.release();
    }
  }

  async _streamMember(name, start, end, res) {
    const e = this.index.entries[name];
    const lock = await this._lock(name);
    try {
      if (e.usize <= FULL_CACHE_MAX) {
        const { data } = this._cachePaths(name);
        if (!fs.existsSync(data)) {
          this.stats.cache_misses++;
          this.evict(path.basename(data), e.usize);
          await this.buildFullCache(name);
        } else {
          this.stats.cache_hits++;
        }
        await pipeline(fs.createReadStream(data, { start, end }), res);
        return;
      }
      if (e.method === 0) {
        const off = await this.dataOffset(name);
        const rs = await this.httpStream(off + start, off + end);
        await pipeline(rs, res);
        return;
      }
      const { data } = this._cachePaths(name);
      const meta = this._readMeta(name);
      if (!meta || meta.have <= end) {
        this.stats.cache_misses++;
        this.evict(path.basename(data), e.usize);
        await this.buildPrefixCache(name, end);
      } else {
        this.stats.cache_hits++;
      }
      await pipeline(fs.createReadStream(data, { start, end }), res);
    } finally {
      lock.release();
    }
  }

  /** Serve a file from the public/ dir (landing page, play page, assets). */
  async _serveStatic(pathname, req, res) {
    let rel;
    if (pathname === '/' || pathname === '') rel = 'index.html';
    else if (pathname === '/play' || pathname === '/play/') rel = 'play.html';
    else rel = pathname.replace(/^\/+/, '');
    const file = path.normalize(path.join(this.publicDir, rel));
    if (file !== this.publicDir && !file.startsWith(this.publicDir + path.sep)) return false;
    let st;
    try {
      st = fs.statSync(file);
    } catch {
      return false;
    }
    if (!st.isFile()) return false;

    const size = st.size;
    let start = 0;
    let end = size - 1;
    let partial = false;
    const rng = req.headers['range'];
    if (rng) {
      const m = /^bytes=(\d*)-(\d*)$/.exec(String(rng).trim());
      if (m && (m[1] || m[2])) {
        if (m[1]) {
          start = parseInt(m[1], 10);
          end = m[2] ? parseInt(m[2], 10) : size - 1;
        } else {
          start = Math.max(0, size - parseInt(m[2], 10));
          end = size - 1;
        }
        partial = true;
      }
    }
    if (start >= size || end < start) {
      res.writeHead(416, { 'Content-Range': `bytes */${size}`, 'Content-Length': '0' });
      res.end();
      return true;
    }
    end = Math.min(end, size - 1);
    const length = end - start + 1;
    const headers = {
      'Content-Type': ctype(file),
      'Content-Length': String(length),
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'same-origin',
      'Accept-Ranges': 'bytes',
      'Cache-Control': 'no-cache',
    };
    if (partial) headers['Content-Range'] = `bytes ${start}-${end}/${size}`;
    res.writeHead(partial ? 206 : 200, headers);
    if (req.method === 'HEAD') {
      res.end();
      return true;
    }
    await pipeline(fs.createReadStream(file, { start, end }), res);
    return true;
  }

  async handle(req, res) {
    this.stats.requests++;
    const u = new URL(req.url, 'http://127.0.0.1');
    const pathname = decodeURIComponent(u.pathname);

    if (pathname === '/__status') {
      const body = Buffer.from(
        JSON.stringify({
          ...this.stats,
          mode: this.mode,
          cache_bytes: this.cacheTotal(),
          cache_limit: this.cacheLimit,
        })
      );
      res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': body.length });
      return res.end(body);
    }

    if (req.method === 'POST' && pathname === '/data/batch') {
      return this.handleBatch(req, res, u);
    }

    if (pathname === '/download' || pathname === '/download/') {
      if (this.downloadUrl) {
        res.writeHead(302, { Location: this.downloadUrl, 'Cache-Control': 'no-store' });
        return res.end();
      }
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      return res.end('download not configured');
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && this.publicDir) {
      if (await this._serveStatic(pathname, req, res)) return;
    }

    if (req.method !== 'GET' && req.method !== 'HEAD') {
      res.writeHead(405, { 'Content-Type': 'text/plain' });
      return res.end('method not allowed');
    }

    const target = this.resolve(pathname);
    if (!target) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('not in bundle: ' + pathname);
    }

    const size = target.size;
    let start = 0;
    let end = Math.max(0, size - 1);
    let partial = false;

    const rng = req.headers['range'];
    if (rng) {
      const m = /^bytes=(\d*)-(\d*)$/.exec(String(rng).trim());
      if (!m || (!m[1] && !m[2])) {
        res.writeHead(416, { 'Content-Range': `bytes */${size}`, 'Content-Length': '0' });
        return res.end();
      }
      if (m[1]) {
        start = parseInt(m[1], 10);
        end = m[2] ? parseInt(m[2], 10) : size - 1;
      } else {
        const n = parseInt(m[2], 10);
        start = Math.max(0, size - n);
        end = size - 1;
      }
      partial = true;
    }

    if (start >= size || end < start) {
      res.writeHead(416, { 'Content-Range': `bytes */${size}`, 'Content-Length': '0' });
      return res.end();
    }
    end = Math.min(end, size - 1);
    const length = end - start + 1;

    const headers = {
      'Content-Type': ctype(target.name || target.file),
      'Content-Length': String(length),
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'same-origin',
      'Accept-Ranges': 'bytes',
      'Cache-Control': 'no-cache',
    };
    if (partial) headers['Content-Range'] = `bytes ${start}-${end}/${size}`;

    res.writeHead(partial ? 206 : 200, headers);
    if (req.method === 'HEAD') return res.end();

    try {
      if (target.file) {
        await pipeline(fs.createReadStream(target.file, { start, end }), res);
      } else {
        await this._streamMember(target.name, start, end, res);
      }
    } catch (e) {
      if (!res.writableEnded) res.destroy();
    }
  }

  async handleBatch(req, res, u) {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = Buffer.concat(chunks);
    if (body.length > 1024 * 1024) {
      res.writeHead(413, { 'Content-Type': 'text/plain' });
      return res.end('batch too large');
    }
    let runs;
    try {
      runs = JSON.parse(body.toString('utf8'));
    } catch {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      return res.end('bad json');
    }
    if (!Array.isArray(runs) || runs.length > 2000) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      return res.end('invalid batch');
    }
    try {
      const parts = [];
      const lengths = [];
      for (const run of runs) {
        const [ref, s, e] = run;
        const target = this.resolveData(ref);
        if (!target) throw new Error('missing ' + ref);
        const end = Math.min(e, target.size - 1);
        if (s < 0 || end < s) throw new Error('bad range');
        const data = await this.readRange(target, s, end);
        parts.push(data);
        lengths.push(data.length);
      }
      let out = Buffer.concat(parts);
      const gz = u.searchParams.get('gz') === '1';
      if (gz) out = zlib.gzipSync(out, { level: 1 });
      const headers = {
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(out.length),
        'X-Run-Lengths': lengths.join(','),
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
        'Cross-Origin-Resource-Policy': 'same-origin',
      };
      if (gz) headers['Content-Encoding'] = 'gzip';
      res.writeHead(200, headers);
      res.end(out);
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end(String((e && e.message) || e));
    }
  }

  /** Warm the cache with the engine, shaders, title art and boot manifests. */
  async prewarm(onProgress) {
    if (!this.index) return { files: 0, bytes: 0 };
    const targets = [];
    const rels = [
      'index.html',
      'b/8b0b5899ed/loader.js',
      'b/8b0b5899ed/game.js',
      'b/8b0b5899ed/io_worker.js',
      'b/8b0b5899ed/wgpu_worker.js',
      'b/8b0b5899ed/audio-worklet.js',
      'b/8b0b5899ed/shaders/index.json',
      'b/8b0b5899ed/shaders/pipelines.json',
      'b/8b0b5899ed/shaders/pipelines_low.json',
      'data/bootset.json',
      'data/bootset_low.json',
      'data/manifest.json',
    ];
    for (const r of rels) targets.push(PREFIX + r);
    for (const n of Object.keys(this.index.entries)) {
      if (n.startsWith(PREFIX + 'b/8b0b5899ed/shaders/') && n.endsWith('.bin')) targets.push(n);
      else if (n.startsWith(PREFIX + 'title/')) targets.push(n);
    }

    const seen = new Set();
    const todo = [];
    for (const n of targets) {
      if (this.index.entries[n] && !seen.has(n)) {
        seen.add(n);
        todo.push(n);
      }
    }

    let bytes = 0;
    let done = 0;
    for (const name of todo) {
      const { data } = this._cachePaths(name);
      if (fs.existsSync(data)) {
        done++;
        continue;
      }
      const e = this.index.entries[name];
      this.evict(path.basename(data), e.usize);
      try {
        await this.buildFullCache(name);
        bytes += e.usize;
      } catch {
        /* skip failures */
      }
      done++;
      if (onProgress) onProgress(done, todo.length, bytes);
    }
    return { files: done, bytes };
  }
}

/** Build the index (remote mode) and return a listening http.Server + proxy. */
async function createServer(opts = {}) {
  let index = opts.index || null;
  if (!index && !opts.mirrorDir) {
    const indexPath = opts.indexPath || path.join(__dirname, 'assets', 'zip_index.json');
    index = JSON.parse(await fsp.readFile(indexPath, 'utf8'));
  }
  const proxy = new GameServer({ ...opts, index });
  const server = http.createServer((req, res) => {
    proxy.handle(req, res).catch(() => {
      if (!res.writableEnded) {
        try {
          res.writeHead(500, { 'Content-Type': 'text/plain' });
        } catch {
          /* ignore */
        }
        res.end('internal error');
      }
    });
  });
  server.keepAliveTimeout = 120000;
  server.headersTimeout = 130000;
  return { server, proxy };
}

module.exports = { GameServer, createServer, PREFIX, FULL_CACHE_MAX };
