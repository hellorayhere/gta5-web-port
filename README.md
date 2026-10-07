# GTA V Web Port — hosted (Render) deployment

This is the **server** half of the GTA V WebAssembly/WebGPU web port, packaged to
run as a normal web service on [Render](https://render.com) (or any Node host).

It does **not** store the game. The full game bundle is ~20 GB, which is far too
large for GitHub or a Render container. Instead this service:

1. Serves the game's web page and engine files (`index.html`, `loader.js`,
   `game.wasm`, shaders, title art, …) by **streaming them on demand** out of the
   20 GB `playgta5-offline.zip` on archive.org using HTTP range requests.
2. Streams the game's `data/` assets the same way.
3. Adds the **COOP / COEP / CORP** headers the engine needs for
   `SharedArrayBuffer` (multi-threaded WebAssembly).
4. Implements the `POST /data/batch` range-batching endpoint and HTTP byte-range
   (206) support the engine expects.

Nothing large is ever written to disk; a small bounded cache holds recently
decompressed members.

## Layout

| File | Purpose |
|------|---------|
| `app.js` | Entry point — binds `0.0.0.0:$PORT`, starts the server |
| `server.js` | Self-contained streaming server (Node built-ins only) |
| `assets/zip_index.json` | Index of the 12,823 members in the archive bundle |
| `render.yaml` | Render Blueprint (web service, free plan) |
| `package.json` | `npm start` → `node app.js` |

## Run locally

```bash
npm start                 # http://localhost:10000
# or
PORT=8080 npm start
```

Then open the URL in a Chromium-based browser (Chrome/Edge 133+ — the engine uses
WebAssembly **memory64**, which needs a recent Chromium).

## Deploy on Render

### Option A — Blueprint (recommended)
1. Push this folder to a GitHub repo.
2. Render Dashboard → **New** → **Blueprint** → pick the repo.
3. Render reads `render.yaml` and creates the `gta5-web-port` web service.

### Option B — Manual web service
- **Environment:** Node
- **Build command:** `npm install`
- **Start command:** `npm start`
- **Health check path:** `/__status`

## Environment variables

| Var | Default | Meaning |
|-----|---------|---------|
| `PORT` | `10000` | Listen port (Render sets this) |
| `GTA5_CACHE_LIMIT` | `268435456` (256 MB) | Decompressed-cache ceiling |
| `GTA5_CACHE_DIR` | `$TMPDIR/gta5-cache` | Where the cache lives |

## Notes & limits

- **Bandwidth:** each full game load streams a large amount of data from
  archive.org. Render's free plan has a monthly bandwidth cap — fine for personal
  use / a few players, not for public scale.
- **Cold starts:** Render free web services sleep after inactivity; the first
  request after sleeping takes a few seconds to wake.
- **GPU:** the *client* needs a browser with WebGPU. The server just streams bytes.
- The game bundle is an unofficial community port; this service only relays it.
