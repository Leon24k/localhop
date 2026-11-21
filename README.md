# localhop

Seamless, ultra-low-latency localhost tunnel written in pure Rust—with a built-in live HTTP & webhook inspector and 1-click replay UI.

```text
Internet ──HTTP──▶ [localhop-server (Axum)] ═══ Multiplexed Binary WS ═══▶ [localhop CLI] ──▶ localhost:3000
                                                                                 │
                                                                                 ▼
                                                                  Live Inspector (:4040)
                                                                  SSE stream · Edit & Replay
```

## Why localhop?

- **Zero-GC predictable tail latency:** Binary framing (`9B` header + raw `bytes::Bytes` payload) streams request and response bodies chunk-by-chunk with no base64/JSON body overhead and no garbage-collector pauses.
- **Built-in Live Inspector & Replay (`localhost:4040`):** Every tunneled request streams live via SSE with headers, pretty JSON, microsecond latency breakdown, **1-click Edit & Replay**, and **Copy as cURL**—stored in a bounded 200-entry ring buffer so memory stays flat.
- **$0 Self-Hostable Relay:** Runs on a single HTTP/WebSocket port—deploy your own private relay for free on **Render** (`render.yaml` included) or any container host, with both zero-config **path routing** (`/t/<id>/...`) and optional **wildcard subdomain routing** (`<id>.yourdomain.com`).
- **Fast Builds & Tiny Footprint:** Lean 3-crate Cargo workspace (`localhop-protocol`, `localhop-server`, `localhop`).

## Quick Start (Local Demo)

1. **Start the relay server** (terminal 1):
   ```bash
   cargo run -p localhop-server
   ```

2. **Start any local dev server** (or use a quick one-liner on port `3000`, terminal 2):
   ```bash
   python3 -m http.server 3000
   ```

3. **Open a tunnel** (terminal 3):
   ```bash
   cargo run -p localhop -- 3000 -s demo
   ```
   - Public URL: `http://127.0.0.1:8080/t/demo`
   - Live Inspector UI: `http://127.0.0.1:4040`

## CLI Usage

```bash
# Expose localhost:3000 via your deployed relay
localhop 3000 --server https://your-relay.onrender.com

# Request a custom tunnel ID & auth token
localhop 3000 -s my-webhook --server https://your-relay.onrender.com --token secret123

# Headless mode (no local inspector UI)
localhop 3000 --no-inspector
```

## Deploy Your Relay for Free (Render)

1. Push this repo to GitHub.
2. In [Render](https://render.com), choose **New → Blueprint** and select the repo (`render.yaml` configures everything automatically on the free plan).
3. Point your CLI at it:
   ```bash
   export LOCALHOP_SERVER=https://<your-service>.onrender.com
   localhop 3000
   ```

### Optional: Subdomain Routing on a Custom Domain

If you point `*.hop.yourdomain.com` to your server, start `localhop-server` with:
```bash
LOCALHOP_BASE_DOMAIN=hop.yourdomain.com localhop-server
```
Tunnels will automatically be served at `https://<id>.hop.yourdomain.com`.

## Workspace Layout

- `crates/localhop-protocol` — Zero-copy binary frame codec (`Bytes`)
- `crates/localhop-server` — Lock-free (`DashMap`) Axum streaming relay server
- `crates/localhop` — Developer CLI, pooled Hyper HTTP/1.1 forwarder, and embedded Axum inspector UI

## License

MIT
