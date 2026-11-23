# Changelog

All notable changes to `localhop` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-22

### Added
- **Stream cancellation (`Frame::Cancel`, tag `0x10`):** When a public client disconnects mid-stream, `localhop-server` immediately cancels the in-flight upstream task on the CLI client to prevent wasted local CPU and bandwidth.
- **Automatic `gzip` and `deflate` preview decompression** in the Live Inspector UI (`flate2`, bounded to `128 KiB`), so compressed API JSON renders cleanly instead of `<binary payload>`.
- **Per-IP token-bucket rate limiter** on `localhop-server` (`--rate-limit-rps` / `LOCALHOP_RATE_LIMIT_RPS`, returning `429 Too Many Requests` with `Retry-After: 1`; enabled at `100 req/s` in `render.yaml`).
- **Path-routing `Location` redirect rewriting:** Relative `Location: /...` headers are automatically prefixed with `/t/<id>` in path-routing mode.
- **Custom header injection (`-H "Key: Value"`)** and **multi-tunnel configuration (`--config localhop.toml`)** so multiple local ports can run together under one inspector.
- **Automated release pipeline:** GitHub Actions upgrades to Node 24-native actions, generates a `localhop.rb` Homebrew formula on every tag release, and supports automatic `crates.io` + `npm` publishing via repository secrets (`CARGO_REGISTRY_TOKEN`, `NPM_TOKEN`), plus `./scripts/release.sh`.
- **1-Click Deploy to Render** buttons in README and live website, plus real Inspector UI screenshot in README.

### Performance
- **Zero-body request fast path (`has_body`):** Skips body-streaming task spawning, extra `mpsc` channels, and `RequestEnd` frames for empty-body HTTP requests (`GET`, `HEAD`, `DELETE`).
- **Batched WebSocket writer (`feed` + `try_recv` + `flush`):** Coalesces up to 32 queued tunnel frames (such as `ResponseHead` + `ResponseBody` + `ResponseEnd`) into a single TCP write syscall.
- **`TCP_NODELAY` on tunnel WebSockets:** Enabled `disable_nagle = true` in `tokio-tungstenite` client connections to eliminate Nagle/Delayed-ACK latency stalls.
- **Zero-overhead headless mode (`--no-inspector`):** Completely bypasses body preview copying, decompression, and inspector `RwLock` contention when disabled.
- **Zero-allocation header filtering:** Replaced `to_ascii_lowercase()` heap allocations in `is_hop_by_hop_header` with `eq_ignore_ascii_case`.

### Fixed
- Fixed `.github/workflows/release.yml` job-level `secrets.*` validation error by moving token checks into step-level environment guards.
- Fixed multi-tunnel replay routing in the Live Inspector so replayed requests target the original tunnel port.
- Normalized `npm/localhop/package.json` `repository.url` to `git+https://github.com/Leon24k/localhop.git`.

## [0.1.0] - 2026-11-22

### Added
- **Zero-copy binary multiplexing protocol** (`localhop-protocol`) over a single persistent WebSocket tunnel with `9 B` body frame headers and continuous `Ping`/`Pong` RTT telemetry.
- **Lock-free Axum streaming relay** (`localhop-server`) supporting zero-config path routing (`/t/<id>/...`) and optional wildcard subdomain routing (`LOCALHOP_BASE_DOMAIN`).
- **Full WebSocket passthrough** for proxied local apps (Vite HMR, Next.js Fast Refresh, Socket.IO, custom WebSocket servers).
- **Seamless CLI** (`localhop <port>`) with automatic reconnect & backoff reset, keep-alive pooled Hyper forwarding, `--basic-auth`, `--preserve-host`, and `--quiet`.
- **Built-in Live HTTP & Webhook Inspector UI (`127.0.0.1:4040`)** with live SSE streaming, bounded 200-entry ring buffer, 1-click Replay, Edit & Replay, Copy as cURL, offline JSON fixture Export/Import, and localhost Origin CSRF protection.
- **Relay security & abuse hardening:** constant-time token & Basic Auth verification, `--max-tunnels`, `--max-body-bytes` (`413 Payload Too Large`), and idle/zombie tunnel eviction (`--idle-timeout-secs`).
- **Distribution & deployment:** published on `crates.io` and `npm`, multi-stage `Dockerfile`, Render free-tier `render.yaml`, GitHub Actions CI & cross-platform release workflows, macOS/Linux `install.sh`, and live GitHub Pages website & docs.
