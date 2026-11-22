# Changelog

All notable changes to `localhop` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-22

### Added
- **Zero-copy binary multiplexing protocol** (`localhop-protocol`) over a single persistent WebSocket tunnel with `9 B` body frame headers and continuous `Ping`/`Pong` RTT telemetry.
- **Lock-free Axum streaming relay** (`localhop-server`) supporting zero-config path routing (`/t/<id>/...`) and optional wildcard subdomain routing (`LOCALHOP_BASE_DOMAIN`).
- **Full WebSocket passthrough** for proxied local apps (Vite HMR, Next.js Fast Refresh, Socket.IO, custom WebSocket servers).
- **Seamless CLI** (`localhop <port>`) with automatic reconnect & backoff reset, keep-alive pooled Hyper forwarding, `--basic-auth`, `--preserve-host`, and `--quiet`.
- **Built-in Live HTTP & Webhook Inspector UI (`127.0.0.1:4040`)** with live SSE streaming, bounded 200-entry ring buffer, 1-click Replay, Edit & Replay, Copy as cURL, offline JSON fixture Export/Import, and localhost Origin CSRF protection.
- **Relay security & abuse hardening:** constant-time token & Basic Auth verification, `--max-tunnels`, `--max-body-bytes` (`413 Payload Too Large`), and idle/zombie tunnel eviction (`--idle-timeout-secs`).
- **Standard reverse-proxy headers:** `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-For`, `X-Localhop-Tunnel-Id`, and microsecond latency headers.
- **Distribution & deployment:** multi-stage `Dockerfile`, Render free-tier `render.yaml`, GitHub Actions CI & cross-platform release workflows, macOS/Linux `install.sh`, and zero-dependency `npm`/`npx` wrapper.
- **Reproducible loopback benchmark suite** (`./scripts/bench.sh`) and end-to-end integration test suite.
