# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in `localhop` or `localhop-server`, please **do not** open a public GitHub issue.

Instead, report it privately via GitHub Security Advisories on this repository (`Security → Advisories → Report a vulnerability`), or contact the maintainer directly. Please include:

- Steps to reproduce or a minimal proof of concept
- Affected crate(s) and version(s)
- Potential impact

We aim to acknowledge reports within 48 hours and release a fix as quickly as possible.

## Security Architecture Notes

- **Relay Authentication (`LOCALHOP_AUTH_TOKEN`):** Verified using constant-time byte comparison to prevent timing side-channels.
- **Per-Tunnel Basic Auth (`--basic-auth user:pass`):** Enforced at the relay edge in constant time before forwarding traffic to your local machine.
- **Abuse & Resource Limits:** `localhop-server` enforces configurable caps on simultaneous tunnels (`--max-tunnels`), request body size (`--max-body-bytes`), per-request timeouts (`--timeout-secs`), and idle/zombie tunnel eviction (`--idle-timeout-secs`).
- **Local Inspector (`127.0.0.1:4040`):** Binds exclusively to loopback (`127.0.0.1`) and validates the HTTP `Origin` header on mutating endpoints (`/api/replay`, `/api/clear`) to block cross-origin browser CSRF attacks.
- **TLS:** Always run public relays behind HTTPS/WSS (e.g., Render, Cloudflare, Caddy, or Nginx TLS termination) so tunnel traffic and credentials are encrypted in transit.
