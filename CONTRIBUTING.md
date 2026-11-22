# Contributing to `localhop`

Thanks for your interest in contributing to `localhop`!

## Development Setup

Requirements:
- Rust stable toolchain (`1.85+`, Edition 2024) with `rustfmt` and `clippy`

```bash
git clone https://github.com/Leon24k/localhop.git
cd localhop
cargo test --workspace
```

## Before Opening a Pull Request

Please make sure all checks pass cleanly:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If your change affects the hot path (protocol framing, relay proxying, or local forwarding), please run the loopback benchmark and include before/after results in your PR description:

```bash
./scripts/bench.sh
```

## Guidelines

- Keep dependencies lean—fast compile times and tiny binary sizes are core goals of `localhop`.
- Avoid unnecessary allocations or blocking calls on the tunnel forwarding hot path.
- Add unit or integration tests in `crates/localhop-server/tests/` for new protocol or relay behavior.
- Write clear, concise commit messages (Conventional Commits style preferred, e.g., `feat:`, `fix:`, `perf:`, `docs:`).
