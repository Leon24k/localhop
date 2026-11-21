#!/usr/bin/env bash
set -euo pipefail

echo "Building release binaries..."
cargo build --release --workspace --bins --examples >/dev/null

echo ""
echo "=== Binary Sizes (stripped, thin-LTO) ==="
ls -lh target/release/localhop target/release/localhop-server | awk '{print $9, $5}'

./target/release/examples/bench --upstream 13001 &
UP_PID=$!
./target/release/localhop-server --port 18080 >/dev/null 2>&1 &
SRV_PID=$!
sleep 0.4

./target/release/localhop 13001 --server ws://127.0.0.1:18080 -s bench --inspector-port 14040 >/dev/null 2>&1 &
CLI_PID=$!
sleep 0.5

cleanup() {
  kill "${CLI_PID:-}" "${SRV_PID:-}" "${UP_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

echo ""
echo "=== Idle Memory (RSS) ==="
printf "localhop-server: %s KB\n" "$(ps -o rss= -p "$SRV_PID" | tr -d ' ')"
printf "localhop CLI:    %s KB\n" "$(ps -o rss= -p "$CLI_PID" | tr -d ' ')"

echo ""
echo "=== Loopback Latency & Throughput (Inspector ON) ==="
./target/release/examples/bench http://127.0.0.1:18080/t/bench/ping

echo ""
echo "=== Peak/Post-Load Memory (RSS) ==="
printf "localhop-server: %s KB\n" "$(ps -o rss= -p "$SRV_PID" | tr -d ' ')"
printf "localhop CLI:    %s KB\n" "$(ps -o rss= -p "$CLI_PID" | tr -d ' ')"

# Also measure headless mode (--no-inspector)
kill "$CLI_PID" 2>/dev/null || true
wait "$CLI_PID" 2>/dev/null || true
sleep 0.2

./target/release/localhop 13001 --server ws://127.0.0.1:18080 -s bench-raw --no-inspector >/dev/null 2>&1 &
CLI_PID=$!
sleep 0.4

echo ""
echo "=== Loopback Latency & Throughput (--no-inspector) ==="
./target/release/examples/bench http://127.0.0.1:18080/t/bench-raw/ping
printf "localhop CLI (headless) RSS: %s KB\n" "$(ps -o rss= -p "$CLI_PID" | tr -d ' ')"
