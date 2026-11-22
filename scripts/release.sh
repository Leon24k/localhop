#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: ./scripts/release.sh <version>  (example: ./scripts/release.sh 0.2.0)" >&2
  exit 1
fi

VER="${1#v}"
echo "Bumping localhop workspace and npm wrapper to v${VER}..."

# Update Cargo.toml workspace version & internal protocol dep
perl -pi -e "s/^version = \"[^\"]+\"/version = \"${VER}\"/" Cargo.toml
perl -pi -e "s/localhop-protocol = \{ version = \"[^\"]+\"/localhop-protocol = { version = \"${VER}\"/" Cargo.toml

# Update npm/localhop/package.json
node -e "
const fs = require('fs');
const p = 'npm/localhop/package.json';
const pkg = JSON.parse(fs.readFileSync(p, 'utf8'));
pkg.version = '${VER}';
fs.writeFileSync(p, JSON.stringify(pkg, null, 2) + '\n');
"

echo "Running checks..."
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

echo ""
echo "Version bumped to v${VER} and all checks passed."
echo "Next steps:"
echo "  git add Cargo.toml Cargo.lock npm/localhop/package.json"
echo "  git commit -m \"release: v${VER}\""
echo "  git tag -a v${VER} -m \"Release v${VER}\""
echo "  git push origin main v${VER}"
