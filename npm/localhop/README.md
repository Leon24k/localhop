# localhop

Seamless, ultra-low-latency Rust localhost tunnel with a built-in live HTTP & webhook inspector and 1-click replay UI.

This npm package downloads the native prebuilt Rust binary (~3.5 MB, zero garbage collector) from [GitHub Releases](https://github.com/Leon24k/localhop/releases) — no Rust toolchain required.

## Usage

```bash
# Run without installing
npx localhop 3000 --server https://<your-relay>.onrender.com

# Or install globally
npm install -g localhop
localhop 3000 --server https://<your-relay>.onrender.com
```

Full documentation, benchmarks, and $0 self-hosting guides: **https://github.com/Leon24k/localhop**
