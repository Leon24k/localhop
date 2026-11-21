#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");
const pkg = require("./package.json");

const REPO = "Leon24k/localhop";

function resolveTarget() {
  const { platform, arch } = process;
  if (platform === "darwin" && arch === "arm64") return { target: "aarch64-apple-darwin", ext: "tar.gz", bin: "localhop" };
  if (platform === "darwin" && arch === "x64") return { target: "x86_64-apple-darwin", ext: "tar.gz", bin: "localhop" };
  if (platform === "linux" && arch === "x64") return { target: "x86_64-unknown-linux-gnu", ext: "tar.gz", bin: "localhop" };
  if (platform === "linux" && arch === "arm64") return { target: "aarch64-unknown-linux-gnu", ext: "tar.gz", bin: "localhop" };
  if (platform === "win32" && arch === "x64") return { target: "x86_64-pc-windows-msvc", ext: "zip", bin: "localhop.exe" };
  throw new Error(`Unsupported platform: ${platform}-${arch}. Install via 'cargo install localhop' instead.`);
}

async function main() {
  if (process.env.LOCALHOP_SKIP_DOWNLOAD === "1") {
    return;
  }

  const { target, ext, bin } = resolveTarget();
  const vendorDir = path.join(__dirname, "vendor");
  const binPath = path.join(vendorDir, bin);

  if (fs.existsSync(binPath)) {
    return;
  }

  const tag = `v${pkg.version}`;
  const asset = `localhop-${tag}-${target}.${ext}`;
  const url = `https://github.com/${REPO}/releases/download/${tag}/${asset}`;

  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) {
    throw new Error(
      `Failed to download ${url} (HTTP ${res.status}). ` +
        `Make sure GitHub Release ${tag} exists, or install via 'cargo install localhop'.`
    );
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "localhop-"));
  const archivePath = path.join(tmpDir, asset);

  try {
    const buf = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(archivePath, buf);
    fs.mkdirSync(vendorDir, { recursive: true });

    if (ext === "tar.gz") {
      execFileSync("tar", ["-xzf", archivePath, "-C", vendorDir], { stdio: "inherit" });
    } else {
      execFileSync(
        "powershell",
        ["-NoProfile", "-Command", `Expand-Archive -Path '${archivePath}' -DestinationPath '${vendorDir}' -Force`],
        { stdio: "inherit" }
      );
    }

    if (process.platform !== "win32") {
      fs.chmodSync(binPath, 0o755);
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error(`[localhop] ${err.message}`);
  process.exit(1);
});
