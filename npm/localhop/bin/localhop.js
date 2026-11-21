#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");

const binName = process.platform === "win32" ? "localhop.exe" : "localhop";
const binPath = path.join(__dirname, "..", "vendor", binName);

if (!fs.existsSync(binPath)) {
  console.error(
    "[localhop] Native binary not found. Re-run `npm rebuild localhop` or install via `cargo install localhop`."
  );
  process.exit(1);
}

const child = spawn(binPath, process.argv.slice(2), {
  stdio: "inherit",
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    if (!child.killed) child.kill(sig);
  });
}

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
  } else {
    process.exit(code ?? 0);
  }
});
