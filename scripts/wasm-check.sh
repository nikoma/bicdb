#!/usr/bin/env bash
# Guard for the wasm32-wasip1 port (browser/OPFS cache builds).
#
# 1. Type-checks bicdb-core/sql/sync for wasm32-wasip1 with native deps off.
# 2. If a `wasmtime` binary is available (PATH or $WASMTIME), builds and runs
#    the wasi_smoke example end-to-end (open/insert/query/reopen/recover).
#
# Usage: scripts/wasm-check.sh
set -euo pipefail
cd "$(dirname "$0")/.."

rustup target add wasm32-wasip1 >/dev/null 2>&1 || true

echo "== cargo check (wasm32-wasip1, --no-default-features) =="
cargo check -p bicdb-core -p bicdb-sql -p bicdb-sync -p bicdb-wasm \
  --target wasm32-wasip1 --no-default-features

WASMTIME="${WASMTIME:-$(command -v wasmtime || true)}"
if [[ -z "$WASMTIME" ]]; then
  echo "wasmtime not found; skipping smoke run (check-only pass)."
  exit 0
fi

echo "== wasi_smoke under $WASMTIME =="
cargo build --release -p bicdb-sql --example wasi_smoke \
  --target wasm32-wasip1 --no-default-features
SMOKE_DIR="$(mktemp -d)"
trap 'rm -rf "$SMOKE_DIR"' EXIT
"$WASMTIME" run --dir "$SMOKE_DIR"::/data \
  target/wasm32-wasip1/release/examples/wasi_smoke.wasm
