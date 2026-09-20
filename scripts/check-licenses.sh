#!/usr/bin/env bash
#
# Dependency licence and supply-chain policy gate.
#
# Required by Phase 0 of docs/server-paged-storage-todo.md: the policy has to be
# enforced automatically on every change, so that a technically attractive
# storage or vector crate is rejected on licence grounds while it is still an
# experiment rather than after it is load-bearing.
#
# The policy itself lives in deny.toml. This script only runs it.
#
# Usage:  scripts/check-licenses.sh
#
# Note: an earlier revision carried a `cargo metadata`-based fallback for
# machines without cargo-deny. It was removed deliberately. Reproducing cargo's
# feature and target resolution by hand is subtly wrong — the fallback flagged
# `terminfo` (WTFPL), which reaches the graph only through ratatui's optional
# `termwiz` backend and is never actually built. A licence gate that disagrees
# with the real one is worse than no second opinion: it either cries wolf or
# quietly passes something the real gate would catch.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! cargo deny --version >/dev/null 2>&1; then
    cat >&2 <<'EOF'
error: cargo-deny is not installed.

The dependency policy gate cannot run without it, and skipping the gate is not
an acceptable outcome — that is the failure mode the policy exists to prevent.

Install it with:

    cargo install cargo-deny --locked

In CI, prefer the pinned action:

    - uses: EmbarkStudios/cargo-deny-action@v2
EOF
    exit 1
fi

echo "==> cargo-deny: licences, banned engines, and source registries (deny.toml)"
exec cargo deny check licenses bans sources
