#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo test -p bicdb-app-runtime \
  compiled_module_cache_is_content_addressed_bounded_and_pooled \
  --locked -- --nocapture
cargo test -p bicdb-app-runtime \
  binary_body_limits_use_raw_bytes_without_json_expansion \
  --locked -- --nocapture
cargo test -p bicdb-app-runtime --release \
  application_zero_hop_latency_and_resource_regression_gate \
  --locked -- --ignored --nocapture

printf '%s\n' 'BicDB embedded application performance gate passed'
