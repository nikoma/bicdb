#!/usr/bin/env bash
set -euo pipefail

for required in cargo rg sort; do
  command -v "$required" >/dev/null || {
    printf 'required test-discovery command is missing: %s\n' "$required" >&2
    exit 2
  }
done

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <workspace-package>" >&2
  exit 2
fi

package=$1
test_dir="crates/${package}/tests"
target_root=${CARGO_TARGET_DIR:-target}
deps_dir="${target_root}/debug/deps"
test_threads=${RUST_TEST_THREADS:-4}

if [[ ! -d ${test_dir} ]]; then
  echo "integration-test directory does not exist: ${test_dir}" >&2
  exit 2
fi

# BicDB's SQL crate has more than one hundred statically linked integration
# targets. Asking Cargo to build all of them before running the first test
# exhausts a hosted runner even with debug symbols and incremental artifacts
# disabled. Run the exact same unit/integration targets one at a time and
# remove only the executable that just ran; dependency artifacts remain hot.
prune_test_executable() {
  local target_name=$1
  if [[ ! -d ${deps_dir} ]]; then
    return
  fi
  find "${deps_dir}" -maxdepth 1 -type f -name "${target_name}-*" -perm -u+x -delete
}

unit_target=${package//-/_}
cargo test -p "${package}" --lib --locked -- --test-threads="${test_threads}"
prune_test_executable "${unit_target}"

# `cargo test -p` also runs doctests. Keep that coverage explicit while avoiding
# the all-integration-target link fan-out that exhausts hosted-runner storage.
cargo test -p "${package}" --doc --locked -- --test-threads="${test_threads}"

# Capture discovery in the main shell: process substitution would hide a
# failed/missing scanner and report success after silently skipping all tests.
test_paths=$(rg --files "${test_dir}" -g '*.rs' | LC_ALL=C sort)
[[ -n $test_paths ]] || { echo "no integration tests discovered" >&2; exit 2; }
while IFS= read -r test_path; do
  relative=${test_path#"${test_dir}/"}
  if [[ ${relative} == */main.rs ]]; then
    test_target=${relative%/main.rs}
    if [[ ${test_target} == */* ]]; then
      continue
    fi
  elif [[ ${relative} == */* ]]; then
    continue
  else
    test_target=${relative%.rs}
  fi
  cargo test -p "${package}" --test "${test_target}" --locked -- \
    --test-threads="${test_threads}"
  prune_test_executable "${test_target}"
done <<< "$test_paths"
