#!/usr/bin/env bash
# Validate the Apache-2.0 license invariants, metadata and packaged legal copies.
set -euo pipefail
cd "$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
python3 scripts/validate-license-transition.py
python3 scripts/sync-package-licenses.py --check
printf 'validate-licensing: ALL CHECKS PASSED\n'
