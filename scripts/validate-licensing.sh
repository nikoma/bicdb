#!/usr/bin/env bash
# Validate operative community terms, separate grants and packaged notices.
set -euo pipefail
cd "$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
python3 scripts/validate-license-transition.py
python3 scripts/sync-package-licenses.py --check
printf 'validate-licensing: ALL CHECKS PASSED\n'
