#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${BICDB_NIGHTMARE_MODE:-quick}"
report_dir="${BICDB_NIGHTMARE_REPORT_DIR:-$repo_root/reports/pg18-nightmare}"
fixtures_dir="${BICDB_NIGHTMARE_FIXTURES:-$repo_root/fixtures/pg18-nightmare}"
generated_dir=""

cleanup() {
  if [[ -n "$generated_dir" ]]; then
    rm -rf "$generated_dir"
  fi
}
trap cleanup EXIT

"$repo_root/scripts/pg18-up.sh"

if [[ "${BICDB_NIGHTMARE_FUZZ_SEEDS:-0}" != "0" ]]; then
  generated_dir="$(mktemp -d "${TMPDIR:-/tmp}/bicdb-pg18-nightmare-fixtures.XXXXXX")"
  cp "$fixtures_dir"/*.json "$generated_dir"/
  python3 - "$generated_dir" "${BICDB_NIGHTMARE_FUZZ_SEEDS}" <<'PY'
import json
import random
import sys
from pathlib import Path

out = Path(sys.argv[1])
seeds = int(sys.argv[2])
types = ["INT", "TEXT", "BOOLEAN"]
for seed in range(2, seeds + 2):
    rng = random.Random(seed)
    table = f"nm_fuzz_{seed:04d}"
    rows = []
    for idx in range(5):
        k = rng.randint(-10, 10)
        v = None if rng.random() < 0.25 else f"v{rng.randint(0, 99)}"
        flag = rng.choice(["true", "false", "NULL"])
        rows.append(
            "('{id}', {k}, {v}, {flag})".format(
                id=f"r{idx}",
                k=k,
                v="NULL" if v is None else repr(v),
                flag=flag,
            )
        )
    fixture = {
        "id": f"nightmare_generated_fuzz_seed_{seed:04d}",
        "description": "Generated deterministic fuzz/property seed; failures shrink to the SQL prefix written by the runner",
        "cleanup_sql": [f"DROP TABLE IF EXISTS {table};"],
        "sql": [
            f"CREATE TABLE {table} (id TEXT PRIMARY KEY, k INT, v TEXT, flag BOOLEAN);",
            f"INSERT INTO {table} (id, k, v, flag) VALUES {', '.join(rows)};",
            f"SELECT id, COALESCE(v, 'nil'), k FROM {table} WHERE flag IS DISTINCT FROM false ORDER BY k, id LIMIT 4 OFFSET 0;",
            f"UPDATE {table} SET k = k + 1 WHERE v IS NULL OR k < 0;",
            f"DELETE FROM {table} WHERE k > 8;",
            f"SELECT flag, COUNT(*), SUM(k) FROM {table} GROUP BY flag ORDER BY flag NULLS FIRST;",
        ],
    }
    (out / f"1{seed:03d}-generated-fuzz-seed-{seed:04d}.json").write_text(
        json.dumps(fixture, indent=2) + "\n"
    )
PY
  fixtures_dir="$generated_dir"
fi

mkdir -p "$report_dir"

cargo run -q -p bicdb-cli --features bench -- compat nightmare \
  "$report_dir/db" \
  --fixtures "$fixtures_dir" \
  --json-out "$report_dir/report.json" \
  --markdown-out "$report_dir/report.md" \
  --repro-dir "$report_dir/repros"

if [[ "$mode" == "full" ]]; then
  "$repo_root/scripts/client-gauntlet.sh"
  cargo test -p bicdb-pgwire --test protocol -- --nocapture
fi

printf 'pg18-nightmare reports written to %s\n' "$report_dir"
