#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
report_dir="${PG18_TYPE_DIFF_REPORT_DIR:-$repo_root/target/postgresql-18-type-diff}"
database_path="${PG18_TYPE_DIFF_DATABASE:-$report_dir/bicdb}"
fixtures_dir="$repo_root/fixtures/postgresql-18/type-diff"
container_name="${PG18_CONTAINER_NAME:-bicdb-pg18-compat}"
postgres_user="${PG18_USER:-postgres}"
postgres_database="${PG18_DB:-compat}"

if [ "${CARGO_BUILD_JOBS:-10}" -gt 10 ]; then
  printf '%s\n' 'CARGO_BUILD_JOBS must be 10 or less' >&2
  exit 1
fi
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-10}"

"$repo_root/scripts/pg18-up.sh"
mkdir -p "$report_dir"
rm -rf "$database_path"
rm -rf "$report_dir/shards"
mkdir -p "$report_dir/shards"

fixture_list="$report_dir/fixtures.list"
find "$fixtures_dir" -maxdepth 1 -type f -name '*.json' | LC_ALL=C sort >"$fixture_list"
fixture_count="$(wc -l <"$fixture_list" | tr -d ' ')"
if [ "$fixture_count" -eq 0 ]; then
  printf '%s\n' "no PostgreSQL 18 type differential fixtures found" >&2
  exit 1
fi

# The in-process differential server has a bounded lifetime. Restart it and
# the comparison database between known-clean fixture groups so catalog-heavy
# fixtures cannot inherit resource or user-type state from earlier groups.
start=1
shard=1
for size in 40 38 999999; do
  if [ "$start" -gt "$fixture_count" ]; then
    break
  fi
  end=$((start + size - 1))
  if [ "$end" -gt "$fixture_count" ]; then
    end="$fixture_count"
  fi

  shard_dir="$report_dir/shards/$shard"
  shard_fixtures="$shard_dir/fixtures"
  rm -rf "$database_path-$shard"
  mkdir -p "$shard_fixtures"
  sed -n "${start},${end}p" "$fixture_list" | while IFS= read -r fixture; do
    cp "$fixture" "$shard_fixtures/"
  done

  docker exec "$container_name" dropdb \
    --username "$postgres_user" --if-exists --force "$postgres_database"
  docker exec "$container_name" createdb \
    --username "$postgres_user" "$postgres_database"

  cargo run --locked -q -p bicdb-cli --features bench -- compat diff \
    "$database_path-$shard" \
    --target-version 18.4 \
    --fixtures "$shard_fixtures" \
    --json-out "$shard_dir/report.json" \
    --markdown-out "$shard_dir/report.md"

  start=$((end + 1))
  shard=$((shard + 1))
done

node "$repo_root/scripts/aggregate-pg18-type-diff.mjs" \
  "$report_dir/report.json" \
  "$report_dir/report.md" \
  "$fixtures_dir" \
  "$database_path" \
  "$report_dir"/shards/*/report.json

rm -f "$fixture_list"

printf 'PostgreSQL 18 type differential report: %s\n' "$report_dir/report.md"
