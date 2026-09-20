#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
fixture_dir="$repo_root/fixtures/postgresql-18/pg-dump"
report_dir="${PG18_DUMP_RESTORE_REPORT_DIR:-$repo_root/target/postgresql-18-dump-restore}"
bicdb_bin="${BICDB_BIN:-$repo_root/target/debug/bicdb}"
pg_image="${PG18_IMAGE:-postgres:18.4}"
bicdb_port="${BICDB_PORT:-55440}"
stock_container="bicdb-pg18-dump-restore-$$"
bicdb_pid=""

stop_bicdb() {
  if [ -n "$bicdb_pid" ] && kill -0 "$bicdb_pid" 2>/dev/null; then
    kill -INT "$bicdb_pid"
    wait "$bicdb_pid"
  fi
  bicdb_pid=""
}

cleanup() {
  stop_bicdb
  docker rm -f "$stock_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if [ ! -x "$bicdb_bin" ]; then
  printf 'BicDB binary is missing or not executable: %s\n' "$bicdb_bin" >&2
  printf '%s\n' 'Build it once or set BICDB_BIN to an existing binary.' >&2
  exit 1
fi
command -v docker >/dev/null

mkdir -p "$report_dir"
rm -rf \
  "$report_dir/source" \
  "$report_dir/restored-full" \
  "$report_dir/restored-split" \
  "$report_dir/restored-postgres-redump"
rm -f "$report_dir"/*.dump "$report_dir"/*.out "$report_dir"/*.log

pg18_client() {
  docker run --rm -i --network host "$pg_image" "$@"
}

pg18_client_with_reports() {
  docker run --rm -i --network host -v "$report_dir:/reports" "$pg_image" "$@"
}

wait_for_bicdb() {
  local attempts=0
  until pg18_client pg_isready -h 127.0.0.1 -p "$bicdb_port" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ] || ! kill -0 "$bicdb_pid" 2>/dev/null; then
      printf 'BicDB did not become ready; log follows:\n' >&2
      cat "$report_dir/bicdb.log" >&2
      exit 1
    fi
    sleep 0.25
  done
}

start_bicdb() {
  local database_path="$1"
  stop_bicdb
  "$bicdb_bin" init "$database_path" >/dev/null
  "$bicdb_bin" serve "$database_path" \
    --host 127.0.0.1 \
    --port "$bicdb_port" \
    --max-active-queries 20 \
    --max-active-reads 20 \
    --max-active-writes 20 \
    >"$report_dir/bicdb.log" 2>&1 &
  bicdb_pid=$!
  wait_for_bicdb
}

bicdb_psql() {
  pg18_client psql \
    -h 127.0.0.1 \
    -p "$bicdb_port" \
    -U bicdb \
    -d bicdb \
    -At \
    -v ON_ERROR_STOP=1 \
    "$@"
}

bicdb_restore() {
  local archive="$1"
  pg18_client_with_reports pg_restore \
    -h 127.0.0.1 \
    -p "$bicdb_port" \
    -U bicdb \
    -d bicdb \
    --exit-on-error \
    "/reports/$archive"
}

assert_canonical() {
  local label="$1"
  local actual="$2"
  if ! diff -u "$report_dir/expected.out" "$actual"; then
    printf '%s canonical result differs from the source fixture\n' "$label" >&2
    exit 1
  fi
}

start_bicdb "$report_dir/source"
bicdb_psql < "$fixture_dir/type-families.sql" >/dev/null
bicdb_psql < "$fixture_dir/canonical-query.sql" > "$report_dir/expected.out"
if [ ! -s "$report_dir/expected.out" ]; then
  printf '%s\n' 'Canonical source result is empty' >&2
  exit 1
fi

for mode in full schema data; do
  option=""
  case "$mode" in
    schema) option="--schema-only" ;;
    data) option="--data-only" ;;
  esac
  pg18_client_with_reports pg_dump \
    -h 127.0.0.1 \
    -p "$bicdb_port" \
    -U bicdb \
    -d bicdb \
    -Fc \
    ${option:+$option} \
    -f "/reports/$mode.dump"
done
stop_bicdb

start_bicdb "$report_dir/restored-full"
bicdb_restore full.dump
bicdb_psql < "$fixture_dir/canonical-query.sql" > "$report_dir/restored-full.out"
assert_canonical 'BicDB full restore' "$report_dir/restored-full.out"
stop_bicdb

start_bicdb "$report_dir/restored-split"
bicdb_restore schema.dump
bicdb_restore data.dump
bicdb_psql < "$fixture_dir/canonical-query.sql" > "$report_dir/restored-split.out"
assert_canonical 'BicDB schema/data restore' "$report_dir/restored-split.out"
stop_bicdb

docker run -d \
  --name "$stock_container" \
  -e POSTGRES_PASSWORD=postgres \
  -v "$report_dir:/reports" \
  "$pg_image" >/dev/null
attempts=0
until docker exec "$stock_container" pg_isready -U postgres >/dev/null 2>&1; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 60 ]; then
    docker logs "$stock_container" >&2
    exit 1
  fi
  sleep 0.25
done
docker exec "$stock_container" psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE ROLE bicdb LOGIN' \
  -c 'CREATE DATABASE bicdb OWNER bicdb' >/dev/null
docker exec "$stock_container" pg_restore \
  -U postgres \
  -d bicdb \
  --exit-on-error \
  /reports/full.dump
docker exec -i "$stock_container" psql \
  -U postgres \
  -d bicdb \
  -At \
  -v ON_ERROR_STOP=1 \
  < "$fixture_dir/canonical-query.sql" \
  > "$report_dir/postgresql-restored.out"
assert_canonical 'PostgreSQL 18 restore' "$report_dir/postgresql-restored.out"
docker exec "$stock_container" pg_dump \
  -U postgres \
  -d bicdb \
  -Fc \
  -f /reports/postgresql-redump.dump
docker rm -f "$stock_container" >/dev/null

start_bicdb "$report_dir/restored-postgres-redump"
bicdb_restore postgresql-redump.dump
bicdb_psql < "$fixture_dir/canonical-query.sql" \
  > "$report_dir/restored-postgres-redump.out"
assert_canonical \
  'PostgreSQL 18 re-dump restored into BicDB' \
  "$report_dir/restored-postgres-redump.out"
stop_bicdb

printf 'PostgreSQL 18 pg_dump/pg_restore parity passed. Reports: %s\n' "$report_dir"
