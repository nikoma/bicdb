#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

build_jobs="${CARGO_BUILD_JOBS:-10}"
if [[ ! "$build_jobs" =~ ^[0-9]+$ ]] || (( build_jobs < 1 || build_jobs > 10 )); then
  printf '%s\n' 'CARGO_BUILD_JOBS must be an integer from 1 through 10' >&2
  exit 1
fi

export CARGO_BUILD_JOBS="$build_jobs"
export PG18_IMAGE="${COGNEE_PG18_IMAGE:-pgvector/pgvector:pg18}"
export PG18_BIND_HOST="${COGNEE_PG18_HOST:-127.0.0.1}"
export PG18_HOST_PORT="${COGNEE_PG18_PORT:-55434}"
export PG18_DB="${COGNEE_PG18_DB:-compat}"
export PG18_USER="${COGNEE_PG18_USER:-postgres}"
export PG18_PASSWORD="${COGNEE_PG18_PASSWORD:-postgres}"
export PG18_CONTAINER_NAME="${COGNEE_PG18_CONTAINER:-bicdb-cognee-pg18}"
export PG18_COMPOSE_PROJECT="${COGNEE_PG18_PROJECT:-bicdb-cognee-pg18}"
export COGNEE_PG18_URL="postgres://${PG18_USER}:${PG18_PASSWORD}@${PG18_BIND_HOST}:${PG18_HOST_PORT}/${PG18_DB}"

cleanup() {
  "$repo_root/scripts/pg18-down.sh" --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
"$repo_root/scripts/pg18-up.sh"

cd "$repo_root"
cargo test --locked -p bicdb-pgwire --test cognee_pg18_diff -- --ignored --nocapture
