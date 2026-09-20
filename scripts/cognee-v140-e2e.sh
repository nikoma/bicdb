#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cognee_dir="${COGNEE_DIR:-}"
expected_cognee_commit="dd8a1659d6a61d7f8b6de2d2a59b94c9b1f90337"
port="${BICDB_COGNEE_PORT:-55439}"
embedding_port="${BICDB_COGNEE_EMBEDDING_PORT:-55440}"
database_dir="${BICDB_COGNEE_DATABASE_DIR:-$(mktemp -d /tmp/bicdb-cognee-v140.XXXXXX)}"
bicdb_bin="${BICDB_BIN:-$repo_root/target/debug/bicdb}"
server_pid=""
embedding_pid=""

if [[ -z "$cognee_dir" || ! -d "$cognee_dir/.git" ]]; then
  printf '%s\n' 'Set COGNEE_DIR to an unchanged Cognee v1.4.0 checkout.' >&2
  exit 1
fi
if [[ "$(git -C "$cognee_dir" rev-parse HEAD)" != "$expected_cognee_commit" ]]; then
  printf '%s\n' "Cognee checkout must be v1.4.0 commit $expected_cognee_commit" >&2
  exit 1
fi
if [[ -n "$(git -C "$cognee_dir" status --porcelain)" ]]; then
  printf '%s\n' 'Cognee checkout must be unchanged.' >&2
  exit 1
fi
build_jobs="${CARGO_BUILD_JOBS:-10}"
if [[ ! "$build_jobs" =~ ^[0-9]+$ ]] || (( build_jobs < 1 || build_jobs > 10 )); then
  printf '%s\n' 'CARGO_BUILD_JOBS must be an integer from 1 through 10.' >&2
  exit 1
fi

cleanup() {
  [[ -z "$server_pid" ]] || kill "$server_pid" >/dev/null 2>&1 || true
  [[ -z "$embedding_pid" ]] || kill "$embedding_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_port() {
  local target_port="$1"
  for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$target_port") 2>/dev/null; then
      exec 3>&-
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_bicdb() {
  "$bicdb_bin" serve "$database_dir" --cluster --default-database cognee_db \
    --host 127.0.0.1 --port "$port" >"$database_dir/server.log" 2>&1 &
  server_pid=$!
  wait_for_port "$port"
}

export CARGO_BUILD_JOBS="$build_jobs"
if [[ ! -x "$bicdb_bin" ]]; then
  (cd "$repo_root" && cargo build --locked -p bicdb-cli)
fi

python3 "$repo_root/scripts/cognee-v140-embedding-fixture.py" \
  --port "$embedding_port" >"$database_dir/embedding.log" 2>&1 &
embedding_pid=$!
wait_for_port "$embedding_port"
start_bicdb

export DB_PROVIDER=postgres DB_HOST=127.0.0.1 DB_PORT="$port" DB_NAME=cognee_db
export DB_USERNAME=cognee DB_PASSWORD=cognee
export VECTOR_DB_PROVIDER=pgvector VECTOR_DB_HOST=127.0.0.1 VECTOR_DB_PORT="$port"
export VECTOR_DB_NAME=cognee_db VECTOR_DB_USERNAME=cognee VECTOR_DB_PASSWORD=cognee
export GRAPH_DATABASE_PROVIDER=postgres GRAPH_DATABASE_HOST=127.0.0.1
export GRAPH_DATABASE_PORT="$port" GRAPH_DATABASE_NAME=cognee_db
export GRAPH_DATABASE_USERNAME=cognee GRAPH_DATABASE_PASSWORD=cognee
export CACHE_BACKEND=postgres ENABLE_BACKEND_ACCESS_CONTROL=false
export REQUIRE_AUTHENTICATION=false TELEMETRY_DISABLED=true
export TEST_PGVECTOR_URL="postgresql+asyncpg://cognee:cognee@127.0.0.1:$port/cognee_db"
export EMBEDDING_PROVIDER=openai_compatible EMBEDDING_MODEL=local-deterministic
export EMBEDDING_DIMENSIONS=3 EMBEDDING_ENDPOINT="http://127.0.0.1:$embedding_port/v1"
export EMBEDDING_API_KEY=test-key

python="$cognee_dir/.venv/bin/python"
pytest="$cognee_dir/.venv/bin/pytest"
if [[ ! -x "$python" || ! -x "$pytest" ]]; then
  printf '%s\n' 'Cognee checkout must have its v1.4.0 test dependencies installed in .venv.' >&2
  exit 1
fi

cd "$cognee_dir"
"$python" -c 'import asyncio; from cognee.run_migrations import run_migrations; asyncio.run(run_migrations())'
"$python" -c 'import asyncio; from cognee.run_migrations import run_migrations; asyncio.run(run_migrations())'
"$pytest" -q cognee/tests/e2e/postgres/test_postgres_adapter.py
"$pytest" -q cognee/tests/integration/infrastructure/graph/test_graph_provenance_adapter_contract.py

belongs_tests=(
  test_create_data_points_merges_belongs_to_set
  test_create_data_points_tolerates_exact_duplicates_in_batch
  test_create_data_points_merges_tags_across_in_batch_duplicates
  test_remove_belongs_to_set_tags_strips_and_deletes
  test_remove_belongs_to_set_tags_scoped_by_node_ids
  test_remove_belongs_to_set_tags_ignores_non_vector_tables
)
for test_name in "${belongs_tests[@]}"; do
  "$pytest" -q "cognee/tests/integration/test_belongs_to_set_pgvector.py::$test_name"
done
"$pytest" -q \
  cognee/tests/e2e/postgres/test_postgres_hybrid_adapter.py::test_combined_write_content_integrity
"$python" "$repo_root/scripts/cognee-v140-contract.py" live

kill "$server_pid"
wait "$server_pid"
server_pid=""
start_bicdb
"$python" "$repo_root/scripts/cognee-v140-contract.py" restart

printf '%s\n' 'Cognee v1.4.0 unchanged compatibility gate passed.'
