#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: bench/tpcc/build_seed.sh

Builds the immutable TPC-C seed configured through environment variables.
This script accepts no positional arguments.
EOF
}

(( $# <= 1 )) || { usage >&2; exit 2; }
case ${1:-} in
  -h|--help)
    usage
    exit 0
    ;;
  "")
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${BIN:-$ROOT/target/release/bicdb}
SEED=${SEED:-/dev/shm/bicdb-tpcc-seed}
PORT=${PORT:-55433}
WAREHOUSES=${WAREHOUSES:-16}
BUILD_VU=${BUILD_VU:-8}
IMAGE=${IMAGE:-tpcorg/hammerdb:postgres}
WORK=${WORK:-}
KEEP_FAILED_SEED=${KEEP_FAILED_SEED:-0}
SEED_CHECKPOINT_WAL_MB=${SEED_CHECKPOINT_WAL_MB:-1024}
SEED_WAL_DRAIN_MB=${SEED_WAL_DRAIN_MB:-$SEED_CHECKPOINT_WAL_MB}
SEED_WAL_DRAIN_SECS=${SEED_WAL_DRAIN_SECS:-1800}
SERVER_PID=
CONTAINER="bicdb-seed-$PPID-$$"
SEED_OWNED=0
WORK_OWNED=0

if [[ -z $WORK ]]; then
  WORK=$(mktemp -d /tmp/bicdb-tpcc-seed.XXXXXX)
  WORK_OWNED=1
else
  [[ ! -e $WORK ]] || { echo "work directory already exists: $WORK" >&2; exit 1; }
fi

stop_server() {
  if [[ -n ${SERVER_PID:-} ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 120); do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.25
    done
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=
}

cleanup() {
  local status=$?
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  stop_server
  if [[ $WORK_OWNED == 1 ]]; then
    rm -rf "$WORK"
  fi
  if (( status != 0 )) && [[ $KEEP_FAILED_SEED != 1 && $SEED_OWNED == 1 ]]; then
    rm -rf "$SEED"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

[[ -x $BIN ]] || { echo "missing executable: $BIN" >&2; exit 1; }
[[ ! -e $SEED ]] || { echo "seed already exists: $SEED" >&2; exit 1; }
[[ $PORT =~ ^[0-9]+$ ]] && (( PORT >= 1 && PORT <= 65535 )) || {
  echo "PORT must be 1..65535: $PORT" >&2
  exit 2
}
[[ $WAREHOUSES =~ ^[1-9][0-9]*$ ]] || { echo "WAREHOUSES must be positive" >&2; exit 2; }
[[ $BUILD_VU =~ ^[1-9][0-9]*$ ]] || { echo "BUILD_VU must be positive" >&2; exit 2; }
[[ $KEEP_FAILED_SEED == 0 || $KEEP_FAILED_SEED == 1 ]] || {
  echo "KEEP_FAILED_SEED must be 0 or 1" >&2
  exit 2
}
for command in docker jq psql sed sha256sum find sort stat du ss awk grep seq tail tr date; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 1
  }
done
[[ $(uname -s) == Linux ]] || {
  echo "the loopback-only HammerDB setup requires Docker host networking on Linux" >&2
  exit 1
}
docker info >/dev/null
if ss -ltnH | awk '{print $4}' | grep -Eq "(^|:)$PORT$"; then
  echo "port already in use: $PORT" >&2
  exit 1
fi
mkdir -p "$WORK"
# A caller-supplied WORK directory is theirs to keep (its logs are the only
# record of a failed build); only the mktemp one above is removed on exit.
SEED_OWNED=1
mkdir -p "$SEED"

sed \
  -e 's/host\.docker\.internal/127.0.0.1/g' \
  -e "s/@PORT@/$PORT/g" \
  -e "s/@WAREHOUSES@/$WAREHOUSES/g" \
  -e "s/@BUILD_VU@/$BUILD_VU/g" \
  "$ROOT/bench/tpcc/buildschema.tcl.tmpl" > "$WORK/buildschema.tcl"

# The loader checkpoints its own WAL into segments (BICDB_AUTO_COMPACT_WAL_MB)
# while it is still resident, and the build waits for the log to drain below
# SEED_WAL_DRAIN_MB before stopping it. Reopening a seed whose whole load sits
# in transactions.log replays every write into memory first: several times the
# log size resident (a 5.9 GB log took `bicdb compact` past 50 GB on a 62 GB
# host), which is what OOM-killed offline seed builds.
# Flags newer than some reference binaries (July high-water profiles): pass
# them only when the binary under test knows them, as run_trial.sh does.
STORAGE_SYNC_ARGS=()
if "$BIN" serve-pg --help 2>/dev/null | grep -q -- '--storage-sync'; then
  STORAGE_SYNC_ARGS=(--storage-sync buffered)
fi

env -i HOME="$HOME" PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
  BICDB_SYNC_OUTBOX=off BICDB_AUTO_COMPACT_WAL_MB="$SEED_CHECKPOINT_WAL_MB" \
  BICDB_MEM_GUARD_MB=2048 \
  "$BIN" serve-pg "$SEED" --host 127.0.0.1 --port "$PORT" \
  "${STORAGE_SYNC_ARGS[@]}" --max-connections 64 \
  --query-timeout-ms 7200000 --write-timeout-ms 7200000 \
  --overload-timeout-ms 7200000 --idle-timeout-seconds 7200 --max-result-rows 1000000 > "$WORK/server.log" 2>&1 &
SERVER_PID=$!

server_ready=0
for _ in $(seq 1 480); do
  if PGPASSWORD=x psql -XAt -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select 1' >/dev/null 2>&1; then
    server_ready=1
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -100 "$WORK/server.log" >&2
    exit 1
  }
  sleep 0.25
done
if [[ $server_ready != 1 ]]; then
  echo "BicDB did not become ready on port $PORT" >&2
  tail -100 "$WORK/server.log" >&2
  exit 1
fi

docker run --rm --name "$CONTAINER" \
  --network host \
  -v "$WORK:/work" "$IMAGE" \
  ./hammerdbcli auto /work/buildschema.tcl > "$WORK/hammerdb.log" 2>&1 || true

failed=$(grep -c 'FINISHED FAILED' "$WORK/hammerdb.log" || true)
if (( failed != 0 )) || ! grep -q 'ALL VIRTUAL USERS COMPLETE' "$WORK/hammerdb.log"; then
  tail -120 "$WORK/hammerdb.log" >&2
  exit 1
fi

dsum=$(PGPASSWORD=x psql -XAt -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
  -c 'select sum(d_next_o_id) from district' | tr -d '[:space:]')
[[ $dsum =~ ^[0-9]+$ ]] || { echo "invalid district sum: $dsum" >&2; exit 1; }

# Let the loader checkpoint its resident load into segments before we stop it,
# so the offline compact below replays at most SEED_WAL_DRAIN_MB of log.
drained=0
for _ in $(seq 1 "$SEED_WAL_DRAIN_SECS"); do
  wal_now=$(stat -c '%s' "$SEED/transactions.log" 2>/dev/null || printf 0)
  if (( wal_now < SEED_WAL_DRAIN_MB * 1048576 )); then
    drained=1
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || { tail -100 "$WORK/server.log" >&2; exit 1; }
  sleep 1
done
if [[ $drained != 1 ]]; then
  echo "loader WAL did not drain below ${SEED_WAL_DRAIN_MB} MB in ${SEED_WAL_DRAIN_SECS}s" >&2
  tail -100 "$WORK/server.log" >&2
  exit 1
fi
stop_server

# A seed containing the bulk load only in transactions.log causes every measured
# lane to begin with a full-dataset checkpoint. Materialize it once here instead.
env -i HOME="$HOME" PATH="$PATH" LANG="${LANG:-C.UTF-8}" BICDB_SYNC_OUTBOX=off \
  "$BIN" compact "$SEED" --force > "$WORK/compact.log" 2>&1

# Verify the compacted seed can be reopened and still has the expected business state.
env -i HOME="$HOME" PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
  BICDB_SYNC_OUTBOX=off BICDB_AUTO_COMPACT_WAL_MB=0 BICDB_MEM_GUARD_MB=2048 \
  "$BIN" serve-pg "$SEED" --host 127.0.0.1 --port "$PORT" \
  "${STORAGE_SYNC_ARGS[@]}" --max-connections 64 \
  --query-timeout-ms 7200000 --idle-timeout-seconds 7200 --max-result-rows 1000000 > "$WORK/verify-server.log" 2>&1 &
SERVER_PID=$!
verified_dsum=
for _ in $(seq 1 1200); do
  verified_dsum=$(PGPASSWORD=x psql -XAt -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select sum(d_next_o_id) from district' 2>/dev/null | tr -d '[:space:]' || true)
  [[ $verified_dsum =~ ^[0-9]+$ ]] && break
  kill -0 "$SERVER_PID" 2>/dev/null || { tail -100 "$WORK/verify-server.log" >&2; exit 1; }
  sleep 0.1
done
[[ $verified_dsum == "$dsum" ]] || {
  echo "compacted seed district sum changed: $dsum -> $verified_dsum" >&2
  exit 1
}
stop_server

wal_bytes=$(stat -c '%s' "$SEED/transactions.log" 2>/dev/null || printf 0)
if (( wal_bytes > 1048576 )); then
  echo "compacted seed WAL is unexpectedly large: $wal_bytes bytes" >&2
  exit 1
fi

manifest_hash=$(
  cd "$SEED"
  find . -type f -printf '%P\0' | sort -z | \
    while IFS= read -r -d '' file; do sha256sum "$file"; done | sha256sum | cut -d' ' -f1
)
seed_bytes=$(du -sb "$SEED" | cut -f1)
binary_hash=$(sha256sum "$BIN" | cut -d' ' -f1)

jq -n \
  --arg created_at "$(date --iso-8601=seconds)" \
  --arg binary "$BIN" \
  --arg binary_sha256 "$binary_hash" \
  --arg content_sha256 "$manifest_hash" \
  --argjson warehouses "$WAREHOUSES" \
  --argjson build_vu "$BUILD_VU" \
  --argjson district_sum "$dsum" \
  --argjson bytes "$seed_bytes" \
  --argjson wal_bytes "$wal_bytes" \
  '{schema_version:1, created_at:$created_at, binary:$binary,
    binary_sha256:$binary_sha256, content_sha256:$content_sha256,
    warehouses:$warehouses, build_vu:$build_vu,
    district_sum:$district_sum, bytes:$bytes, wal_bytes:$wal_bytes,
    materialized:true}' > "$SEED/seed-manifest.json"

printf 'seed ready: %s (%s bytes, sha256 %s)\n' "$SEED" "$seed_bytes" "$manifest_hash"
