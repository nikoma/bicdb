#!/usr/bin/env bash
set -uo pipefail

HARNESS=${HARNESS:-/root/bicdb-bench}
WORK=${WORK:-$HARNESS/work}
BIN=${BIN:-/root/bicdb-head/target/release/bicdb}
PORT=${PORT:-55433}
IMAGE=${IMAGE:-tpcorg/hammerdb:postgres}
VU=${VU:-16}
DURATION=${DURATION:-1}
RAMPUP=${RAMPUP:-1}
SEED=${SEED:-/dev/shm/bicdb-seed-snap}
TAG=${TAG:-trial-$(date +%H%M%S)}
DD=${DD:-/dev/shm/bicdb-$TAG}
MAX_ACTIVE_QUERIES=${MAX_ACTIVE_QUERIES:-32}
MAX_ACTIVE_READS=${MAX_ACTIVE_READS:-32}
MAX_ACTIVE_WRITES=${MAX_ACTIVE_WRITES:-16}
MAX_CONNECTIONS=${MAX_CONNECTIONS:-64}
MEM_GUARD_MB=${MEM_GUARD_MB:-4096}
MEM_TRACE_SECS=${MEM_TRACE_SECS:-10}
RUN_TCL="$WORK/run-${TAG}.tcl"
LOG="$HARNESS/${TAG}.log"
SLOG="$HARNESS/${TAG}-srv.log"
VLOG="$HARNESS/${TAG}-vu.log"

ts() { date +%H:%M:%S; }
shm_free() { df -h /dev/shm | awk 'NR==2{print $4}'; }
sumq() {
  PGPASSWORD=x psql -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb -t -A \
    -c 'select sum(d_next_o_id) from district' 2>&1 | tr -d '[:space:]'
}
server_statsq() {
  PGPASSWORD=x psql -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb -t -A -F '|' \
    -c "select * from bicdb_server_stats" 2>&1
}
wait_sumq() {
  local value
  for _ in $(seq 1 240); do
    value=$(sumq)
    if [[ "$value" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$value"
      return 0
    fi
    sleep 0.5
  done
  printf '%s\n' "$value"
  return 1
}
hdb() {
  docker run --rm --name "hdb-${TAG}-$RANDOM" --add-host=host.docker.internal:host-gateway \
    -v "$WORK":/work "$IMAGE" ./hammerdbcli auto "$1"
}
stop_server() {
  if [ -n "${SRV:-}" ]; then
    kill "$SRV" 2>/dev/null || true
    for _ in $(seq 1 60); do kill -0 "$SRV" 2>/dev/null || break; sleep 0.5; done
  fi
  SRV=""
  pkill -9 -x bicdb 2>/dev/null || true
}
clean_tmpfs() {
  stop_server
  for c in $(docker ps -q --filter "name=hdb-"); do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for entry in /dev/shm/bicdb-*; do
    [ -e "$entry" ] || continue
    [ "$entry" = "$SEED" ] && continue
    rm -rf "$entry"
  done
}
serve_dir() {
  local dir=$1 log=$2
  BICDB_MEM_GUARD_MB="$MEM_GUARD_MB" BICDB_MEM_TRACE=1 BICDB_MEM_TRACE_SECS="$MEM_TRACE_SECS" \
    "$BIN" serve-pg "$dir" \
      --host 0.0.0.0 --port "$PORT" --allow-remote-no-auth \
      --max-connections "$MAX_CONNECTIONS" \
      --max-active-queries "$MAX_ACTIVE_QUERIES" \
      --max-active-reads "$MAX_ACTIVE_READS" \
      --max-active-writes "$MAX_ACTIVE_WRITES" \
      --max-result-rows 1000000 > "$log" 2>&1 &
  SRV=$!
  for _ in $(seq 1 240); do
    ss -ltn 2>/dev/null | grep -q ":$PORT" && return 0
    sleep 0.5
  done
  return 1
}

exec > "$LOG" 2>&1
echo "TAG=$TAG"
echo "BIN=$BIN"
echo "COMMIT=${COMMIT:-unknown}"
echo "CONFIG vu=$VU rampup=$RAMPUP duration=$DURATION maq=$MAX_ACTIVE_QUERIES mar=$MAX_ACTIVE_READS maw=$MAX_ACTIVE_WRITES mem_guard=$MEM_GUARD_MB"
echo "ENV index_store=${BICDB_INDEX_STORE:-unset} index_shards=${BICDB_INDEX_SHARDS:-unset} orch_shards=${BICDB_ORCH_SHARDS:-unset} global_lock=${BICDB_GLOBAL_COMMIT_LOCK:-0}"

if [ ! -d "$SEED" ]; then
  echo "MISSING_SEED $SEED"
  exit 1
fi

clean_tmpfs
echo "SHM_AFTER_CLEAN used=$(df -h /dev/shm | awk 'NR==2{print $3}') free=$(shm_free)"
cp -a "$SEED" "$DD"
echo "START $(ts) seed=$(du -sh "$SEED" | cut -f1) dd=$(du -sh "$DD" | cut -f1) shm_free=$(shm_free)"
sed -e "s/@VU@/$VU/g" -e "s/pg_rampup 1/pg_rampup $RAMPUP/" -e "s/pg_duration 1/pg_duration $DURATION/" "$WORK/run-vu.tcl.tmpl" > "$RUN_TCL"
serve_dir "$DD" "$SLOG" || { echo "SERVER_START_FAILED"; exit 1; }
before=$(wait_sumq)
if ! [[ "$before" =~ ^[0-9]+$ ]]; then
  echo "SERVER_READY_FAILED last_sumq=$before"
  stop_server
  rm -rf "$DD" "$RUN_TCL"
  exit 1
fi
echo "READY $(ts) srv=$SRV SUM_BEFORE=$before"
hdb "/work/$(basename "$RUN_TCL")" > "$VLOG" 2>&1
after=$(wait_sumq)
if ! [[ "$after" =~ ^[0-9]+$ ]]; then
  echo "SUM_AFTER_FAILED last_sumq=$after"
  echo "RESULT_LINES:"
  grep -E 'TEST RESULT|FINISHED (SUCCESS|FAILED)' "$VLOG" || true
  stop_server
  rm -rf "$DD" "$RUN_TCL"
  exit 1
fi
minutes=$((RAMPUP + DURATION))
delta=$((after - before))
nopm=$((delta / minutes))
echo "SUM_AFTER=$after"
echo "DSUM_DELTA=$delta DSUM_NOPM=$nopm DSUM_MINUTES=$minutes"
echo "RESULT_LINES:"
grep -E 'TEST RESULT|FINISHED (SUCCESS|FAILED)' "$VLOG" || true
echo "FAILED_COUNT=$(grep -c 'FINISHED FAILED' "$VLOG" 2>/dev/null || true)"
echo "MEMTRACE_TAIL:"
grep MEMTRACE "$SLOG" | tail -12 | sed -E 's/^.*(rss_mb=.*)$/\1/' || true
echo "SERVER_MIX_TAIL:"
grep 'proc_mix' "$SLOG" | tail -4 || true
echo "PGWIRE_MSG_TAIL:"
grep 'pgwire messages' "$SLOG" | tail -4 || true
echo "SERVER_STATS:"
server_statsq || true
echo "SERVER_ERRORS:"
grep -inE 'panic|No space|serialization_failure|deadlock_detected|query.failed' "$SLOG" | tail -20 || true
stop_server
rm -rf "$DD" "$RUN_TCL"
echo "END $(ts) shm_used=$(df -h /dev/shm | awk 'NR==2{print $3}') shm_free=$(shm_free)"
