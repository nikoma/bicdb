#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: bench/tpcc/run_trial.sh [--print-runtime-names]

Runs one environment-configured TPC-C trial. Required variables:
  CAMPAIGN_ID CAMPAIGN_DIR TRIAL_ID LANE

--print-runtime-names validates the IDs, prints the derived data-directory and
container names, and exits without starting Docker or BicDB.
EOF
}

PRINT_RUNTIME_NAMES=0
(( $# <= 1 )) || { usage >&2; exit 2; }
case ${1:-} in
  -h|--help)
    usage
    exit 0
    ;;
  --print-runtime-names)
    PRINT_RUNTIME_NAMES=1
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
CAMPAIGN_ID=${CAMPAIGN_ID:?set CAMPAIGN_ID}
CAMPAIGN_DIR=${CAMPAIGN_DIR:?set CAMPAIGN_DIR}
TRIAL_ID=${TRIAL_ID:?set TRIAL_ID}
LANE=${LANE:?set LANE}
REP=${REP:-0}
POSITION=${POSITION:-0}
WARMUP=${WARMUP:-0}
VU=${VU:-32}
RAMPUP=${RAMPUP:-1}
DURATION=${DURATION:-2}
PORT=${PORT:-55433}
IMAGE=${IMAGE:-tpcorg/hammerdb:postgres}
DATA_ROOT=${DATA_ROOT:-/dev/shm}
SAMPLE_SECS=${SAMPLE_SECS:-1}
TIMEPROFILE=${TIMEPROFILE:-false}
PERF_STAT=${PERF_STAT:-1}
ALLOCATION_TRACE=${ALLOCATION_TRACE:-0}
GRACEFUL_REOPEN_CHECK=${GRACEFUL_REOPEN_CHECK:-${RECOVERY_CHECK:-1}}
KEEP_DATA=${KEEP_DATA:-0}
MEM_GUARD_MB=${MEM_GUARD_MB:-4096}
PROFILE_KIND=${PROFILE_KIND:-throughput}
BUSINESS_COUNTERS=${BUSINESS_COUNTERS:-0}
# Strict qualification is automatic when business counters are requested.
# Explicitly disable only for labelled diagnostics/legacy comparisons.
REQUIRE_BUSINESS_COMPLETION=${REQUIRE_BUSINESS_COMPLETION:-$BUSINESS_COUNTERS}
if [[ $REQUIRE_BUSINESS_COMPLETION == 1 && $BUSINESS_COUNTERS != 1 ]]; then
  echo "REQUIRE_BUSINESS_COMPLETION=1 requires BUSINESS_COUNTERS=1" >&2
  exit 2
fi
# The former two-attempt override abandoned otherwise valid contended work
# when procedure handlers swallowed the conflict. Keep it explicit for old
# diagnostic reproduction; the default must allow ordinary row-owner waits.
RC_UPDATE_LOCK_ATTEMPTS=${RC_UPDATE_LOCK_ATTEMPTS:-20000}
MAX_ACTIVE_WRITES=${MAX_ACTIVE_WRITES:-24}
TERMINAL_ASSIGNMENTS=${TERMINAL_ASSIGNMENTS:-}
# Instrumented startup can be much slower than the production binary. This
# changes only the readiness polling budget, never the timed workload.
READY_ATTEMPTS=${READY_ATTEMPTS:-1200}

validate_id() {
  local label=$1 value=$2
  [[ $value =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$ ]] || {
    echo "$label must match [a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}: $value" >&2
    exit 2
  }
}

validate_uint() {
  local label=$1 value=$2
  [[ $value =~ ^[0-9]+$ ]] || {
    echo "$label must be a non-negative integer: $value" >&2
    exit 2
  }
}

validate_uint RC_UPDATE_LOCK_ATTEMPTS "$RC_UPDATE_LOCK_ATTEMPTS"
validate_uint MAX_ACTIVE_WRITES "$MAX_ACTIVE_WRITES"
validate_uint READY_ATTEMPTS "$READY_ATTEMPTS"
(( READY_ATTEMPTS >= 1 && READY_ATTEMPTS <= 60000 )) || {
  echo "READY_ATTEMPTS must be between 1 and 60000" >&2
  exit 2
}
(( MAX_ACTIVE_WRITES >= 1 && MAX_ACTIVE_WRITES <= 32 )) || {
  echo "MAX_ACTIVE_WRITES must be between 1 and 32" >&2
  exit 2
}

validate_id CAMPAIGN_ID "$CAMPAIGN_ID"
validate_id TRIAL_ID "$TRIAL_ID"
for setting in WARMUP PERF_STAT ALLOCATION_TRACE GRACEFUL_REOPEN_CHECK KEEP_DATA; do
  value=${!setting}
  [[ $value == 0 || $value == 1 ]] || {
    echo "$setting must be 0 or 1: $value" >&2
    exit 2
  }
done
[[ $REP =~ ^-?[0-9]+$ ]] || { echo "REP must be an integer: $REP" >&2; exit 2; }
for setting in POSITION VU RAMPUP DURATION PORT SAMPLE_SECS MEM_GUARD_MB; do
  validate_uint "$setting" "${!setting}"
done
(( PORT >= 1 && PORT <= 65535 )) || { echo "PORT must be 1..65535" >&2; exit 2; }
(( VU >= 1 )) || { echo "VU must be at least 1" >&2; exit 2; }
(( SAMPLE_SECS >= 1 )) || { echo "SAMPLE_SECS must be at least 1" >&2; exit 2; }
[[ $TIMEPROFILE == true || $TIMEPROFILE == false ]] || {
  echo "TIMEPROFILE must be true or false: $TIMEPROFILE" >&2
  exit 2
}
[[ $PROFILE_KIND =~ ^(warmup|throughput|latency|allocation)$ ]] || {
  echo "unknown PROFILE_KIND: $PROFILE_KIND" >&2
  exit 2
}

TRIAL_DIR="$CAMPAIGN_DIR/trials/$TRIAL_ID"
RAW="$TRIAL_DIR/raw"
RUN_KEY="$CAMPAIGN_ID-$TRIAL_ID"
DD="$DATA_ROOT/bicdb-$RUN_KEY"
CONTAINER="hdb-$RUN_KEY"
SERVER_PID=
SAMPLER_PID=
PERF_PID=
ALLOC_PID=
HDB_PID=
CONTAINER_ID=
DD_OWNED=0

if [[ $PRINT_RUNTIME_NAMES == 1 ]]; then
  printf 'data_dir=%s\ncontainer=%s\n' "$DD" "$CONTAINER"
  exit 0
fi

stop_pid() {
  local pid=${1:-}
  local signal=${2:-TERM}
  [[ -n $pid ]] || return 0
  kill -"$signal" "$pid" 2>/dev/null || true
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.25
  done
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# SERVER_STOP_SIGNAL=KILL skips the shutdown checkpoint. A 2-min VU32 run in
# the cpu-ceiling lane leaves ~14 GB of unmaterialized WAL and the graceful
# shutdown materializes all of it (13 GB -> 40+ GB RSS, cgroup OOM). The trial
# data dir is discarded, so a throughput trial with GRACEFUL_REOPEN_CHECK=0
# has nothing to lose by killing the server once dsum_after is captured.
SERVER_STOP_SIGNAL=${SERVER_STOP_SIGNAL:-TERM}
[[ $SERVER_STOP_SIGNAL == TERM || ( $SERVER_STOP_SIGNAL == KILL && $GRACEFUL_REOPEN_CHECK == 0 ) ]] || {
  echo "SERVER_STOP_SIGNAL=KILL requires GRACEFUL_REOPEN_CHECK=0" >&2
  exit 2
}
stop_server() {
  stop_pid "${SERVER_PID:-}" "$SERVER_STOP_SIGNAL"
  SERVER_PID=
}

stop_perf() {
  local pid=${PERF_PID:-} exit_code=0
  [[ -n $pid ]] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    kill -INT "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || exit_code=$?
  if [[ -d ${RAW:-} ]]; then
    printf '%s\n' "$exit_code" > "$RAW/perf_exit_code"
  fi
  PERF_PID=
}

cleanup() {
  local status=$?
  if [[ -n ${CONTAINER_ID:-} ]]; then
    docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
  fi
  stop_pid "${HDB_PID:-}" TERM
  stop_pid "${ALLOC_PID:-}" INT
  stop_perf
  stop_pid "${SAMPLER_PID:-}" TERM
  stop_server
  if [[ $KEEP_DATA != 1 && $DD_OWNED == 1 ]]; then
    rm -rf "$DD"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

[[ -x $BIN ]] || { echo "missing executable: $BIN" >&2; exit 1; }
[[ -d $SEED ]] || { echo "missing seed: $SEED" >&2; exit 1; }
[[ -f $SEED/seed-manifest.json ]] || { echo "missing seed manifest" >&2; exit 1; }
[[ ! -e $TRIAL_DIR ]] || { echo "trial already exists: $TRIAL_DIR" >&2; exit 1; }
[[ ! -e $DD ]] || { echo "trial data directory already exists: $DD" >&2; exit 1; }
[[ $LANE =~ ^(default-durable|tuned-durable|cpu-ceiling)$ ]] || {
  echo "unknown lane: $LANE" >&2
  exit 1
}
for command in docker jq psql python3 sha256sum ss findmnt stat df awk sed cp git rustc \
  nproc hostname tr grep tail date seq; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 1
  }
done
if [[ $PERF_STAT == 1 ]]; then
  command -v perf >/dev/null || { echo "missing required command: perf" >&2; exit 1; }
fi
if [[ $ALLOCATION_TRACE == 1 ]]; then
  command -v bpftrace >/dev/null || { echo "missing required command: bpftrace" >&2; exit 1; }
fi
[[ $(uname -s) == Linux ]] || {
  echo "the loopback-only HammerDB setup requires Docker host networking on Linux" >&2
  exit 1
}
docker info >/dev/null
if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "trial container already exists: $CONTAINER" >&2
  exit 1
fi
if ss -ltnH | awk '{print $4}' | grep -Eq "(^|:)$PORT$"; then
  echo "port already in use: $PORT" >&2
  exit 1
fi

mkdir -p "$RAW/hammer-tmp" "$DATA_ROOT"
seed_bytes=$(jq -r '.bytes' "$SEED/seed-manifest.json")
available_bytes=$(df -PB1 "$DATA_ROOT" | awk 'NR==2 {print $4}')
minimum_bytes=${MIN_FREE_BYTES:-$((seed_bytes * 3))}
if (( available_bytes < minimum_bytes )); then
  echo "insufficient space in $DATA_ROOT: $available_bytes < $minimum_bytes" >&2
  exit 1
fi

COMMON_ENV=(
  "BICDB_MEM_GUARD_MB=$MEM_GUARD_MB"
  "BICDB_MEM_TRACE=1"
  "BICDB_MEM_TRACE_SECS=$SAMPLE_SECS"
  "BICDB_PROC_MIX_TRACE=1"
  "MIMALLOC_SHOW_STATS=1"
)
# Extra server environment for an experimental arm (space-separated
# NAME=VALUE pairs), e.g. EXTRA_ENV="BICDB_ROW_LOCK_POLICY=wait-die". Recorded
# in result.json as part of common_env so arms stay distinguishable.
if [[ -n ${EXTRA_ENV:-} ]]; then
  read -r -a extra_env_pairs <<<"$EXTRA_ENV"
  COMMON_ENV+=("${extra_env_pairs[@]}")
fi
LANE_ENV=()
SERVER_LIMIT_ARGS=()
# Flags newer than some reference binaries (bisection, July high-water
# profiles): pass them only when the binary under test knows them.
PER_IP_ARGS=()
if "$BIN" serve-pg --help 2>/dev/null | grep -q -- '--max-connections-per-ip'; then
  PER_IP_ARGS=(--max-connections-per-ip 64)
fi
STORAGE_SYNC=durable
case "$LANE" in
  default-durable)
    ;;
  tuned-durable)
    LANE_ENV=(
      "BICDB_INDEX_STORE=sharded"
      "BICDB_INDEX_SHARDS=64"
      "BICDB_ORCH_SHARDS=16"
      "BICDB_RC_UPDATE_LOCK_ATTEMPTS=$RC_UPDATE_LOCK_ATTEMPTS"
      "BICDB_SYNC_OUTBOX=off"
      "BICDB_PLPGSQL_INTO_NO_DATA=1"
      "BICDB_AUTO_COMPACT_WAL_MB=4096"
    )
    SERVER_LIMIT_ARGS=(
      --max-connections 64 "${PER_IP_ARGS[@]}" --max-active-queries 32
      --max-active-reads 32 --max-active-writes "$MAX_ACTIVE_WRITES"
    )
    ;;
  cpu-ceiling)
    STORAGE_SYNC=buffered
    LANE_ENV=(
      "BICDB_INDEX_STORE=sharded"
      "BICDB_INDEX_SHARDS=64"
      "BICDB_ORCH_SHARDS=16"
      "BICDB_RC_UPDATE_LOCK_ATTEMPTS=$RC_UPDATE_LOCK_ATTEMPTS"
      "BICDB_SYNC_OUTBOX=off"
      "BICDB_PLPGSQL_INTO_NO_DATA=1"
      "BICDB_AUTO_COMPACT_WAL_MB=0"
    )
    SERVER_LIMIT_ARGS=(
      --max-connections 64 "${PER_IP_ARGS[@]}" --max-active-queries 32
      --max-active-reads 32 --max-active-writes "$MAX_ACTIVE_WRITES"
    )
    ;;
esac

# The lane's storage sync becomes a server flag only for binaries that know
# it (reference binaries predate --storage-sync). This used to be a
# self-assignment, so no lane ever passed the flag: cpu-ceiling ran with the
# server default instead of buffered writes.
STORAGE_SYNC_ARGS=()
if "$BIN" serve-pg --help 2>/dev/null | grep -q -- '--storage-sync'; then
  STORAGE_SYNC_ARGS=(--storage-sync "$STORAGE_SYNC")
fi

SERVER_ARGS=(
  serve-pg "$DD" --host 127.0.0.1 --port "$PORT"
  "${STORAGE_SYNC_ARGS[@]}"
  --max-result-rows 1000000
  "${SERVER_LIMIT_ARGS[@]}"
)

terminal_assignment_sha256=
if [[ -n $TERMINAL_ASSIGNMENTS ]]; then
  [[ -f $TERMINAL_ASSIGNMENTS ]] || { echo "missing terminal assignments: $TERMINAL_ASSIGNMENTS" >&2; exit 1; }
  cp "$TERMINAL_ASSIGNMENTS" "$RAW/terminal-assignments.tsv"
  terminal_assignment_sha256=$(sha256sum "$RAW/terminal-assignments.tsv" | cut -d' ' -f1)
fi
sed \
  -e "s/@PORT@/$PORT/g" \
  -e "s/@VU@/$VU/g" \
  -e "s/@RAMPUP@/$RAMPUP/g" \
  -e "s/@DURATION@/$DURATION/g" \
  -e "s/@TIMEPROFILE@/$TIMEPROFILE/g" \
  "$ROOT/bench/tpcc/run-vu.tcl.tmpl" > "$RAW/run.tcl"
if [[ ${CORRECTED_WORKLOAD_V2:-0} == 1 ]]; then
  [[ $BUSINESS_COUNTERS == 1 && $REQUIRE_BUSINESS_COMPLETION == 1 ]] || {
    echo "Corrected v2 requires exact business completion checks" >&2; exit 2;
  }
  cp "$ROOT/bench/tpcc/corrected_outcomes.tcl" "$RAW/corrected_outcomes.tcl"
fi

git_commit=$(git -C "$ROOT" rev-parse HEAD)
source_status=$(
  git -C "$ROOT" status --porcelain=v1 --untracked-files=all -- \
    . ':(exclude)reports/performance'
)
git_dirty=false
if [[ -n $source_status ]]; then
  git_dirty=true
fi
source_state_sha256=$(
  {
    git -C "$ROOT" diff --binary HEAD -- . ':(exclude)reports/performance'
    while IFS= read -r -d '' file; do
      file_sha256=$(sha256sum "$ROOT/$file" | cut -d' ' -f1)
      printf 'untracked %s %s\n' "$file" "$file_sha256"
    done < <(
      git -C "$ROOT" ls-files --others --exclude-standard -z -- \
        . ':(exclude)reports/performance'
    )
  } | sha256sum | cut -d' ' -f1
)
source_status_sha256=$(printf '%s' "$source_status" | sha256sum | cut -d' ' -f1)
binary_sha256=$(sha256sum "$BIN" | cut -d' ' -f1)
seed_manifest_sha256=$(sha256sum "$SEED/seed-manifest.json" | cut -d' ' -f1)
cpu_model=$(awk -F: '/model name/ {sub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)
lane_env_json=$(printf '%s\n' "${LANE_ENV[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')
common_env_json=$(printf '%s\n' "${COMMON_ENV[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')
server_args_json=$(printf '%s\n' "${SERVER_ARGS[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')
time_profile_json=false
[[ $TIMEPROFILE == true ]] && time_profile_json=true

jq -n \
  --arg trial_id "$TRIAL_ID" --arg lane "$LANE" \
  --arg started_at "$(date --iso-8601=ns)" \
  --arg git_commit "$git_commit" --arg source_state_sha256 "$source_state_sha256" \
  --arg source_status_sha256 "$source_status_sha256" --arg source_status "$source_status" \
  --arg binary "$BIN" --arg binary_sha256 "$binary_sha256" \
  --arg binary_git_commit "${BINARY_SOURCE_COMMIT:-}" \
  --arg seed "$SEED" --arg seed_manifest_sha256 "$seed_manifest_sha256" \
  --arg host "$(hostname -f 2>/dev/null || hostname)" --arg kernel "$(uname -srmo)" \
  --arg cpu_model "$cpu_model" --arg rustc "$(rustc -Vv | tr '\n' ';')" \
  --arg data_root "$DATA_ROOT" --arg data_fs "$(findmnt -no SOURCE,FSTYPE,OPTIONS -T "$DATA_ROOT")" \
  --arg image "$IMAGE" --arg image_id "$(docker image inspect "$IMAGE" --format '{{.Id}}')" \
  --argjson git_dirty "$git_dirty" --argjson rep "$REP" --argjson position "$POSITION" \
  --argjson warmup "$WARMUP" --argjson vu "$VU" --argjson rampup "$RAMPUP" \
  --argjson duration "$DURATION" --argjson port "$PORT" \
  --argjson logical_cpus "$(nproc)" \
  --argjson allocation_trace "$ALLOCATION_TRACE" \
  --argjson perf_stat "$PERF_STAT" \
  --argjson time_profile "$time_profile_json" \
  --argjson require_business_completion "$REQUIRE_BUSINESS_COMPLETION" \
  --argjson graceful_reopen_check "$GRACEFUL_REOPEN_CHECK" \
  --arg profile_kind "$PROFILE_KIND" \
  --arg terminal_assignment_sha256 "$terminal_assignment_sha256" \
  --argjson lane_env "$lane_env_json" --argjson common_env "$common_env_json" \
  --argjson server_args "$server_args_json" \
  '{schema_version:1, trial_id:$trial_id, lane:$lane, rep:$rep, position:$position,
    warmup:($warmup == 1), started_at:$started_at,
    source:{git_commit:$git_commit, git_dirty:$git_dirty,
      state_sha256:$source_state_sha256, diff_sha256:$source_state_sha256,
      status_sha256:$source_status_sha256,
      status:($source_status | split("\n") | map(select(length > 0))),
      binary:$binary, binary_sha256:$binary_sha256,
      binary_git_commit:(if $binary_git_commit == "" then null else $binary_git_commit end),
      rustc:$rustc},
    seed:{path:$seed, manifest_sha256:$seed_manifest_sha256},
    host:{hostname:$host, kernel:$kernel, cpu_model:$cpu_model,
      logical_cpus:$logical_cpus, data_root:$data_root, filesystem:$data_fs},
    workload:{name:"hammerdb-tpcc", vu:$vu, rampup_minutes:$rampup,
      require_business_completion:($require_business_completion == 1),
      duration_minutes:$duration, time_profile:$time_profile,
      terminal_assignment_capture:true,
      terminal_assignment_mode:(if $terminal_assignment_sha256 == "" then "random" else "replay" end),
      terminal_assignment_sha256:$terminal_assignment_sha256},
    server:{port:$port, args:$server_args, common_env:$common_env, lane_env:$lane_env},
    container:{image:$image, image_id:$image_id},
    profiling:{kind:$profile_kind, perf_stat:($perf_stat == 1),
      allocation_trace:($allocation_trace == 1),
      excluded_from_throughput:($profile_kind != "throughput")},
    reopen:{graceful_check:($graceful_reopen_check == 1)}}' > "$TRIAL_DIR/metadata.json"

DD_OWNED=1
cp -a --reflink=auto "$SEED" "$DD"

sumq() {
  PGPASSWORD=x psql -XAt -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select sum(d_next_o_id) from district' 2>/dev/null | tr -d '[:space:]'
}

statsq() {
  PGPASSWORD=x psql --csv -X -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select * from bicdb_server_stats'
}

businessq() {
  # Outside the timed workload: committed Payment work, not merely CALLs that
  # returned successfully after a handled serialization failure.
  PGPASSWORD=x psql --csv -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select count(*) as payment_history_rows from history'
}

replicationq() {
  PGPASSWORD=x psql --csv -X -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c 'select * from bicdb_replication_status'
}

start_server() {
  local log=$1
  env -i HOME="$HOME" PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
    "${COMMON_ENV[@]}" "${LANE_ENV[@]}" \
    "$BIN" "${SERVER_ARGS[@]}" > "$log" 2>&1 &
  SERVER_PID=$!
}

wait_ready() {
  local value
  for _ in $(seq 1 "$READY_ATTEMPTS"); do
    value=$(sumq || true)
    if [[ $value =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$value"
      return 0
    fi
    kill -0 "$SERVER_PID" 2>/dev/null || return 1
    sleep 0.1
  done
  return 1
}

sample_resources() {
  local pid=$1 output=$2
  printf 'epoch_ns,rss_kb,rss_hwm_kb,threads,voluntary_ctx,nonvoluntary_ctx,minflt,majflt,utime_ticks,stime_ticks,read_bytes,write_bytes,wal_bytes,mem_available_kb,fs_available_kb\n' > "$output"
  while kill -0 "$pid" 2>/dev/null; do
    local status statline io epoch rss hwm threads vctx nvctx minflt majflt utime stime readb writeb wal memb fsb
    status=$(cat "/proc/$pid/status" 2>/dev/null || true)
    statline=$(cat "/proc/$pid/stat" 2>/dev/null || true)
    io=$(cat "/proc/$pid/io" 2>/dev/null || true)
    epoch=$(date +%s%N)
    rss=$(awk '/^VmRSS:/ {print $2}' <<<"$status")
    hwm=$(awk '/^VmHWM:/ {print $2}' <<<"$status")
    threads=$(awk '/^Threads:/ {print $2}' <<<"$status")
    vctx=$(awk '/^voluntary_ctxt_switches:/ {print $2}' <<<"$status")
    nvctx=$(awk '/^nonvoluntary_ctxt_switches:/ {print $2}' <<<"$status")
    minflt=$(awk '{print $10}' <<<"$statline")
    majflt=$(awk '{print $12}' <<<"$statline")
    utime=$(awk '{print $14}' <<<"$statline")
    stime=$(awk '{print $15}' <<<"$statline")
    readb=$(awk '/^read_bytes:/ {print $2}' <<<"$io")
    writeb=$(awk '/^write_bytes:/ {print $2}' <<<"$io")
    wal=$(stat -c '%s' "$DD/transactions.log" 2>/dev/null || printf 0)
    memb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
    fsb=$(df -Pk "$DATA_ROOT" | awk 'NR==2 {print $4}')
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$epoch" "${rss:-0}" "${hwm:-0}" "${threads:-0}" "${vctx:-0}" \
      "${nvctx:-0}" "${minflt:-0}" "${majflt:-0}" "${utime:-0}" \
      "${stime:-0}" "${readb:-0}" "${writeb:-0}" "$wal" "${memb:-0}" "${fsb:-0}" >> "$output"
    sleep "$SAMPLE_SECS"
  done
}

start_ns=$(date +%s%N)
start_server "$RAW/server.log"
initial_sum=$(wait_ready) || { tail -120 "$RAW/server.log" >&2; exit 1; }
if [[ ${CORRECTED_WORKLOAD_V2:-0} == 1 ]]; then
  cp "$ROOT/bench/tpcc/corrected_workload_v2.sql" "$RAW/initial.sql"
  sha256sum "$RAW/initial.sql" "$RAW/corrected_outcomes.tcl" > "$RAW/workload-inputs.sha256"
  PGPASSWORD=x psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U tpcc -d bicdb \
    -f "$RAW/initial.sql" > "$RAW/initial-sql.log" 2>&1
  jq --arg sha "$(sha256sum "$RAW/initial.sql" | awk '{print $1}')" \
    '.workload.variant="corrected-v2" | .workload.initial_sql_sha256=$sha | .workload.client_outcomes=true' \
    "$TRIAL_DIR/metadata.json" > "$TRIAL_DIR/metadata.corrected.json"
  mv "$TRIAL_DIR/metadata.corrected.json" "$TRIAL_DIR/metadata.json"
fi
ready_ns=$(date +%s%N)
printf '%s\n' "$((ready_ns - start_ns))" > "$RAW/startup_ns"
printf '%s\n' "$initial_sum" > "$RAW/dsum_before"
if [[ $BUSINESS_COUNTERS == 1 ]]; then
  businessq > "$RAW/payment_before.csv"
fi
statsq > "$RAW/stats_before.csv"
replicationq > "$RAW/replication_before.csv" || true
stat -c '%s' "$DD/transactions.log" > "$RAW/wal_before_bytes" 2>/dev/null || printf '0\n' > "$RAW/wal_before_bytes"

sample_resources "$SERVER_PID" "$RAW/samples.csv" &
SAMPLER_PID=$!

if [[ $PERF_STAT == 1 ]]; then
  perf stat -j -o "$RAW/perf.json" \
    -e task-clock,cycles,instructions,branches,branch-misses,cache-misses,context-switches,cpu-migrations,page-faults \
    -p "$SERVER_PID" &
  PERF_PID=$!
fi

if [[ $ALLOCATION_TRACE == 1 ]]; then
  bpftrace -o "$RAW/allocations.txt" -e "
    uprobe:$BIN:mi_malloc_aligned /pid == $SERVER_PID/ { @allocation_count = count(); @allocated_bytes = sum(arg0); }
    uprobe:$BIN:mi_zalloc_aligned /pid == $SERVER_PID/ { @allocation_count = count(); @allocated_bytes = sum(arg0); }
    uprobe:$BIN:mi_realloc_aligned /pid == $SERVER_PID/ { @allocation_count = count(); @allocated_bytes = sum(arg1); }
  " > "$RAW/bpftrace.log" 2>&1 &
  ALLOC_PID=$!
  sleep 2
fi

workload_start_ns=$(date +%s%N)
CONTAINER_ID=$(docker create --name "$CONTAINER" \
  --network host \
  -v "$RAW:/trial" -v "$RAW/hammer-tmp:/tmp" \
  "$IMAGE" ./hammerdbcli auto /trial/run.tcl)
printf '%s\n' "$VU" > "$RAW/hammerdb_expected_active_vus"
docker start -a "$CONTAINER_ID" > "$RAW/hammerdb.log" 2>&1 &
HDB_PID=$!
while kill -0 "$HDB_PID" 2>/dev/null; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    printf 'server exited while HammerDB was active\n' >> "$RAW/harness-errors.log"
    docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
    break
  fi
  sleep 1
done
hammerdb_exit_code=0
wait "$HDB_PID" 2>/dev/null || hammerdb_exit_code=$?
printf '%s\n' "$hammerdb_exit_code" > "$RAW/hammerdb_exit_code"
HDB_PID=
workload_end_ns=$(date +%s%N)
printf '%s\n' "$((workload_end_ns - workload_start_ns))" > "$RAW/workload_ns"

stop_pid "${ALLOC_PID:-}" INT
ALLOC_PID=
stop_perf
stop_pid "${SAMPLER_PID:-}" TERM
SAMPLER_PID=

final_sum=$(wait_ready || true)
printf '%s\n' "$final_sum" > "$RAW/dsum_after"
statsq > "$RAW/stats_after.csv" || true
if [[ $BUSINESS_COUNTERS == 1 ]]; then
  businessq > "$RAW/payment_after.csv"
  PGPASSWORD=x psql --csv -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb \
    -c "select proname, prosrc from pg_proc where proname in ('neword','payment','delivery','ostat','slev') order by proname" \
    > "$RAW/procedures.csv"
fi
# bicdb_replication_status allocates memory proportional to the commit
# history after a long run (tens of GB after ~800k commits) and can trip the
# memory guard before the server is stopped; REPLICATION_STATUS=0 skips it.
if [[ ${REPLICATION_STATUS:-1} == 1 ]]; then
  replicationq > "$RAW/replication_after.csv" || true
else
  : > "$RAW/replication_after.csv"
fi
stat -c '%s' "$DD/transactions.log" > "$RAW/wal_after_bytes" 2>/dev/null || printf '0\n' > "$RAW/wal_after_bytes"
stop_server

if [[ $GRACEFUL_REOPEN_CHECK == 1 ]]; then
  graceful_reopen_start_ns=$(date +%s%N)
  start_server "$RAW/graceful-reopen-server.log"
  graceful_reopen_dsum=$(wait_ready) || {
    tail -120 "$RAW/graceful-reopen-server.log" >&2
    exit 1
  }
  printf '%s\n' "$graceful_reopen_dsum" > "$RAW/graceful_reopen_dsum"
  graceful_reopen_ready_ns=$(date +%s%N)
  printf '%s\n' "$((graceful_reopen_ready_ns - graceful_reopen_start_ns))" \
    > "$RAW/graceful_reopen_ns"
  if [[ -n $final_sum && $graceful_reopen_dsum != "$final_sum" ]]; then
    printf 'graceful reopen district sum changed: %s -> %s\n' \
      "$final_sum" "$graceful_reopen_dsum" >> "$RAW/harness-errors.log"
  fi
  stop_server
fi

python3 "$ROOT/bench/tpcc/summarize.py" trial "$TRIAL_DIR"
printf 'trial complete: %s\n' "$TRIAL_DIR/result.json"
