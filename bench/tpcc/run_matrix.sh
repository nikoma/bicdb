#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: bench/tpcc/run_matrix.sh

Runs the environment-configured three-lane TPC-C campaign. This script accepts
no positional arguments; use environment variables documented in the campaign
guide to configure it.
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
CAMPAIGN_ID=${CAMPAIGN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-baseline}
CAMPAIGN_DIR=${CAMPAIGN_DIR:-$ROOT/reports/performance/$CAMPAIGN_ID}
BIN=${BIN:-$ROOT/target/release/bicdb}
SEED=${SEED:-/dev/shm/bicdb-tpcc-seed}
PERMUTATIONS=${PERMUTATIONS:-6}
RUN_WARMUPS=${RUN_WARMUPS:-1}
RUN_LATENCY_PROFILES=${RUN_LATENCY_PROFILES:-1}
RUN_ALLOCATION_PROFILES=${RUN_ALLOCATION_PROFILES:-1}
BASE_PORT=${BASE_PORT:-55433}
CHILD_PID=

cleanup() {
  local status=$?
  if [[ -n ${CHILD_PID:-} ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

[[ $CAMPAIGN_ID =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$ ]] || {
  echo "CAMPAIGN_ID must match [a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}: $CAMPAIGN_ID" >&2
  exit 2
}
[[ $PERMUTATIONS =~ ^[1-6]$ ]] || { echo "PERMUTATIONS must be 1..6" >&2; exit 1; }
for setting in RUN_WARMUPS RUN_LATENCY_PROFILES RUN_ALLOCATION_PROFILES; do
  value=${!setting}
  [[ $value == 0 || $value == 1 ]] || { echo "$setting must be 0 or 1: $value" >&2; exit 2; }
done
graceful_reopen_setting=${GRACEFUL_REOPEN_CHECK:-${RECOVERY_CHECK:-1}}
[[ $graceful_reopen_setting == 0 || $graceful_reopen_setting == 1 ]] || {
  echo "GRACEFUL_REOPEN_CHECK must be 0 or 1: $graceful_reopen_setting" >&2
  exit 2
}
for setting in VU RAMPUP DURATION ALLOCATION_VU ALLOCATION_RAMPUP \
  ALLOCATION_DURATION LATENCY_VU LATENCY_RAMPUP LATENCY_DURATION PERF_STAT; do
  case $setting in
    VU) value=${VU:-32} ;;
    RAMPUP) value=${RAMPUP:-1} ;;
    DURATION) value=${DURATION:-2} ;;
    ALLOCATION_VU) value=${ALLOCATION_VU:-4} ;;
    ALLOCATION_RAMPUP) value=${ALLOCATION_RAMPUP:-0} ;;
    ALLOCATION_DURATION) value=${ALLOCATION_DURATION:-1} ;;
    LATENCY_VU) value=${LATENCY_VU:-${VU:-32}} ;;
    LATENCY_RAMPUP) value=${LATENCY_RAMPUP:-${RAMPUP:-1}} ;;
    LATENCY_DURATION) value=${LATENCY_DURATION:-${DURATION:-2}} ;;
    PERF_STAT) value=${PERF_STAT:-1} ;;
  esac
  [[ $value =~ ^[0-9]+$ ]] || { echo "$setting must be a non-negative integer: $value" >&2; exit 2; }
done
[[ ${PERF_STAT:-1} == 0 || ${PERF_STAT:-1} == 1 ]] || {
  echo "PERF_STAT must be 0 or 1: ${PERF_STAT:-1}" >&2
  exit 2
}
[[ ${VU:-32} -ge 1 && ${ALLOCATION_VU:-4} -ge 1 && ${LATENCY_VU:-${VU:-32}} -ge 1 ]] || {
  echo "VU counts must be at least 1" >&2
  exit 2
}
[[ $BASE_PORT =~ ^[0-9]+$ ]] && (( BASE_PORT >= 1 && BASE_PORT <= 65535 )) || {
  echo "BASE_PORT must be 1..65535: $BASE_PORT" >&2
  exit 2
}
for command in docker jq psql python3 sha256sum ss findmnt stat df awk sed cp git rustc \
  nproc hostname tr grep tail date find sort cut; do
  command -v "$command" >/dev/null || { echo "missing required command: $command" >&2; exit 1; }
done
if [[ ${PERF_STAT:-1} == 1 ]]; then
  command -v perf >/dev/null || { echo "missing required command: perf" >&2; exit 1; }
fi
if [[ $RUN_ALLOCATION_PROFILES == 1 ]]; then
  command -v bpftrace >/dev/null || { echo "missing required command: bpftrace" >&2; exit 1; }
fi
[[ -x $ROOT/bench/tpcc/run_trial.sh ]] || {
  echo "missing executable: $ROOT/bench/tpcc/run_trial.sh" >&2
  exit 1
}
[[ -x $BIN ]] || { echo "missing executable: $BIN" >&2; exit 1; }
[[ -d $SEED && -f $SEED/seed-manifest.json ]] || {
  echo "missing seed or seed manifest: $SEED" >&2
  exit 1
}
[[ $(uname -s) == Linux ]] || {
  echo "the loopback-only HammerDB setup requires Docker host networking on Linux" >&2
  exit 1
}
docker info >/dev/null
[[ ! -e $CAMPAIGN_DIR ]] || { echo "campaign already exists: $CAMPAIGN_DIR" >&2; exit 1; }

expected_seed_sha256=$(jq -er '.content_sha256' "$SEED/seed-manifest.json")
actual_seed_sha256=$(
  cd "$SEED"
  find . -type f ! -path './seed-manifest.json' -printf '%P\0' | sort -z | \
    while IFS= read -r -d '' file; do sha256sum "$file"; done | \
    sha256sum | cut -d' ' -f1
)
[[ $actual_seed_sha256 == "$expected_seed_sha256" ]] || {
  echo "seed content hash mismatch: $actual_seed_sha256 != $expected_seed_sha256" >&2
  exit 1
}
mkdir -p "$CAMPAIGN_DIR/logs"

permutations=(
  "default-durable tuned-durable cpu-ceiling"
  "tuned-durable cpu-ceiling default-durable"
  "cpu-ceiling default-durable tuned-durable"
  "default-durable cpu-ceiling tuned-durable"
  "cpu-ceiling tuned-durable default-durable"
  "tuned-durable default-durable cpu-ceiling"
)

jq -n \
  --arg campaign_id "$CAMPAIGN_ID" \
  --arg created_at "$(date --iso-8601=ns)" \
  --arg binary "$BIN" --arg seed "$SEED" \
  --arg seed_content_sha256 "$actual_seed_sha256" \
  --argjson permutations "$PERMUTATIONS" \
  --argjson warmups "$RUN_WARMUPS" \
  --argjson latency_profiles "$RUN_LATENCY_PROFILES" \
  --argjson allocation_profiles "$RUN_ALLOCATION_PROFILES" \
  --argjson vu "${VU:-32}" --argjson rampup "${RAMPUP:-1}" \
  --argjson duration "${DURATION:-2}" \
  '{schema_version:1, campaign_id:$campaign_id, created_at:$created_at,
    binary:$binary, seed:$seed, seed_content_sha256:$seed_content_sha256,
    matrix:{permutations:$permutations,
      discarded_warmups:($warmups == 1),
      latency_profiles:($latency_profiles == 1),
      allocation_profiles:($allocation_profiles == 1)},
    workload:{vu:$vu, rampup_minutes:$rampup, duration_minutes:$duration,
      time_profile:false},
    lanes:{
      "default-durable":{purpose:"out-of-box durable product configuration"},
      "tuned-durable":{purpose:"best proven durable configuration"},
      "cpu-ceiling":{purpose:"non-durable diagnostic upper bound; never a production keeper"}
    }}' > "$CAMPAIGN_DIR/manifest.json"

run_trial() {
  local trial_id=$1 lane=$2 rep=$3 position=$4 warmup=$5 profile_kind=${6:-throughput}
  local trial_vu=${VU:-32} trial_rampup=${RAMPUP:-1} trial_duration=${DURATION:-2}
  local trial_timeprofile=false trial_reopen=0 trial_perf=0 trial_alloc=0
  case $profile_kind in
    throughput)
      trial_reopen=${GRACEFUL_REOPEN_CHECK:-${RECOVERY_CHECK:-1}}
      trial_perf=${PERF_STAT:-1}
      ;;
    warmup)
      ;;
    latency)
      trial_vu=${LATENCY_VU:-${VU:-32}}
      trial_rampup=${LATENCY_RAMPUP:-${RAMPUP:-1}}
      trial_duration=${LATENCY_DURATION:-${DURATION:-2}}
      trial_timeprofile=true
      ;;
    allocation)
      trial_vu=${ALLOCATION_VU:-4}
      trial_rampup=${ALLOCATION_RAMPUP:-0}
      trial_duration=${ALLOCATION_DURATION:-1}
      trial_alloc=1
      ;;
    *)
      echo "unknown profile kind: $profile_kind" >&2
      return 2
      ;;
  esac
  printf '[%s] start %s lane=%s rep=%s position=%s warmup=%s profile=%s\n' \
    "$(date --iso-8601=seconds)" "$trial_id" "$lane" "$rep" "$position" "$warmup" "$profile_kind" \
    | tee -a "$CAMPAIGN_DIR/campaign.log"
  CAMPAIGN_ID="$CAMPAIGN_ID" CAMPAIGN_DIR="$CAMPAIGN_DIR" \
    TRIAL_ID="$trial_id" LANE="$lane" REP="$rep" \
    POSITION="$position" WARMUP="$warmup" PROFILE_KIND="$profile_kind" \
    ALLOCATION_TRACE="$trial_alloc" PERF_STAT="$trial_perf" \
    VU="$trial_vu" RAMPUP="$trial_rampup" DURATION="$trial_duration" \
    TIMEPROFILE="$trial_timeprofile" GRACEFUL_REOPEN_CHECK="$trial_reopen" \
    PORT="$BASE_PORT" BIN="$BIN" SEED="$SEED" \
    "$ROOT/bench/tpcc/run_trial.sh" > "$CAMPAIGN_DIR/logs/$trial_id.log" 2>&1 &
  CHILD_PID=$!
  wait "$CHILD_PID"
  CHILD_PID=
  python3 "$ROOT/bench/tpcc/summarize.py" campaign "$CAMPAIGN_DIR"
  jq -r '[.validity.valid, .throughput.district_sum_nopm, .memory.rss_peak_bytes,
    .wal.generated_bytes, (.validity.reasons | join("; "))] | @tsv' \
    "$CAMPAIGN_DIR/trials/$trial_id/result.json" | tee -a "$CAMPAIGN_DIR/campaign.log"
}

if [[ $RUN_WARMUPS == 1 ]]; then
  position=0
  for lane in default-durable tuned-durable cpu-ceiling; do
    position=$((position + 1))
    run_trial "warmup-$position-${lane}" "$lane" -1 "$position" 1 warmup
  done
fi

for ((rep = 1; rep <= PERMUTATIONS; rep++)); do
  position=0
  for lane in ${permutations[$((rep - 1))]}; do
    position=$((position + 1))
    run_trial "rep${rep}-pos${position}-${lane}" "$lane" "$rep" "$position" 0 throughput
  done
done

if [[ $RUN_LATENCY_PROFILES == 1 ]]; then
  position=0
  for lane in default-durable tuned-durable cpu-ceiling; do
    position=$((position + 1))
    run_trial "latency-${lane}" "$lane" -3 "$position" 1 latency
  done
fi

if [[ $RUN_ALLOCATION_PROFILES == 1 ]]; then
  position=0
  for lane in default-durable tuned-durable cpu-ceiling; do
    position=$((position + 1))
    run_trial "alloc-${lane}" "$lane" -2 "$position" 1 allocation
  done
fi

python3 "$ROOT/bench/tpcc/summarize.py" campaign "$CAMPAIGN_DIR"
printf '[%s] campaign complete: %s\n' "$(date --iso-8601=seconds)" "$CAMPAIGN_DIR" \
  | tee -a "$CAMPAIGN_DIR/campaign.log"
