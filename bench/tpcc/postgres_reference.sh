#!/usr/bin/env bash
# Same-host PostgreSQL reference. Keeps every log/data directory for audit;
# stops only the containers it creates. No existing cluster is modified.
set -Eeuo pipefail
MODE=${1:?Usage: postgres_reference.sh seed|trial}
[[ $MODE == seed || $MODE == trial ]] || exit 2
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PG_IMAGE=${PG_IMAGE:-postgres:18.6}
HDB_IMAGE=${HDB_IMAGE:-tpcorg/hammerdb:postgres}
PORT=${PORT:-55434}
VU=${VU:-32}
RAMPUP=${RAMPUP:-1}
DURATION=${DURATION:-1}
REFERENCE_LANE=${REFERENCE_LANE:-durable}
case "$REFERENCE_LANE" in
  durable)
    SYNC_ARGS=(-c synchronous_commit=on -c fsync=on -c max_wal_size=4GB -c checkpoint_timeout=5min)
    ;;
  cpu-ceiling)
    # Buffered comparison with BicDB's cpu-ceiling lane. Keep checkpoints
    # outside the short timed run; this lane does not promise durable commits.
    SYNC_ARGS=(-c synchronous_commit=off -c fsync=off -c max_wal_size=64GB -c checkpoint_timeout=1d)
    ;;
  *) echo "unknown REFERENCE_LANE: $REFERENCE_LANE" >&2; exit 2 ;;
esac
OUT=${OUT:?Set a new OUT directory for artifacts}
[[ ! -e $OUT ]] || { echo "OUT already exists: $OUT" >&2; exit 1; }
[[ $PORT =~ ^[0-9]+$ ]] && ((PORT > 1024 && PORT < 65536)) || exit 2
for value in "$VU" "$RAMPUP" "$DURATION"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || exit 2
done
if ss -ltnH | awk '{print $4}' | grep -Eq "(^|:)$PORT$"; then
  echo "port already occupied: $PORT" >&2; exit 1
fi
mkdir -p "$OUT"
OUT=$(realpath "$OUT")
printf '%s\n' "$REFERENCE_LANE" > "$OUT/reference_lane.txt"
PG_WORK=$(mktemp -d "${DATA_ROOT:-/dev/shm}/codex-pg186.XXXXXX")
# The entrypoint creates/chowns PGDATA, then drops privileges; its parent
# mount must remain traversable by the postgres uid (PGDATA itself is 0700).
chmod 755 "$PG_WORK"
printf '%s\n' "$PG_WORK" > "$OUT/data_path"
PG_CONTAINER="codex-pg186-$MODE-$$"
HDB_CONTAINER="codex-pg186-hdb-$MODE-$$"
PG_ID=
HDB_ID=
cleanup() {
  local status=$?
  [[ -z $HDB_ID ]] || docker stop -t 30 "$HDB_ID" >/dev/null 2>&1 || true
  [[ -z $PG_ID ]] || docker stop -t 60 "$PG_ID" >/dev/null 2>&1 || true
  echo "Preserved data: $PG_WORK; artifacts: $OUT"
  exit "$status"
}
trap cleanup EXIT
if [[ $MODE == trial ]]; then
  SEED=${SEED:?Set SEED to the stopped seed cluster data_path}
  [[ -f $SEED/data/PG_VERSION && ! -f $SEED/data/postmaster.pid ]] || {
    echo 'Seed must be a stopped PostgreSQL cluster' >&2; exit 1;
  }
  [[ $(<"$SEED/data/PG_VERSION") == 18 ]] || exit 1
  cp -a "$SEED/data" "$PG_WORK/data"
fi
docker image inspect "$PG_IMAGE" > "$OUT/postgres-image.json"
docker image inspect "$HDB_IMAGE" > "$OUT/hammerdb-image.json"
lscpu > "$OUT/lscpu.txt"
df -h "$PG_WORK" > "$OUT/filesystem.txt"
# Match the BicDB server memory ceiling; Docker containers do not inherit
# the systemd wrapper's memory limit. Equal memory/swap limits disable swap.
PG_ID=$(docker run -d --name "$PG_CONTAINER" --network host --shm-size=1g \
  --memory=90g --memory-swap=90g \
  -v "$PG_WORK:/var/lib/postgresql" -e PGDATA=/var/lib/postgresql/data \
  -e POSTGRES_USER=bicdb -e POSTGRES_PASSWORD=bicdb -e POSTGRES_DB=bicdb \
  "$PG_IMAGE" postgres -c listen_addresses=127.0.0.1 -c port="$PORT" \
  -c shared_buffers=8GB -c max_connections=128 "${SYNC_ARGS[@]}" \
  -c full_page_writes=on -c jit=off -c track_functions=all)
docker inspect "$PG_ID" > "$OUT/postgres-container.json"
sha256sum "$ROOT/bench/tpcc/postgres_reference.sh" > "$OUT/harness.sha256"
query() {
  PGPASSWORD=bicdb psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" \
    -U bicdb -d "${PG_DATABASE:-bicdb}" "$@"
}
ready=0
for _ in $(seq 1 120); do
  if query -Atc 'select 1' >/dev/null 2>&1; then ready=1; break; fi
  [[ $(docker inspect -f '{{.State.Running}}' "$PG_ID") == true ]] || break
  sleep 1
done
[[ $ready == 1 ]] || { docker logs "$PG_ID"; exit 1; }
query -Atc 'select version()' > "$OUT/version.txt"
query -Atc 'show server_version' | grep -Eq '^18\.6([[:space:]]|$)' || {
  echo 'Expected exactly PostgreSQL 18.6' >&2; exit 1;
}
query --csv -c 'select name,setting,unit,source from pg_settings order by name' > "$OUT/settings.csv"
if [[ $MODE == seed ]]; then
  sed -e "s/@PORT@/$PORT/g" -e 's/@WAREHOUSES@/16/g' -e 's/@BUILD_VU@/8/g' \
    -e 's/host\.docker\.internal/127.0.0.1/g' -e 's/^quit$/exit 0/' \
    "$ROOT/bench/tpcc/buildschema.tcl.tmpl" > "$OUT/run.tcl"
else
  PG_DATABASE=tpcc
  if [[ ${CORRECTED_WORKLOAD_V2:-0} == 1 ]]; then
    cp "$ROOT/bench/tpcc/corrected_workload_v2.sql" "$OUT/initial.sql"
    cp "$ROOT/bench/tpcc/corrected_outcomes.tcl" "$OUT/corrected_outcomes.tcl"
    sha256sum "$OUT/initial.sql" "$OUT/corrected_outcomes.tcl" > "$OUT/workload-inputs.sha256"
    PGPASSWORD=tpcc psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U tpcc -d tpcc \
      -f "$OUT/initial.sql" > "$OUT/initial-sql.log" 2>&1
  fi
  if [[ -n ${TERMINAL_ASSIGNMENTS:-} ]]; then
    cp "$TERMINAL_ASSIGNMENTS" "$OUT/terminal-assignments.tsv"
    sha256sum "$OUT/terminal-assignments.tsv" > "$OUT/terminal-assignments.sha256"
  fi
  # State changes count committed business work even if routines catch errors.
  snapshot() {
    local phase=$1
    query --csv -c 'select sum(d_next_o_id) as district_sum from district' > "$OUT/district-$phase.csv"
    query --csv -c 'select count(*) as payment_history_rows from history' > "$OUT/payment-$phase.csv"
    query --csv -c "select * from pg_stat_database where datname='tpcc'" > "$OUT/database-$phase.csv"
    query --csv -c 'select * from pg_stat_user_functions order by funcid' > "$OUT/functions-$phase.csv"
    query --csv -c 'select * from pg_stat_wal' > "$OUT/wal-$phase.csv"
  }
  snapshot before
  sed -e "s/@PORT@/$PORT/g" -e 's/@TIMEPROFILE@/false/g' \
    -e "s/@RAMPUP@/$RAMPUP/g" -e "s/@DURATION@/$DURATION/g" -e "s/@VU@/$VU/g" \
    "$ROOT/bench/tpcc/run-vu.tcl.tmpl" > "$OUT/run.tcl"
fi
HDB_ID=$(docker create --name "$HDB_CONTAINER" --network host \
  -v "$OUT:/trial" "$HDB_IMAGE" ./hammerdbcli auto /trial/run.tcl)
date +%s%N > "$OUT/start_ns"
status=0
docker start -a "$HDB_ID" > "$OUT/hammerdb.log" 2>&1 || status=$?
date +%s%N > "$OUT/end_ns"
printf '%s\n' "$status" > "$OUT/hammerdb_exit_code"
docker logs "$PG_ID" > "$OUT/postgres.log" 2>&1
[[ $status == 0 ]] && ! grep -q 'FINISHED FAILED' "$OUT/hammerdb.log" && \
  grep -q 'ALL VIRTUAL USERS COMPLETE' "$OUT/hammerdb.log" || {
    tail -80 "$OUT/hammerdb.log"; exit 1;
  }
PG_DATABASE=tpcc
query --csv -c "select proname, prosrc from pg_proc where proname in ('neword','payment','delivery','ostat','slev') order by proname" > "$OUT/procedures.csv"
if [[ $MODE == seed ]]; then
  query -c 'VACUUM ANALYZE' > "$OUT/vacuum.log"
  query --csv -c 'select count(*) as warehouses from warehouse' > "$OUT/warehouses.csv"
  query --csv -c 'select sum(d_next_o_id) as district_sum from district' > "$OUT/district-seed.csv"
else
  # Workload sessions have disconnected and flushed their cumulative stats.
  snapshot after
  printf 'VU=%s RAMPUP=%s DURATION=%s\n' "$VU" "$RAMPUP" "$DURATION" > "$OUT/parameters.txt"
  if [[ ${CORRECTED_WORKLOAD_V2:-0} == 1 ]]; then
    python3 "$ROOT/bench/tpcc/verify_postgres_outcomes.py" "$OUT" "$VU" "$RAMPUP" "$DURATION"
  fi
fi
query -c 'CHECKPOINT' > "$OUT/checkpoint.log"
echo "PostgreSQL $MODE completed successfully"
