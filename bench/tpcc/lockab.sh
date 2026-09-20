#!/usr/bin/env bash
# Rigorous lock-isolation A/B: SAME binary (main 5d77756), lock-free vs serialized.
# Controls for warmup drift: 1 discarded warmup trial, balanced order, 3 reps/arm,
# 2-min duration. seed-restore + district-sum NOPM + per-trial tmpfs cleanup.
set -uo pipefail
HARNESS=/root/bicdb-bench; WORK=$HARNESS/work
BIN=/root/bicdb-concurrent/target/release/bicdb
PORT=55433; IMAGE=tpcorg/hammerdb:postgres
SNAP=/dev/shm/bicdb-seed-snap
SUM=$HARNESS/lockab3-summary.txt
LOG=$HARNESS/lockab3.log
exec > "$LOG" 2>&1
ts(){ date +%H:%M:%S; }
sumq(){ PGPASSWORD=x psql -h 127.0.0.1 -p "$PORT" -U bicdb -d bicdb -t -A -c 'select sum(d_next_o_id) from district' 2>&1 | tr -d '[:space:]'; }
shm_free_mb(){ df --output=avail -m /dev/shm | tail -1 | tr -d ' '; }
# 2-min duration, 1-min rampup driver
sed -e 's/@VU@/16/g' -e 's/pg_duration 1/pg_duration 2/' "$WORK/run-vu.tcl.tmpl" > "$WORK/run-d2.tcl"
DIV=3   # rampup1+duration2 minutes
SRV=""
serve(){ BICDB_MEM_GUARD_MB=2048 "$BIN" serve-pg "$1" --host 0.0.0.0 --port $PORT --allow-remote-no-auth \
  --max-connections 64 --max-active-queries 32 --max-active-reads 32 --max-active-writes 16 --max-result-rows 1000000 \
  > "$HARNESS/lockab3-srv.log" 2>&1 & SRV=$!
  for _ in $(seq 1 240); do ss -ltn 2>/dev/null|grep -q ":$PORT" && return 0; sleep 0.5; done; echo "!! SERVER FAIL"; return 1; }
stop(){ [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; for _ in $(seq 1 60); do kill -0 "$SRV" 2>/dev/null||break; sleep 0.5; done; SRV=""; pkill -9 -x bicdb 2>/dev/null; sleep 1; }
hdb(){ docker run --rm --name "hdb-$$-$RANDOM" --add-host=host.docker.internal:host-gateway -v "$WORK":/work "$IMAGE" ./hammerdbcli auto "/work/$1"; }

echo "===== LOCKAB3 START $(date) bin=5d77756 (warmup+balanced, dur=2m, 3 reps/arm) ====="
echo "LOCKAB3 START $(date)" > "$SUM"
pkill -9 -x bicdb 2>/dev/null; sleep 1
for c in $(docker ps -q --filter name=hdb-); do docker rm -f "$c">/dev/null 2>&1; done

trial(){ # $1=label $2=mode(free|lock) $3=count(yes|warmup)
  pkill -9 -x bicdb 2>/dev/null; sleep 1
  for c in $(docker ps -q --filter name=hdb-); do docker rm -f "$c">/dev/null 2>&1; done
  local dd=/dev/shm/bicdb-lab3-$1; rm -rf "$dd"
  local fm=$(shm_free_mb); if [ "${fm:-0}" -lt 15000 ]; then echo "$1 ($2): ABORT tmpfs ${fm}MB" | tee -a "$SUM"; return; fi
  cp -a "$SNAP" "$dd"
  export BICDB_INDEX_STORE=sharded BICDB_INDEX_SHARDS=64 BICDB_ORCH_SHARDS=16
  if [ "$2" = lock ]; then export BICDB_GLOBAL_COMMIT_LOCK=1; else unset BICDB_GLOBAL_COMMIT_LOCK; fi
  echo "==== TRIAL $1 mode=$2 ($3) tmpfs=${fm}MB $(ts) ===="
  serve "$dd" || { echo "$1 ($2): SERVER FAIL" | tee -a "$SUM"; return; }
  local b=$(sumq); hdb run-d2.tcl > "$HARNESS/lab3-$1.log" 2>&1 || true; local a=$(sumq)
  local d=$(( a - b )); local nopm=$(( d / DIV )); local failed=$(grep -c 'FINISHED FAILED' "$HARNESS/lab3-$1.log" 2>/dev/null)
  local tag="$1 ($2): delta=$d NOPM~$nopm FAILED=$failed"
  [ "$3" = warmup ] && tag="[DISCARD] $tag"
  echo "$tag" | tee -a "$SUM"
  stop; rm -rf "$dd"
  unset BICDB_INDEX_STORE BICDB_INDEX_SHARDS BICDB_ORCH_SHARDS BICDB_GLOBAL_COMMIT_LOCK
}

trial warm free warmup     # discarded: absorbs settling/turbo warmup
trial free_a free yes
trial lock_a lock yes
trial lock_b lock yes
trial free_b free yes
trial free_c free yes
trial lock_c lock yes
pkill -9 -x bicdb 2>/dev/null
echo "LOCKAB3 DONE $(date)" >> "$SUM"
# averages (exclude warmup)
awk -F'[ =]' '/NOPM~/ && !/DISCARD/ {for(i=1;i<=NF;i++) if($i ~ /^NOPM~/){v=substr($i,6)}; if($2=="(free):"||$0 ~ /\(free\)/){fs+=v;fn++} else {ls+=v;ln++}} END{if(fn)printf "AVG free NOPM = %d (n=%d)\n",fs/fn,fn; if(ln)printf "AVG lock NOPM = %d (n=%d)\n",ls/ln,ln}' "$SUM" >> "$SUM"
