#!/usr/bin/env bash
set -Eeuo pipefail
export TZ=UTC

usage() {
  cat <<'EOF'
Usage: bench/perf-hunt/run.sh <scenario>

Starts a throwaway BicDB server on port 5599, runs load for the named
scenario using 2-4 concurrent connections, stops the server, and prints
ops/sec + p50/p99 latency on stdout.

HARD RULES:
 - NEVER touch 127.0.0.1:5433 (production)
 - Data dir is a fresh /tmp directory cleaned after every run.
EOF
}

SCENARIO="${1:-}"
[[ -n $SCENARIO ]] || { usage >&2; exit 2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/target/release/bicdb"
PORT="${PERF_PORT:-5599}"
DATA_DIR=$(mktemp -d -t bicdb-perfhunt.XXXXXX)
TMP_PY="/tmp/bicdb-perfhunt-$$.py"
DURATION=8
CONCURRENCY=2

cleanup() {
  local status=$?
  if [[ -n ${SERVER_PID:-} ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    kill -KILL "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR" "$TMP_PY" 2>/dev/null || true
  exit "$status"
}
trap cleanup EXIT INT TERM

[[ -x $BIN ]] || { echo "=== building bicdb (release) ===" >&2; cargo build --release --manifest-path "$ROOT/Cargo.toml" 2>&1; }
[[ -x $BIN ]] || { echo "missing executable: $BIN" >&2; exit 1; }

command -v python3 >/dev/null || { echo "missing python3" >&2; exit 1; }
python3 -c 'import psycopg2' 2>/dev/null || { pip install --break-system-packages psycopg2-binary >&2 2>/dev/null; }
python3 -c 'import psycopg2' 2>/dev/null || { echo "missing psycopg2" >&2; exit 1; }

echo "=== starting server on port $PORT, data=$DATA_DIR ===" >&2
"$BIN" serve-pg "$DATA_DIR" --host 127.0.0.1 --port "$PORT" --storage-sync buffered > "$DATA_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait until ready (timeout 30s)
python3 -c "
import time, psycopg2
for i in range(60):
    try:
        c=psycopg2.connect(host='127.0.0.1',port=$PORT,user='bicdb',password='x',dbname='bicdb',connect_timeout=1)
        c.close()
        break
    except Exception:
        time.sleep(0.5)
else:
    exit(1)
" || { echo "=== server failed to start ===" >&2; tail -40 "$DATA_DIR/server.log" >&2; exit 1; }

echo "=== running scenario '$SCENARIO' for $DURATION s with $CONCURRENCY connections ===" >&2

cat > "$TMP_PY" <<'PYEOF'
import argparse, concurrent.futures, random, sys, threading, time
import psycopg2

class ctx:
    host="127.0.0.1"
    port=5599

def connect():
    return psycopg2.connect(host=ctx.host,port=ctx.port,user="bicdb",password="x",dbname="bicdb",connect_timeout=3)

def ruuid():
    return "%08x-%04x-%04x-%04x-%012x" % (random.getrandbits(32),random.getrandbits(16),random.getrandbits(16),random.getrandbits(16),random.getrandbits(48))

# --------------------------------------------------------
# Scenario bodies — pure Python-driven queries, no server-side random()
# All random values are generated client-side and passed as parameters
# or interpolated as safe literals (since they're numeric/uuid).
def body_baseline(conn):
    c=conn.cursor()
    k=ruuid(); v=random.randint(0,999999)
    c.execute("SELECT 1"); c.fetchall()
    c.execute(f"INSERT INTO perf_t (key,val) VALUES ('{k}',{v}) ON CONFLICT (key) DO UPDATE SET val={v}")
    conn.commit()
    c.execute("SELECT * FROM perf_t ORDER BY val DESC LIMIT 5"); c.fetchall()
    c.execute("SELECT count(*) FROM perf_t WHERE val>500000"); c.fetchall()

def body_writes_only(conn):
    c=conn.cursor()
    k=ruuid(); v=random.randint(0,999999)
    c.execute(f"INSERT INTO perf_t (key,val) VALUES ('{k}',{v})"); conn.commit()

def body_reads_only(conn):
    c=conn.cursor()
    c.execute("SELECT * FROM perf_t ORDER BY key DESC LIMIT 1"); c.fetchall()

def body_scan_full(conn):
    c=conn.cursor()
    c.execute("SELECT count(*),avg(val),max(val),min(val) FROM perf_t"); c.fetchall()

def body_index_lookup(conn):
    c=conn.cursor()
    c.execute("SELECT * FROM perf_t WHERE key=(SELECT key FROM perf_t ORDER BY key DESC LIMIT 1)"); c.fetchall()

def body_pgwire_simple(conn):
    c=conn.cursor()
    c.execute("SELECT 1"); c.fetchall()

def body_connection_setup(conn):
    with connect() as f:
        cur=f.cursor(); cur.execute("SELECT 1"); cur.fetchall()

def body_returning(conn):
    c=conn.cursor()
    k=ruuid(); v=random.randint(0,999999)
    c.execute(f"INSERT INTO perf_t (key,val) VALUES ('{k}',{v}) RETURNING key,val"); c.fetchall(); conn.commit()

def body_jsonb(conn):
    c=conn.cursor()
    c.execute("SELECT * FROM perf_t WHERE payload->>'status'='active'"); c.fetchall()

def body_write_no_commit(conn):
    c=conn.cursor()
    k=ruuid(); v=random.randint(0,999999)
    c.execute(f"INSERT INTO perf_t (key,val) VALUES ('{k}',{v})")

def body_batch_insert(conn):
    c=conn.cursor(); b=20
    vals=",".join("('%s',%d)"%(ruuid(),random.randint(0,999999)) for _ in range(b))
    c.execute("INSERT INTO perf_t (key,val) VALUES %s" % vals); conn.commit()

def body_event_append(conn):
    c=conn.cursor()
    n=random.randint(0,999)
    c.execute(f"INSERT INTO bicdb_events (stream,event_type,payload) VALUES ('bench','tick','{{\"n\":{n}}}')"); conn.commit()

def body_listen_notify(conn):
    c=conn.cursor(); c.execute("LISTEN perf_notify"); conn.commit()
    done=threading.Event()
    def sender():
        try:
            with connect() as sc:
                scur=sc.cursor()
                while not done.is_set():
                    scur.execute("NOTIFY perf_notify, 'hello'"); sc.commit(); time.sleep(0.001)
        except Exception:
            pass
    t=threading.Thread(target=sender,daemon=True); t.start()
    for _ in range(200):
        conn.poll()
        while conn.notifies: conn.notifies.pop(0)
        time.sleep(0.001)
    done.set(); t.join(timeout=1)

BODIES={
 "baseline":body_baseline,"writes-only":body_writes_only,
 "reads-only":body_reads_only,"scan-full":body_scan_full,
 "index-lookup":body_index_lookup,"pgwire-simple":body_pgwire_simple,
 "connection-setup":body_connection_setup,"returning":body_returning,
 "jsonb":body_jsonb,"write-no-commit":body_write_no_commit,
 "batch-insert":body_batch_insert,"event-append":body_event_append,
 "listen-notify":body_listen_notify,
}

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--host",default="127.0.0.1")
    p.add_argument("--port",type=int,default=5599)
    p.add_argument("--scenario",required=True)
    p.add_argument("--duration",type=float,default=8)
    p.add_argument("--concurrency",type=int,default=2)
    args=p.parse_args()
    ctx.host=args.host; ctx.port=args.port
    fn=BODIES.get(args.scenario)
    if fn is None:
        print("unknown scenario",args.scenario,file=sys.stderr); sys.exit(2)

    # seed
    admin=connect(); admin.autocommit=True
    ac=admin.cursor()
    ac.execute("CREATE TABLE IF NOT EXISTS perf_t (key TEXT PRIMARY KEY, val BIGINT, payload JSONB DEFAULT '{}')")
    ac.execute("SELECT count(*) FROM perf_t")
    n=ac.fetchone()[0]
    if n<200:
        need=200-n
        for off in range(0,need,100):
            sz=min(100,need-off)
            vals=",".join("('%s',%d,'{\"status\":\"active\"}')"%(ruuid(),random.randint(0,999999)) for _ in range(sz))
            ac.execute("INSERT INTO perf_t (key,val,payload) VALUES %s ON CONFLICT DO NOTHING" % vals)
    try:
        ac.execute("CREATE TABLE IF NOT EXISTS bicdb_events (stream TEXT, event_type TEXT, payload JSONB)")
    except Exception:
        pass
    admin.close()

    lats=[]; lock=threading.Lock(); stop=threading.Event()
    barrier=threading.Barrier(args.concurrency+1)

    def worker(idx):
        conn=connect()
        try:
            if args.scenario=="write-no-commit":
                conn.autocommit=True
            # warmup
            wc=conn.cursor()
            try: wc.execute("SELECT 1"); wc.fetchall()
            except: pass
            barrier.wait()
            while not stop.is_set():
                t0=time.perf_counter()
                fn(conn)
                dt=time.perf_counter()-t0
                with lock: lats.append(dt)
        except Exception as e:
            if not stop.is_set():
                print(f"worker {idx} error: {e}",file=sys.stderr)
        finally:
            try: conn.close()
            except: pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures=[pool.submit(worker,i) for i in range(args.concurrency)]
        barrier.wait()
        time.sleep(args.duration)
        stop.set()
        concurrent.futures.wait(futures,timeout=10)

    if not lats:
        print("no measurements",file=sys.stderr)
        print("ops_sec=0 p50_ms=0 p99_ms=0")
        return

    total=len(lats)
    elapsed=sum(lats)
    ops=total/elapsed if elapsed>0 else 0
    lats.sort()
    p50=lats[int(len(lats)*0.5)]
    p99=lats[min(int(len(lats)*0.99),len(lats)-1)]
    print(f"ops_sec={ops:.1f} p50_ms={p50*1000:.3f} p99_ms={p99*1000:.3f}")

if __name__=="__main__":
    main()
PYEOF

set +e
python3 "$TMP_PY" \
  --host 127.0.0.1 --port "$PORT" \
  --scenario "$SCENARIO" \
  --duration "$DURATION" \
  --concurrency "$CONCURRENCY"
HARNESS_EXIT=$?
set -e

if [[ $HARNESS_EXIT -ne 0 ]]; then
  echo "=== harness failed (exit=$HARNESS_EXIT) ===" >&2
  echo "ops_sec=0 p50_ms=0 p99_ms=0"
fi

echo "=== stopping server ===" >&2
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
echo "=== run complete ===" >&2