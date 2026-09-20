#!/usr/bin/env python3
"""Process-crash audit: external acknowledgement ledger and atomic transfers.

Not a throughput benchmark and not a host/power-loss test. Uses only its own
fresh schema/data directory. Connection losses are uncertain, never called
successful or blindly retried. Confirmed serialization/deadlock rollbacks may
retry the complete transaction with the same unique operation identifier.
"""
import argparse
import concurrent.futures
import json
import os
import pathlib
import shlex
import subprocess
import threading
import time
import uuid

def main():
    import psycopg2

    parser = argparse.ArgumentParser()
    parser.add_argument('--out', required=True)
    parser.add_argument('--port', type=int, default=55439)
    parser.add_argument('--acks', type=int, default=10000)
    parser.add_argument('--ssh-host', required=True)
    parser.add_argument('--remote-data', required=True,
                        help='Absolute path of this audit\'s dedicated database')
    parser.add_argument('--pid-file', required=True,
                        help='Absolute path of this audit\'s server PID file')
    parser.add_argument('--restart-command', required=True,
                        help='Remote command that restarts ONLY this audit server and waits for readiness')
    args = parser.parse_args()
    if args.acks < 1:
        parser.error('--acks must be positive')
    for value in (args.remote_data, args.pid_file):
        path = pathlib.PurePosixPath(value)
        if not path.is_absolute() or len(path.parts) < 4 or '..' in path.parts:
            parser.error('remote paths must be specific absolute audit-owned paths')
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    ledger = (out / 'client-events.jsonl').open('w', buffering=1)
    lock = threading.Lock()
    stop = threading.Event()
    crashing = threading.Event()
    acknowledged = set()
    attempted = set()
    failures = []
    run = uuid.uuid4().hex

    def event(kind, **values):
        with lock:
            ledger.write(json.dumps(dict(kind=kind, time_ns=time.time_ns(), **values)) + '\n')
            ledger.flush()
            os.fsync(ledger.fileno())
            if kind == 'attempt':
                attempted.add(values['id'])
            elif kind == 'ack':
                acknowledged.add(values['id'])

    def connect():
        return psycopg2.connect(host='127.0.0.1', port=args.port, user='bicdb',
                                password='x', dbname='bicdb', connect_timeout=5)

    c = connect()
    c.autocommit = True
    with c.cursor() as q:
        q.execute('CREATE TABLE audit_accounts (id INTEGER PRIMARY KEY, balance BIGINT NOT NULL)')
        q.execute('CREATE TABLE audit_operations (id TEXT PRIMARY KEY, amount INTEGER NOT NULL)')
        q.execute('INSERT INTO audit_accounts VALUES (1,1000000),(2,1000000)')
    c.close()

    def worker(index):
        c = connect()
        try:
            n = 0
            while not stop.is_set():
                op = f'{run}-{index}-{n}'
                n += 1
                event('attempt', id=op)
                for retry in range(100):
                    try:
                        with c.cursor() as q:
                            q.execute('INSERT INTO audit_operations VALUES (%s,1)', (op,))
                            q.execute('UPDATE audit_accounts SET balance=balance-1 WHERE id=1')
                            q.execute('UPDATE audit_accounts SET balance=balance+1 WHERE id=2')
                        c.commit()
                        event('ack', id=op, retry=retry)
                        break
                    except psycopg2.Error as exc:
                        if exc.pgcode in ('40001', '40P01'):
                            c.rollback()
                            event('confirmed_abort', id=op, sqlstate=exc.pgcode, retry=retry)
                            if stop.is_set():
                                break
                            continue
                        event('unknown', id=op, sqlstate=exc.pgcode, error=str(exc))
                        if not crashing.is_set() and not stop.is_set():
                            failures.append(str(exc))
                        return
                else:
                    failures.append(f'exhausted confirmed-abort retries: {op}')
                    stop.set()
        finally:
            c.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(worker, i) for i in range(16)]
        deadline = time.monotonic() + 600
        while len(acknowledged) < args.acks:
            if failures or all(f.done() for f in futures) or time.monotonic() > deadline:
                stop.set()
                raise RuntimeError(f'could not reach crash threshold: {len(acknowledged)}, {failures}')
            time.sleep(0.05)
        crashing.set()
        event('crash_requested', acknowledged=len(acknowledged))
        # Server PID is resolved from this audit's own file and command checked.
        # Compare whole NUL-separated argv entries, never a substring or name
        # glob. Refuse to signal a process not serving the exact dedicated DB.
        kill_script = (
            'set -euo pipefail\n'
            f'pid=$(<{shlex.quote(args.pid_file)})\n'
            '[[ $pid =~ ^[0-9]+$ && $pid -gt 1 ]]\n'
            'mapfile -d "" -t argv < /proc/$pid/cmdline\n'
            'matched=0\n'
            'for ((i=1; i<${#argv[@]}; i++)); do\n'
            f'  if [[ ${{argv[i-1]}} == serve-pg && ${{argv[i]}} == {shlex.quote(args.remote_data)} ]]; then matched=1; fi\n'
            'done\n'
            '[[ $matched == 1 ]]\n'
            'kill -KILL "$pid"\n'
        )
        remote = 'sudo -n bash -c ' + shlex.quote(kill_script)
        subprocess.run(['ssh', '-o', 'BatchMode=yes', args.ssh_host, remote], check=True)
        stop.set()
        for f in futures:
            f.result(timeout=30)
    event('crash_complete', acknowledged=len(acknowledged), attempted=len(attempted))
    subprocess.run(['ssh', '-o', 'BatchMode=yes', args.ssh_host,
                    args.restart_command], check=True)
    c = connect()
    c.autocommit = True
    with c.cursor() as q:
        q.execute('SELECT id,amount FROM audit_operations')
        rows = q.fetchall()
        q.execute('SELECT id,balance FROM audit_accounts ORDER BY id')
        balances = dict(q.fetchall())
    c.close()
    recovered = {row[0] for row in rows}
    missing = sorted(acknowledged - recovered)
    unexpected = sorted(recovered - attempted)
    atomic = balances == {1: 1000000-len(recovered), 2: 1000000+len(recovered)}
    invalid_amounts = sum(amount != 1 for _, amount in rows)
    passed = not (missing or unexpected or failures or invalid_amounts) and atomic and len(rows) == len(recovered)
    result = dict(acknowledged=len(acknowledged), attempted=len(attempted), recovered=len(recovered),
                  missing_acknowledged=missing, unexpected=unexpected, balances=balances,
                  atomic_balances=atomic, duplicate_ids=len(rows)-len(recovered),
                  uncertain_but_committed=len(recovered-acknowledged), failures=failures,
                  invalid_amounts=invalid_amounts, passed=passed,
                  test_kind='persistent-filesystem process SIGKILL, not host or power loss')
    event('verification', **result)
    ledger.close()
    (out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
