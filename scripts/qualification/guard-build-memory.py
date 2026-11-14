#!/usr/bin/env python3
"""Monitor one disposable build group; never signal a reaped leader's group ID."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--log', type=Path, required=True)
p.add_argument('--min-available-mib', type=int, default=1024)
p.add_argument('--interval-seconds', type=float, default=2)
p.add_argument('--low-samples', type=int, default=2)
p.add_argument('command', nargs=argparse.REMAINDER)
a = p.parse_args()
command = a.command[1:] if a.command[:1] == ['--'] else a.command
if not command or a.min_available_mib < 0 or a.interval_seconds <= 0 or a.low_samples < 1:
    p.error('supply a command and positive monitoring intervals/sample count')
a.log.parent.mkdir(parents=True, exist_ok=True)
log = a.log.open('a', buffering=1)
started = time.monotonic()
samples = omitted_samples = requested_signal = 0
child = None
result = None

def record(event, **fields):
    global samples, omitted_samples
    if event == 'sample':
        if samples >= 4096:
            omitted_samples += 1
            return
        samples += 1
    log.write(json.dumps({'event': event, 'elapsed_seconds': time.monotonic()-started,
                          'omitted_sample_records': omitted_samples, **fields}, sort_keys=True)+'\n')

def memory():
    values = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        name, value = line.split(':', 1)
        if name in {'MemAvailable', 'SwapFree'}:
            values[name] = int(value.strip().split()[0])*1024
    values['pressure'] = Path('/proc/pressure/memory').read_text().strip()[:1024]
    return values

def request_stop(signum, _frame):
    global requested_signal
    requested_signal = signum

# Handlers only set state, so they cannot throw between spawn and ownership.
for signum in (signal.SIGINT, signal.SIGTERM):
    signal.signal(signum, request_stop)
signal.signal(signal.SIGCHLD, signal.SIG_DFL)

def leader_exit():
    # WNOWAIT retains the leader PID, including when it has already exited.
    # Never use Popen.poll()/wait() before the last group signal.
    return os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)

def group_signal(signum):
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass

try:
    child = subprocess.Popen(command, start_new_session=True)
    record('started', child_pid=child.pid, process_group=child.pid,
           minimum_available_bytes=a.min_available_mib*1024*1024, command=command)
    low = 0
    while leader_exit() is None:
        if requested_signal:
            record('caller_interrupted', signal=requested_signal)
            result = 128+requested_signal
            break
        sample = memory()
        record('sample', **sample)
        low = low+1 if sample['MemAvailable'] < a.min_available_mib*1024*1024 else 0
        if low >= a.low_samples:
            record('resource_guard_interrupted_build', low_samples=low, **sample)
            print('RESOURCE_GUARD_INTERRUPTED_BUILD: task-owned build stopped for low memory',
                  file=sys.stderr, flush=True)
            result = 75
            break
        time.sleep(a.interval_seconds)
except Exception as error:
    result = 74
    try:
        record('monitor_failed', error_type=type(error).__name__, detail=str(error)[:2048])
    except Exception:
        pass
    print(f'BUILD_MONITOR_FAILED: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
finally:
    if child is not None:
        # All signals occur while the leader PID remains reserved. Killing the
        # disposable group's remaining descendants is intentional even after
        # Cargo exits: none of these build/check/list processes may survive.
        group_signal(signal.SIGTERM)
        until = time.monotonic()+5
        while leader_exit() is None and time.monotonic() < until:
            time.sleep(0.01)
        group_signal(signal.SIGKILL)
        code = child.wait()  # First reap, after the final group signal.
        if result is None:
            result = code if code >= 0 else 128-code
        try:
            record('completed', child_returncode=code, guard_returncode=result,
                   final_group_signal_before_leader_reap=True)
        except Exception:
            pass
        print(f'OWNED_BUILD_GROUP_CLEANED pid={child.pid} returncode={code}',
              file=sys.stderr, flush=True)
    try:
        log.close()
    except Exception:
        pass
raise SystemExit(result if result is not None else 74)
