#!/usr/bin/env python3
"""Exploratory SSH-mediated event traffic with an independent durable ACK ledger.

The orchestrator/ledger must run outside the data nodes. Actual ClusterEventClient
commands still run on members; SSH/process/fsync overhead is included in goodput.
This is not an independent RPC client or a throughput qualification harness.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import threading
import time
import uuid


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


class Ledger:
    def __init__(self, path):
        self.path = Path(path)
        self.file = self.path.open('x', encoding='utf-8')
        self.lock = threading.Lock()
        self.failed = False
        self.identities = {}
        # The directory entry must survive controller loss too.
        self.file.flush()
        os.fsync(self.file.fileno())
        fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def append(self, record):
        with self.lock:
            if self.failed:
                raise RuntimeError('ledger fenced after a persistence failure')
            if record.get('kind') == 'attempt':
                identity = tuple(record[k] for k in ('namespace', 'key', 'payload_sha256'))
                old = self.identities.get(record['operation_id'])
                if old is not None and old != identity:
                    raise ValueError('retry changed operation identity or payload')
            try:
                self.file.write(json.dumps(record, sort_keys=True) + '\n')
                self.file.flush()
                os.fsync(self.file.fileno())
                if record.get('kind') == 'attempt':
                    self.identities[record['operation_id']] = identity
            except Exception:
                self.failed = True
                raise

    def close(self):
        self.file.close()


def validate_inventory(inventory):
    if inventory.get('format') != 1 or not inventory.get('members'):
        raise ValueError('format1 inventory with existing member contexts required')
    for member in inventory['members']:
        if not re.fullmatch(r'(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9_][A-Za-z0-9_.-]*', member.get('ssh', '')):
            raise ValueError('invalid SSH alias')
        for key in ('binary', 'member_root'):
            value = member.get(key, '')
            if not value.startswith('/') or any(c in value for c in '\n\r\0'):
                raise ValueError('absolute binary/member paths required')


class RemoteEvents:
    def __init__(self, inventory):
        validate_inventory(inventory)
        self.members = inventory['members']

    def call(self, member_index, operation, namespace, key, value=None):
        member = self.members[member_index % len(self.members)]
        command = [member['binary'], 'cluster', 'events', operation,
                   member['member_root'], namespace, key]
        if operation == 'append':
            command.append(encoded(value).decode())
        result = subprocess.run(['ssh', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
                                 '-oConnectTimeout=5', member['ssh'],
                                 ' '.join(shlex.quote(arg) for arg in command)],
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError('remote event call failed: ' + result.stderr[-2000:])
        answer = json.loads(result.stdout)
        if operation == 'append' and type(answer.get('inserted')) is not bool:
            raise RuntimeError('invalid append acknowledgment')
        if operation == 'get' and 'value' not in answer:
            raise RuntimeError('invalid read response')
        return answer


def append_once(ledger, call, operation_id, namespace, key, value, member_index):
    """An uncertain response never becomes an ACK; retries retain exact identity."""
    payload_hash = digest(value)
    ledger.append({'kind': 'attempt', 'unix_ns': time.time_ns(), 'operation_id': operation_id,
                   'namespace': namespace, 'key': key, 'value': value,
                   'payload_sha256': payload_hash})
    before = time.monotonic_ns()
    try:
        result = call(member_index, 'append', namespace, key, value)
        if type(result.get('inserted')) is not bool:
            raise RuntimeError('invalid append acknowledgment')
    except Exception as error:
        ledger.append({'kind': 'uncertain', 'unix_ns': time.time_ns(), 'operation_id': operation_id,
                       'error': str(error)[:2000]})
        return {'acknowledged': False, 'latency_us': (time.monotonic_ns() - before) // 1000}
    elapsed = (time.monotonic_ns() - before) // 1000
    # If persistence fails here, do not return acknowledged=True to the caller.
    ledger.append({'kind': 'ack', 'unix_ns': time.time_ns(), 'operation_id': operation_id,
                   'payload_sha256': payload_hash, 'inserted': result['inserted'],
                   'transport_latency_us': elapsed})
    return {'acknowledged': True, 'latency_us': elapsed}


def acknowledged_records(path):
    attempts, acknowledged = {}, {}
    with Path(path).open() as stream:
        for line_number, line in enumerate(stream, 1):
            # A torn final record is an error, not permission to discard an ACK.
            if not line.endswith('\n'):
                raise ValueError('incomplete ledger line ' + str(line_number))
            record = json.loads(line)
            operation_id = record['operation_id']
            if record['kind'] == 'attempt':
                if digest(record['value']) != record['payload_sha256']:
                    raise ValueError('attempt payload digest mismatch')
                old = attempts.get(operation_id)
                if old and any(old[k] != record[k] for k in ('namespace', 'key', 'payload_sha256')):
                    raise ValueError('retry changed operation identity or payload')
                attempts[operation_id] = record
            elif record['kind'] == 'ack':
                attempt = attempts.get(operation_id)
                if not attempt or record['payload_sha256'] != attempt['payload_sha256']:
                    raise ValueError('ACK has no matching durable attempt')
                acknowledged[operation_id] = attempt
            elif record['kind'] != 'uncertain':
                raise ValueError('unknown ledger record')
    return list(acknowledged.values())



def ack_within_confirmed_off_window(call, control):
    """Strict final evidence window; None means a required boundary is unknown.

    A call must begin after confirmed power-off and complete before the first
    guest-start request. A late response to a pre-fault call cannot prove writes
    were admitted while the victim was off. Live observations are provisional
    until the final guest-start boundary has been recorded.
    """
    off = control.get('power_off_confirmed_unix_ns')
    start = control.get('start_requested_unix_ns')
    if type(off) is not int or type(start) is not int:
        return None
    if off <= 0 or start < off:
        raise ValueError('invalid confirmed-off window')
    before, after = call['started_unix_ns'], call['completed_unix_ns']
    if type(before) is not int or type(after) is not int or after < before:
        raise ValueError('invalid call observation timestamps')
    return call.get('acknowledged') is True and before >= off and after < start


def verify(path, call):
    records = acknowledged_records(path)
    failures = []
    for index, record in enumerate(records):
        try:
            actual = call(index, 'get', record['namespace'], record['key'])['value']
            if actual != record['value']:
                failures.append({'operation_id': record['operation_id'], 'reason': 'payload mismatch'})
        except Exception as error:
            failures.append({'operation_id': record['operation_id'], 'reason': str(error)[:2000]})
    return {'checked_acknowledged_operations': len(records), 'failures': failures,
            'verification_passed': bool(records) and not failures}


def run_traffic(args, remote, ledger):
    run_id = uuid.uuid4().hex
    deadline = time.monotonic() + args.seconds
    counter, counter_lock = 0, threading.Lock()
    started = time.monotonic()
    samples, uncertain = [], 0
    def worker(worker_index):
        nonlocal counter
        local_samples, local_uncertain = [], 0
        while time.monotonic() < deadline:
            with counter_lock:
                if counter >= args.max_operations:
                    break
                index = counter
                counter += 1
            key = f'{run_id}-{index}'
            value = {'run': run_id, 'index': index, 'payload': 'x' * args.payload_bytes}
            result = append_once(ledger, remote.call, key, args.namespace, key, value, worker_index)
            if result['acknowledged']:
                local_samples.append(result['latency_us'])
            else:
                local_uncertain += 1
        return local_samples, local_uncertain
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for result in pool.map(worker, range(args.concurrency)):
            samples.extend(result[0])
            uncertain += result[1]
    elapsed = time.monotonic() - started
    samples.sort()
    def percentile(p):
        return samples[max(0, (len(samples) * p + 99) // 100 - 1)] if samples else None
    return {'run_id': run_id, 'seconds': elapsed, 'attempted_operations': counter,
            'acknowledged_operations': len(samples), 'uncertain_operations': uncertain,
            'acknowledged_operations_per_second': len(samples) / elapsed,
            'ssh_append_latency_us': {'p50': percentile(50), 'p95': percentile(95),
                                      'p99': percentile(99), 'max': max(samples) if samples else None}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    traffic = sub.add_parser('traffic')
    traffic.add_argument('--namespace', required=True)
    traffic.add_argument('--output', type=Path, required=True, help='new directory on independent driver')
    traffic.add_argument('--seconds', type=int, default=60)
    traffic.add_argument('--concurrency', type=int, default=4)
    traffic.add_argument('--max-operations', type=int, default=10000)
    traffic.add_argument('--payload-bytes', type=int, default=1024)
    check = sub.add_parser('verify')
    check.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args()
    remote = RemoteEvents(json.loads(args.inventory.read_text()))
    if args.command == 'verify':
        result = verify(args.ledger, remote.call)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['verification_passed'] else 2
    if not (1 <= args.seconds <= 3600 and 1 <= args.concurrency <= 64
            and 1 <= args.max_operations <= 1000000 and 0 <= args.payload_bytes <= 32768):
        parser.error('invalid workload bounds')
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    ledger = Ledger(args.output / 'ledger.jsonl')
    try:
        result = run_traffic(args, remote, ledger)
    finally:
        ledger.close()
    result.update({'format': 1, 'qualification_status': 'not_qualified',
                   'classification': 'ssh_mediated_external_ack_ledger',
                   'orchestrator_and_ledger_placement': 'operator_asserted_independent_driver',
                   'rpc_client_placement': 'data_members', 'fsync_per_attempt_and_ack': True,
                   'throughput_certification': False,
                   'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    report = args.output / 'report.json'
    with report.open('x') as output:
        output.write(json.dumps(result, indent=2, sort_keys=True) + '\n')
        output.flush()
        os.fsync(output.fileno())
    print(json.dumps(result, sort_keys=True))
    return 0 if result['acknowledged_operations'] and not result['uncertain_operations'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
