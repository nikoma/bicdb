#!/usr/bin/env python3
"""Exercise event traffic during node growth (including 4 -> 5) and failure.

No cloud provisioning or system-wide process management. Remote mode uses
existing SSH hosts, verifies distinct machine IDs and pins the supplied binary.
Local mode is always labelled a shared-host functional simulation.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import ipaddress
import json
import os
import re
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid


WORKER = r'''
import base64, hashlib, json, os, pathlib, signal, subprocess, sys, time
r=json.loads(base64.b64decode(sys.argv[1])); root=pathlib.Path(r['root']); action=r['action']
def owned():
    p=json.loads((root/'process.json').read_text()); pid=p['pid']
    try:
        stat=pathlib.Path(f'/proc/{pid}/stat').read_text().split()
        command=pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
    except FileNotFoundError: return None
    if stat[2]=='Z': return None
    if stat[21]!=p['start'] or str(root/'db').encode() not in command:
        raise RuntimeError('refusing to signal a process outside this run')
    return pid
if action=='probe':
    machine=pathlib.Path('/etc/machine-id').read_text().strip()
    if not machine: raise RuntimeError('missing machine identity')
    print(json.dumps({'machine_hash':hashlib.sha256(machine.encode()).hexdigest(),
      'cpu_count':os.cpu_count(),'kernel':os.uname().release,
      'meminfo':pathlib.Path('/proc/meminfo').read_text().splitlines()[:3]}))
elif action=='mkdir':
    root.mkdir(mode=0o700,parents=False,exist_ok=False); (root/'tls').mkdir(mode=0o700)
    print(json.dumps({'created':True}))
elif action=='run':
    p=subprocess.run(r['argv'],capture_output=True,text=True,timeout=r.get('timeout',180),cwd=root)
    print(json.dumps({'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}))
elif action=='launch':
    with (root/'server.log').open('ab') as log:
        p=subprocess.Popen(r['argv'],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,cwd=root)
    start=pathlib.Path(f'/proc/{p.pid}/stat').read_text().split()[21]
    (root/'process.json.tmp').write_text(json.dumps({'pid':p.pid,'start':start}))
    os.replace(root/'process.json.tmp',root/'process.json')
    print(json.dumps({'pid':p.pid}))
elif action=='signal':
    pid=owned()
    if pid: os.kill(pid, getattr(signal,r['signal']))
    print(json.dumps({'pid':pid}))
elif action=='sample':
    pid=owned()
    if not pid: print(json.dumps({'running':False}))
    else:
        try:
            sample={'running':True,'pid':pid,
              'stat':pathlib.Path(f'/proc/{pid}/stat').read_text(),
              'status':pathlib.Path(f'/proc/{pid}/status').read_text(),
              'io':pathlib.Path(f'/proc/{pid}/io').read_text(),
              'clock_ticks':os.sysconf('SC_CLK_TCK'), 'page_size':os.sysconf('SC_PAGE_SIZE')}
        except (FileNotFoundError, ProcessLookupError): sample={'running':False}
        print(json.dumps(sample))
elif action=='read':
    p=root/r['file']
    if p.stat().st_size>128*1024*1024: raise RuntimeError('artifact too large')
    print(json.dumps({'contents':p.read_text()}))
else: raise RuntimeError('unknown runner action')
'''


class Host:
    def __init__(self, spec, index, run, binary, local, storage_mode=None, buffer_pool_bytes=None):
        self.spec, self.index, self.local = spec, index, local
        self.root = f"{spec.get('directory', '/tmp')}/bicdb-scale-{run}-n{index}"
        self.binary = str(binary) if local else self.root + '/bicdb'
        self.db = self.root + '/db'
        self.address = spec['address']
        self.sql_port = spec.get('sql_port', 5432)
        self.engine_env = storage_environment(storage_mode, buffer_pool_bytes)
        self.tls = [] if local else [
            '--cluster-tls-cert', self.root + '/tls/node.crt',
            '--cluster-tls-key', self.root + '/tls/node.key',
            '--cluster-tls-ca', self.root + '/tls/ca.crt']

    def action(self, action, **args):
        payload = base64.b64encode(json.dumps(dict(root=self.root, action=action, **args)).encode()).decode()
        command = ['python3', '-c', WORKER, payload]
        if not self.local:
            command = ['ssh', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes', self.spec['ssh'],
                       ' '.join(shlex.quote(value) for value in command)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=args.get('timeout', 180) + 20)
        if result.returncode:
            raise RuntimeError(f"node {self.index} {action}: {result.stderr[-4000:]}")
        return json.loads(result.stdout)

    def command(self, *args, timeout=180):
        result = self.action('run', argv=[*self.engine_env, self.binary, *map(str, args)], timeout=timeout)
        if result['code']:
            raise RuntimeError(f"node {self.index}: {result['stderr'][-4000:]}\n{result['stdout'][-4000:]}")
        return result['stdout']

    def start(self):
        return self.action('launch', argv=[*self.engine_env, self.binary, 'serve', self.db, '--host', '127.0.0.1',
                           '--port', str(self.sql_port), '--shutdown-grace-seconds', '3'])

    def copy(self, source, destination):
        subprocess.run(['scp', '-q', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes', str(source),
                        f"{self.spec['ssh']}:{self.root}/{destination}"], check=True, timeout=120)

    def status(self):
        return json.loads(self.command('cluster', 'status', self.db, '--json'))



def validate_storage_options(storage_mode, buffer_pool_bytes, node_capacity_bytes):
    if storage_mode not in (None, 'embedded_memory', 'server_paged'):
        raise ValueError('invalid explicit storage mode')
    if buffer_pool_bytes is not None and (storage_mode != 'server_paged' or not 8192 <= buffer_pool_bytes <= 1024**4):
        raise ValueError('buffer pool requires explicit server_paged and 8192..1099511627776 bytes')
    if not 1 <= node_capacity_bytes <= 2**64 - 1:
        raise ValueError('node capacity must be a positive u64 byte count')


def storage_environment(storage_mode, buffer_pool_bytes):
    environment = []
    if storage_mode is not None:
        environment.append('BICDB_STORAGE_MODE=' + storage_mode)
    if buffer_pool_bytes is not None:
        environment.append('BICDB_PAGED_BUFFER_POOL_BYTES=' + str(buffer_pool_bytes))
    return ['env', *environment] if environment else []


def persisted_storage(hosts):
    records = []
    for host in hosts:
        metadata = json.loads(host.action('read', file='db/format.json')['contents'])
        records.append({'node': host.index, 'format': metadata})
    return records


def validate_persisted_storage(records, requested_mode):
    if requested_mode is not None:
        for record in records:
            actual = record['format'].get('storage_mode')
            if actual != requested_mode:
                raise RuntimeError(f"node {record['node']} persisted storage mode {actual!r} differs from requested {requested_mode!r}")


def validate_inventory(specs, count):
    if len(specs) < count:
        raise ValueError(f'{count} existing hosts are required')
    for spec in specs[:count]:
        if not isinstance(spec.get('ssh'), str) or not re.fullmatch(r'(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9_][A-Za-z0-9_.-]*', spec['ssh']):
            raise ValueError('each host needs a valid SSH alias')
        if any(ch.isspace() for ch in spec['ssh']):
            raise ValueError('SSH aliases cannot contain whitespace')
        address, port = spec['address'].rsplit(':', 1)
        ipaddress.IPv4Address(address)
        if not 1 <= int(port) <= 65535:
            raise ValueError('invalid data RPC port')
        directory = spec.get('directory', '/tmp')
        if not directory.startswith('/') or any(c in directory for c in '\n\r:'):
            raise ValueError('host directory must be absolute')
    if len({s['address'] for s in specs[:count]}) != count:
        raise ValueError('data RPC addresses must be unique')


def capacity_ready(status, count):
    topology, metrics = status['topology'], status['metrics']
    return (len(topology['nodes']) == count
            and all(node.get('metadata_role', 'voter') == 'voter' for node in topology['nodes'].values())
            and metrics['under_replicated_ranges'] == 0 and metrics['unavailable_ranges'] == 0
            and metrics['active_relocations'] == 0
            and metrics.get('failed_relocations', 0) == 0
            and len({r['leader'] for r in topology['ranges'].values()}) == count)


def parse_sizes(value):
    """Keep growth bounded and preserve the supported historical profiles."""
    try:
        sizes = [int(size) for size in value.split(',')]
    except ValueError as error:
        raise ValueError('sizes must be an increasing selection of 3,4,5,9') from error
    if sizes != sorted(set(sizes)) or any(size not in (3, 4, 5, 9) for size in sizes):
        raise ValueError('sizes must be an increasing selection of 3,4,5,9')
    return sizes


def resource_summary(samples):
    nodes, previous = {}, {}
    for sample in samples:
        for index, node in enumerate(sample['nodes']):
            if not node['running']: continue
            result = nodes.setdefault(str(index), dict(max_rss_bytes=0, peak_cpu_percent=0,
                                                       sampled_read_bytes=0, sampled_write_bytes=0))
            status = dict(line.split(':', 1) for line in node['status'].splitlines() if ':' in line)
            rss = int(status.get('VmRSS', '0 kB').split()[0]) * 1024
            result['max_rss_bytes'] = max(result['max_rss_bytes'], rss)
            stat = node['stat'].split()
            cpu = int(stat[13]) + int(stat[14])
            io = dict((key, int(value)) for key, value in (line.split(':', 1) for line in node['io'].splitlines()))
            old = previous.get(index)
            if old and old[0] == node['pid']:
                seconds = (sample['unix_ms'] - old[1]) / 1000
                if seconds > 0:
                    result['peak_cpu_percent'] = max(result['peak_cpu_percent'],
                        max(0, cpu - old[2]) / node['clock_ticks'] / seconds * 100)
                result['sampled_read_bytes'] += max(0, io['read_bytes'] - old[3]['read_bytes'])
                result['sampled_write_bytes'] += max(0, io['write_bytes'] - old[3]['write_bytes'])
            previous[index] = (node['pid'], sample['unix_ms'], cpu, io)
    return nodes


def ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets: sock.bind(('127.0.0.1', 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets: sock.close()


def certificates(hosts, directory):
    def openssl(*args):
        subprocess.run(['openssl', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ca, key = directory / 'ca.crt', directory / 'ca.key'
    openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '7', '-subj', '/CN=BicDB scale test CA', '-keyout', key, '-out', ca)
    key.chmod(0o600)
    for host in hosts:
        prefix = directory / f'n{host.index}'
        address = host.address.rsplit(':', 1)[0]
        ext = prefix.with_suffix('.ext')
        ext.write_text(f'subjectAltName=IP:{address}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n')
        openssl('req', '-newkey', 'rsa:2048', '-nodes', '-subj', f'/CN=n{host.index}', '-keyout', str(prefix)+'.key', '-out', str(prefix)+'.csr')
        Path(str(prefix)+'.key').chmod(0o600)
        openssl('x509', '-req', '-in', str(prefix)+'.csr', '-CA', ca, '-CAkey', key, '-CAcreateserial', '-days', '7', '-extfile', ext, '-out', str(prefix)+'.crt')
        for source, target in [(ca, 'ca.crt'), (Path(str(prefix)+'.key'), 'node.key'), (Path(str(prefix)+'.crt'), 'node.crt')]:
            host.copy(source, 'tls/' + target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--inventory', type=Path)
    mode.add_argument('--local', action='store_true')
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--sizes', default='3,5,9')
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--failure-seconds', type=int,
                        help='override the crash/restart traffic window independently')
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--rpc-timeout-ms', type=int, default=1000)
    parser.add_argument('--max-operations', type=int, default=100000)
    parser.add_argument('--ranges', type=int, default=64)
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--storage-mode', choices=('embedded_memory', 'server_paged'),
                        help='explicit storage mode for init/join/serve and all engine commands; omitted preserves inherited defaults')
    parser.add_argument('--buffer-pool-bytes', type=int, help='explicit server_paged buffer budget per node')
    parser.add_argument('--node-capacity-bytes', type=int, default=1024**4,
                        help='declared placement capacity per node, not measured storage capacity')
    parser.add_argument('--build-label', default='unspecified')
    parser.add_argument('--source-commit', help='source Git object used to build the supplied binary')
    parser.add_argument('--require-independent-qualification', action='store_true',
                        help='fail closed: this event harness does not yet supply independent qualification')
    args = parser.parse_args()
    if args.require_independent_qualification:
        parser.error('independent qualification unavailable: first-data-node client and end-of-phase ledger; '
                     'external per-acknowledgment durability and relocation-phase fault injection are not implemented')
    if args.source_commit and not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', args.source_commit):
        parser.error('source-commit must be a full lowercase Git object ID')
    try:
        validate_storage_options(args.storage_mode, args.buffer_pool_bytes, args.node_capacity_bytes)
    except ValueError as error:
        parser.error(str(error))
    failure_seconds = args.failure_seconds if args.failure_seconds is not None else args.seconds
    try:
        sizes = parse_sizes(args.sizes)
    except ValueError as error:
        parser.error(str(error))
    if not (5 <= args.seconds <= 3600 and 5 <= failure_seconds <= 3600 and 1 <= args.concurrency <= 256
            and 100 <= args.rpc_timeout_ms <= 5000
            and 1 <= args.max_operations <= 1000000 and max(sizes) <= args.ranges <= 1024
            and args.ranges & (args.ranges - 1) == 0):
        parser.error('invalid workload bounds; ranges must be a power of two and at least the node count')
    binary = args.binary.resolve(strict=True)
    run = uuid.uuid4().hex
    if args.local:
        available = ports(max(sizes) * 2)
        specs = [dict(address=f'127.0.0.1:{available[i*2]}', sql_port=available[i*2+1]) for i in range(max(sizes))]
    else:
        inventory = json.loads(args.inventory.read_text())
        if inventory.get('format') != 1: parser.error('unsupported inventory format')
        specs = inventory['hosts']
        validate_inventory(specs, max(sizes))
    hosts = [Host(spec, i, run, binary, args.local, args.storage_mode, args.buffer_pool_bytes) for i, spec in enumerate(specs[:max(sizes)])]
    identities = [host.action('probe') for host in hosts]
    if not args.local and len({identity['machine_hash'] for identity in identities}) != len(hosts):
        parser.error('remote scale results require distinct machine IDs; use --local for shared-host checks')
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(format=1, run=run, classification='shared_host_functional_simulation' if args.local else 'distinct_machine_measurement',
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), build_label=args.build_label,
                  source_commit=args.source_commit,
                  workload='immutable_events_append_read', replication_factor=3, fsync=True,
                  ranges=args.ranges, machines=identities, results=[], complete=False,
                  node_counts=sizes, timeline=[],
                  requested_storage_mode=args.storage_mode, requested_buffer_pool_bytes=args.buffer_pool_bytes,
                  node_capacity_bytes=args.node_capacity_bytes, engine_environment=storage_environment(args.storage_mode, args.buffer_pool_bytes),
                  persisted_storage_checkpoints=[],
                  client_placement='first_data_node',
                  acknowledged_ledger_placement='first_data_node_copied_after_phase',
                  physical_failure_domains_verified=False,
                  qualification_status='not_qualified',
                  independent_user_adoption='not_measured', large_scale_certification=False)
    active, ledgers = [], []
    save_lock = threading.Lock()
    def save():
        # Growth and workload threads both checkpoint evidence. Never expose a
        # partial JSON report to an external collector or a concurrent reader.
        with save_lock:
            temporary = args.output / 'report.json.tmp'
            with temporary.open('w') as output:
                output.write(json.dumps(report, indent=2)+'\n')
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, args.output / 'report.json')
    def record(name, value):
        (args.output / (name + '.json')).write_text(json.dumps(value, indent=2)+'\n')
    def event(kind, **details):
        report['timeline'].append(dict(unix_ms=int(time.time()*1000), kind=kind, **details))
        save()
    def wait_ready(count):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status = hosts[0].status()
            if capacity_ready(status, count):
                record(f'topology-{count}-{time.time_ns()}', status)
                formats = persisted_storage(active)
                report['persisted_storage_checkpoints'].append({'nodes': count, 'formats': formats})
                save()
                validate_persisted_storage(formats, args.storage_mode)
                event('capacity_converged', nodes=count)
                time.sleep(2)  # Range leadership recovery follows topology publication.
                print(f'{count}-node capacity converged', file=sys.stderr, flush=True)
                return
            time.sleep(1)
        raise RuntimeError(f'{count}-node capacity did not converge before timeout')
    def add(host):
        event('join_started', node=host.index)
        host.command('cluster', 'join', '--seed-node-id', 'n0', '--seed-address', hosts[0].address,
                     '--cluster-id', 'scale-' + run, '--node-id', f'n{host.index}', '--address', host.address,
                     '--node-root', host.db, '--capacity-bytes', str(args.node_capacity_bytes), *host.tls)
        event('join_registered', node=host.index)
        host.start(); active.append(host)
        event('serve_started', node=host.index)
    def traffic(name, seconds=None):
        print(f'traffic phase: {name}', file=sys.stderr, flush=True)
        seconds = args.seconds if seconds is None else seconds
        event('traffic_started', phase=name, seconds=seconds)
        ledger = hosts[0].root + f'/{name}-ledger.json'
        result = json.loads(hosts[0].command('cluster', 'events', 'workload', hosts[0].db,
                      '--namespace', 'scale-' + run, '--seconds', seconds, '--concurrency', args.concurrency,
                      '--rpc-timeout-ms', args.rpc_timeout_ms,
                      '--max-operations', args.max_operations, '--ledger-out', ledger, timeout=seconds + args.timeout))
        ledgers.append((name, ledger)); report['results'].append(dict(phase=name, **result)); save()
        record(name, result)
        record(name + '-ledger', json.loads(hosts[0].action('read', file=f'{name}-ledger.json')['contents']))
        event('traffic_completed', phase=name)
        return result
    def monitored(pool, name, during=None, seconds=None):
        task = pool.submit(traffic, name, seconds)
        change = pool.submit(during) if during else None
        samples = []
        while not task.done() or (change and not change.done()):
            samples.append(dict(unix_ms=int(time.time()*1000), nodes=[host.action('sample') for host in active]))
            time.sleep(1)
        record(name + '-resources', samples)
        if change: change.result()
        result = task.result()
        result['resources'] = resource_summary(samples)
        next(row for row in report['results'] if row['phase'] == name)['resources'] = result['resources']
        save()
        return result
    save()
    try:
        for host in hosts:
            host.action('mkdir')
            if not args.local:
                host.copy(binary, 'bicdb')
                host.action('run', argv=['chmod', '700', host.binary])
                copied = host.action('run', argv=['sha256sum', host.binary])
                if copied['code'] or copied['stdout'].split()[0] != report['binary_sha256']:
                    raise RuntimeError(f'node {host.index} binary hash mismatch')
        if not args.local:
            with tempfile.TemporaryDirectory(prefix='bicdb-scale-tls-') as directory:
                certificates(hosts, Path(directory))
        hosts[0].command('cluster', 'init', hosts[0].db, '--cluster-id', 'scale-' + run,
                         '--node-id', 'n0', '--address', hosts[0].address, '--replication-factor', '3',
                         '--initial-ranges', args.ranges, '--capacity-bytes', args.node_capacity_bytes, '--application-events', *hosts[0].tls)
        hosts[0].start(); active.append(hosts[0]); time.sleep(2)
        for host in hosts[1:sizes[0]]: add(host)
        wait_ready(sizes[0])
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            monitored(pool, f'steady-{sizes[0]}')
            for size in sizes[1:]:
                previous = len(active)
                monitored(pool, f'growth-{previous}-to-{size}', lambda: [add(host) for host in hosts[previous:size]])
                wait_ready(size)
                monitored(pool, f'steady-{size}')
            victim = active[-1]
            def failure():
                time.sleep(max(1, failure_seconds // 4))
                event('process_kill_requested', node=victim.index)
                victim.action('signal', signal='SIGKILL')
                event('process_kill_returned', node=victim.index)
                time.sleep(max(1, failure_seconds // 3))
                victim.start()
                event('process_restart_started', node=victim.index)
            monitored(pool, f'failure-{len(active)}', failure, seconds=failure_seconds)
            wait_ready(len(active))
            monitored(pool, f'recovered-{len(active)}')
        report['post_recovery_verification'] = []
        for name, ledger in ledgers:
            verification = json.loads(hosts[0].command('cluster', 'events', 'verify', hosts[0].db, ledger, timeout=args.timeout))
            report['post_recovery_verification'].append(dict(phase=name, **verification))
        report['complete'] = True; save()
        print(json.dumps(dict(output=str(args.output), classification=report['classification'], complete=True)))
    except Exception as error:
        report['error'] = str(error); save()
        raise
    finally:
        for host in active:
            try:
                record(f'node-{host.index}-log', host.action('read', file='server.log'))
                host.action('signal', signal='SIGINT')
            except Exception as error:
                print(f'node {host.index} cleanup: {error}', file=sys.stderr)
        # Only this run's PIDs, verified against /proc start time and DB argv.
        time.sleep(4)
        for host in active:
            try: host.action('signal', signal='SIGKILL')
            except Exception: pass


if __name__ == '__main__': main()
