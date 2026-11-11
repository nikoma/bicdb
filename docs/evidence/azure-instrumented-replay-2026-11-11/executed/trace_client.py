#!/usr/bin/env python3
"""Controller-side diagnostic event client. Frozen461 policy replica, not stock CLI.
All logical operations, individual event/discovery RPCs, wire responses and errors
are journaled externally. Member-side wire workers retain their private keys.
"""
import argparse, concurrent.futures, hashlib, json, os, pathlib, queue, re, resource, selectors, shutil, signal, struct, subprocess, sys, threading, time, uuid
POLICY_SOURCE = 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb'

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def token(namespace, key):
    a = namespace.encode()
    b = key.encode()
    return int.from_bytes(hashlib.sha256(b'bicdb-distribution-key-v1\x00' + struct.pack('>Q', len(a)) + a + struct.pack('>Q', len(b)) + b).digest()[:8], 'big')

class TraceFailure(RuntimeError):

    def __init__(self, detail):
        self.detail = detail
        super().__init__(json.dumps(detail, ensure_ascii=False))

class LedgerFailure(RuntimeError):
    pass

class Journal:

    def __init__(self, path, max_bytes=2 * 1024 ** 3, min_free_bytes=4 * 1024 ** 3):
        self.path = pathlib.Path(path)
        self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
        self.lock = threading.Lock()
        self.sequence = 0
        self.previous = '0' * 64
        self.bytes = 0
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_bytes
        self.failed = False
        directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def append(self, kind, **fields):
        with self.lock:
            if self.failed:
                raise LedgerFailure('journal already fenced')
            try:
                available = os.statvfs(self.path.parent)
                free = available.f_bavail * available.f_frsize
                if free < self.min_free_bytes:
                    raise OSError('journal disk headroom exhausted')
                row = {'sequence': self.sequence, 'previous_sha256': self.previous, 'kind': kind, 'controller_unix_ns': time.time_ns(), 'controller_monotonic_ns': time.monotonic_ns(), **fields}
                checksum = sha(row)
                row['record_sha256'] = checksum
                data = canonical(row) + b'\n'
                if self.bytes + len(data) > self.max_bytes:
                    raise OSError('journal byte ceiling exhausted')
                started = time.monotonic_ns()
                view = memoryview(data)
                while view:
                    count = os.write(self.fd, view)
                    if count <= 0:
                        raise OSError('journal made no write progress')
                    view = view[count:]
                os.fsync(self.fd)
                receipt = {'sequence': self.sequence, 'record_sha256': checksum, 'fsync_elapsed_ns': time.monotonic_ns() - started, 'durable_unix_ns': time.time_ns(), 'durable_monotonic_ns': time.monotonic_ns()}
                self.sequence += 1
                self.previous = checksum
                self.bytes += len(data)
                return receipt
            except BaseException as error:
                self.failed = True
                raise LedgerFailure(str(error)) from error

    def close(self):
        os.close(self.fd)

def read_journal(path):
    previous = '0' * 64
    with open(path, 'rb') as stream:
        for index, line in enumerate(stream):
            if not line.endswith(b'\n'):
                raise ValueError('torn journal tail')
            row = json.loads(line)
            checksum = row.pop('record_sha256')
            if row['sequence'] != index or row['previous_sha256'] != previous or sha(row) != checksum:
                raise ValueError('journal chain/sequence/digest mismatch')
            row['record_sha256'] = checksum
            previous = checksum
            yield row

class WireWorker:

    def __init__(self, command, index, journal, output, ready_timeout=30, absolute_deadline=None):
        self.index = index
        self.journal = journal
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.sent_generation = None
        self.channel_error = None
        self.stderr_error = None
        self.stderr_bytes = 0
        self.absolute_deadline = absolute_deadline
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.select = selectors.DefaultSelector()
        self.select.register(self.process.stdout, selectors.EVENT_READ)
        self.stderr_path = pathlib.Path(output) / f'worker-{index}.stderr'
        self.stderr_thread = threading.Thread(target=self._stderr, daemon=True)
        self.stderr_thread.start()
        try:
            self.ready = self._line(ready_timeout)
            assert self.ready['kind'] == 'ready'
            self.sent_generation = self.ready['topology']['generation']
            self.journal.append('worker_ready', worker=index, controller_child_pid=self.process.pid, controller_child_start_ticks=pathlib.Path(f'/proc/{self.process.pid}/stat').read_text().split()[21], command=command, ready=self.ready)
        except BaseException:
            self.stop()
            raise

    def _stderr(self):
        fd = None
        try:
            fd = os.open(self.stderr_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
            while True:
                chunk = self.process.stderr.read(65536)
                if not chunk:
                    break
                usage = os.statvfs(self.stderr_path.parent)
                if self.stderr_bytes + len(chunk) > 16 * 1024 ** 2 or usage.f_bavail * usage.f_frsize < self.journal.min_free_bytes:
                    raise OSError('stderr capture budget/headroom exhausted')
                view = memoryview(chunk)
                while view:
                    count = os.write(fd, view)
                    if count <= 0:
                        raise OSError('stderr capture made no write progress')
                    self.stderr_bytes += count
                    view = view[count:]
                os.fsync(fd)
        except BaseException as error:
            self.stderr_error = {'category': 'worker_stderr_capture_failed', 'type': type(error).__name__, 'message': str(error), 'captured_bytes': self.stderr_bytes}
            try:
                self.journal.append('diagnostic_capture_fenced', worker=self.index, error=self.stderr_error)
            except LedgerFailure:
                pass
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def _line(self, timeout):
        deadline = time.monotonic() + timeout
        while b'\n' not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.select.select(max(0, remaining)):
                raise TraceFailure({'category': 'worker_response_timeout', 'timeout_seconds': timeout, 'worker': self.index, 'response_unknown': True})
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise TraceFailure({'category': 'worker_disconnected', 'exit_code': self.process.poll(), 'worker': self.index, 'response_unknown': True})
            self.buffer.extend(chunk)
            if len(self.buffer) > 512 * 1024 ** 2:
                raise TraceFailure({'category': 'worker_frame_ceiling', 'response_unknown': True})
        line, _, rest = self.buffer.partition(b'\n')
        self.buffer = bytearray(rest)
        try:
            return json.loads(line)
        except ValueError as error:
            raise TraceFailure({'category': 'worker_invalid_json', 'raw_response_hex': line.hex(), 'response_unknown': True}) from error

    def rpc(self, context, destination, request, topology, io_ms=5000, connect_ms=500):
        with self.lock:
            if getattr(self, 'channel_error', None):
                raise TraceFailure({'category': 'worker_channel_fenced', 'cause': self.channel_error, 'response_unknown': False, 'no_rpc_dispatched': True})
            if getattr(self, 'absolute_deadline', None) is not None and time.monotonic() >= self.absolute_deadline:
                self.channel_error = {'category': 'controller_budget_exhausted', 'no_rpc_dispatched': True}
                self.journal.append('controller_budget_guard', worker=self.index, reason='overall controller budget exhausted before RPC dispatch')
                raise TraceFailure(self.channel_error)
            rpc_id = uuid.uuid4().hex
            command = {'rpc_id': rpc_id, 'destination_node_id': destination, 'request': request, 'connect_timeout_ms': connect_ms, 'io_timeout_ms': io_ms}
            if topology['generation'] != self.sent_generation:
                command['topology'] = topology
            receipt = self.journal.append('rpc_started', **context, rpc_id=rpc_id, worker=self.index, destination_node_id=destination, cached_topology_generation=topology['generation'], request=request, connect_timeout_ms=connect_ms, io_timeout_ms=io_ms)
            command['wire_request_id'] = receipt['sequence'] + 1
            started = time.monotonic_ns()
            dispatched = time.time_ns()
            try:
                self.process.stdin.write(canonical(command) + b'\n')
                self.process.stdin.flush()
                timeout = (connect_ms + 3 * io_ms) / 1000 + 10
                if getattr(self, 'absolute_deadline', None) is not None:
                    timeout = min(timeout, max(0.001, self.absolute_deadline - time.monotonic()))
                result = self._line(timeout)
                if result.get('rpc_id') != rpc_id or result.get('wire_request_id') != command['wire_request_id']:
                    raise TraceFailure({'category': 'worker_rpc_identity_mismatch', 'result': result, 'response_unknown': True})
                self.sent_generation = topology['generation']
                elapsed = time.monotonic_ns() - started
                sync = self.journal.append('rpc_completed', **context, rpc_id=rpc_id, worker=self.index, dispatch_unix_ns=dispatched, controller_rpc_elapsed_ns=elapsed, started_record_durability=receipt, wire=result)
                if not result.get('transport_success'):
                    raise TraceFailure({'category': 'wire_transport_or_validation_error', 'rpc_id': rpc_id, 'wire': result, 'response_unknown': True})
                envelope = result['response_envelope']
                response = envelope['response']
                if request in ('FetchBootstrapSnapshot', 'FetchTopology') and 'Ok' in response and (not result.get('discovery_topology_validated')):
                    raise TraceFailure({'category': 'unexpected_or_unvalidated_discovery_response', 'wire': result, 'response_unknown': False})
                if 'Err' in response:
                    raise TraceFailure({'category': 'server_remote_error', 'rpc_id': rpc_id, 'code': response['Err']['code'], 'message': response['Err']['message'], 'response_envelope': envelope, 'response_body_raw': result['response_body_raw'], 'response_unknown': False, 'write_outcome_unknown': isinstance(request, dict) and 'ApplicationEvent' in request and (request['ApplicationEvent']['operation']['operation'] == 'append')})
                return (response['Ok'], {'rpc_id': rpc_id, 'result_sync': sync, 'controller_rpc_elapsed_ns': elapsed, 'wire_elapsed_ns': result['elapsed_ns']})
            except LedgerFailure:
                raise
            except TraceFailure as error:
                if error.detail.get('category') in ('worker_response_timeout', 'worker_disconnected', 'worker_frame_ceiling', 'worker_invalid_json', 'worker_rpc_identity_mismatch'):
                    self.channel_error = error.detail
                    self.journal.append('client_channel_fenced', **context, worker=self.index, reason='unusable/misaligned persistent RPC channel; no reuse or auto restart', error=error.detail)
                self.journal.append('rpc_failure', **context, rpc_id=rpc_id, worker=self.index, controller_rpc_elapsed_ns=time.monotonic_ns() - started, error=error.detail)
                raise
            except BaseException as error:
                detail = {'category': 'worker_or_controller_io_error', 'type': type(error).__name__, 'message': str(error), 'response_unknown': True}
                self.channel_error = detail
                self.journal.append('client_channel_fenced', **context, worker=self.index, reason='controller/worker stream I/O failed; no reuse', error=detail)
                self.journal.append('rpc_failure', **context, rpc_id=rpc_id, worker=self.index, controller_rpc_elapsed_ns=time.monotonic_ns() - started, error=detail)
                raise TraceFailure(detail) from error

    def stop(self):
        try:
            self.process.stdin.write(b'{"action":"stop"}\n')
            self.process.stdin.flush()
        except (OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait()
        self.stderr_thread.join(timeout=5)
        if self.stderr_thread.is_alive():
            self.stderr_error = {'category': 'worker_stderr_capture_incomplete', 'message': 'reader did not terminate within cleanup bound'}
        self.select.close()
        for stream in [self.process.stdin, self.process.stdout, self.process.stderr]:
            if stream:
                try:
                    stream.close()
                except OSError:
                    pass

class Topology:

    def __init__(self, initial):
        self.value = initial
        self.lock = threading.RLock()

    def snapshot(self):
        with self.lock:
            return self.value

    def install(self, next):
        with self.lock:
            if next['cluster_id'] != self.value['cluster_id']:
                raise TraceFailure({'category': 'invalid_discovery', 'message': 'cluster mismatch'})
            if next['generation'] < self.value['generation']:
                return False
            if next['generation'] == self.value['generation']:
                if next != self.value:
                    raise TraceFailure({'category': 'invalid_discovery', 'message': 'conflicting equal-generation topology'})
                return False
            self.value = next
            return True

    def route(self, namespace, key):
        with self.lock:
            value = self.value
            t = token('__bic_cluster_events', namespace + '/' + key)
            matches = [r for r in value['ranges'].values() if t >= r['start_token'] and (r['end_token'] is None or t < r['end_token'])]
            if len(matches) != 1:
                raise TraceFailure({'category': 'invalid_route', 'matches': len(matches)})
            r = matches[0]
            return (value, {'range_id': r['id'], 'range_epoch': r['epoch'], 'leader': r['leader'], 'topology_generation': value['generation'], 'token': t, 'replicas': r['replicas']})

class FrozenPolicy:

    def __init__(self, worker, topology, journal, namespace, request_budget_ms=30000, io_ms=5000, probe=None):
        self.worker = worker
        self.topology = topology
        self.journal = journal
        self.namespace = namespace
        self.budget = request_budget_ms / 1000
        self.io_ms = io_ms
        self.probe = probe

    def rpc(self, context, destination, request, topology, io_ms=None):
        return self.worker.rpc(context, destination, request, topology, io_ms or self.io_ms, min(io_ms or self.io_ms, 500))

    def refresh(self, context, deadline):
        observed = False
        for member in sorted(self.topology.snapshot()['nodes']):
            if time.monotonic() >= deadline:
                break
            snapshot = None
            try:
                result, _ = self.rpc({**context, 'purpose': 'discovery_snapshot'}, member, 'FetchBootstrapSnapshot', self.topology.snapshot())
                snapshot = result['BootstrapSnapshot']['topology']
            except TraceFailure as error:
                if self.worker.channel_error is not None:
                    raise
                if error.detail.get('wire', {}).get('last_stage') == 'discovery_validate':
                    raise
                if time.monotonic() < deadline:
                    try:
                        result, _ = self.rpc({**context, 'purpose': 'discovery_legacy_topology'}, member, 'FetchTopology', self.topology.snapshot())
                        snapshot = result['Topology']
                    except TraceFailure as error:
                        if self.worker.channel_error is not None or error.detail.get('wire', {}).get('last_stage') == 'discovery_validate':
                            raise
            if snapshot is not None:
                installed = self.topology.install(snapshot)
                self.journal.append('topology_install', **context, source_node_id=member, installed=installed, generation=snapshot['generation'], topology_sha256=sha(snapshot))
                observed = True
        return observed

    def request(self, context, operation):
        deadline = time.monotonic() + self.budget
        last = {'category': 'event_request_failed'}
        rpc_timings = []
        for attempt in range(5):
            if time.monotonic() >= deadline:
                break
            current = {**context, 'route_attempt': attempt}
            self.journal.append('route_attempt_started', **current, request_deadline_monotonic_ns=int(deadline * 1000000000.0))
            if attempt:
                delay = min((100 << attempt - 1) / 1000, max(0, deadline - time.monotonic()))
                self.journal.append('retry_backoff', **current, seconds=delay)
                time.sleep(delay)
                if not self.refresh(current, deadline):
                    last = {'category': 'no_event_member_reachable'}
                    continue
            if time.monotonic() >= deadline:
                break
            topology, route = self.topology.route(operation['namespace'], operation['key'])
            request = {'ApplicationEvent': {'range_id': route['range_id'], 'range_epoch': route['range_epoch'], 'operation': operation}}
            self.journal.append('event_route', **current, route=route, gateway_node_id=self.worker.ready['caller_node_id'])
            try:
                result, timing = self.rpc({**current, 'purpose': 'application', 'route': route}, route['leader'], request, topology)
                rpc_timings.append(timing)
                if 'ApplicationEvent' not in result:
                    raise TraceFailure({'category': 'unexpected_application_response', 'response': result})
                return (result['ApplicationEvent'], {'route': route, 'rpc_timings': rpc_timings})
            except TraceFailure as error:
                last = error.detail
                if self.worker.channel_error is not None:
                    raise
                if self.probe:
                    self.probe.submit(context, route, topology, 'event_rpc_error', last)
        raise TraceFailure({'category': 'logical_request_failed', 'last_error': last, 'maximum_route_attempts': 5, 'request_budget_ms': int(self.budget * 1000), 'write_outcome_unknown': operation['operation'] == 'append'})

class ProbeScheduler:

    def __init__(self, workers, journal, stop_dispatch, period_seconds=5, session_factory=None):
        self.workers = workers
        self.session_factory = session_factory
        self.leader_sessions = {}
        self.registry_lock = threading.Lock()
        self.journal = journal
        self.stop_dispatch = stop_dispatch
        self.queue = queue.Queue(maxsize=32)
        self.cache = {}
        self.routes = {}
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.workers_exiting = threading.Event()
        self.period = period_seconds
        self.threads = [threading.Thread(target=self._work, args=(w,), daemon=True) for w in workers]
        for thread in self.threads:
            thread.start()
        self.periodic = threading.Thread(target=self._periodic, daemon=True)
        self.periodic.start()

    def record_route(self, context, route, topology):
        with self.lock:
            self.routes[route['range_id']] = (context, route, topology)

    def submit(self, context, route, topology, reason, error=None):
        self.record_route(context, route, topology)
        job = {'observation_id': uuid.uuid4().hex, 'context': context, 'route': route, 'topology': topology, 'reason': reason, 'trigger_error': error, 'trigger_unix_ns': time.time_ns()}
        voters = [v['node_id'] for v in route['replicas'] if v['role'] == 'voter']
        with self.lock:
            before = {node: self.cache.get((node, route['range_id'], route['range_epoch'])) for node in voters}
        self.journal.append('watermark_observation_trigger', **context, observation_id=job['observation_id'], reason=reason, route=route, cached_pre_watermarks=before, pre_watermarks_are_read_barrier=False, error=error)
        if self.workers_exiting.is_set():
            self.journal.append('observation_coverage_gap', **context, observation_id=job['observation_id'], reason='observer shutdown raced a recorded trigger; not dispatched')
            self.stop_dispatch.set()
            return
        try:
            self.queue.put_nowait(job)
        except queue.Full:
            self.journal.append('observation_coverage_gap', **context, observation_id=job['observation_id'], reason='bounded observer queue full; new logical dispatch fenced')
            self.stop_dispatch.set()

    def _periodic(self):
        cursor = 0
        while not self.stopping.wait(self.period):
            with self.lock:
                routes = list(self.routes.values())
            if routes:
                context, route, topology = routes[cursor % len(routes)]
                cursor += 1
                try:
                    self.submit(context, route, topology, 'periodic_cached_sample')
                except LedgerFailure:
                    self.stop_dispatch.set()
                    return

    def _work(self, worker):
        while not self.workers_exiting.is_set() or not self.queue.empty():
            try:
                job = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                route = job['route']
                if self.session_factory:
                    with self.registry_lock:
                        if self.workers_exiting.is_set():
                            raise TraceFailure({'category': 'observer_shutdown_before_session_selection'})
                        if route['leader'] not in self.leader_sessions:
                            self.leader_sessions[route['leader']] = self.session_factory(route['leader'], self.workers_exiting)
                        worker = self.leader_sessions[route['leader']]
                if worker is None or worker.ready['caller_node_id'] != route['leader']:
                    raise TraceFailure({'category': 'observer_requires_actual_cached_leader_identity', 'cached_leader': route['leader'], 'caller_node_id': worker.ready.get('caller_node_id') if worker else None})
                context = {**job['context'], 'purpose': 'watermark', 'observation_id': job['observation_id'], 'observer': True, 'trigger_reason': job['reason'], 'observer_caller_node_id': worker.ready['caller_node_id']}
                samples = []
                for replica in route['replicas']:
                    if replica['role'] != 'voter':
                        continue
                    node = replica['node_id']
                    request = {'RangeWriteProgress': {'range_id': route['range_id'], 'range_epoch': route['range_epoch']}}
                    try:
                        result, _ = worker.rpc(context, node, request, job['topology'], io_ms=1000, connect_ms=500)
                        progress = result['RangeWriteProgress']
                        sample = {'node': node, 'observed_unix_ns': time.time_ns(), 'requested_routing_epoch': route['range_epoch'], 'returned_retained_command_epoch': progress['current_epoch'], 'epoch_equality_is_not_a_health_gate': True, 'progress': progress}
                        with self.lock:
                            self.cache[node, route['range_id'], route['range_epoch']] = sample
                    except TraceFailure as error:
                        sample = {'node': node, 'observed_unix_ns': time.time_ns(), 'error': error.detail}
                        if worker.process.poll() is not None or worker.channel_error is not None or worker.stderr_error is not None:
                            self.journal.append('observation_coverage_gap', **job['context'], observation_id=job['observation_id'], reason='observer bridge lost/fenced; remaining probe outcomes unknown', node=node)
                            self.stop_dispatch.set()
                            self.stopping.set()
                            self.workers_exiting.set()
                            samples.append(sample)
                            break
                    samples.append(sample)
                self.journal.append('watermark_observation_completed', **job['context'], observation_id=job['observation_id'], reason=job['reason'], route=route, trigger_unix_ns=job['trigger_unix_ns'], samples=samples, scope='bounded progress reads from voters in triggering cached route; no read barrier/current-global-quorum proof/global snapshot')
            except LedgerFailure:
                self.stop_dispatch.set()
                return
            except BaseException as error:
                try:
                    self.journal.append('observation_coverage_gap', observation_id=job['observation_id'], error_type=type(error).__name__, message=str(error))
                except LedgerFailure:
                    pass
                self.stop_dispatch.set()
                self.stopping.set()
                self.workers_exiting.set()
            finally:
                self.queue.task_done()

    def close(self, budget=30):
        self.stopping.set()
        self.periodic.join(timeout=1)
        if self.periodic.is_alive():
            self.journal.append('observation_coverage_gap', reason='periodic observer submit did not terminate within shutdown bound')
            self.stop_dispatch.set()
        deadline = time.monotonic() + budget
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.1)
        if self.queue.unfinished_tasks:
            self.journal.append('observation_coverage_gap', reason='observer drain budget exhausted', pending=self.queue.unfinished_tasks)
        self.workers_exiting.set()
        for worker in [w for w in self.workers if w is not None] + list(self.leader_sessions.values()):
            worker.stop()
        for thread in self.threads:
            thread.join(timeout=5)
            if thread.is_alive():
                self.journal.append('observation_coverage_gap', reason='observer worker thread did not terminate within shutdown bound')
                self.stop_dispatch.set()

def scan_journal(path):
    counts = {}
    attempts = {}
    terminals = {}
    rpc_started = {}
    rpc_finished = set()
    unknown_responses = set()
    dispatched_applications = set()
    ack_rows = {}
    ack_receipts = set()
    database_ack_rows = {}
    latencies = {}
    gaps = []
    fences = []
    observation_triggers = set()
    observation_finished = set()
    last_sequence = -1
    run_completed = None
    required_reconciliation = None
    attempted_targets = set()
    terminal_targets = set()
    verified_targets = set()
    for row in read_journal(path):
        last_sequence = row['sequence']
        kind = row['kind']
        counts[kind] = counts.get(kind, 0) + 1
        if kind == 'reconciliation_inputs_loaded':
            required_reconciliation = {record['target_id']: record for record in row['targets']}
        if kind == 'operation_started':
            if row.get('reconciliation_target_id'):
                attempted_targets.add(row['reconciliation_target_id'])
            attempts[row['logical_id']] = {key: row.get(key) for key in ['operation', 'key', 'expected_payload', 'expected_payload_sha256', 'phase', 'namespace', 'reconciliation_target_id', 'source_ack_evidence']}
        if kind == 'operation_terminal':
            terminals[row['logical_id']] = row['outcome']
            if row.get('reconciliation_target_id'):
                terminal_targets.add(row['reconciliation_target_id'])
                if row['outcome'] == 'exact_match':
                    verified_targets.add(row['reconciliation_target_id'])
            latencies.setdefault(row['operation'] + ':' + row['outcome'], []).append(row['logical_elapsed_ns'])
        if kind == 'ack_observed':
            ack_rows[row['sequence']] = row
        if kind == 'ack_durable_receipt':
            receipt = row['receipt']
            ack = ack_rows.get(receipt['sequence'])
            if ack is None or ack['logical_id'] != row['logical_id'] or ack['record_sha256'] != receipt['record_sha256']:
                raise ValueError('invalid ACK durability linkage')
            ack_receipts.add(row['logical_id'])
        if kind == 'rpc_completed' and row.get('operation') == 'append' and (row.get('purpose') == 'application') and row['wire'].get('transport_success') and (row['wire'].get('response_envelope', {}).get('response', {}).get('Ok', {}).get('ApplicationEvent', {}).get('result') == 'appended'):
            database_ack_rows[row['logical_id']] = row['sequence']
        if kind == 'rpc_started':
            rpc_started[row['rpc_id']] = row['logical_id']
            if row.get('purpose') == 'application':
                dispatched_applications.add(row['logical_id'])
        if kind == 'rpc_failure' and row['error'].get('response_unknown'):
            unknown_responses.add(row['rpc_id'])
        if kind in ('rpc_completed', 'rpc_failure'):
            rpc_finished.add(row['rpc_id'])
        if kind == 'watermark_observation_trigger':
            observation_triggers.add(row['observation_id'])
        if kind == 'watermark_observation_completed' or (kind == 'observation_coverage_gap' and row.get('observation_id')):
            observation_finished.add(row['observation_id'])
        if kind == 'observation_coverage_gap':
            gaps.append(row)
        if kind in ('controller_resource_guard', 'controller_budget_guard', 'client_transport_fenced', 'client_channel_fenced', 'diagnostic_capture_fenced', 'reconciliation_incomplete') or (kind == 'worker_stderr_final_status' and row.get('error')) or (kind == 'run_completed' and row.get('dispatch_fenced')):
            fences.append({'kind': kind, 'sequence': row['sequence'], 'reason': row.get('reason')})
        if kind == 'run_completed':
            run_completed = row
    client_acks = ack_receipts & {logical for logical, outcome in terminals.items() if outcome == 'acknowledged'}
    database_replies = set(database_ack_rows)
    durable_database_replies = {logical for logical, sequence in database_ack_rows.items() if sequence < last_sequence}
    uncertain = [logical for logical, value in attempts.items() if value['operation'] == 'append' and logical in dispatched_applications and (logical not in client_acks | database_replies)]

    def distribution(values):
        values = sorted(values)

        def at(p):
            return values[max(0, (len(values) * p + 99) // 100 - 1)] if values else None
        return {'samples': len(values), 'p50_ns': at(50), 'p95_ns': at(95), 'p99_ns': at(99), 'max_ns': values[-1] if values else None}
    return {'format': 1, 'policy_source': POLICY_SOURCE, 'counts': counts, 'logical_attempts': len(attempts), 'externally_durable_ack_records': len(client_acks), 'ack_observed_rows_present': len(ack_rows), 'bare_ack_logical_ids_without_durable_receipt': sorted({row['logical_id'] for row in ack_rows.values()} - ack_receipts), 'ack_observed_rows_with_unproven_fsync': sorted((row['logical_id'] for sequence, row in ack_rows.items() if sequence == last_sequence)), 'database_success_reply_rows_present': len(database_replies), 'externally_durable_database_ack_replies': len(durable_database_replies), 'database_success_reply_ids_with_unproven_fsync': sorted(database_replies - durable_database_replies), 'durability_derivation': 'Only a subsequent serialized journal append or an explicit validated receipt proves that a prior fsync returned; a visible trailing row alone does not. Client ACK count additionally requires acknowledged terminal linkage.', 'database_ack_reply_ids_without_client_completion': sorted(database_replies - client_acks), 'uncertain_write_logical_ids': uncertain, 'unterminated_logical_ids': sorted(set(attempts) - set(terminals)), 'unknown_rpc_ids': sorted(set(rpc_started) - rpc_finished), 'rpc_unknown_response_ids': sorted(unknown_responses), 'acknowledged_logical_ids_with_unknown_rpc_history': sorted({rpc_started[rpc] for rpc in unknown_responses if rpc in rpc_started} & (client_acks | database_replies)), 'write_attempt_ids_not_dispatched': sorted((logical for logical, value in attempts.items() if value['operation'] == 'append' and logical not in dispatched_applications)), 'outcomes': {name: sum((v == name for v in terminals.values())) for name in sorted(set(terminals.values()))}, 'raw_latency_distributions': {key: distribution(values) for key, values in latencies.items()}, 'observation_coverage_gaps': len(gaps), 'pending_observation_ids': sorted(observation_triggers - observation_finished), 'dispatch_fenced': bool(fences), 'dispatch_fence_records': fences, 'reconciliation': target_coverage(required_reconciliation, attempted_targets, terminal_targets, verified_targets) if required_reconciliation is not None else None, 'run_completed': run_completed is not None, 'termination_reason': run_completed.get('termination_reason') if run_completed else None, 'qualification_status': 'not_qualified', 'server_declared_read_contract': 'eventual/local-applied-on-named-leader', 'oracle_expectation': 'read-your-ACK', 'strong_read_proof': False, 'client_path': 'controller frozen-policy replica through member-side SSH TLS wire bridge; not stock CLI or PostgreSQL'}

def validate_trace(path, allow_incomplete=False):
    operations = {}
    rpcs = {}
    database_acks = set()
    ack_records = {}
    durable_receipts = set()
    terminal = set()
    errors = 0
    wire_bodies = 0
    run_complete = False
    planned_termination = False
    observation_triggers = {}
    observation_finished = set()
    observation_gaps = set()
    reconciliation_required = None
    reconciliation_attempted = set()
    reconciliation_terminal = set()
    reconciliation_verified = set()
    dispatch_fenced = False

    def require(condition, message):
        if not condition:
            raise ValueError(message)
    for row in read_journal(path):
        kind = row['kind']
        if kind == 'reconciliation_inputs_loaded':
            reconciliation_required = {record['target_id']: record for record in row['targets']}
            require(len(reconciliation_required) == row['unique_write_keys'], 'duplicate/missing required reconciliation target IDs')
        if kind == 'operation_started':
            if row.get('reconciliation_target_id'):
                target_id = row['reconciliation_target_id']
                require(reconciliation_required is not None and target_id in reconciliation_required, 'Get target absent from required reconciliation set')
                require(target_id not in reconciliation_attempted, 'duplicate logical Get for reconciliation target')
                target = reconciliation_required[target_id]
                require(row['operation'] == 'get' and row['key'] == target['key'] and (row['expected_payload_sha256'] == target['expected_payload_sha256']), 'reconciliation target identity/value mismatch')
                reconciliation_attempted.add(target_id)
            logical = row['logical_id']
            require(logical not in operations, 'duplicate logical operation identity')
            require(sha(row['expected_payload']) == row['expected_payload_sha256'], 'expected payload hash mismatch')
            operations[logical] = {key: row[key] for key in ('logical_id', 'operation', 'key', 'namespace', 'expected_payload_sha256')}
        elif kind == 'rpc_started':
            require(row['logical_id'] in operations, 'RPC lacks durable logical attempt')
            require(row['rpc_id'] not in rpcs, 'duplicate RPC identity')
            rpcs[row['rpc_id']] = {'start': {key: row[key] for key in ('sequence', 'logical_id', 'worker')}, 'finished': False}
            if row.get('purpose') == 'application':
                op = operations[row['logical_id']]
                event = row['request']['ApplicationEvent']
                mutation = event['operation']
                require(mutation['namespace'] == op['namespace'] and mutation['key'] == op['key'] and (mutation['operation'] == op['operation']), 'retry changed logical identity')
                require(event['range_id'] == row['route']['range_id'] and event['range_epoch'] == row['route']['range_epoch'], 'event route/epoch mismatch')
                require(row['cached_topology_generation'] == row['route']['topology_generation'], 'route generation mismatch')
                if op['operation'] == 'append':
                    require(sha(mutation['value']) == op['expected_payload_sha256'], 'retry changed expected payload')
        elif kind == 'rpc_completed':
            require(row['rpc_id'] in rpcs, 'completed RPC lacks start')
            rpc = rpcs[row['rpc_id']]
            rpc['finished'] = True
            wire = row['wire']
            require(wire['rpc_id'] == row['rpc_id'] and wire['wire_request_id'] == rpc['start']['sequence'] + 1, 'wire/RPC correlation mismatch')
            if 'response_body_raw' in wire:
                wire_bodies += 1
                raw = wire['response_body_raw']
                require(hashlib.sha256(raw.encode()).hexdigest() == wire['response_body_sha256'], 'wire response digest mismatch')
                require(json.loads(raw) == wire['response_envelope'], 'raw/parsed wire response mismatch')
            if wire.get('transport_success'):
                envelope = wire['response_envelope']
                request = wire['request_envelope']
                require(envelope['request_id'] == request['request_id'] and envelope['cluster_id'] == request['cluster_id'] and (envelope['source_node_id'] == request['destination_node_id']) and (envelope['protocol_version'] == request['protocol_version']), 'successful response identity mismatch')
                if 'Err' in envelope['response']:
                    rpc['remote_error'] = envelope['response']['Err']
                if row.get('purpose') == 'application' and row.get('operation') == 'append' and (envelope['response'].get('Ok', {}).get('ApplicationEvent', {}).get('result') == 'appended'):
                    database_acks.add(row['logical_id'])
        elif kind == 'rpc_failure':
            require(row['rpc_id'] in rpcs, 'failed RPC lacks start')
            rpc = rpcs[row['rpc_id']]
            rpc['finished'] = True
            errors += 1
            if row['error']['category'] == 'server_remote_error':
                require(row['error']['code'] == rpc['remote_error']['code'] and row['error']['message'] == rpc['remote_error']['message'], 'remote error code/message changed')
                rpc.pop('remote_error', None)
        elif kind == 'ack_observed':
            require(row['logical_id'] in database_acks, 'ACK lacks previously durable database success response')
            ack_records[row['sequence']] = row
        elif kind == 'ack_durable_receipt':
            receipt = row['receipt']
            require(receipt['sequence'] in ack_records, 'ACK receipt lacks durable ACK record')
            ack = ack_records[receipt['sequence']]
            require(ack['record_sha256'] == receipt['record_sha256'] and ack['logical_id'] == row['logical_id'], 'ACK receipt identity mismatch')
            require(receipt['durable_monotonic_ns'] <= row['controller_monotonic_ns'], 'ACK durability receipt precedes fsync')
            durable_receipts.add(row['logical_id'])
        elif kind == 'operation_terminal':
            require(row['logical_id'] in operations and row['logical_id'] not in terminal, 'duplicate/missing operation terminal')
            terminal.add(row['logical_id'])
            if row.get('reconciliation_target_id'):
                reconciliation_terminal.add(row['reconciliation_target_id'])
                if row['outcome'] == 'exact_match':
                    reconciliation_verified.add(row['reconciliation_target_id'])
            require(row['logical_elapsed_ns'] >= 0, 'negative raw logical latency')
            if row['outcome'] == 'acknowledged':
                require(row['logical_id'] in durable_receipts, 'client completed ACK before external durability receipt')
        elif kind == 'watermark_observation_trigger':
            require(row['observation_id'] not in observation_triggers, 'duplicate observation trigger')
            observation_triggers[row['observation_id']] = row
        elif kind == 'watermark_observation_completed':
            require(row['observation_id'] in observation_triggers, 'observation result lacks trigger')
            observation_finished.add(row['observation_id'])
        elif kind == 'run_completed':
            run_complete = True
            planned_termination = row.get('termination_reason') in ('traffic_deadline', 'operation_ceiling', 'reconciliation_inputs_complete')
            dispatch_fenced = dispatch_fenced or bool(row.get('dispatch_fenced'))
        elif kind in ('controller_resource_guard', 'controller_budget_guard', 'client_transport_fenced', 'client_channel_fenced', 'diagnostic_capture_fenced', 'observation_coverage_gap', 'reconciliation_incomplete') or (kind == 'worker_stderr_final_status' and row.get('error')):
            dispatch_fenced = True
            if kind == 'observation_coverage_gap' and row.get('observation_id'):
                observation_gaps.add(row['observation_id'])
    missing = sorted(set(operations) - terminal)
    pending = sorted((key for key, value in rpcs.items() if not value['finished']))
    pending_observations = sorted(set(observation_triggers) - observation_finished - observation_gaps)
    reconciliation = target_coverage(reconciliation_required, reconciliation_attempted, reconciliation_terminal, reconciliation_verified) if reconciliation_required is not None else None
    if not allow_incomplete:
        require(reconciliation is None or reconciliation['coverage_complete'], 'required reconciliation targets were not all attempted and terminated')
        require(not pending_observations, 'observation triggers lack completed or explicitly incomplete outcome')
        require(run_complete and planned_termination and operations and (not missing) and (not pending) and (not dispatch_fenced), 'incomplete or resource-fenced logical/RPC/run trace')
    return {'trace_valid': True, 'reconciliation': reconciliation, 'complete_run_record': run_complete, 'planned_termination': planned_termination, 'dispatch_fenced': dispatch_fenced, 'logical_attempts': len(operations), 'individual_rpc_calls': len(rpcs), 'rpc_failures': errors, 'full_wire_response_bodies': wire_bodies, 'durable_client_acks': len(durable_receipts), 'database_ack_reply_identities': len(database_acks), 'unterminated_logical_ids': missing, 'unfinished_rpc_ids': pending, 'pending_observation_ids': pending_observations, 'server_declared_read_contract': 'eventual/local-applied-on-named-leader', 'oracle_expectation': 'read-your-ACK', 'strong_read_proof': False, 'server_internal_retries_observed': False, 'qualification_status': 'not_qualified'}

def target_coverage(required, attempted, terminal, verified):
    required_ids = set(required)
    ack_ids = {identifier for identifier, record in required.items() if record.get('source_ack_evidence') == 'durable_client_ack'}
    return {'required_target_ids': sorted(required_ids), 'attempted_target_ids': sorted(attempted), 'terminal_target_ids': sorted(terminal), 'verified_target_ids': sorted(verified), 'unattempted_target_ids': sorted(required_ids - attempted), 'unterminated_target_ids': sorted(required_ids - terminal), 'unverified_target_ids': sorted(required_ids - verified), 'required_prior_ack_target_ids': sorted(ack_ids), 'prior_ack_unverified_target_ids': sorted(ack_ids - verified), 'coverage_complete': bool(required_ids) and attempted == required_ids and (terminal == required_ids), 'all_targets_verified_observation': bool(required_ids) and verified == required_ids, 'all_prior_ack_targets_verified_observation': bool(ack_ids) and ack_ids <= verified}

def reconciliation_targets(paths, namespace):
    targets = {}
    receipts = set()
    ack_terminals = set()
    database_replies = set()
    for path in paths:
        validate_trace(path, allow_incomplete=True)
        for row in read_journal(path):
            logical = row.get('logical_id')
            if row['kind'] == 'operation_started' and row['operation'] == 'append':
                if row['namespace'] != namespace:
                    raise ValueError('reconciliation namespace differs from scoped worker')
                if sha(row['expected_payload']) != row['expected_payload_sha256']:
                    raise ValueError('reconciliation expected-payload hash mismatch')
                key = row['key']
                previous = targets.get(key)
                if previous and previous['expected_payload_sha256'] != row['expected_payload_sha256']:
                    raise ValueError('same replay key has conflicting expected values')
                target = targets.setdefault(key, {'key': key, 'expected_payload': row['expected_payload'], 'expected_payload_sha256': row['expected_payload_sha256'], 'source_logical_ids': []})
                target['source_logical_ids'].append(logical)
            if row['kind'] == 'ack_durable_receipt':
                receipts.add(logical)
            if row['kind'] == 'operation_terminal' and row['outcome'] == 'acknowledged':
                ack_terminals.add(logical)
            if row['kind'] == 'rpc_completed' and row.get('purpose') == 'application' and (row.get('operation') == 'append') and row['wire'].get('transport_success') and (row['wire'].get('response_envelope', {}).get('response', {}).get('Ok', {}).get('ApplicationEvent', {}).get('result') == 'appended'):
                database_replies.add(logical)
    for target in targets.values():
        ids = set(target['source_logical_ids'])
        target['target_id'] = sha({'namespace': namespace, 'key': target['key'], 'expected_payload_sha256': target['expected_payload_sha256']})
        target['source_ack_evidence'] = 'durable_client_ack' if ids & (receipts & ack_terminals) else 'database_reply_observed' if ids & database_replies else 'no_ack_observed'
    return list(targets.values())

def json_safe(value):
    if isinstance(value, pathlib.Path):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    return value

def run(args):
    absolute_deadline = time.monotonic() + args.overall_budget_seconds
    output = args.output
    output.mkdir(mode=448, parents=True, exist_ok=False)
    identity = {'pid': os.getpid(), 'start_ticks': pathlib.Path(f'/proc/{os.getpid()}/stat').read_text().split()[21], 'argv': sys.argv, 'created_unix_ns': time.time_ns(), 'workers': []}
    (output / 'controller-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    stop = threading.Event()
    journal = Journal(output / 'trace.jsonl', args.max_ledger_bytes, args.minimum_free_bytes)
    workers = []
    probes = None
    errors = []
    phase = args.phase
    run_id = args.run_id or uuid.uuid4().hex
    command = json.loads(args.worker_command.read_text())
    assert isinstance(command, list) and all((isinstance(v, str) for v in command))
    source = pathlib.Path(__file__)
    journal.append('run_started', run_id=run_id, namespace=args.namespace, phase=phase, policy_source=POLICY_SOURCE, client_sha256=hashlib.sha256(source.read_bytes()).hexdigest(), arguments={k: json_safe(v) for k, v in vars(args).items()}, coverage={'logical_attempts': True, 'event_and_discovery_rpc_retries': True, 'wire_response_envelopes': True, 'raw_operation_latencies': True, 'server_internal_consensus_or_relocation_retries': False, 'network_origin': 'member gateway; logical client and fsynced ledger external controller', 'stock_client_policy': False, 'initial_discovery': 'validated gateway member store; stock workload upfront refresh is not copied; retry discovery is captured', 'policy_replica': 'frozen461 event loop/discovery/backoff; observer RPCs additional and separately timed'})
    try:
        initial_available = int(next((line for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:'))).split()[1]) * 1024
        if initial_available < args.minimum_available_bytes:
            journal.append('controller_resource_guard', reason='initial shared-host memory headroom', available_bytes=initial_available)
            stop.set()
            raise RuntimeError('initial shared-host memory headroom insufficient; no worker/application dispatch')
        session_lock = threading.RLock()
        observer_commands = json.loads(args.observer_command_map.read_text()) if args.observer_command_map else None
        if observer_commands is not None and (not isinstance(observer_commands, dict) or not 1 <= len(observer_commands) <= 5 or any((not isinstance(node, str) or not isinstance(argv, list) or (not all((isinstance(v, str) for v in argv))) for node, argv in observer_commands.items()))):
            raise ValueError('observer command map must name at most five bounded member argv lists')

        def allocate_session(argv, expected_caller=None, lazy=False, closing=None):
            with session_lock:
                index = len(workers)
                if index >= args.concurrency + 5:
                    raise TraceFailure({'category': 'worker_session_ceiling', 'maximum': args.concurrency + 5})
                remaining = absolute_deadline - time.monotonic()
                if remaining <= 0:
                    journal.append('controller_budget_guard', reason='overall budget during worker startup')
                    stop.set()
                    raise RuntimeError('overall budget during worker startup')
                journal.append('session_start_started', worker=index, intended_role='leader_local_observer' if lazy else 'application_or_fixed_observer', expected_caller_node_id=expected_caller, command=argv, ready_timeout_seconds=min(5 if lazy else 30, remaining))
                try:
                    worker = WireWorker(argv, index, journal, output, ready_timeout=min(5 if lazy else 30, remaining), absolute_deadline=absolute_deadline)
                except BaseException as error:
                    journal.append('session_start_failed', worker=index, intended_role='leader_local_observer' if lazy else 'application_or_fixed_observer', expected_caller_node_id=expected_caller, error_type=type(error).__name__, error=error.detail if isinstance(error, TraceFailure) else str(error), no_application_rpc_dispatched=True)
                    if time.monotonic() >= absolute_deadline:
                        journal.append('controller_budget_guard', reason='overall budget while waiting for worker readiness')
                        stop.set()
                    raise
                workers.append(worker)
                process = worker.process
                identity['workers'].append({'pid': process.pid, 'start_ticks': pathlib.Path(f'/proc/{process.pid}/stat').read_text().split()[21]})
                temporary = output / 'controller-identity.json.tmp'
                temporary.write_text(json.dumps(identity, indent=2) + '\n')
                os.replace(temporary, output / 'controller-identity.json')
                if index and worker.ready.get('cluster_id') != workers[0].ready.get('cluster_id'):
                    raise TraceFailure({'category': 'worker_cluster_identity_mismatch', 'worker': index})
                if worker.ready.get('wire_worker_sha256') != args.expected_worker_sha256:
                    raise TraceFailure({'category': 'wire_worker_binary_identity_mismatch', 'worker': index})
                if worker.ready.get('linked_core_source') != args.expected_linked_core_source or worker.ready.get('linked_core_version') != args.expected_linked_core_version:
                    raise TraceFailure({'category': 'wire_worker_linked_core_identity_mismatch', 'worker': index})
                if worker.ready.get('namespace') != args.namespace or worker.ready.get('policy_source') != POLICY_SOURCE or worker.ready.get('transport', {}).get('dev_localhost_plaintext'):
                    raise TraceFailure({'category': 'worker_scope_identity_or_mtls_mismatch', 'worker': index})
                if expected_caller is not None and worker.ready.get('caller_node_id') != expected_caller:
                    raise TraceFailure({'category': 'observer_member_identity_mismatch', 'expected': expected_caller, 'actual': worker.ready.get('caller_node_id')})
                if closing is not None and closing.is_set():
                    worker.stop()
                    raise TraceFailure({'category': 'observer_shutdown_during_session_start'})
                journal.append('session_admitted', worker=index, role='leader_local_observer' if lazy else 'application_or_fixed_observer', caller_node_id=worker.ready['caller_node_id'], expected_caller_node_id=expected_caller)
                return worker
        for i in range(args.concurrency):
            allocate_session(command, expected_caller=args.application_gateway_node_id)
        topology = Topology(workers[0].ready['topology'])
        for worker in workers[1:]:
            topology.install(worker.ready['topology'])
        if args.observer_workers:
            if observer_commands is None:
                fixed = [allocate_session(command, expected_caller=args.application_gateway_node_id) for _ in range(args.observer_workers)]
                probes = ProbeScheduler(fixed, journal, stop, args.watermark_period_seconds)
            else:

                def observer_session(node, closing):
                    if node not in observer_commands:
                        raise TraceFailure({'category': 'missing_leader_local_observer_command', 'node': node, 'no_key_export_or_authentication_relaxation': True})
                    return allocate_session(observer_commands[node], expected_caller=node, lazy=True, closing=closing)
                probes = ProbeScheduler([None] * args.observer_workers, journal, stop, args.watermark_period_seconds, session_factory=observer_session)
                for node in sorted({r['leader'] for r in topology.snapshot()['ranges'].values()}):
                    probes.leader_sessions[node] = observer_session(node, probes.workers_exiting)
                journal.append('observer_current_leaders_warmed', nodes=sorted(probes.leader_sessions), application_gateway_node_id=args.application_gateway_node_id, not_yet_joined_observers_intentionally_absent=True)
        reconcile_records = reconciliation_targets(args.reconcile_journal, args.namespace) if args.reconcile_journal else None
        if reconcile_records is not None:
            journal.append('reconciliation_inputs_loaded', unique_write_keys=len(reconcile_records), targets=[{key: record[key] for key in ('target_id', 'key', 'expected_payload_sha256', 'source_ack_evidence', 'source_logical_ids')} for record in reconcile_records], inputs=[{'sha256': file_sha256(path), 'path': str(path)} for path in args.reconcile_journal], new_application_writes=False, server_read_contract='eventual/local-applied-on-named-leader')
        corpus = json.loads(args.corpus_ack_ledger.read_text()) if args.corpus_ack_ledger else None
        if corpus:
            if corpus['namespace'] != args.namespace or not 0 < len(corpus['keys']) <= 1000000 or len(corpus['keys']) != len(set(corpus['keys'])) or any((not isinstance(key, str) or not re.fullmatch('[A-Za-z0-9_.-]{1,256}', key) for key in corpus['keys'])) or (not 0 <= corpus['payload_bytes'] <= 32768):
                raise ValueError('historical ACK corpus identity/bounds invalid')
            journal.append('historical_corpus_loaded', source_sha256=hashlib.sha256(args.corpus_ack_ledger.read_bytes()).hexdigest(), key_count=len(corpus['keys']), original_namespace=corpus['namespace'], original_run=corpus['run'], original_attempt_order_known=False, after_exhaustion='new generated replay keys at same append/read mix')
        journal.append('traffic_window_started', phase=phase, requested_seconds=args.seconds, application_workers=args.concurrency, total_admitted_sessions=len(workers), application_gateway_node_id=args.application_gateway_node_id)
        deadline = time.monotonic() + args.seconds
        reconciliation_attempted = set()
        reconciliation_terminal = set()
        reconciliation_verified = set()
        reconciliation_required = {record['target_id']: record for record in reconcile_records} if reconcile_records is not None else None
        counter = 0
        write_counter = 0
        counter_lock = threading.Lock()
        cursor_lock = threading.Lock()
        cursors = {}

        def traffic(index):
            nonlocal counter, write_counter
            policy = FrozenPolicy(workers[index], topology, journal, args.namespace, args.request_budget_ms, args.rpc_timeout_ms, probes)
            last = None
            successful_reads = 0
            while time.monotonic() < deadline and (not stop.is_set()):
                with counter_lock:
                    position = counter
                    counter += 1
                if position >= args.max_operations or (reconcile_records is not None and position >= len(reconcile_records)):
                    return
                logical = f'{run_id}-{phase}-{index}-{position}'
                operation = 'get' if reconcile_records is not None else 'append' if position % 2 == 0 or last is None else 'get'
                origin = 'new_replay_generated'
                corpus_index = None
                source_target = reconcile_records[position] if reconcile_records is not None else None
                if source_target is not None:
                    key = source_target['key']
                    expected = source_target['expected_payload']
                    origin = 'reconciliation_input'
                elif operation == 'append':
                    with counter_lock:
                        corpus_index = write_counter
                        write_counter += 1
                    if corpus and corpus_index < len(corpus['keys']):
                        key = corpus['keys'][corpus_index]
                        expected = {'run': corpus['run'], 'payload': 'x' * corpus['payload_bytes']}
                        origin = 'historical_ack_corpus'
                    else:
                        key = logical
                        expected = {'run': run_id, 'payload': 'x' * args.payload_bytes}
                else:
                    key = last['key']
                    expected = last['expected']
                    origin = last['origin']
                context = {'logical_id': logical, 'logical_attempt': 0, 'phase': phase, 'operation': operation, 'key': key, 'namespace': args.namespace, 'expected_payload_sha256': sha(expected), 'key_origin': origin, 'corpus_write_index': corpus_index, 'read_after_ack_logical_id': last['logical_id'] if operation == 'get' and last is not None else None, 'application_gateway_node_id': workers[index].ready['caller_node_id'], 'server_declared_read_contract': 'eventual/local-applied-on-named-leader' if operation == 'get' else None, 'oracle_expectation': ('observe-uncertain-write-outcome' if source_target and source_target['source_ack_evidence'] == 'no_ack_observed' else 'read-your-ACK') if operation == 'get' else None, 'strong_read_proof': False, 'source_ack_evidence': source_target['source_ack_evidence'] if source_target else None, 'source_logical_ids': source_target['source_logical_ids'] if source_target else None, 'reconciliation_target_id': source_target['target_id'] if source_target else None}
                started = time.monotonic_ns()
                outcome = 'unknown'
                result = None
                detail = None
                try:
                    journal.append('operation_started', **context, expected_payload=expected)
                    if source_target is not None:
                        with counter_lock:
                            reconciliation_attempted.add(source_target['target_id'])
                    with cursor_lock:
                        cursors[index] = {'logical_id': logical, 'started_monotonic_ns': started, 'state': 'request'}
                    request = {'operation': operation, 'namespace': args.namespace, 'key': key}
                    if operation == 'append':
                        request['value'] = expected
                    result, meta = policy.request(context, request)
                    response_elapsed = time.monotonic_ns() - started
                    if operation == 'append':
                        if result.get('result') != 'appended':
                            raise TraceFailure({'category': 'unexpected_append_result', 'response': result, 'response_unknown': True})
                        receipt = journal.append('ack_observed', **context, response=result, route=meta['route'], logical_latency_to_response_ns=response_elapsed)
                        journal.append('ack_durable_receipt', **context, receipt=receipt, client_ack_durable_elapsed_ns=time.monotonic_ns() - started)
                        outcome = 'acknowledged'
                        last = {'key': key, 'expected': expected, 'origin': origin, 'logical_id': logical}
                    else:
                        if result.get('result') != 'value':
                            raise TraceFailure({'category': 'unexpected_get_result', 'response': result})
                        value = result.get('value')
                        outcome = 'exact_match' if sha(value) == sha(expected) else 'absent' if value is None else 'mismatch'
                        if outcome == 'exact_match':
                            successful_reads += 1
                        journal.append('read_observed', **context, response=result, route=meta['route'], successful_read_counter=successful_reads, logical_latency_to_response_ns=response_elapsed)
                        if probes:
                            probes.record_route(context, meta['route'], topology.snapshot())
                            if outcome != 'exact_match' or successful_reads % args.success_control_every == 0:
                                probes.submit(context, meta['route'], topology.snapshot(), 'read_' + outcome if outcome != 'exact_match' else 'selected_success_control')
                except LedgerFailure as error:
                    errors.append({'worker': index, 'category': 'journal_fenced', 'message': str(error)})
                    stop.set()
                    return
                except TraceFailure as error:
                    detail = error.detail
                    outcome = 'uncertain_write' if operation == 'append' else 'read_error'
                except BaseException as error:
                    detail = {'category': 'controller_exception', 'type': type(error).__name__, 'message': str(error)}
                    errors.append({'worker': index, **detail})
                    outcome = 'uncertain_write' if operation == 'append' else 'read_error'
                    stop.set()
                try:
                    journal.append('operation_terminal', **context, outcome=outcome, response=result, error=detail, logical_elapsed_ns=time.monotonic_ns() - started)
                    if source_target is not None:
                        with counter_lock:
                            reconciliation_terminal.add(source_target['target_id'])
                            if outcome == 'exact_match':
                                reconciliation_verified.add(source_target['target_id'])
                    if workers[index].process.poll() is not None or workers[index].channel_error is not None or workers[index].stderr_error is not None:
                        errors.append({'worker': index, 'category': 'worker_process_lost', 'exit_code': workers[index].process.returncode})
                        journal.append('client_transport_fenced', **context, reason='worker exited, poisoned channel, or failed stderr capture; no automatic restart')
                        stop.set()
                    with cursor_lock:
                        cursors[index] = {'logical_id': logical, 'state': 'terminal', 'outcome': outcome}
                except LedgerFailure as error:
                    errors.append({'worker': index, 'category': 'journal_fenced', 'message': str(error)})
                    stop.set()
                    return
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            jobs = [pool.submit(traffic, i) for i in range(args.concurrency)]
            while any((not job.done() for job in jobs)):
                memory = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in pathlib.Path('/proc/meminfo').read_text().splitlines()}
                available = memory['MemAvailable']
                with cursor_lock:
                    progress = {'unix_ns': time.time_ns(), 'logical_operations_allocated': counter, 'cursors': dict(cursors), 'memory_available_bytes': available, 'dispatch_fenced': stop.is_set(), 'journal_last_durable_sequence': journal.sequence - 1, 'journal_bytes': journal.bytes, 'journal_failed': journal.failed, 'admitted_sessions': len(workers), 'observer_pending': probes.queue.unfinished_tasks if probes else 0, 'topology_generation': topology.snapshot()['generation']}
                temporary = output / 'progress.json.tmp'
                temporary.write_text(json.dumps(progress, indent=2) + '\n')
                os.replace(temporary, output / 'progress.json')
                if time.monotonic() >= absolute_deadline:
                    journal.append('controller_budget_guard', reason='overall controller budget exhausted during traffic')
                    stop.set()
                for worker in workers:
                    if worker.stderr_error is not None:
                        errors.append({'worker': worker.index, **worker.stderr_error})
                        stop.set()
                if available < args.minimum_available_bytes:
                    journal.append('controller_resource_guard', reason='shared-host memory headroom', available_bytes=available)
                    stop.set()
                time.sleep(0.5)
            for job in jobs:
                job.result()
        if reconciliation_required is not None:
            coverage = target_coverage(reconciliation_required, reconciliation_attempted, reconciliation_terminal, reconciliation_verified)
            journal.append('reconciliation_coverage', **coverage, server_declared_contract='eventual/local-applied-on-named-leader', strong_read_proof=False)
            if not coverage['coverage_complete']:
                journal.append('reconciliation_incomplete', **coverage, reason='configured cap/budget/fence left required targets unattempted or unterminated')
                stop.set()
        if probes:
            probes.close(args.observer_drain_seconds)
            probes = None
        if time.monotonic() >= absolute_deadline:
            journal.append('controller_budget_guard', reason='overall controller budget exhausted before run completion')
            stop.set()
        journal.append('run_completed', run_id=run_id, phase=phase, dispatch_fenced=stop.is_set(), termination_reason='dispatch_fenced' if stop.is_set() else 'reconciliation_inputs_complete' if reconciliation_required is not None and bool(reconciliation_required) and (reconciliation_terminal == set(reconciliation_required)) else 'operation_ceiling' if counter >= args.max_operations else 'traffic_deadline', controller_errors=errors)
    except BaseException as error:
        errors.append({'category': 'run_exception', 'type': type(error).__name__, 'message': str(error)})
        stop.set()
        try:
            journal.append('run_failed', errors=errors)
        except LedgerFailure:
            pass
    finally:
        if probes:
            try:
                probes.close(args.observer_drain_seconds)
            except BaseException as error:
                errors.append({'category': 'observer_shutdown', 'message': str(error)})

        def stop_owned(worker):
            try:
                worker.stop()
            except BaseException as error:
                errors.append({'worker': worker.index, 'category': 'worker_cleanup_failed', 'message': str(error)})
                stop.set()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as cleanup:
            list(cleanup.map(stop_owned, workers))
        for worker in workers:
            if worker.stderr_error is not None:
                errors.append({'worker': worker.index, **worker.stderr_error})
                stop.set()
            try:
                journal.append('worker_stderr_final_status', worker=worker.index, captured_bytes=worker.stderr_bytes, error=worker.stderr_error)
            except LedgerFailure:
                pass
        journal.close()
    try:
        summary = scan_journal(output / 'trace.jsonl')
        summary['trace_validation'] = validate_trace(output / 'trace.jsonl', allow_incomplete=True)
    except (OSError, ValueError) as error:
        summary = {'journal_integrity_passed': False, 'error': str(error), 'qualification_status': 'not_qualified'}
    summary['controller_errors'] = errors
    summary['telemetry_complete'] = not (stop.is_set() or summary.get('dispatch_fenced') or errors or summary.get('observation_coverage_gaps') or summary.get('pending_observation_ids') or summary.get('unknown_rpc_ids') or summary.get('unterminated_logical_ids') or (summary.get('reconciliation') is not None and (not summary['reconciliation']['coverage_complete'])) or (summary.get('journal_integrity_passed') is False))
    summary['planned_experiment_complete'] = summary['telemetry_complete'] and summary.get('trace_validation', {}).get('complete_run_record', False) and summary.get('trace_validation', {}).get('planned_termination', False) and bool(summary.get('logical_attempts'))
    summary['workload_passed'] = summary['planned_experiment_complete'] and all((name in ('acknowledged', 'exact_match') for name in summary.get('outcomes', {}))) and bool(summary.get('logical_attempts'))
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, sort_keys=True))
    return 2 if not summary['telemetry_complete'] or not summary['planned_experiment_complete'] else 0 if summary['workload_passed'] else 1

def main():
    if os.environ.get('MALLOC_ARENA_MAX') != '2':
        os.execvpe(sys.executable, [sys.executable, *sys.argv], {**os.environ, 'MALLOC_ARENA_MAX': '2'})
    threading.stack_size(1024 * 1024)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('traffic')
    p.add_argument('--worker-command', type=pathlib.Path, required=True)
    p.add_argument('--expected-worker-sha256', required=True)
    p.add_argument('--expected-linked-core-source', required=True)
    p.add_argument('--expected-linked-core-version', required=True)
    p.add_argument('--output', type=pathlib.Path, required=True)
    p.add_argument('--namespace', required=True)
    p.add_argument('--phase', required=True)
    p.add_argument('--corpus-ack-ledger', type=pathlib.Path)
    p.add_argument('--reconcile-journal', action='append', type=pathlib.Path, default=[])
    p.add_argument('--run-id')
    p.add_argument('--seconds', type=int, default=60)
    p.add_argument('--overall-budget-seconds', type=float)
    p.add_argument('--concurrency', type=int, default=4)
    p.add_argument('--observer-workers', type=int, default=1)
    p.add_argument('--observer-command-map', type=pathlib.Path)
    p.add_argument('--application-gateway-node-id', default='n0')
    p.add_argument('--max-operations', type=int, default=10000)
    p.add_argument('--payload-bytes', type=int, default=1024)
    p.add_argument('--rpc-timeout-ms', type=int, default=5000)
    p.add_argument('--request-budget-ms', type=int, default=30000)
    p.add_argument('--watermark-period-seconds', type=float, default=5)
    p.add_argument('--success-control-every', type=int, default=32)
    p.add_argument('--observer-drain-seconds', type=int, default=30)
    p.add_argument('--max-ledger-bytes', type=int, default=2 * 1024 ** 3)
    p.add_argument('--minimum-free-bytes', type=int, default=4 * 1024 ** 3)
    p.add_argument('--minimum-available-bytes', type=int, default=8 * 1024 ** 3)
    p.add_argument('--maximum-address-space-bytes', type=int, default=2 * 1024 ** 3)
    p = sub.add_parser('summarize')
    p.add_argument('journal', type=pathlib.Path)
    p = sub.add_parser('validate')
    p.add_argument('journal', type=pathlib.Path)
    p.add_argument('--allow-incomplete', action='store_true')
    a = parser.parse_args()
    if a.command == 'summarize':
        print(json.dumps(scan_journal(a.journal), sort_keys=True))
        return 0
    if a.command == 'validate':
        print(json.dumps(validate_trace(a.journal, a.allow_incomplete), sort_keys=True))
        return 0
    if not (1 <= a.concurrency <= 16 and 0 <= a.observer_workers <= 2 and (1 <= a.seconds <= 900) and (1 <= a.max_operations <= 1000000) and (0 <= a.payload_bytes <= 32768) and (100 <= a.rpc_timeout_ms <= 30000) and (1 <= a.request_budget_ms <= 300000) and (a.success_control_every >= 1) and (a.watermark_period_seconds >= 1) and (1 <= a.observer_drain_seconds <= 60)):
        parser.error('invalid bounded reproduction settings')
    if a.reconcile_journal and a.corpus_ack_ledger:
        parser.error('reconciliation and append corpus modes are mutually exclusive')
    if not re.fullmatch('[A-Za-z0-9_.-]{1,128}', a.namespace) or not re.fullmatch('[A-Za-z0-9_.-]{1,64}', a.phase) or (not re.fullmatch('[0-9a-f]{64}', a.expected_worker_sha256)) or (a.run_id and (not re.fullmatch('[A-Za-z0-9_.-]{1,64}', a.run_id))):
        parser.error('invalid namespace, phase, run or helper SHA identity')
    if not 256 * 1024 ** 2 <= a.maximum_address_space_bytes <= 4 * 1024 ** 3:
        parser.error('controller address-space ceiling must be256MiB..4GiB')
    if a.overall_budget_seconds is None:
        a.overall_budget_seconds = a.seconds + a.request_budget_ms / 1000 + 90
    if not 0.1 <= a.overall_budget_seconds <= 1200:
        parser.error('overall budget must be0.1..1200 seconds')
    resource.setrlimit(resource.RLIMIT_AS, (a.maximum_address_space_bytes, a.maximum_address_space_bytes))
    try:
        os.nice(10)
    except OSError:
        pass
    if shutil.which('ionice'):
        applied = subprocess.run(['ionice', '-c', '3', '-p', str(os.getpid())], capture_output=True, timeout=5)
        observed = subprocess.run(['ionice', '-p', str(os.getpid())], capture_output=True, text=True, timeout=5)
        a.effective_io_priority = {'set_exit_code': applied.returncode, 'query_exit_code': observed.returncode, 'description': observed.stdout.strip()}
    else:
        a.effective_io_priority = {'available': False}
    a.effective_nice = os.getpriority(os.PRIO_PROCESS, 0)
    a.effective_address_space_limit = resource.getrlimit(resource.RLIMIT_AS)
    try:
        return run(a)
    except BaseException as error:
        failure = {'format': 1, 'telemetry_complete': False, 'planned_experiment_complete': False, 'workload_passed': False, 'qualification_status': 'not_qualified', 'controller_errors': [{'category': 'controller_initialization_or_journal_failure', 'type': type(error).__name__, 'message': str(error)}]}
        try:
            identity = json.loads((a.output / 'controller-identity.json').read_text())
            if identity.get('pid') == os.getpid():
                fd = os.open(a.output / 'summary.json', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
                with os.fdopen(fd, 'w') as stream:
                    json.dump(failure, stream, indent=2)
                    stream.write('\n')
                    stream.flush()
                    os.fsync(stream.fileno())
        except (OSError, ValueError):
            pass
        print(json.dumps(failure), file=sys.stderr)
        return 2
if __name__ == '__main__':
    raise SystemExit(main())
