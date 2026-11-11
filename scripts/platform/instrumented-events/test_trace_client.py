"""Instrumentation boundary tests. Fake RPC peers are not HA qualification."""
import contextlib, hashlib, io, queue, types, importlib.util, json, os, pathlib, socket, ssl, struct, subprocess, tempfile, threading, time, unittest
from unittest import mock
HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('trace_client', HERE / 'trace_client.py')
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)
FROZEN = pathlib.Path(os.environ.get('BICDB_FROZEN_BINARY', '/tmp/bicdb-azure-artifact-evidence/bicdb-1.0.461-beta-b05385d4'))
CORE_SOURCE = os.environ.get('BICDB_TRACE_CORE_SOURCE', 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb')
CORE_VERSION = os.environ.get('BICDB_TRACE_CORE_VERSION', '1.0.461-beta')
WORKER = pathlib.Path(os.environ.get('BICDB_TRACE_WIRE_WORKER', '/tmp/bicdb-event-trace-build-v4/wire-worker'))

class JournalTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)

    def test_fsync_failure_fences_dispatch(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        journal.append('operation_started', logical_id='known', operation='append')
        worker = client.WireWorker.__new__(client.WireWorker)
        worker.lock = threading.Lock()
        worker.index = 0
        worker.journal = journal
        worker.sent_generation = 1
        worker.process = mock.Mock()
        with mock.patch.object(client.os, 'fsync', side_effect=OSError('injected fsync failure')):
            with self.assertRaises(client.LedgerFailure):
                worker.rpc({'logical_id': 'known'}, 'n0', 'FetchTopology', {'generation': 1})
        self.assertTrue(journal.failed)
        worker.process.stdin.write.assert_not_called()
        with self.assertRaises(client.LedgerFailure):
            journal.append('ack_observed', logical_id='known')

    def test_torn_tail_and_changed_hash_are_rejected(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        journal.append('a')
        journal.close()
        original = (self.root / 'journal').read_bytes()
        self.assertEqual(len(list(client.read_journal(self.root / 'journal'))), 1)
        (self.root / 'journal').write_bytes(original + b'{')
        with self.assertRaisesRegex(ValueError, 'torn'):
            list(client.read_journal(self.root / 'journal'))
        (self.root / 'journal').write_bytes(original.replace(b'"kind":"a"', b'"kind":"b"'))
        with self.assertRaisesRegex(ValueError, 'digest'):
            list(client.read_journal(self.root / 'journal'))

    def test_validator_rejects_ack_without_database_success(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        payload = {'stable': True}
        journal.append('operation_started', logical_id='fake-ack', operation='append', key='k', namespace='n', expected_payload=payload, expected_payload_sha256=client.sha(payload))
        journal.append('ack_observed', logical_id='fake-ack')
        journal.close()
        with self.assertRaisesRegex(ValueError, 'lacks previously durable database success'):
            client.validate_trace(self.root / 'journal', allow_incomplete=True)

    def test_trailing_ack_after_failed_fsync_is_not_counted_as_client_ack(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        context = {'logical_id': 'ack-stage', 'operation': 'append', 'purpose': 'application'}
        journal.append('operation_started', **context)
        journal.append('rpc_started', **context, rpc_id='rpc')
        journal.append('rpc_completed', **context, rpc_id='rpc', wire={'transport_success': True, 'response_envelope': {'response': {'Ok': {'ApplicationEvent': {'result': 'appended'}}}}})
        with mock.patch.object(client.os, 'fsync', side_effect=OSError('ACK-stage fsync failure')):
            with self.assertRaises(client.LedgerFailure):
                journal.append('ack_observed', **context)
        journal.close()
        summary = client.scan_journal(self.root / 'journal')
        self.assertEqual(summary['ack_observed_rows_present'], 1)
        self.assertEqual(summary['externally_durable_ack_records'], 0)
        self.assertEqual(summary['ack_observed_rows_with_unproven_fsync'], ['ack-stage'])
        self.assertEqual(summary['externally_durable_database_ack_replies'], 1)

    def test_fences_remain_monotonic_and_empty_trace_is_not_complete(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        journal.append('controller_resource_guard', reason='fixture')
        journal.append('run_completed', dispatch_fenced=False)
        journal.close()
        self.assertTrue(client.scan_journal(self.root / 'journal')['dispatch_fenced'])
        self.assertTrue(client.validate_trace(self.root / 'journal', allow_incomplete=True)['dispatch_fenced'])
        with self.assertRaisesRegex(ValueError, 'incomplete or resource-fenced'):
            client.validate_trace(self.root / 'journal')

    def test_stderr_partial_writes_and_failed_sync_are_observable(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        worker = client.WireWorker.__new__(client.WireWorker)
        worker.process = types.SimpleNamespace(stderr=io.BytesIO(b'complete error bytes'))
        worker.stderr_path = self.root / 'stderr'
        worker.stderr_bytes = 0
        worker.stderr_error = None
        worker.journal = journal
        worker.index = 0
        original = client.os.write
        with mock.patch.object(client.os, 'write', side_effect=lambda fd, data: original(fd, data[:1])):
            worker._stderr()
        self.assertEqual(worker.stderr_path.read_bytes(), b'complete error bytes')
        self.assertIsNone(worker.stderr_error)
        worker.stderr_path = self.root / 'stderr-failed'
        worker.process.stderr = io.BytesIO(b'failed sync')
        worker.stderr_bytes = 0
        with mock.patch.object(client.os, 'fsync', side_effect=OSError('stderr fsync failure')):
            worker._stderr()
        self.assertIsNotNone(worker.stderr_error)
        self.assertTrue(journal.failed)

    def test_observer_queue_gap_is_recorded_and_fences_dispatch(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        observer = client.ProbeScheduler.__new__(client.ProbeScheduler)
        observer.workers = []
        observer.journal = journal
        observer.stop_dispatch = threading.Event()
        observer.queue = queue.Queue(maxsize=1)
        observer.lock = threading.Lock()
        observer.routes = {}
        observer.cache = {}
        observer.stopping = threading.Event()
        observer.workers_exiting = observer.stopping
        route = {'range_id': 1, 'range_epoch': 1, 'leader': 'n0', 'replicas': [{'node_id': 'n0', 'role': 'voter'}]}
        context = {'logical_id': 'trigger'}
        observer.submit(context, route, {}, 'first')
        observer.submit(context, route, {}, 'second')
        self.assertTrue(observer.stop_dispatch.is_set())
        self.assertEqual(client.scan_journal(self.root / 'journal')['observation_coverage_gaps'], 1)

    def test_late_or_misaligned_worker_channel_is_never_reused(self):
        for category in ['worker_response_timeout', 'worker_rpc_identity_mismatch']:
            with self.subTest(category=category):
                root = self.root / category
                root.mkdir()
                journal = client.Journal(root / 'journal', min_free_bytes=0)
                self.addCleanup(journal.close)
                script = root / 'worker.py'
                script.write_text('import json,sys,time\nprint(json.dumps({"kind":"ready","topology":{"generation":1}}),flush=True)\nfor line in sys.stdin:\n c=json.loads(line)\n if c.get("action")=="stop":break\n time.sleep(.1)\n print(json.dumps({"rpc_id":c["rpc_id"],"wire_request_id":c["wire_request_id"]}),flush=True)\n')
                worker = client.WireWorker(['python3', str(script)], 0, journal, root)
                self.addCleanup(worker.stop)
                effect = client.TraceFailure({'category': category, 'response_unknown': True})
                with mock.patch.object(worker, '_line', side_effect=effect):
                    with self.assertRaises(client.TraceFailure):
                        worker.rpc({'logical_id': 'uncertain'}, 'n0', 'FetchTopology', {'generation': 1})
                self.assertIsNotNone(worker.channel_error)
                with self.assertRaises(client.TraceFailure) as caught:
                    worker.rpc({'logical_id': 'second'}, 'n0', 'FetchTopology', {'generation': 1})
                self.assertEqual(caught.exception.detail['category'], 'worker_channel_fenced')
                self.assertEqual(sum((row['kind'] == 'rpc_started' for row in client.read_journal(root / 'journal'))), 1)

    def test_lost_observer_is_an_explicit_coverage_gap(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        observer = client.ProbeScheduler.__new__(client.ProbeScheduler)
        observer.journal = journal
        observer.stop_dispatch = threading.Event()
        observer.queue = queue.Queue(maxsize=32)
        observer.lock = threading.Lock()
        observer.cache = {}
        observer.routes = {}
        observer.stopping = threading.Event()
        observer.workers_exiting = observer.stopping
        observer.session_factory = None
        observer.leader_sessions = {}
        observer.registry_lock = threading.Lock()
        route = {'range_id': 1, 'range_epoch': 1, 'leader': 'n0', 'replicas': [{'node_id': 'n0', 'role': 'voter'}, {'node_id': 'n1', 'role': 'voter'}]}
        observer.submit({'logical_id': 'trigger'}, route, {}, 'fixture')
        worker = types.SimpleNamespace(ready={'caller_node_id': 'n0'}, process=types.SimpleNamespace(poll=lambda: 1), channel_error={'category': 'worker_disconnected'}, stderr_error=None, rpc=mock.Mock(side_effect=client.TraceFailure({'category': 'worker_disconnected', 'response_unknown': True})))
        observer._work(worker)
        self.assertTrue(observer.stop_dispatch.is_set())
        self.assertTrue(observer.stopping.is_set())
        self.assertEqual(client.scan_journal(self.root / 'journal')['observation_coverage_gaps'], 1)
        self.assertEqual(worker.rpc.call_count, 1)

    def test_leader_local_observer_session_preserves_caller_authority(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        observer = client.ProbeScheduler.__new__(client.ProbeScheduler)
        observer.journal = journal
        observer.stop_dispatch = threading.Event()
        observer.queue = queue.Queue(maxsize=32)
        observer.lock = threading.Lock()
        observer.cache = {}
        observer.routes = {}
        observer.stopping = threading.Event()
        observer.workers_exiting = observer.stopping
        observer.leader_sessions = {}
        observer.registry_lock = threading.Lock()
        route = {'range_id': 1, 'range_epoch': 7, 'leader': 'n1', 'replicas': [{'node_id': 'n1', 'role': 'voter'}, {'node_id': 'n2', 'role': 'voter'}]}

        def rpc(context, node, request, topology, **kwargs):
            self.assertEqual(context['observer_caller_node_id'], 'n1')
            self.assertEqual(request['RangeWriteProgress']['range_epoch'], 7)
            if node == 'n2':
                observer.stopping.set()
            return ({'RangeWriteProgress': {'node_id': node, 'current_epoch': 6}}, {})
        worker = types.SimpleNamespace(ready={'caller_node_id': 'n1'}, rpc=rpc)
        created = []

        def factory(node, closing):
            created.append(node)
            return worker
        observer.session_factory = factory
        observer.submit({'logical_id': 'trigger', 'application_gateway_node_id': 'n0'}, route, {}, 'fixture')
        observer._work(None)
        self.assertEqual(created, ['n1'])
        self.assertFalse(observer.stop_dispatch.is_set())
        completed = [row for row in client.read_journal(self.root / 'journal') if row['kind'] == 'watermark_observation_completed']
        self.assertEqual({sample['node'] for sample in completed[0]['samples']}, {'n1', 'n2'})
        self.assertTrue(all((sample['requested_routing_epoch'] == 7 and sample['returned_retained_command_epoch'] == 6 for sample in completed[0]['samples'])))

    def test_unmatched_fixed_observer_identity_is_gap_not_authentication_bypass(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        observer = client.ProbeScheduler.__new__(client.ProbeScheduler)
        observer.journal = journal
        observer.stop_dispatch = threading.Event()
        observer.queue = queue.Queue(maxsize=32)
        observer.lock = threading.Lock()
        observer.cache = {}
        observer.routes = {}
        observer.stopping = threading.Event()
        observer.workers_exiting = observer.stopping
        observer.session_factory = None
        observer.leader_sessions = {}
        observer.registry_lock = threading.Lock()
        route = {'range_id': 1, 'range_epoch': 7, 'leader': 'n1', 'replicas': [{'node_id': 'n1', 'role': 'voter'}]}
        worker = types.SimpleNamespace(ready={'caller_node_id': 'n0'}, rpc=mock.Mock())
        observer.submit({'logical_id': 'trigger'}, route, {}, 'fixture')
        observer._work(worker)
        self.assertTrue(observer.stop_dispatch.is_set())
        worker.rpc.assert_not_called()
        self.assertEqual(client.scan_journal(self.root / 'journal')['observation_coverage_gaps'], 1)

    def test_pending_observer_trigger_and_late_stderr_error_are_not_complete(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        journal.append('watermark_observation_trigger', observation_id='pending')
        journal.append('run_completed', dispatch_fenced=False, termination_reason='traffic_deadline')
        journal.append('worker_stderr_final_status', worker=0, error={'category': 'lingering_capture'})
        journal.close()
        summary = client.scan_journal(self.root / 'journal')
        self.assertEqual(summary['pending_observation_ids'], ['pending'])
        self.assertTrue(summary['dispatch_fenced'])
        with self.assertRaisesRegex(ValueError, 'observation triggers lack'):
            client.validate_trace(self.root / 'journal')

    def test_shutdown_drains_previously_admitted_lazy_observation(self):
        journal = client.Journal(self.root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        route = {'range_id': 1, 'range_epoch': 7, 'leader': 'n0', 'replicas': [{'node_id': 'n0', 'role': 'voter'}]}
        fake = types.SimpleNamespace(ready={'caller_node_id': 'n0'}, rpc=lambda *args, **kwargs: ({'RangeWriteProgress': {'node_id': 'n0', 'current_epoch': 7}}, {}), stop=lambda: None)

        def factory(node, closing):
            self.assertFalse(closing.is_set())
            return fake
        observer = client.ProbeScheduler([None], journal, threading.Event(), period_seconds=60, session_factory=factory)
        observer.registry_lock.acquire()
        observer.submit({'logical_id': 'trigger'}, route, {}, 'selected_success_control')
        closer = threading.Thread(target=lambda: observer.close(2))
        closer.start()
        self.assertTrue(observer.stopping.wait(1))
        self.assertFalse(observer.workers_exiting.is_set())
        observer.registry_lock.release()
        closer.join(3)
        self.assertFalse(closer.is_alive())
        summary = client.scan_journal(self.root / 'journal')
        self.assertEqual(summary['observation_coverage_gaps'], 0)
        self.assertEqual(summary['pending_observation_ids'], [])
        self.assertEqual(summary['counts']['watermark_observation_completed'], 1)

    def test_frozen_hash_vector(self):
        self.assertEqual(client.token('public.documents', 'doc-42'), 8072627585289240671)

class FakePeer:

    def __init__(self, root, wrong_pin=False, lost_first=False, always_error=False, read_mode=None, invalid_snapshot=False, invalid_legacy=False):
        self.root = root
        self.stop = threading.Event()
        self.requests = []
        self.events = {}
        self.lock = threading.Lock()
        self.lost_first = lost_first
        self.always_error = always_error
        self.read_mode = read_mode
        self.invalid_snapshot = invalid_snapshot
        self.invalid_legacy = invalid_legacy
        self.message = 'verbatim remote error: ' + '0123456789\nλ' * 600
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.address = '127.0.0.1:' + str(self.listener.getsockname()[1])
        self.fixture = root / 'db'
        self._certificates()
        command = [str(FROZEN), 'cluster', 'init', str(self.fixture), '--cluster-id', 'trace-fixture', '--node-id', 'n0', '--address', self.address, '--replication-factor', '1', '--initial-ranges', '4', '--application-events', '--cluster-tls-cert', str(root / 'n0.crt'), '--cluster-tls-key', str(root / 'n0.key'), '--cluster-tls-ca', str(root / 'ca.crt')]
        env = {**os.environ, 'BICDB_STORAGE_MODE': 'server_paged', 'BICDB_PAGED_BUFFER_POOL_BYTES': '8388608'}
        result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr + result.stdout)
        self.topology = json.loads((self.fixture / 'cluster-topology.json').read_text())['topology']
        self.config = json.loads((self.fixture / 'bicdb-distribution.json').read_text())
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        prefix = 'other' if wrong_pin else 'n0'
        self.context.load_cert_chain(root / (prefix + '.crt'), root / (prefix + '.key'))
        self.context.load_verify_locations(root / 'ca.crt')
        self.context.verify_mode = ssl.CERT_REQUIRED
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _certificates(self):

        def run(*args):
            subprocess.run(['openssl', *map(str, args)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=20)
        run('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=trace fixture CA', '-keyout', self.root / 'ca.key', '-out', self.root / 'ca.crt')
        (self.root / 'extension').write_text('subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n')
        for name in ['n0', 'other']:
            run('req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=' + name, '-keyout', self.root / (name + '.key'), '-out', self.root / (name + '.csr'))
            run('x509', '-req', '-in', self.root / (name + '.csr'), '-CA', self.root / 'ca.crt', '-CAkey', self.root / 'ca.key', '-CAcreateserial', '-days', '1', '-extfile', self.root / 'extension', '-out', self.root / (name + '.crt'))

    def _serve(self):
        while not self.stop.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._connection, args=(connection,), daemon=True).start()

    @staticmethod
    def _read(stream, count):
        data = b''
        while len(data) < count:
            part = stream.recv(count - len(data))
            if not part:
                raise EOFError()
            data += part
        return data

    def _connection(self, raw):
        try:
            with self.context.wrap_socket(raw, server_side=True) as stream:
                stream.settimeout(5)
                size = struct.unpack('>I', self._read(stream, 4))[0]
                request = json.loads(self._read(stream, size))
                kind = request['request']
                with self.lock:
                    self.requests.append(request)
                if kind == 'FetchBootstrapSnapshot':
                    template = dict(self.config)
                    if self.invalid_snapshot:
                        template['cluster_id'] = 'wrong-cluster'
                    response = {'Ok': {'BootstrapSnapshot': {'topology': self.topology, 'config_template': template, 'metadata_leader_id': 'n0'}}}
                    if self.invalid_legacy:
                        response = {'Err': {'code': 'legacy_fixture', 'message': 'use legacy response fixture'}}
                elif kind == 'FetchTopology':
                    legacy = dict(self.topology)
                    if self.invalid_legacy:
                        legacy['cluster_id'] = 'wrong-cluster'
                    response = {'Ok': {'Topology': legacy}}
                elif 'RangeWriteProgress' in kind:
                    q = kind['RangeWriteProgress']
                    response = {'Ok': {'RangeWriteProgress': {'node_id': 'n0', 'range_id': q['range_id'], 'current_epoch': q['range_epoch'], 'last_index': len(self.events), 'resolved_through': len(self.events), 'compacted_through': 0}}}
                else:
                    event = kind['ApplicationEvent']['operation']
                    key = event['namespace'] + '/' + event['key']
                    if self.always_error:
                        response = {'Err': {'code': 'fixture_failure_exact', 'message': self.message}}
                    elif event['operation'] == 'append':
                        with self.lock:
                            inserted = key not in self.events
                            self.events[key] = event['value']
                            lose = self.lost_first
                            self.lost_first = False
                        if lose:
                            return
                        response = {'Ok': {'ApplicationEvent': {'result': 'appended', 'inserted': inserted}}}
                    else:
                        value = self.events.get(key)
                        if self.read_mode == 'absent':
                            value = None
                        elif self.read_mode == 'mismatch':
                            value = {'unexpected': True}
                        response = {'Ok': {'ApplicationEvent': {'result': 'value', 'value': value}}}
                envelope = {'protocol_version': request['protocol_version'], 'cluster_id': request['cluster_id'], 'source_node_id': request['destination_node_id'], 'request_id': request['request_id'], 'response': response}
                body = json.dumps(envelope, ensure_ascii=False, separators=(',', ':')).encode()
                stream.sendall(struct.pack('>I', len(body)) + body)
        except (EOFError, OSError, ssl.SSLError):
            raw.close()

    def close(self):
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=2)

@unittest.skipUnless(FROZEN.is_file() and WORKER.is_file(), 'requires explicitly frozen CLI and linked wire-worker; no automatic engine build')
class ProtocolTests(unittest.TestCase):

    def fixture(self, **kwargs):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        peer = FakePeer(root, **kwargs)
        self.addCleanup(peer.close)
        return (root, peer)

    def controller(self, root, peer, max_operations=2, observers=0, corpus=None, extra_args=None, allow_incomplete=False, concurrency=1):
        namespace = 'trace-selftest'
        command = [str(WORKER), str(peer.fixture), namespace]
        command_file = root / 'command.json'
        command_file.write_text(json.dumps(command))
        output = root / 'controller'
        args = ['python3', str(HERE / 'trace_client.py'), 'traffic', '--worker-command', str(command_file), '--expected-worker-sha256', hashlib.sha256(WORKER.read_bytes()).hexdigest(), '--expected-linked-core-source', CORE_SOURCE, '--expected-linked-core-version', CORE_VERSION, '--output', str(output), '--namespace', namespace, '--phase', 'selftest', '--seconds', '30', '--concurrency', str(concurrency), '--observer-workers', str(observers), '--max-operations', str(max_operations), '--rpc-timeout-ms', '500', '--minimum-free-bytes', '0']
        if corpus is not None:
            source = root / 'source-corpus.json'
            source.write_text(json.dumps(corpus))
            args += ['--corpus-ack-ledger', str(source)]
        args += extra_args or []
        result = subprocess.run(args, capture_output=True, text=True, timeout=90)
        summary = json.loads((output / 'summary.json').read_text())
        try:
            client.validate_trace(output / 'trace.jsonl', allow_incomplete=allow_incomplete)
        except BaseException:
            import shutil
            saved = pathlib.Path('/tmp/bicdb-instrumentation-selftest-failures') / str(time.time_ns())
            saved.mkdir(mode=448, parents=True, exist_ok=False)
            shutil.copytree(output, saved / 'controller')
            (saved / 'reason.txt').write_text('strict validation failure in ' + self.id() + '\n')
            raise
        rows = list(client.read_journal(output / 'trace.jsonl'))
        return (result, summary, rows)

    def test_lost_reply_retries_keep_identity_and_external_ack(self):
        root, peer = self.fixture(lost_first=True)
        result, summary, rows = self.controller(root, peer)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertEqual(summary['externally_durable_ack_records'], 1)
        self.assertEqual(summary['outcomes'], {'acknowledged': 1, 'exact_match': 1})
        event_calls = [r for r in rows if r['kind'] == 'rpc_started' and r.get('purpose') == 'application']
        appends = [r['request']['ApplicationEvent']['operation'] for r in event_calls if r['operation'] == 'append']
        self.assertEqual(len(appends), 2)
        self.assertEqual(appends[0], appends[1])
        self.assertTrue(any((r['kind'] == 'rpc_failure' and r['operation'] == 'append' for r in rows)))
        self.assertTrue(any((r.get('purpose') == 'discovery_snapshot' for r in rows)))
        self.assertEqual(len(peer.events), 1)
        terminals = {r['logical_id']: r for r in rows if r['kind'] == 'operation_terminal'}
        for row in rows:
            if row['kind'] == 'ack_durable_receipt':
                self.assertLess(row['receipt']['durable_unix_ns'], terminals[row['logical_id']]['controller_unix_ns'])

    def test_all_errors_retained_beyond_twenty_with_verbatim_wire_body(self):
        root, peer = self.fixture(always_error=True)
        result, summary, rows = self.controller(root, peer, max_operations=5)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertFalse(summary['workload_passed'])
        self.assertEqual(summary['outcomes'], {'uncertain_write': 5})
        errors = [r for r in rows if r['kind'] == 'rpc_completed' and r.get('purpose') == 'application']
        self.assertEqual(len(errors), 25)
        for row in errors:
            envelope = json.loads(row['wire']['response_body_raw'])
            self.assertEqual(envelope['response']['Err'], {'code': 'fixture_failure_exact', 'message': peer.message})
            self.assertEqual(row['wire']['response_body_sha256'], hashlib.sha256(row['wire']['response_body_raw'].encode()).hexdigest())

    def test_worker_disconnect_is_uncertain_and_never_auto_acked(self):
        root, peer = self.fixture()
        journal = client.Journal(root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        fake = root / 'disconnected.py'
        ready = {'kind': 'ready', 'topology': peer.topology, 'caller_node_id': 'n0'}
        fake.write_text('import json,sys\nprint(' + repr(json.dumps(ready)) + ',flush=True)\nsys.stdin.readline()\n')
        worker = client.WireWorker(['python3', str(fake)], 0, journal, root)
        self.addCleanup(worker.stop)
        context = {'logical_id': 'uncertain', 'operation': 'append'}
        journal.append('operation_started', **context)
        operation = {'ApplicationEvent': {'range_id': 1, 'range_epoch': 1, 'operation': {'operation': 'append', 'namespace': 'trace-selftest', 'key': 'uncertain', 'value': {'same': True}}}}
        with self.assertRaises(client.TraceFailure) as caught:
            worker.rpc(context, 'n0', operation, peer.topology, io_ms=100, connect_ms=100)
        self.assertTrue(caught.exception.detail['response_unknown'])
        journal.append('operation_terminal', **context, outcome='uncertain_write', logical_elapsed_ns=1)
        rows = list(client.read_journal(root / 'journal'))
        self.assertFalse(any((row['kind'] == 'ack_observed' for row in rows)))
        self.assertTrue(any((row['kind'] == 'rpc_failure' and row['error']['response_unknown'] for row in rows)))

    def test_historical_corpus_preserves_original_keys_and_payload(self):
        root, peer = self.fixture()
        corpus = {'namespace': 'trace-selftest', 'run': 'original-retained-run', 'payload_bytes': 123, 'keys': ['original-ack-key-A']}
        result, summary, rows = self.controller(root, peer, max_operations=4, corpus=corpus)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        historical = [r for r in rows if r['kind'] == 'operation_started' and r.get('key_origin') == 'historical_ack_corpus']
        self.assertEqual(len(historical), 2)
        for row in historical:
            self.assertEqual(row['key'], 'original-ack-key-A')
            self.assertEqual(row['expected_payload'], {'run': 'original-retained-run', 'payload': 'x' * 123})
        self.assertTrue(any((row['kind'] == 'operation_started' and row['key_origin'] == 'new_replay_generated' for row in rows)))

    def test_missing_value_is_distinct_from_transport_error(self):
        root, peer = self.fixture(read_mode='absent')
        result, summary, rows = self.controller(root, peer, observers=1)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertEqual(summary['outcomes'], {'acknowledged': 1, 'absent': 1})
        self.assertTrue(any((row['kind'] == 'read_observed' and row['response']['value'] is None for row in rows)))
        self.assertTrue(any((row['kind'] == 'watermark_observation_trigger' and row['reason'] == 'read_absent' for row in rows)))

    def test_wrong_value_is_distinct_from_missing_and_transport(self):
        root, peer = self.fixture(read_mode='mismatch')
        result, summary, rows = self.controller(root, peer, observers=1)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertEqual(summary['outcomes'], {'acknowledged': 1, 'mismatch': 1})
        self.assertTrue(any((row['kind'] == 'read_observed' and row['response']['value'] == {'unexpected': True} for row in rows)))

    def test_invalid_bootstrap_does_not_fall_back_to_weaker_authority(self):
        root, peer = self.fixture(lost_first=True, invalid_snapshot=True)
        result, summary, rows = self.controller(root, peer, max_operations=1)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertEqual(summary['externally_durable_ack_records'], 0)
        self.assertFalse(any((row.get('purpose') == 'discovery_legacy_topology' for row in rows)))
        self.assertTrue(any((row['kind'] == 'rpc_completed' and row['wire']['last_stage'] == 'discovery_validate' and (not row['wire']['transport_success']) for row in rows)))

    def test_initial_memory_fence_cannot_report_complete_or_success(self):
        root, peer = self.fixture()
        result, summary, rows = self.controller(root, peer, extra_args=['--minimum-available-bytes', str(2 ** 60)], allow_incomplete=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(summary['telemetry_complete'])
        self.assertFalse(summary['planned_experiment_complete'])
        self.assertFalse(summary['workload_passed'])
        self.assertEqual(summary['logical_attempts'], 0)
        self.assertTrue(summary['dispatch_fenced'])

    def test_overall_budget_abort_is_distinct_from_planned_duration(self):
        root, peer = self.fixture()
        result, summary, rows = self.controller(root, peer, max_operations=100000, extra_args=['--overall-budget-seconds', '.2'], allow_incomplete=True)
        self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
        self.assertFalse(summary['planned_experiment_complete'])
        self.assertFalse(summary['workload_passed'])
        self.assertTrue(any((row['kind'] == 'controller_budget_guard' for row in rows)))

    def test_default_success_control_is_reachable(self):
        root, peer = self.fixture()
        result, summary, rows = self.controller(root, peer, max_operations=66, observers=1)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(any((row['kind'] == 'watermark_observation_trigger' and row['reason'] == 'selected_success_control' for row in rows)))

    def test_disk_headroom_fence_has_explicit_incomplete_verdict(self):
        root, peer = self.fixture()
        result, summary, rows = self.controller(root, peer, extra_args=['--minimum-free-bytes', str(2 ** 60)], allow_incomplete=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(summary['telemetry_complete'])
        self.assertFalse(summary['planned_experiment_complete'])
        self.assertFalse(summary['workload_passed'])
        self.assertEqual(rows, [])

    def test_invalid_legacy_topology_aborts_instead_of_being_swallowed(self):
        root, peer = self.fixture(lost_first=True, invalid_legacy=True)
        result, summary, rows = self.controller(root, peer, max_operations=1)
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertTrue(summary['telemetry_complete'])
        self.assertFalse(summary['workload_passed'])
        legacy = [row for row in rows if row['kind'] == 'rpc_completed' and row.get('purpose') == 'discovery_legacy_topology']
        self.assertEqual(len(legacy), 1)
        self.assertEqual(legacy[0]['wire']['last_stage'], 'discovery_validate')
        self.assertFalse(legacy[0]['wire']['transport_success'])
        self.assertEqual(sum((row['kind'] == 'rpc_started' and row.get('purpose') == 'application' for row in rows)), 1)

    def test_actual_sixteen_client_concurrency_under_resource_cap(self):
        root, peer = self.fixture()
        result, summary, rows = self.controller(root, peer, max_operations=64, observers=2, concurrency=16)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(summary['planned_experiment_complete'])
        self.assertEqual(summary['logical_attempts'], 64)
        self.assertEqual(sum((row['kind'] == 'worker_ready' for row in rows)), 18)

    def test_twenty_one_actual_channels_under_two_gib_address_limit(self):
        root, peer = self.fixture()
        script = root / 'session-pressure.py'
        script.write_text("import concurrent.futures,importlib.util,json,pathlib,resource,sys,threading\nthreading.stack_size(1024*1024)\nresource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))\nspec=importlib.util.spec_from_file_location('client',sys.argv[1]);client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)\ncommand=json.loads(sys.argv[2]);directory=pathlib.Path(sys.argv[3]);journal=client.Journal(directory/'pressure.jsonl',min_free_bytes=0);workers=[]\ntry:\n for i in range(21):workers.append(client.WireWorker(command,i,journal,directory,ready_timeout=5))\n print(json.dumps({'sessions':len(workers),'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'status':pathlib.Path('/proc/self/status').read_text()}))\nfinally:\n with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(lambda worker:worker.stop(),workers))\n journal.close()\n")
        result = subprocess.run(['python3', str(script), str(HERE / 'trace_client.py'), json.dumps([str(WORKER), str(peer.fixture), 'trace-selftest']), str(root)], capture_output=True, text=True, timeout=90, env={**os.environ, 'MALLOC_ARENA_MAX': '2'})
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data['sessions'], 21)
        print('session-pressure-fixture ' + json.dumps({key: data[key] for key in ['sessions', 'max_rss_kib']}))

    def test_reconciliation_reads_all_prior_writes_without_new_appends(self):
        root, peer = self.fixture()
        first, summary, rows = self.controller(root, peer, max_operations=4)
        self.assertEqual(first.returncode, 0)
        before = dict(peer.events)
        output = root / 'reconciliation'
        argv = ['python3', str(HERE / 'trace_client.py'), 'traffic', '--worker-command', str(root / 'command.json'), '--expected-worker-sha256', hashlib.sha256(WORKER.read_bytes()).hexdigest(), '--expected-linked-core-source', CORE_SOURCE, '--expected-linked-core-version', CORE_VERSION, '--namespace', 'trace-selftest', '--phase', 'reconcile', '--seconds', '30', '--concurrency', '2', '--observer-workers', '0', '--max-operations', '100', '--minimum-free-bytes', '0', '--output', str(output), '--reconcile-journal', str(root / 'controller/trace.jsonl')]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        client.validate_trace(output / 'trace.jsonl')
        reconciled = json.loads((output / 'summary.json').read_text())
        self.assertTrue(reconciled['planned_experiment_complete'])
        self.assertEqual(reconciled['outcomes'], {'exact_match': 2})
        self.assertEqual(reconciled['termination_reason'], 'reconciliation_inputs_complete')
        self.assertEqual(before, peer.events)
        replay = list(client.read_journal(output / 'trace.jsonl'))
        self.assertTrue(all((row['operation'] == 'get' for row in replay if row['kind'] == 'operation_started')))
        self.assertTrue(all((row['source_ack_evidence'] == 'durable_client_ack' for row in replay if row['kind'] == 'operation_started')))

    def test_reconciliation_cap_under_concurrency_cannot_claim_all_targets(self):
        root, peer = self.fixture()
        first, _, _ = self.controller(root, peer, max_operations=4)
        self.assertEqual(first.returncode, 0)
        output = root / 'capped-reconciliation'
        argv = ['python3', str(HERE / 'trace_client.py'), 'traffic', '--worker-command', str(root / 'command.json'), '--expected-worker-sha256', hashlib.sha256(WORKER.read_bytes()).hexdigest(), '--expected-linked-core-source', CORE_SOURCE, '--expected-linked-core-version', CORE_VERSION, '--namespace', 'trace-selftest', '--phase', 'capped-reconcile', '--seconds', '30', '--concurrency', '2', '--observer-workers', '0', '--max-operations', '1', '--minimum-free-bytes', '0', '--output', str(output), '--reconcile-journal', str(root / 'controller/trace.jsonl')]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=90)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
        self.assertFalse(summary['planned_experiment_complete'])
        self.assertFalse(summary['workload_passed'])
        coverage = summary['reconciliation']
        self.assertEqual(len(coverage['required_target_ids']), 2)
        self.assertEqual(len(coverage['attempted_target_ids']), 1)
        self.assertEqual(len(coverage['terminal_target_ids']), 1)
        self.assertEqual(len(coverage['unattempted_target_ids']), 1)
        self.assertFalse(coverage['coverage_complete'])
        self.assertEqual(summary['termination_reason'], 'dispatch_fenced')
        with self.assertRaisesRegex(ValueError, 'required reconciliation targets'):
            client.validate_trace(output / 'trace.jsonl')

    def test_tls_membership_pin_is_not_relaxed(self):
        root, peer = self.fixture(wrong_pin=True)
        journal = client.Journal(root / 'journal', min_free_bytes=0)
        self.addCleanup(journal.close)
        worker = client.WireWorker([str(WORKER), str(peer.fixture), 'trace-selftest'], 0, journal, root)
        self.addCleanup(worker.stop)
        with self.assertRaises(client.TraceFailure) as caught:
            worker.rpc({'logical_id': 'pin-test'}, 'n0', 'FetchTopology', peer.topology, io_ms=500, connect_ms=100)
        self.assertIn('certificate not bound', caught.exception.detail['wire']['error']['display'])
        self.assertEqual(caught.exception.detail['wire']['last_stage'], 'tls_handshake')
        self.assertEqual(peer.requests, [])

    def test_watermark_probes_record_all_voters_as_observers(self):
        root, peer = self.fixture(lost_first=True)
        result, summary, rows = self.controller(root, peer, observers=1)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        probes = [r for r in rows if r['kind'] == 'watermark_observation_completed']
        self.assertTrue(probes)
        self.assertEqual(probes[0]['samples'][0]['node'], 'n0')
        self.assertTrue(any((r.get('observer') and r.get('purpose') == 'watermark' for r in rows)))
        self.assertEqual(summary['observation_coverage_gaps'], 0)
if __name__ == '__main__':
    unittest.main()
