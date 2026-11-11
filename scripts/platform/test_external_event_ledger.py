import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('external_event_ledger', Path(__file__).with_name('external-event-ledger.py'))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class ExternalLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'ledger.jsonl'
        self.ledger = driver.Ledger(self.path)
        self.addCleanup(self.ledger.close)
        self.value = {'fixture': 'expected'}

    def append(self, call):
        return driver.append_once(self.ledger, call, 'op', 'namespace', 'key', self.value, 0)

    def test_ack_return_follows_successful_fsync(self):
        with mock.patch.object(driver.os, 'fsync') as sync:
            self.assertTrue(self.append(lambda *args: {'inserted': True})['acknowledged'])
            self.assertEqual(sync.call_count, 2)
        self.assertEqual(len(driver.acknowledged_records(self.path)), 1)

    def test_uncertain_transport_is_not_an_ack(self):
        def crash(*args):
            raise TimeoutError('response lost after possible commit')
        self.assertFalse(self.append(crash)['acknowledged'])
        self.assertEqual(driver.acknowledged_records(self.path), [])
        self.assertEqual([json.loads(line)['kind'] for line in self.path.read_text().splitlines()], ['attempt', 'uncertain'])

    def test_persistence_failure_before_rpc_does_not_dispatch(self):
        call = mock.Mock()
        with mock.patch.object(driver.os, 'fsync', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.append(call)
        call.assert_not_called()
        with self.assertRaises(RuntimeError):
            self.append(call)
        call.assert_not_called()

    def test_ack_fsync_failure_never_returns_success(self):
        call = mock.Mock(return_value={'inserted': True})
        with mock.patch.object(driver.os, 'fsync', side_effect=[None, OSError('I/O')]):
            with self.assertRaises(OSError):
                self.append(call)
        self.assertTrue(self.ledger.failed)

    def test_retry_identity_stays_stable_and_deduplicated(self):
        self.append(lambda *args: {'inserted': True})
        self.append(lambda *args: {'inserted': False})
        records = driver.acknowledged_records(self.path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['value'], self.value)
        self.value = {'fixture': 'changed'}
        call = mock.Mock(return_value={'inserted': True})
        with self.assertRaises(ValueError):
            self.append(call)
        call.assert_not_called()

    def test_payload_mismatch_or_unavailable_reader_is_failure(self):
        self.append(lambda *args: {'inserted': True})
        self.assertFalse(driver.verify(self.path, lambda *args: {'value': None})['verification_passed'])
        self.assertTrue(driver.verify(self.path, lambda *args: {'value': self.value})['verification_passed'])

    def test_torn_ledger_is_not_silently_truncated(self):
        self.append(lambda *args: {'inserted': True})
        with self.path.open('a') as output:
            output.write('{')
        with self.assertRaises(ValueError):
            driver.acknowledged_records(self.path)

    def test_remote_command_is_quoted_and_no_topology_is_copied(self):
        remote = driver.RemoteEvents({'format': 1, 'members': [{'ssh': 'bench', 'member_root': '/tmp/db space', 'binary': '/tmp/bicdb'}]})
        with mock.patch.object(driver.subprocess, 'run', return_value=mock.Mock(returncode=0, stdout='{"inserted":false}')) as execute:
            self.assertFalse(remote.call(0, 'append', 'ns', 'key', {'body': "'; touch bad"})['inserted'])
        argv = execute.call_args.args[0]
        self.assertEqual(argv[0], 'ssh')
        self.assertIn("'/tmp/db space'", argv[-1])
        self.assertNotIn('scp', argv)


if __name__ == '__main__':
    unittest.main()
