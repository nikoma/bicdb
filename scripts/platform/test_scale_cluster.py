import importlib.util
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid

spec = importlib.util.spec_from_file_location('scale_cluster', Path(__file__).with_name('scale-cluster.py'))
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)


class HarnessContracts(unittest.TestCase):
    def test_four_to_five_and_historical_growth_profiles_are_supported(self):
        self.assertEqual(scale.parse_sizes('4,5'), [4, 5])
        self.assertEqual(scale.parse_sizes('3,5,9'), [3, 5, 9])
        self.assertEqual(scale.parse_sizes('3,4,5,9'), [3, 4, 5, 9])
        for invalid in ('', '4,4', '5,4', '2,4', '4,6', '4,five', '4,'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                scale.parse_sizes(invalid)

    def test_four_to_five_cli_reaches_workload_validation_without_starting_nodes(self):
        result = subprocess.run([sys.executable, str(Path(scale.__file__)), '--local',
                                 '--binary', '/does/not/exist', '--output', '/does/not/exist',
                                 '--sizes', '4,5', '--seconds', '0'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('invalid workload bounds', result.stderr)
        self.assertNotIn('sizes must be', result.stderr)

    def test_independent_qualification_request_refuses_before_accessing_hosts_or_binary(self):
        result = subprocess.run([sys.executable, str(Path(scale.__file__)), '--local',
                                 '--binary', '/does/not/exist', '--output', '/does/not/exist',
                                 '--sizes', '4,5', '--require-independent-qualification'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('independent qualification unavailable', result.stderr)
        self.assertIn('external per-acknowledgment durability', result.stderr)
        self.assertNotIn('Traceback', result.stderr)

    def test_remote_inventory_rejects_duplicate_addresses_and_option_injection(self):
        with self.assertRaises(ValueError):
            scale.validate_inventory([dict(ssh='-oProxyCommand=anything', address='10.0.0.1:9444')], 1)
        with self.assertRaises(ValueError):
            scale.validate_inventory([dict(ssh='first', address='10.0.0.1:9444'),
                                      dict(ssh='second', address='10.0.0.1:9444')], 2)

    def test_runner_creates_only_a_new_directory_and_refuses_an_unowned_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            host = scale.Host(dict(address='127.0.0.1:9444', directory=directory), 0,
                              uuid.uuid4().hex, '/bin/true', True)
            self.assertTrue(host.action('mkdir')['created'])
            with self.assertRaises(RuntimeError): host.action('mkdir')
            Path(host.root, 'process.json').write_text(json.dumps(dict(pid=os.getpid(), start='invalid')))
            with self.assertRaisesRegex(RuntimeError, 'outside this run'):
                host.action('signal', signal='SIGKILL')

    def test_capacity_requires_serving_leaders_and_no_remaining_relocation(self):
        status = dict(topology=dict(nodes={'n0':dict(metadata_role='voter')}, ranges={'0':dict(leader='n0')}),
                      metrics=dict(under_replicated_ranges=0, unavailable_ranges=0, active_relocations=1))
        self.assertFalse(scale.capacity_ready(status, 1))
        status['metrics']['active_relocations'] = 0
        self.assertTrue(scale.capacity_ready(status, 1))
        del status['topology']['nodes']['n0']['metadata_role']
        self.assertTrue(scale.capacity_ready(status, 1), 'default voter roles are omitted on the wire')
        status['metrics']['failed_relocations'] = 1
        self.assertFalse(scale.capacity_ready(status, 1), 'failed movements are not usable capacity')
        status['metrics']['failed_relocations'] = 0
        status['topology']['nodes']['n1'] = dict(metadata_role='learner')
        self.assertFalse(scale.capacity_ready(status, 2))


if __name__ == '__main__': unittest.main()
