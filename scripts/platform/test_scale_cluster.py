import importlib.util
from pathlib import Path
import json
import os
import tempfile
import unittest
import uuid

spec = importlib.util.spec_from_file_location('scale_cluster', Path(__file__).with_name('scale-cluster.py'))
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)


class HarnessContracts(unittest.TestCase):
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
