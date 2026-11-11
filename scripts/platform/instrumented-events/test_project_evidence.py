import gzip
import hashlib
import json
import pathlib
import tempfile
import unittest

import project_evidence as evidence
import trace_client


class ProjectionTests(unittest.TestCase):
    def test_every_record_failure_latency_and_original_source_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory); source=root/'private.jsonl'
            journal=trace_client.Journal(source,min_free_bytes=0)
            journal.append('operation_started',logical_id='id',operation='append',expected_payload={'payload':'xxx'},expected_payload_sha256='expected')
            journal.append('rpc_failure',logical_id='id',error={'code':'code','message':'10.0.0.1:9444 full error body'})
            journal.append('operation_terminal',logical_id='id',operation='append',outcome='uncertain_write',logical_elapsed_ns=123)
            journal.close(); original=source.read_bytes()
            result=evidence.collect(source,root/'records.gz',trace_client.read_journal,{'10.0.0.1:9444':'n0:9444'})
            rows=[json.loads(line)for line in gzip.open(root/'records.gz','rt')]
            self.assertEqual(3,result['projected_records']);self.assertEqual(3,len(rows))
            self.assertEqual({'append:uncertain_write':1},result['raw_latency_sample_counts'])
            self.assertEqual('n0:9444 full error body',rows[1]['error']['message'])
            self.assertEqual(123,rows[2]['logical_elapsed_ns']);self.assertEqual(original,source.read_bytes())
            self.assertEqual(hashlib.sha256(original).hexdigest(),result['raw_journal_sha256'])
            self.assertFalse(result['strong_read_proof']);self.assertFalse(result['projection_is_raw_wire_capture'])
            self.assertEqual('not_qualified',result['qualification_status'])

    def test_payload_and_received_body_limits_are_explicit(self):
        value={'value':{'payload':'xx','run':'r'},'response_body_raw':'body','response_body_sha256':'raw-original-sha'}
        result=evidence.project(value,{})
        self.assertEqual(evidence.sha(value['value']),result['original_value_canonical_sha256'])
        self.assertEqual(2,result['value']['payload_bytes_omitted'])
        self.assertNotIn('response_body_raw',result);self.assertTrue(result['raw_response_body_omitted'])
        self.assertEqual('raw-original-sha',result['response_body_sha256'])

    def test_private_key_text_is_refused(self):
        with self.assertRaisesRegex(ValueError,'private key'):
            evidence.project({'error':'-----BEGIN PRIVATE KEY----- secret'}, {})

    def test_torn_tail_never_produces_completed_receipt(self):
        with tempfile.TemporaryDirectory()as directory:
            root=pathlib.Path(directory);source=root/'private.jsonl';source.write_bytes(b'{"kind":"unfinished"}')
            with self.assertRaisesRegex(ValueError,'torn'):
                evidence.collect(source,root/'records.gz',trace_client.read_journal,{})

    def test_deterministic_gzip_projection(self):
        with tempfile.TemporaryDirectory()as directory:
            root=pathlib.Path(directory)
            evidence.write_gzip(root/'one.gz',[{'x':1}]);evidence.write_gzip(root/'two.gz',[{'x':1}])
            self.assertEqual((root/'one.gz').read_bytes(),(root/'two.gz').read_bytes())


if __name__=='__main__':
    unittest.main()
