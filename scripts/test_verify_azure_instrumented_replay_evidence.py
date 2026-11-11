import gzip
import collections
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import tempfile
import unittest

HERE=pathlib.Path(__file__).resolve().parent
SPEC=importlib.util.spec_from_file_location('replay_verifier',HERE/'verify-azure-instrumented-replay-evidence.py')
VERIFIER=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(VERIFIER)
BUNDLE=HERE.parent/'docs/evidence/azure-instrumented-replay-2026-11-11'


def write(path,value):
    temporary=path.with_name(path.name+'.replacement')
    temporary.write_text(json.dumps(value,indent=2)+'\n')
    os.replace(temporary,path)


class EvidenceTests(unittest.TestCase):
    def clone(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        root=pathlib.Path(temporary.name)/'bundle'
        shutil.copytree(BUNDLE,root,copy_function=os.link)
        return root

    def rehash(self,root,relative):
        path=root/relative;manifest=json.loads((root/'manifest.json').read_text())
        manifest['files'][relative]={'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        write(root/'manifest.json',manifest)

    def test_complete_projection_and_uncertain_outcome_accounting(self):
        result=VERIFIER.verify(BUNDLE)
        self.assertEqual(3261,result['known_ack_keys_observed_exact_after_restart'])
        self.assertEqual({'exact_match':7,'absent':1},result['uncertain_write_observations'])
        self.assertFalse(result['production_qualified']);self.assertFalse(result['strong_read_proof'])
        self.assertFalse(result['four_to_five_cluster_join_executed'])
        first=result['phases']['first-attempt/baseline-4']
        self.assertEqual(237,first['acknowledged_logical_ids_with_unknown_rpc_history'])
        self.assertEqual(8817,first['individual_rpc_calls'])
        self.assertEqual(10534189113,first['logical_latency_ns']['append:acknowledged']['p99_ns'])

    def test_critical_omission_rejected_even_when_manifest_is_updated(self):
        root=self.clone();(root/'claims.json').unlink();m=json.loads((root/'manifest.json').read_text());del m['files']['claims.json'];write(root/'manifest.json',m)
        with self.assertRaisesRegex(ValueError,'critical artifact'):VERIFIER.verify(root)

    def test_tamper_rejected(self):
        root=self.clone();(root/'README.md').unlink();(root/'README.md').write_text('altered')
        with self.assertRaisesRegex(ValueError,'manifest mismatch'):VERIFIER.verify(root)

    def test_unlisted_file_rejected(self):
        root=self.clone();(root/'unlisted.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'exact artifact'):VERIFIER.verify(root)

    def test_symlink_rejected(self):
        root=self.clone();(root/'README.md').unlink();(root/'README.md').symlink_to(BUNDLE/'README.md')
        with self.assertRaisesRegex(ValueError,'symlink'):VERIFIER.verify(root)

    def test_unsupported_qualification_claim_rejected_after_rehash(self):
        root=self.clone();p=root/'claims.json';claims=json.loads(p.read_text());claims['production_qualified']=True;write(p,claims);self.rehash(root,'claims.json')
        with self.assertRaisesRegex(ValueError,'unsupported qualification'):VERIFIER.verify(root)

    def test_missing_phase_rejected_after_rehash(self):
        root=self.clone();name='light-attempt/two-target-fixture/projection.json';(root/name).unlink();m=json.loads((root/'manifest.json').read_text());del m['files'][name];write(root/'manifest.json',m)
        with self.assertRaisesRegex(ValueError,'critical phase'):VERIFIER.verify(root)

    def test_reconciliation_verified_set_is_independently_recomputed(self):
        root=self.clone();p=root/'light-attempt/two-target-finally-reconciliation/summary.json';d=json.loads(p.read_text());d['reconciliation']['verified_target_ids'].pop();write(p,d)
        with self.assertRaisesRegex(ValueError,'target-set mismatch'):VERIFIER.phase(p.parent)

    def rewrite_records(self,root,relative,mutate):
        folder=root/relative;path=folder/'records.jsonl.gz'
        with gzip.open(path,'rt')as stream:rows=[json.loads(line)for line in stream]
        mutate(rows)
        payload=''.join(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n'for row in rows).encode()
        temporary=path.with_name('records.replacement');temporary.write_bytes(gzip.compress(payload,mtime=0));os.replace(temporary,path)
        metadata=json.loads((folder/'projection.json').read_text());metadata['projection_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();metadata['record_counts']=dict(collections.Counter(row['kind']for row in rows));write(folder/'projection.json',metadata)

    def test_ack_requires_prior_appended_response(self):
        root=self.clone();relative='light-attempt/two-target-fixture'
        def change(rows):
            for row in rows:
                if row['kind']=='rpc_completed'and row.get('operation')=='append'and row.get('purpose')=='application'and row['wire'].get('transport_success'):
                    row['wire']['response_envelope']['response']['Ok']['ApplicationEvent']['result']='not_appended';break
        self.rewrite_records(root,relative,change)
        with self.assertRaisesRegex(ValueError,'prior same-operation validated Appended'):VERIFIER.phase(root/relative)

    def test_ack_requires_durable_receipt(self):
        root=self.clone();relative='light-attempt/two-target-fixture'
        def change(rows):
            next(row for row in rows if row['kind']=='ack_durable_receipt')['kind']='receipt_missing'
        self.rewrite_records(root,relative,change)
        with self.assertRaisesRegex(ValueError,'durable receipt'):VERIFIER.phase(root/relative)

    def test_observer_pending_is_recomputed(self):
        root=self.clone();relative='light-attempt/two-target-finally-reconciliation'
        def change(rows):
            next(row for row in rows if row['kind']=='watermark_observation_completed')['kind']='completion_missing'
        self.rewrite_records(root,relative,change)
        with self.assertRaisesRegex(ValueError,'observer pending'):VERIFIER.phase(root/relative)

    def test_retained_client_source_is_bound_to_identity(self):
        root=self.clone();name='executed/trace_client.py';path=root/name;data=path.read_bytes();path.unlink();path.write_bytes(data+b'\n');self.rehash(root,name)
        with self.assertRaisesRegex(ValueError,'retained client source'):VERIFIER.verify(root)

    def test_numeric_claim_is_recomputed(self):
        root=self.clone();path=root/'claims.json';d=json.loads(path.read_text());d['first_attempt']['acknowledged']+=1;write(path,d);self.rehash(root,'claims.json')
        with self.assertRaisesRegex(ValueError,'numeric/phase claim'):VERIFIER.verify(root)

    def test_private_comparison_suppresses_bytecode_and_preserves_inputs(self):
        with tempfile.TemporaryDirectory()as directory:
            base=pathlib.Path(directory);private=base/'private';public=base/'public';private.mkdir();public.mkdir();source=private/'checker.py';source.write_text('VALUE=1\n')
            def run():
                spec=importlib.util.spec_from_file_location('no_bytecode_fixture',source);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);self.assertEqual(1,module.VALUE);return {}
            result=VERIFIER.readonly_private_comparison(public,private,run)
            self.assertTrue(result['private_and_public_file_sets_modes_sizes_unchanged']);self.assertFalse((private/'__pycache__').exists())

    def test_private_comparison_detects_side_effect(self):
        with tempfile.TemporaryDirectory()as directory:
            base=pathlib.Path(directory);private=base/'private';public=base/'public';private.mkdir();public.mkdir()
            def run():(private/'unexpected').write_text('side effect');return {}
            with self.assertRaisesRegex(ValueError,'changed evidence'):VERIFIER.readonly_private_comparison(public,private,run)

    def test_nearest_rank_extreme_boundaries(self):
        self.assertEqual({'samples':0,'p50_ns':None,'p95_ns':None,'p99_ns':None,'max_ns':None},VERIFIER.distribution([]))
        self.assertEqual(99,VERIFIER.distribution(list(range(1,101)))['p99_ns'])
        self.assertEqual(17,VERIFIER.distribution([17])['p95_ns'])


if __name__=='__main__':unittest.main()
