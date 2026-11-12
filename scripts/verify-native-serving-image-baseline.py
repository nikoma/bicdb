#!/usr/bin/env python3
"""Read-only baseline counterexample verification, never production qualification."""
import argparse
import hashlib
import json
from pathlib import Path

SOURCE='1978a0ea8eec099ab5285c7ff5d30118ae5c1825'
DEFAULT=Path(__file__).resolve().parents[1]/'docs/evidence/native-serving-image-baseline-2026-11-11'


def verify(root=DEFAULT):
    root=Path(root)
    manifest=json.loads((root/'manifest.json').read_text())
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p.name!='manifest.json'}
    assert set(manifest)==actual
    for rel,digest in manifest.items():
        path=Path(rel);assert not path.is_absolute() and '..' not in path.parts
        assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,rel
    summary=json.loads((root/'summary.json').read_text())
    assert summary['source_revision']==SOURCE
    assert summary['scope']=='UNMERGED_NATIVE_CANDIDATE_INTEGRATION_COUNTEREXAMPLE'
    assert summary['current_main_or_azure_attribution'] is False
    assert summary['outcome']=='REPRODUCED_STALE_SERVING_BASE_BINDING_REFUSAL'
    assert summary['qualification']=={'physically_tested':False,'production_qualified':False,'deployed':False,'activation_proven':False,'subsequent_acknowledged_write_proven':False,'irretrievable_data_loss_claimed':False,'rpo':None,'rto':None,'p50':None,'p95':None,'p99':None}
    result=json.loads((root/summary['confirmed_result']).read_text())
    assert result['source_revision']==SOURCE and result['exit_code']==101
    assert result['dirty_diff_sha256']==hashlib.sha256(b'').hexdigest()
    assert result['guard_stop_reason'] is None and result['pause_episodes']==0
    stdout=(root/'runs/attempt-2-serving-repair.stdout').read_text()
    stderr=(root/'runs/attempt-2-serving-repair.stderr').read_text()
    assert 'test result: FAILED. 0 passed; 1 failed;' in stdout
    assert 'Compiling bicdb-core v1.0.471-beta (/tmp/bicdb-native-serving-repair/crates/bicdb-core)' in stderr
    assert 'a new actual compatible image must prepare' in stderr
    assert 'recovered serving record does not bind the actual same-voter physical/native domain' in stderr
    initial=json.loads((root/summary['initial_result']).read_text())
    assert initial['source_revision'].startswith('8d7e0d4c') and initial['exit_code']==101
    assert 'closed native-ready quorum belongs to a different actual recipient' in (root/'runs/baseline-serving-repair.stderr').read_text()
    before=json.loads((root/'observations/confirmed/serving-image-witness-before-b.json').read_text())
    after=json.loads((root/'observations/confirmed/serving-image-witness-after-b.json').read_text())
    key=str(before['new_scope']['range_id'])
    a=before['catalogue_before_b']['catalog']['native_image_bases'][key]
    b=after['catalogue']['catalog']['native_image_bases'][key]
    assert a['range_index']==b['range_index']==1
    assert a['logical_history_sha256']==b['logical_history_sha256']
    assert a['native_birth_id']==b['native_birth_id']
    assert a['native_birth_sha256']==b['native_birth_sha256']
    assert a['native_receipt_sha256']!=b['native_receipt_sha256']
    findings=summary['findings']
    assert findings['source_revision']==SOURCE and findings['source_is_unmerged_native_candidate'] is True
    for flag in ['current_main_or_azure_defect_attribution','irretrievable_data_loss_claimed','physical_qualification','activation_proven','ordinary_second_mvcc_get_separately_measured']:
        assert findings[flag] is False,flag
    assert findings['native_receipt_a']==a['native_receipt_sha256']
    assert findings['native_receipt_b']==b['native_receipt_sha256']
    assert findings['logical_history_a']==findings['logical_history_b']==a['logical_history_sha256']
    assert before['attempt_a']!=before['attempt_b']
    assert before['old_nonce']!=before['new_scope']['recovery_nonce']
    assert before['serving_a']==after['serving_record']
    assert before['serving_a']['physical_base_sha256']==a['native_receipt_sha256']
    assert before['serving_a']['range']['epoch']==before['new_scope']['range_epoch']==1
    assert after['catalogue']['catalog']['recovery_fences'][key]['phase']=='Active'
    intent=after['catalogue']['catalog']['image_install_intents'][key]
    assert intent['attempt_id']==before['attempt_b']
    assert ['items',before['target_uid']] in intent['target_namespace_ids']
    assert summary['database_artifacts_in_public_bundle'] is False
    assert summary['independent_cold_snapshot_a_retained'] is False
    assert summary['ordinary_second_mvcc_get_separately_measured'] is False
    custody=json.loads((root/summary['custody']).read_text())
    assert custody['files']==118 and custody['all_source_destination_hashes_match'] is True
    assert custody['manifest_sha256']=='d1e10ab900e82f49bf95f677ae143d98d9c4ffeaf71cd8fe716b54ffef2dbdf8'
    for path,digest in summary['source_snapshots'].items():
        assert hashlib.sha256((root/'source'/path).read_bytes()).hexdigest()==digest
    return {'status':'VERIFIED_EXPECTED_RED','source_revision':SOURCE,'scope':summary['scope'],'production_qualified':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root',type=Path,default=DEFAULT)
    print(json.dumps(verify(parser.parse_args().evidence_root),indent=2))
