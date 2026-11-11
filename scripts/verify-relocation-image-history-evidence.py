#!/usr/bin/env python3
"""Check retained characterization only; no engine, cloud, or archive writes."""
import hashlib
import json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
p=root/'docs/evidence/relocation-image-history-2026-11-11'
manifest=json.loads((p/'manifest.json').read_text())
for name,digest in manifest.items():
    assert Path(name).name==name
    assert hashlib.sha256((p/name).read_bytes()).hexdigest()==digest,name
build=json.loads((p/'build.json').read_text())
assert build['source_commit']=='d49a391b3c6157f90f29e9f2f5214143c60f9f4b'
assert hashlib.sha256((root/'scripts/experiments/relocation-image-characterization.rs').read_bytes()).hexdigest()==build['source_sha256']
assert build['libraries']['bicdb_core']=='dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356'
for name,count in [('below',8),('compacted',1025)]:
    result=json.loads((p/(name+'-result.json')).read_text())
    assert result['exit_code']==0 and result['stop_reason'] is None
    assert result['source_sha256']==build['source_sha256']
    event=next(json.loads(line[line.index('{'):]) for line in (p/(name+'.stdout')).read_text().splitlines() if '{"case":' in line)
    assert event['commits_returned_success']==count
    assert event['target_payload_after_recovery']==event['target_payload']=={'last_command':count}
    assert event['target_range_log_before_recovery']['resolved_through']==0
    assert set(event['control_plane_reopened_phases'])=={'SnapshotCopying','CatchingUp'}
    topology=event['final_topology']; r=next(iter(topology['ranges'].values()))
    assert r['leader']=='n3' and r['epoch']==4
    assert {replica['node_id'] for replica in r['replicas']}=={'n1','n2','n3'}
    relocation=next(iter(topology['relocations'].values()))
    assert relocation['phase']=='completed' and relocation['cleanup_records_deleted']==1
    assert relocation['snapshot_commit_sequence']==relocation['source_commit_sequence']==relocation['destination_durable_commit_sequence']==count
    assert any(step['after']=='snapshot_copying' and step['target_payload'] is not None and step['learner']['snapshot_complete'] is False for step in event['relocation_phases'])
    assert any(step['after']=='catching_up' and step['learner']['snapshot_complete'] is True for step in event['relocation_phases'])
    for step in event['relocation_phases']:
        if step['after'] in ['snapshot_copying','catching_up']:
            assert step['premature_promotion_error'] is not None
    if count==8:
        assert event['recovery']=='Ok(1)'
        assert event['target_range_log_after_recovery']['resolved_through']==8
    else:
        assert all(progress['resolved_through']==1025 and progress['compacted_through']==1 for progress in event['before_relocation'][:3])
        assert 'requires a range snapshot at index 0; quorum resolved through 1025' in event['recovery']
        assert event['target_range_log_after_recovery']['resolved_through']==0
summary=json.loads((p/'summary.json').read_text())
assert summary['findings']['compacted_quorum_recovery_availability']=='FAILED'
assert summary['findings']['retained_suffix_healthy_control']=='PASSED'
assert summary['findings']['physical_ha']=='NOT_TESTED'
print('Source-pinned relocation image/history finding verified: compacted recovery FAILED; healthy retained-suffix control PASSED; no physical HA qualification.')
