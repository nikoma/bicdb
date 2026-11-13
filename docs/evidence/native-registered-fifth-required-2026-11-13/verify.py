#!/usr/bin/env python3
"""Offline failure evidence. Optional ZIP verifies private bytes without extraction or engine access."""
import argparse,gzip,hashlib,json,pathlib,re,zipfile
ROOT=pathlib.Path(__file__).resolve().parent
SOURCE='a98c6a6b1390b0a3853e4ae658e8e103a7749b19'
ERROR='cluster error: cluster node n4 rejected RPC cluster_data_operation_failed: cluster error: enforced native writer host policy is not installed'
ZIP_SHA='b2faf53366223b5bbb5487b30d2c945d358c1582c298ab1c9009b0f8482c1c6e'
MANIFEST_SHA='482e8aeb41317301d7d3f0d213ac388f39dbf05f49d121bcb3721ebfec896953'

def sha(data):return hashlib.sha256(data).hexdigest()
def same(a,b):return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))
def terminal(raw):
    lines=[line for line in raw.decode().splitlines() if 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in line]
    assert len(lines)==1
    return json.loads(lines[0][lines[0].index('{'):])
def observation(z,base,index,manifest):
    path=f'node{index}/cluster-range-write-log.json';data=z.read(base+'/custody/'+path);envelope=json.loads(data);catalog=envelope['catalog']
    assert sha(json.dumps(catalog,separators=(',',':'),ensure_ascii=False).encode())==envelope['checksum_sha256'],'range catalogue JSON envelope checksum'
    ranges={key:{'current_epoch':value['current_epoch'],'last_index':value['last_index'],'resolved_through':value['resolved_through'],'compacted_through':value['compacted_through'],'entries':{idx:{'state':entry['state'],'command_sha256':entry['command']['checksum_sha256']} for idx,entry in value['entries'].items()}} for key,value in catalog['ranges'].items()}
    meta_path=next(n for n in z.namelist() if n.startswith(base+f'/custody/node{index}/') and n.endswith('/cluster-metadata-consensus.json'));meta_data=z.read(meta_path);meta=json.loads(meta_data)
    assert sha(json.dumps(meta['state'],separators=(',',':'),ensure_ascii=False).encode())==meta['checksum_sha256'],'metadata JSON envelope checksum'
    return dict(node=f'n{index}',range_catalogue_path=path,range_catalogue_bytes=len(data),range_catalogue_sha256=sha(data),range_catalogue_envelope_checksum=envelope['checksum_sha256'],range_catalogue_envelope_checksum_verified=True,format_version=catalog['format_version'],writer_enforcement_version=catalog.get('native_writer_enforcement_version'),ranges=ranges,recovery_fences=catalog.get('recovery_fences',{}),abort_domains=sorted(catalog['native_abort_protocol']['ranges']),metadata_path=meta_path.split(base+'/custody/',1)[1],metadata_bytes=len(meta_data),metadata_sha256=sha(meta_data),metadata_envelope_checksum=meta['checksum_sha256'],metadata_envelope_checksum_verified=True,metadata_status=manifest['metadata'][index],native_pages_decoded_or_replayed=False)

def verify(root=ROOT,private_zip=None):
    m=json.loads((root/'MANIFEST.json').read_text())
    for item in m['files']:
        data=(root/item['path']).read_bytes();assert len(data)==item['bytes'] and sha(data)==item['sha256'],item['path']
    result=json.loads((root/'results.json').read_text());receipt=json.loads((root/'artifact-receipt.json').read_text())
    logs={}
    for label,job,status in [('core',131616827260,'failure'),('compile',131616783865,'success')]:
        compressed=(root/(label+'.log.gz')).read_bytes();raw=gzip.decompress(compressed);idx=json.loads((root/(label+'-index.json')).read_text())
        assert idx['source_revision']==SOURCE and idx['run_id']==43343369839 and idx['job_id']==job and idx['job_conclusion']==status
        assert len(raw)==idx['raw_bytes'] and sha(raw)==idx['raw_sha256']
        assert sha(compressed)==idx.get('gzip_sha256',idx.get('compressed_sha256'))
        assert SOURCE in [re.sub(r'^\S+Z ','',x) for x in raw.decode().splitlines()],'actual checkout source line'
        logs[label]=raw
    assert 'test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 980 filtered out; finished in 4.31s' in logs['core'].decode()
    actual=terminal(logs['core']);acks=json.loads((root/'ack-ledger.json').read_text());fences=json.loads((root/'fence-ledger.json').read_text())
    assert actual['source']==result['source']==receipt['source']==SOURCE
    assert same(actual['ack_ledger'],acks) and same(actual['reports'],fences),'full actual log ledger/scope/error linkage'
    assert actual['phase']==result['phase']=='required_fence_acceptance'
    assert actual['required_baseline_succeeded'] is result['required_baseline_succeeded'] is False
    assert actual['actual_original_writer_guards_retained']==result['original_writer_guards_retained']==4
    assert actual['primary_error']==result['primary_error'] and 'left: 0\n right: 2' in result['primary_error']
    assert result['core']==dict(passed=0,failed=1,ignored=0,filtered=980,duration_s=4.31)
    assert result['actual_required_fence_successes']==0 and result['required_fence_successes']==2
    assert len(acks)==len(fences)==2
    for index,ack in enumerate(acks,1):
        expected={'nested':[None,17,{'full':'payload'}],'object':{},'registered_fifth_source':index}
        assert ack['actual_api_inserted'] is True and ack['actual_api_error'] is None and ack['outcome']=='returned'
        assert ack['operation']=='ClusterEventClient.append' and ack['actual_api_caller']=='n0' and ack['logical_ledger_not_raw_wire']
        assert ack['range']==index and ack['range_epoch']==1 and ack['topology_generation']==4
        assert ack['expected_route_leader']==f'n{index}' and same(ack['value'],expected)
        assert same(ack['actual_full_record'],dict(id=ack['record_id'],metadata=ack['value'],payload=None,timestamp=None,vector=None))
        assert ack['record_id']=='items/'+ack['key']
        assert sha(json.dumps(ack['value'],sort_keys=True,separators=(',',':')).encode())==ack['payload_json_sha256']
    for index,fence in enumerate(fences,1):
        assert fence['range']==index and fence['range_epoch']==2 and fence['topology_generation']==5
        assert fence['actual_target']=='n4' and fence['actual_source']['node_id']==f'n{index}' and not fence['actual_source']['plaintext_loopback']
        assert fence['actual_error']==ERROR and fence['actual_reply_ok'] is False and fence['actual_success_observation'] is None,'actual missing-writer refusal is not success/timeout'
        assert fence['outcome']=='error_reply_unknown_effect' and fence['logical_ledger_not_raw_wire']
        assert fence['original_role_and_caller_live_at_reply'] and fence['role_error_at_reply'] is None and fence['caller_error_at_reply'] is None
        assert fence['original_parent_remaining_ms']==[4939,4937][index-1] and fence['original_role_remaining_ms']==[1437,1435][index-1]
        assert fence['configured_parent_budget_ms']==5000 and fence['configured_request_budget_ms']==4000
        assert fence['configured_source_network']['io_timeout_ms']==fence['configured_target_network']['io_timeout_ms']==1500
        assert not fence['rows_copied'] and not fence['serving_granted']
        assert same(fence['before'],fence['after']),'full actual target state must remain identical'
        before=fence['before'];assert before['B']==0 and not before['writer_installed'] and not before['slots']
        # B0 is an actual Core counter, NEVER a verified native range R0.
        for point in before['points'].values():
            assert point['native_applied_index'] is None and point['native_base_index'] is None and point['native_receipt_sha256'] is None
            assert point['pending_command_index'] is None and point['materialized_pending_index'] is None and point['invalidated'] is True,'UNKNOWN remains UNKNOWN/invalidated, never minted zero'
        scope=fence['scope'];assert scope['range_id']==index and scope['range_epoch']==2 and scope['initiator_node_id']==f'n{index}'
        assert scope['business_request_id']==fence['business_id'] and same(scope['purpose'],fence['purpose'])
        purpose=scope['purpose']['CommittedHandoff'];assert purpose['nonce']==scope['recovery_nonce']
        assert scope['metadata_term']==1 and scope['metadata_read_index']==5
        config=fence['configuration'];assert config['generation']==5
        plan=config['relocations'][str(index)];assert plan['target']=='n4' and plan['learner_epoch']==2 and plan['expected_epoch']==1
        assert fence['receiver']['receiver_ready'] and not fence['receiver']['data_serving'] and fence['receiver']['writer_admission']=='not installed by replica-only owner'
    manifest=json.loads((root/'custody-manifest.json').read_text());assert sha((root/'custody-manifest.json').read_bytes())==MANIFEST_SHA
    assert manifest['source']==SOURCE and manifest['all_five_metadata_service_db_native_journal_guards_held'] and manifest['engine_owners_alive']
    assert not manifest['listeners_drained'] and not manifest['engine_reopened'] and not manifest['native_replay_or_checkpoint'] and not manifest['atomic_powerloss_snapshot_claimed']
    assert len(manifest['files'])==96 and sum(x['bytes'] for x in manifest['files'])==manifest['total_bytes']==2909809
    assert {x['node_id'] for x in manifest['metadata']}=={'n0','n1','n2','n3','n4'}
    assert all(x['topology_generation']==5 and x['commit_index']==5 and x['current_term']==1 for x in manifest['metadata'])
    assert receipt['zip_sha256']==ZIP_SHA and receipt['custody_manifest_sha256']==MANIFEST_SHA and receipt['zip_bytes']==233323
    assert receipt['custody_payload_files_verified']==96 and receipt['custody_payload_bytes_verified']==2909809
    observations=json.loads((root/'catalogue-observations.json').read_text());files={x['path']:x for x in manifest['files']}
    for obs in observations:
        assert obs['format_version']==15 and obs['writer_enforcement_version']==1
        assert obs['range_catalogue_sha256']==files[obs['range_catalogue_path']]['sha256'] and obs['range_catalogue_bytes']==files[obs['range_catalogue_path']]['bytes']
        assert obs['metadata_sha256']==files[obs['metadata_path']]['sha256'] and obs['metadata_bytes']==files[obs['metadata_path']]['bytes']
        assert not obs['native_pages_decoded_or_replayed']
    assert observations[4]['ranges']==observations[4]['recovery_fences']=={} and observations[4]['abort_domains']==[]
    cleanup=json.loads((root/'cleanup.json').read_text());assert same(cleanup,result['cleanup']) and cleanup['primary_failure_preserved']==actual['primary_error'] and cleanup['errors']==[]
    assert json.loads((root/'evidence-errors.json').read_text())=={'errors':[]}
    assert actual['evidence_errors_so_far']==result['evidence_errors']==[] and actual['custody_error'] is result['custody_error'] is None
    assert result['workflow_conclusion']=='failure' and not result['qualified'] and not result['engine_opened_or_replayed']
    assert not result['automatic_movement_tested'] and not result['physical_or_vm_tested'] and result['core_host_fixture_not_five_processes']
    assert not actual['old_closed_data_quorum_exercised'] and not actual['source_image_capture_exercised'] and actual['local_image_source_session_wire_not_exercised']
    if private_zip:
        private=pathlib.Path(private_zip);assert sha(private.read_bytes())==ZIP_SHA
        with zipfile.ZipFile(private) as z:
            names=z.namelist();assert len(names)==len(set(names))==106
            assert all(not n.startswith('/') and '..' not in pathlib.PurePosixPath(n).parts for n in names)
            assert z.read('source-revision.txt').decode().strip()==SOURCE
            base=next(n.rsplit('/',1)[0] for n in names if n.endswith('/custody-manifest.json'))
            assert z.read(base+'/custody-manifest.json')==(root/'custody-manifest.json').read_bytes()
            for item in manifest['files']:
                data=z.read(base+'/custody/'+item['path']);assert len(data)==item['bytes'] and sha(data)==item['sha256']
            assert same(json.loads(z.read(base+'/terminal.json')),actual)
            assert same(json.loads(z.read(base+'/ack-ledger.json')),acks) and same(json.loads(z.read(base+'/fence-ledger.json')),fences)
            assert [observation(z,base,i,manifest) for i in range(5)]==observations
        print('PASS OPTIONAL PRIVATE ZIP: all96 payload files/2909809B and10 JSON envelopes, no extraction/engine/replay')
    return actual

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--private-zip');a=parser.parse_args();verify(private_zip=a.private_zip)
    print('PASS public raw/source/0of1failed gate;2actualACKs/2live NoWriter refusals;UNKNOWN/cleanup/privacy distinctions')
