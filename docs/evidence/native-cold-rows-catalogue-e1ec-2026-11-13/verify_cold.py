#!/usr/bin/env python3
"""Verify bounded logical/custody evidence without opening any database.

Optional original ZIP adds byte/CRC/payload and custody-file verification. Keys
and page contents are never decoded; omitted native files are streamed to SHA.
"""
import argparse,hashlib,json,pathlib,re,struct,sys,zipfile,zlib
if not __debug__:raise SystemExit("Run without Python -O; assertions are required")
SOURCE='e1ecfeab71d3f1e104530e9d6aa3bf37971abd65'
PUBLIC_SHA='a1b5a04be33f724181be4665fcbedcd116f033657c9ba35904550db5a0ed1bd5'
PRIVATE_SHA='72bc86052384d59c1c2bdf1c590a7b29301caacb7b534b54438c12c830c0506b'
def sha(b):return hashlib.sha256(b).hexdigest()
def encoded(v):return json.dumps(v,ensure_ascii=False,separators=(',',':')).encode()
def one(z,leaf):
 found=[n for n in z.namelist()if n.endswith('/'+leaf)];assert len(found)==1,(leaf,found)
 return found[0],json.loads(z.read(found[0]))
def verify(public,original=None):
 assert sha(public.read_bytes())==PUBLIC_SHA
 with zipfile.ZipFile(public)as z:
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  assert not any('/custody/'in n or n.endswith('.bin')for n in z.namelist())
  selected=re.findall(r'^(\S+): test$',z.read('selected-tests.log').decode(),re.M)
  assert len(selected)==len(set(selected))==7
  log=z.read('tests.log').decode();outcomes=re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',log,re.M)
  assert len(outcomes)==7 and {name for name,_ in outcomes}==set(selected) and all(v=='ok'for _,v in outcomes)
  result=re.findall(r'^test result: ok\. (\d+) passed; (\d+) failed;.*finished in ([0-9.]+)s$',log,re.M)
  assert len(result)==1 and result[0][:2]==('7','0')
  _,identity=one(z,'execution-identity.json');assert identity['source_commit']==SOURCE
  assert identity['source_case']=='TargetImageRowsPostWalCanceled'
  _,acks=one(z,'ack-ledger.json');assert len(acks)==2
  for a in acks:
   assert a['outcome']=='returned'and a['actual_api_inserted']and a['actual_api_error']is None and not a['effect_unknown_on_error']
   assert a['record_id']==a['actual_full_record']['id']
   assert sha(encoded(a['value']))==a['payload_json_sha256']
  _,old=one(z,'remote-old-ledger.json');closed=[v for v in old if v['operation']=='remote_old_close']
  assert len(closed)==3 and all(v['outcome']=='returned_actual_closure'and v['exact_error']is None for v in closed)
  report_name,report=one(z,'registered-row-cold-preservation.json')
  cold_prefix=report_name.rsplit('/',1)[0]+'/'
  _,marker=one(z,'actual-applied-known-marker.json');_,pre=one(z,'actual-applied-known-marker.pre.json')
  _,facts=one(z,'registered-row-cold-readonly-wal-facts.json');_,custody=one(z,'stopped-target-custody-inventory.json')
  assert report['signal']==9 and report['actual_marker']==marker and report['pre']==pre and report['readonly_wal_facts']==facts and report['stopped_target_custody']==custody
  assert marker['phase']=='actual_applied_known_before_protected_progress'and marker['no_target_ack_claim']
  b=marker['actual_inner_boundary'];assert b['actual_boundary']=='retain_applied_known'
  pending=report['actual_pending']['pending'];assert len(pending)==1;entry=pending[0];intent=entry['intent']
  assert b['attempt']==intent['attempt']==facts['attempt']and b['range_id']==intent['range']==facts['range']
  assert b['logical_transaction_id']==intent['logical_txid']==facts['logical_transaction_id']
  assert b['native_intent_xid']==intent['native_xid']==facts['original_native_intent_xid']
  assert intent['logical_txid']!=intent['native_xid']
  assert b['native_intent_checksum_sha256']==intent['checksum_sha256']
  assert b['mutations_sha256']==intent['mutations_sha256']==facts['expected_mutations_sha256']==facts['observed_mutations_sha256']
  assert b['original_database_open']==entry['original_database_open']==facts['original_database_open_from_native_index']
  assert entry['native_birth_sha256']==facts['native_birth_sha256']
  assert b['actual_logical_sequence']==facts['observed_decision']['sequence']>0
  assert facts['observed_decision']['kind']=='commit'and facts['observed_decision']['materialized_marker']
  assert facts['observed_payload_matches_intent']and not facts['establishes_current_image_or_serving']
  assert facts['observed_commit_bytes_are_not_a_durable_sync_receipt']and facts['original_open_is_native_index_correlation_not_a_wal_incarnation_field']
  assert report['logical_wal_bytes']==facts['wal_bytes']>0 and report['logical_wal_sha256']==facts['wal_sha256']
  assert 'cold repair required before logical replay/cut'in report['exact_open_error']
  assert 'actual pending native entry absent'in report['absent_selector_error']
  assert not report['cold_repair_implemented']and not report['serving_or_ha_qualification']
  assert report['all_five_fixture_nodes_died_in_same_process']and not report['independent_host_or_other_four_node_custody_claim']
  assert all(v['unchanged']for v in report['business_file_comparison'])
  assert custody['original_inventory_unchanged']and custody['copy_inventory_identical']
  inventory=custody['inventory'];assert inventory==custody['original_inventory_after_copy']==custody['copied_inventory']
  expected=pre['source_acknowledged'];records=pre['original_step']['batch']['records']
  assert expected in acks and len(records)==1 and records[0]==expected['actual_full_record']
  private_verification=None
  if original:
   assert sha(original.read_bytes())==PRIVATE_SHA
   with zipfile.ZipFile(original)as raw:
    wal=raw.read(cold_prefix+'retained-original-logical-wal.bin');assert sha(wal)==report['logical_wal_sha256']and len(wal)==report['logical_wal_bytes']
    copy_prefix=cold_prefix+'custody/stopped-target-before-first-open/'
    retained_files={n[len(copy_prefix):]for n in raw.namelist()if n.startswith(copy_prefix)and not n.endswith('/')}
    assert retained_files=={n for n,v in inventory.items()if v['kind']=='file'}
    for name,v in inventory.items():
     if v['kind']!='file':continue
     info=raw.getinfo(copy_prefix+name);assert info.file_size==v['bytes'];h=hashlib.sha256()
     with raw.open(info)as stream:
      for chunk in iter(lambda:stream.read(65536),b''):h.update(chunk)
     assert h.hexdigest()==v['sha256']
    assert raw.read(copy_prefix+'transactions.log')==wal
    cursor=0;frames=[]
    while cursor<len(wal):
     assert cursor+16<=len(wal);header=wal[cursor:cursor+16]
     assert header[:4]==b'BICF'and header[4]==1 and header[5]==4
     flags,length,crc=struct.unpack('<HII',header[6:16]);assert flags==0 and length<=8*1024*1024
     end=cursor+16+length;assert end<=len(wal);payload=wal[cursor+16:end]
     assert zlib.crc32(header[4:12]+payload)&0xffffffff==crc
     frames.append(json.loads(payload));cursor=end
    chosen=[f for f in frames if f['tx_id']==intent['logical_txid']]
    writes=[f['write']for f in chosen if f['type']=='tx_write'];assert len(writes)==facts['observed_writes']==1
    decisions=[f for f in chosen if f['type']in('tx_commit','tx_commit_materialized')]
    assert [f['type']for f in chosen]==['tx_write','tx_commit','tx_commit_materialized']
    assert all(f['commit_seq']==b['actual_logical_sequence']for f in decisions)
    mutations=[]
    for w in writes:
     assert w['op']=='upsert';record=w['record']
     ordered={k:record[k]for k in ['id','vector','metadata','geometry','timestamp','payload']if k in record}
     mutations.append({'collection':w['collection'],'record_id':w['record_id'],'record':ordered})
    actual_hash=sha(b'bicdb.native-applied-mutations.v1\0'+encoded(mutations))
    assert actual_hash==intent['mutations_sha256'];assert writes[0]['record']==records[0]
    private_verification={'files_independently_hashed':len(retained_files),'custody_bytes':sum(v['bytes']for v in inventory.values()),'wal_frames_crc_checked':len(frames),'original_txid_payload_hash_independently_recomputed':actual_hash,'copied_wal_equal_original':True,'private_native_keys_or_pages_decoded':False,'database_opened':False}
  return {'source_revision':SOURCE,'source_package_version':identity['package_version'],'selected':selected,'executed_passed':7,'executed_failed':0,'suite_duration_seconds':float(result[0][2]),'duration_is_RTO':False,'actual_crash_seam':'AppliedKnown before protected RowsHeader','signal':9,'source_APP':{'acknowledged':len(acks),'failed':0,'uncertain':0,'retried':0,'record_ids':[a['record_id']for a in acks]},'remote_old_actual_closes':len(closed),'target_projection':{'attempted':1,'acknowledged':0,'returned':0,'killed_before_return':1,'client_outcome':'uncertain/no reply; actual committed WAL observed afterward','attempt':intent['attempt'],'logical_txid':intent['logical_txid'],'native_intent_xid':intent['native_xid'],'logical_sequence':b['actual_logical_sequence'],'record_id':records[0]['id'],'mutations_sha256':intent['mutations_sha256']},'pending_preserved':True,'normal_open_refused_before_generic_replay':report['exact_open_error'],'business_files_unchanged':report['business_file_comparison'],'private_original_verification':private_verification,'cold_repair_or_self_healing_proven':False,'physical_HA_scale_or_Serving_proven':False,'limits':['Only target stopped-root file custody; other four node roots not archived by this control.','All five services died in one process, not independent machines.','Precrash post-apply marker and raw commit bytes establish observed outcome, not a new durable-sync receipt or recovery grant.','Before/after runtime inventories and unchanged-file observations are CI assertions; retained copied-file bytes are independently hashed when original ZIP supplied.','No cold repair/materialization/authority re-admission or client Strong read after recovery was executed.']}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('projection',type=pathlib.Path);p.add_argument('--original',type=pathlib.Path);p.add_argument('--output',type=pathlib.Path);a=p.parse_args();result=verify(a.projection,a.original)
 data=json.dumps(result,indent=2)+'\n'
 if a.output:a.output.write_text(data)
 else:print(data)
if __name__=='__main__':main()
