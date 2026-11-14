#!/usr/bin/env python3
"""Bounded readonly inspection of original e1ec native WAL; never open an engine.

Requires the exact private original ZIP and source repository. Outputs only
identities/digests/structural observations, never page, record or secret bytes.
CRC-valid WAL decisions do not themselves prove fsync or power-loss durability.
"""
import argparse,copy,hashlib,json,pathlib,re,struct,subprocess,sys,zipfile,zlib
if not __debug__:raise SystemExit('Run without Python -O; assertions required')
SOURCE='e1ecfeab71d3f1e104530e9d6aa3bf37971abd65'
ORIGINAL='5445c509695372da8f5038c5e8073e9e8ce9619349c311221dbed743d97ab026'
CASES={'TargetCatalogueFloorDirSyncRetry','TargetCatalogueFloorCanceled'}
NS=b'\x00\x00\x10'
MAX_ZIP=64*1024*1024;MAX_MEMBERS=4096;MAX_TOTAL=128*1024*1024;MAX_WAL=64*1024*1024;MAX_FRAMES=100000;MAX_PAGE=1024*1024;MAX_VALUE=128*1024;MAX_CHAIN=32;MAX_PAGES=4096
PINS=['crates/bicdb-page/src/wal.rs','crates/bicdb-page/src/page.rs','crates/bicdb-page/src/btree.rs','crates/bicdb-page/src/slotted.rs','crates/bicdb-page/src/heap.rs','crates/bicdb-page/src/overflow.rs','crates/bicdb-page/src/mvcc.rs','crates/bicdb-page/src/paged.rs','crates/bicdb-core/src/paged_collection.rs','crates/bicdb-core/src/db/spatial_paged.rs','crates/bicdb-core/src/db/range_image_native.rs','crates/bicdb-core/src/db/range_image_install.rs','crates/bicdb-core/src/db/range_image_install/registered_remote.rs','crates/bicdb-core/src/distribution_range_image.rs','crates/bicdb-core/src/distribution_range_image/remote_source.rs','crates/bicdb-core/src/db/range_image_native/registered_remote_image/final_publication.rs','crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/target_final_image_tests/catalogue_tests.rs']
def sha(b):return hashlib.sha256(b).hexdigest()
def enc(d):return json.dumps(d,ensure_ascii=False,separators=(',',':')).encode()
def u16(b,o):return int.from_bytes(b[o:o+2],'little')
def u32(b,o):return int.from_bytes(b[o:o+4],'little')
def u64(b,o):return int.from_bytes(b[o:o+8],'little')
def scan(b,expected=None):
 assert len(b)<=MAX_WAL
 offset=0;records=[]
 while offset<len(b):
  assert len(records)<MAX_FRAMES and len(b)-offset>=24,'partial/oversize native WAL'
  h=b[offset:offset+24];assert h[:4]==b'BWAL'and h[13:16]==b'\0'*3
  lsn=u64(h,4);kind=h[12];size=u32(h,16)
  assert kind in (1,2,3,4)and 16<=size<=16+MAX_PAGE and lsn!=(1<<64)-1
  assert expected is None or lsn==expected,'non-contiguous retained WAL'
  end=offset+24+size;assert end<=len(b),'partial native WAL body'
  body=b[offset+24:end];assert zlib.crc32(h[:20]+body)&0xffffffff==u32(h,20),'native WAL CRC mismatch'
  page,xid=struct.unpack('<QQ',body[:16]);payload=body[16:]
  if kind==1:assert 512<=len(payload)<=MAX_PAGE and len(payload)&(len(payload)-1)==0
  else:assert len(payload)==0 and page==0
  records.append({'lsn':lsn,'kind':kind,'page':page,'xid':xid,'offset':offset,'bytes':end-offset,'frame_sha256':sha(b[offset:end]),'payload':payload})
  expected=lsn+1;offset=end
 return records,expected

def page(p,id,kind):
 assert len(p)>=512 and p[:4]==b'BPG1'and u16(p,4)==1 and p[6]==kind and p[7]==0 and u64(p,8)==id
 # WAL after-images may precede disk finalization. Framing CRC validates
 # their exact bytes; do not pretend the page's disk CRC is finalized here.
 assert 64<=u16(p,36)<=len(p)-8 and u16(p,36)<=u16(p,38)<=len(p)-8

def leaf_entries(p,id):
 page(p,id,5);count=u16(p,40);assert count<=4096 and 64+8*count<=u16(p,36)
 entries=[];previous=None
 for i in range(count):
  at=64+i*8;off,k,v,res=struct.unpack('<HHHH',p[at:at+8]);assert res==0 and k>0
  assert u16(p,38)<=off and off+k+v<=len(p)-8
  key=p[off:off+k];value=p[off+k:off+k+v];assert previous is None or previous<key
  entries.append((key,value));previous=key
 return entries

def decode_value(pages,locator):
 assert len(locator)==14;id,slot,generation=struct.unpack('<QHI',locator);assert id in pages
 p=pages[id]['payload'];page(p,id,2);count=u16(p,40);assert slot<count<=4096 and 64+count*8<=u16(p,36)
 off,size,actual_generation=struct.unpack('<HHI',p[64+slot*8:72+slot*8]);assert off and generation==actual_generation
 assert u16(p,38)<=off and off+size<=len(p)-8;stored=p[off:off+size];assert stored
 used=[id];overflow=[]
 if stored[0]==0:value=stored[1:]
 else:
  assert stored[0]==1 and len(stored)==17
  head=u64(stored,1);expected_size=u64(stored,9);assert 30<=expected_size<=MAX_VALUE
  parts=[];seen=set()
  while head:
   assert head in pages and head not in seen and len(seen)<MAX_CHAIN;seen.add(head)
   q=pages[head]['payload'];page(q,head,3);size=u32(q,52);assert 0<size<=len(q)-72
   parts.append(q[64:64+size]);used.append(head);overflow.append(head);head=u64(q,44)
   assert sum(map(len,parts))<=expected_size
  value=b''.join(parts);assert len(value)==expected_size
 assert 30<=len(value)<=MAX_VALUE
 xmin,xmax=struct.unpack('<QQ',value[:16]);previous=value[16:30]
 return value[30:],{'locator':{'page_id':id,'slot':slot,'generation':generation},'xmin':xmin,'xmax':xmax,'previous_is_none':previous==b'\0'*14,'heap_and_overflow_pages':used,'overflow_pages':overflow}

def validate_receipt(raw,mf,final,xid):
 assert len(raw)<=MAX_VALUE
 d=json.loads(raw);assert enc(d)==raw,'receipt not exact canonical serde field order'
 unsigned=copy.deepcopy(d);unsigned['checksum_sha256']='';assert sha(enc(unsigned))==d['checksum_sha256'],'receipt checksum'
 assert d['format_version']==2 and d['native_page_xid']==xid>0 and d['purpose']=='RelocationLearner'
 assert d['registered_final_image']==final['pointer']and d['registered_final_image']['attempt_id']==mf['target_attempt']
 for a,b in [('cluster_id','cluster_id'),('bootstrap_id','bootstrap_id'),('target_node_id','target_id'),('capture_range','capture_range'),('intended_activation_range','activation_range'),('base_members','old_members'),('intended_activation_members','new_members'),('restore_activation','restore'),('range_index','range_index'),('logical_genesis_sha256','h0'),('logical_history_sha256','h'),('attempt_id','target_attempt'),('session_id','handoff_session'),('nonce','nonce'),('source_native_receipt_sha256','source_native_receipt'),('old_config_sha256','old_config_sha256'),('intended_config_sha256','new_config_sha256'),('schema_sha256','schema_sha256'),('reader_features','readers'),('portable_catalog_sha256','portable_catalogue_sha256')]:assert d[a]==mf[b],a
 assert d['history_verified_through']==d['range_index']
 assert d['source_purpose']=={'CommittedHandoff':{'relocation_id':mf['relocation_id'],'plan_id':mf['plan_id']}}
 assert d['birth_id']==mf['target_birth']['birth_id']and d['birth_sha256']==mf['target_birth']['birth_sha256']
 assert d['target_member_incarnation']==mf['target_birth']['node_incarnation']
 assert d['local_namespace_ids']==final['after']['namespace_ids']and d['birth_id']==final['after']['birth']['birth_id']
 owner=final['owner_after'];history=owner['history'];rows=owner['rows']['progress']
 assert d['history_generation']==history['generation']and d['intent_sha256']==history['intent_sha256']
 assert d['record_count']==rows['records']==1 and d['serialized_record_bytes']==rows['bytes']==177
 assert d['image_root_sha256']==owner['verification']['content_sha256']==owner['final_image']['record']['source_seal']['image_root_sha256']
 assert d['global_image_generation']==owner['final_image']['record']['image_generations'][0]and d['range_image_generation']==owner['final_image']['record']['image_generations'][1]
 assert d['reserved_db_upper_bound']==owner['final_image']['record']['target_reserved_b']and d['min_snapshot_db_sequence']==owner['final_image']['record']['target_min_scan_b']
 assert d['registered_final_image']['native_page_xid']==owner['final_image']['record']['native_page_xid']and d['registered_final_image']['checksum_sha256']==owner['final_image']['record']['checksum_sha256']
 assert d['reserved_db_upper_bound']==final['original_target_b']and d['min_snapshot_db_sequence']>=d['reserved_db_upper_bound']
 return d

def inspect(original,repo,self_test=False):
 assert original.stat().st_size<=MAX_ZIP
 archive_bytes=original.read_bytes();assert sha(archive_bytes)==ORIGINAL
 source_pins={}
 for name in PINS:
  b=subprocess.check_output(['git','show',SOURCE+':'+name],cwd=repo)
  blob=subprocess.check_output(['git','rev-parse',SOURCE+':'+name],cwd=repo,text=True).strip()
  source_pins[name]={'git_blob':blob,'sha256':sha(b)}
 results=[];neg=[]
 with zipfile.ZipFile(original)as z:
  infos=z.infolist();names=z.namelist();assert len(names)==len(set(names))<=MAX_MEMBERS and sum(x.file_size for x in infos)<=MAX_TOTAL
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  for name in names:
   assert not pathlib.PurePosixPath(name).is_absolute()and '..'not in pathlib.PurePosixPath(name).parts
   if not name.endswith('/execution-identity.json'):continue
   prefix=name.rsplit('/',1)[0]+'/';identity=json.loads(z.read(name));case=identity['source_case']
   if case not in CASES:continue
   assert identity['source_commit']==SOURCE
   read=lambda leaf:json.loads(z.read(prefix+leaf))
   custody=read('custody-manifest.json');assert custody['source']==SOURCE and custody['status']=='observed_stable_with_live_actual_owners'
   assert custody['all_five_metadata_service_db_native_journal_guards_held']and custody['engine_owners_alive']and not custody['engine_reopened']and not custody['atomic_powerloss_snapshot_claimed']
   mf=read('registered-proposal-1.json')['record_after_first']['source'];final=read('registered-final-image-before-assertions.json')
   assert mf['target_id']=='n4'and mf['target_attempt']==final['pointer']['attempt_id']
   target=[f for f in custody['files']if f['path'].startswith('node4/')];manifest_by_path={f['path']:f for f in target}
   wal_files=[f for f in target if re.fullmatch(r'node4/paged/store\.wal(?:\.[1-9][0-9]*)?',f['path'])]
   assert wal_files and any(f['path']=='node4/paged/store.wal'for f in wal_files)
   # Verify actual archive inventory rather than silently omit a segment.
   archive_wal={n[len(prefix+'custody/'):]for n in names if n.startswith(prefix+'custody/node4/paged/store.wal')}
   assert archive_wal=={f['path']for f in wal_files}
   wal_files.sort(key=lambda f:int(f['path'].rsplit('.',1)[-1])if f['path']!='node4/paged/store.wal'else (1<<64))
   records=[];expected=None;members=[]
   for f in wal_files:
    raw=z.read(prefix+'custody/'+f['path']);assert len(raw)==f['bytes']and sha(raw)==f['sha256']
    recs,expected=scan(raw,expected);records.extend(recs);assert len(records)<=MAX_FRAMES
    members.append({'path':prefix+'custody/'+f['path'],'bytes':len(raw),'sha256':sha(raw),'records':len(recs),'first_lsn':recs[0]['lsn']if recs else None,'last_lsn':recs[-1]['lsn']if recs else None})
   # Only candidate images with an exact selected native key are considered.
   # Never guess an xid from a previous fixture's numbering.
   basekey=NS+f"install:base:{mf['capture_range']['id']}".encode();archive_prefix=NS+f"registered-remote-base/{mf['target_attempt']}/archive/".encode()
   candidates={}
   for record in records:
    p=record['payload']
    if record['kind']!=1 or p[6]!=5:continue
    entries=leaf_entries(p,record['page'])
    if not any(k==basekey for k,v in entries):continue
    selected={k:v for k,v in entries if k==basekey or k.startswith(archive_prefix)}
    if len(selected)==2:candidates[record['xid']]={'leaf':record,'entries':selected}
   assert len(candidates)==1,'exact selected base/archive candidate not uniquely identified'
   xid,candidate=next(iter(candidates.items()));pages={r['page']:r for r in records if r['kind']==1 and r['xid']==xid}
   assert len(pages)<=MAX_PAGES
   expected_archive=archive_prefix+str(xid).encode();assert set(candidate['entries'])=={basekey,expected_archive}
   values=[];bindings=[]
   for key in [basekey,expected_archive]:
    raw,binding=decode_value(pages,candidate['entries'][key]);d=validate_receipt(raw,mf,final,xid)
    assert binding['xmin']==xid and binding['xmax']==0 and binding['previous_is_none']
    values.append(raw);bindings.append({'native_key':key[3:].decode(),'receipt_bytes':len(raw),'receipt_sha256':sha(raw),'mvcc':binding})
   assert values[0]==values[1];receipt=json.loads(values[0])
   pointer=receipt['registered_final_image'];fxid=pointer['native_page_xid']
   fpages={r['page']:r for r in records if r['kind']==1 and r['xid']==fxid};assert len(fpages)<=MAX_PAGES
   fkeys=[NS+f"registered-remote-image/{receipt['attempt_id']}/final/current".encode(),NS+f"registered-remote-image/{receipt['attempt_id']}/final/archive/{fxid:020}".encode()]
   fentries={};fleafs={}
   for id,r in fpages.items():
    if r['payload'][6]!=5:continue
    for key,loc in leaf_entries(r['payload'],id):
     if key in fkeys:
      assert key not in fentries;fentries[key]=loc;fleafs[key]=r
   assert set(fentries)==set(fkeys)
   fvalues=[];fbindings=[]
   for key in fkeys:
    raw,binding=decode_value(fpages,fentries[key]);f=json.loads(raw);assert enc(f)==raw
    assert binding['xmin']==fxid and binding['xmax']==0 and binding['previous_is_none']
    unsigned=copy.deepcopy(f);unsigned['checksum_sha256']=''
    assert sha(enc(['registered-remote-final-image-v2',unsigned]))==f['checksum_sha256']==pointer['checksum_sha256']
    assert f['manifest']==mf and f==final['owner_after']['final_image']['record']
    native_mf=copy.deepcopy(f['manifest']);native_mf['checksum_sha256']=''
    assert sha(enc(['native-remote-image-manifest-v1',native_mf]))==mf['checksum_sha256']
    assert f['database_open']==final['after']['open']and f['target_birth']==final['after']['birth']and f['catalogue_id']==final['after']['protected_catalogue_uuid']
    fvalues.append(raw);fbindings.append({'native_key':key[3:].decode(),'record_bytes':len(raw),'record_sha256':sha(raw),'mvcc':binding})
   assert fvalues[0]==fvalues[1]
   fdecisions=[r for r in records if r['xid']==fxid and r['kind']in (2,4)]
   assert len(fdecisions)==1 and fdecisions[0]['kind']==2 and fdecisions[0]['lsn']<candidate['leaf']['lsn']
   assert all(r['lsn']<fdecisions[0]['lsn']for r in fpages.values())
   final_wal={'pointer_from_exact_base_receipt':pointer,'actual_current_and_archive_bytes_equal':True,'checksummed_native_final_record_matches_report':True,'native_source_manifest_checksum_independently_recomputed':True,'current_root_visibility_proven':False,'key_and_tuple_bindings':fbindings,'commit_record':{k:v for k,v in fdecisions[0].items()if k!='payload'}}
   decisions=[r for r in records if r['xid']==xid and r['kind']in (2,4)]
   assert len(decisions)==1 and decisions[0]['kind']==2,'no unique observed commit for exact identified receipt'
   commit=decisions[0];assert all(r['lsn']<commit['lsn']for r in pages.values())
   relevant={candidate['leaf']['page']}|{p for b in bindings for p in b['mvcc']['heap_and_overflow_pages']}
   assert relevant<=set(pages)
   if self_test:
    corrupted=bytearray(z.read(members[0]['path']));corrupted[-1]^=1
    try:scan(corrupted)
    except AssertionError:neg.append(case+':native_CRC_corruption_refused')
    else:raise AssertionError('bad native CRC accepted')
    bad=bytearray(candidate['entries'][basekey]);bad[-1]^=1
    try:decode_value(pages,bad)
    except AssertionError:neg.append(case+':wrong_locator_generation_refused')
    else:raise AssertionError('bad locator generation accepted')
    bad=json.loads(values[0]);bad['native_page_xid']+=1
    try:validate_receipt(enc(bad),mf,final,xid)
    except AssertionError:neg.append(case+':bad_receipt_checksum_refused')
    else:raise AssertionError('bad receipt accepted')
   # No material or secret file contents enter the result. Full source/receipt
   # identities are represented by selected public fields and exact hashes.
   results.append({'source_case':case,'fixture_prefix':prefix,'initial_frozen_classification':'inconclusive; post-error runtime diagnostics missing','new_observation':'exact selected base/archive publication bytes logged in leaf/heap/overflow after-images under MVCC xmin matching the unique native WAL Commit record; no current root reachability claim','native_xid_identified_from_exact_key_and_receipt':xid,'commit_record':{k:v for k,v in commit.items()if k!='payload'},'receipt':{'attempt_id':receipt['attempt_id'],'target_node_id':receipt['target_node_id'],'birth_id':receipt['birth_id'],'birth_sha256':receipt['birth_sha256'],'source_manifest_sha256':mf['checksum_sha256'],'range_id':receipt['capture_range']['id'],'range_index':receipt['range_index'],'logical_genesis_sha256':receipt['logical_genesis_sha256'],'logical_history_sha256':receipt['logical_history_sha256'],'native_receipt_checksum_sha256':receipt['checksum_sha256'],'registered_final_pointer':receipt['registered_final_image'],'record_count':receipt['record_count'],'serialized_record_bytes':receipt['serialized_record_bytes'],'full_manifest_and_final_report_binding_checked':True},'base_and_archive':bindings,'native_final_image_WAL_correlation':final_wal,'relevant_page_image_frames':[{k:v for k,v in r.items()if k!='payload'}for id,r in sorted(pages.items())if id in relevant],'WAL_members':members,'page_validation_boundary':'WAL-frame CRC plus page identity/type/plaintext/slot/locator/overflow bounds; not disk-page finalization CRC or committed Btree root reachability','full_retained_chain_CRC_checked':True,'retained_chain_records':len(records),'first_lsn':records[0]['lsn'],'last_lsn':records[-1]['lsn'],'retained_prefix_before_first_lsn_available':False,'native_store_pages_not_decoded':{k:manifest_by_path['node4/paged/store.pages'][k]for k in ['path','bytes','sha256']},'custody_manifest_sha256':sha(z.read(prefix+'custody-manifest.json')),'target_root_file_count':len(target),'custody_mode':custody['status'],'original_post_error_API_owner_state_available':False,'native_commit_API_return_or_fsync_receipt_observed':False,'actual_root_reachability_or_current_native_key_visibility_proven':False,'powerloss_durability_proven':False,'serving_or_read_consistency_proven':False,'named_catalogue_fault_seam_reached':False})
 assert {r['source_case']for r in results}==CASES
 return {'source_revision':SOURCE,'source_package_version':'1.0.494-beta','original_run':43493938281,'original_artifact':14070838685,'original_zip_sha256':ORIGINAL,'original_zip_bytes':len(archive_bytes),'inspector_sha256':sha(pathlib.Path(__file__).read_bytes()),'source_pins':source_pins,'limits':{'zip_bytes':MAX_ZIP,'zip_members':MAX_MEMBERS,'uncompressed_total_bytes':MAX_TOTAL,'WAL_bytes_per_member':MAX_WAL,'frames_per_case':MAX_FRAMES,'page_payload_bytes':MAX_PAGE,'receipt_value_bytes':MAX_VALUE,'overflow_chain_pages':MAX_CHAIN,'selected_xid_page_images':MAX_PAGES},'cases':results,'self_test_controls':neg,'source_inference':{'observation':'Both original attempts have exact native Commit decisions and matching base/archive receipt images in the retained WAL. This resolves the existence/decision question for these retained bytes.','inference':'Under pinned single-publication control flow, the observed QueryTimedOut accompanies an operation that reached its native Commit decision; closure refusal before commit would take abort, not produce this identified Commit. The exact later check or deadline cause is not identified.','not_inferred':['successful native commit API return','successful fsync','power-loss survival','actual committed root reachability/current native key visibility','catalogue adoption or fault retry','the original failed tests now passed'],'fsync_boundary':'in_paged_transaction uses ordinary paged.commit, whose Wal commit appends then syncs. The raw Commit frame records the decision bytes, not the subsequent sync result; live-copy custody may include page-cache contents. A copied frame cannot certify completed fsync.'},'database_opened':False,'engine_recovery_or_checkpoint_invoked':False,'raw_or_secret_bytes_published':False,'frozen_packet_modified':False,'original_two_tests_remain_failed':True,'original_named_fault_controls_remain_not_tested':True,'production_qualified':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--original',required=True,type=pathlib.Path);p.add_argument('--repo',required=True,type=pathlib.Path);p.add_argument('--self-test',action='store_true');p.add_argument('--output',type=pathlib.Path);a=p.parse_args();d=inspect(a.original,a.repo,a.self_test);text=json.dumps(d,indent=2)+'\n'
 if a.output:a.output.write_text(text)
 else:print(text)
if __name__=='__main__':main()
