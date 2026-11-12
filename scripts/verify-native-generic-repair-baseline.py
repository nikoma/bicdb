#!/usr/bin/env python3
"""Offline source-scoped regression receipt; never grants native readiness."""
from pathlib import Path
import hashlib,json,sys
DEFAULT=Path(__file__).resolve().parents[1]/'docs/evidence/native-generic-repair-baseline-2026-11-11'
def need(value,message):
 if not value:raise ValueError(message)
def verify(root=DEFAULT):
 root=Path(root).resolve()
 def read(name):return json.loads((root/name).read_text())
 files=read('MANIFEST.json')['files'];need({f['path'] for f in files}=={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'manifest file set differs')
 for f in files:
  p=(root/f['path']).resolve();need(p.is_relative_to(root) and not p.is_symlink(),'unsafe path');b=p.read_bytes();need(len(b)==f['bytes'] and hashlib.sha256(b).hexdigest()==f['sha256'],'digest differs')
 f=read('findings.json');source='035fd22f40028a4d7c23e4a086af19791674677f';need((f['source_revision'],f['baseline_outcome'],f['exit_code'])==(source,'FAIL',101),'baseline source/failure rewritten')
 for k in ['production_rpc_dynamically_exercised','main_or_azure_cause_claim','new_leader_ready_tested','strong_get_tested','physical_qualification','production_qualified','irretrievable_loss_proven']:need(f[k] is False,'unsupported claim: '+k)
 need(f['native_after'] is None and f['native_after_meaning']=='UNKNOWN_INVALIDATED','unknown native relabelled')
 r=read('runs/attempt-1-native-generic-repair-result.json');need(r['source_revision']==source and r['exit_code']==101 and r['guard_stop_reason'] is None,'wrong run source/result')
 a=read('actual-report.json');need(a['actual_origin_acks']==2 and a['source_native_after']==2 and a['repair_result']['successful'] is True,'actual positive trigger facts changed')
 need(a['target_native_before']['native_applied_index']==0 and a['target_native_before']['invalidated'] is False,'valid native baseline changed')
 n=a['target_native_after'];need(n['native_applied_index'] is None and n['invalidated'] is True and n['factory_initialization_complete'] is False,'unknown observation changed')
 need(a['target_raw_after']['resolved_through']==1 and a['target_copied_row_present'] is True,'raw/physical effect changed')
 c=read('custody-receipt.json');need(c['files']==55 and c['bytes']==686899 and c['manifest_sha256']=='4aedc3ce0768b4575406791bc01d526f130b0d3c8b2b048e0f7f253345050877','custody changed')
 return {'result':'VERIFIED_EXPECTED_NATIVE_REPAIR_FAILURE','actual_acks':2,'qualification':False}
if __name__=='__main__':print(json.dumps(verify(Path(sys.argv[1]) if len(sys.argv)>1 else DEFAULT),sort_keys=True))
