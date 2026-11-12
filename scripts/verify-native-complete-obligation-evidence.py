#!/usr/bin/env python3
"""Offline exact-source component receipt check; no database execution."""
from pathlib import Path
import hashlib,json,sys
DEFAULT=Path(__file__).resolve().parents[1]/'docs/evidence/native-complete-obligation-capacity-2026-11-11'
def need(value,message):
 if not value:raise ValueError(message)
def verify(root=DEFAULT):
 root=Path(root).resolve()
 def read(name):return json.loads((root/name).read_text())
 files=read('MANIFEST.json')['files'];need({f['path'] for f in files}=={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'manifest file set differs')
 for f in files:
  p=(root/f['path']).resolve();need(p.is_relative_to(root) and not p.is_symlink(),'unsafe path');b=p.read_bytes();need(len(b)==f['bytes'] and hashlib.sha256(b).hexdigest()==f['sha256'],'evidence digest differs')
 f=read('findings.json');source='d2c202202943f047618784994b929a49738cd627';need(f['source_revision']==source and f['distinct_tests']==21,'source/count mismatch')
 for k in ['full_repair_demonstrated','new_application_ack_tested','strong_get_tested','physical_qualified','production_qualified','ci_tested','irretrievable_loss_proven','main_or_azure_cause_claim']:need(f[k] is False,'unsupported claim: '+k)
 need(f['n2_native_history']=='UNKNOWN_INVALIDATED','unknown native point relabelled')
 s=read('runs/SUMMARY.json');names=[name for run in s['runs'] for name in run['tests']];need(len(names)==len(set(names))==s['distinct_tests']==21,'distinct result count differs')
 for r in s['runs']:need(r['result']['source_revision']==source and r['result']['exit_code']==0 and r['result']['guard_stop_reason'] is None,'run is not exact passing source')
 need(s['full_wire_envelope_bytes']==f['measured_full_envelope_bytes']==2283918 and s['native_frame_floor_bytes']==4194304,'wire size evidence differs')
 a=read('actual-copied-report.json');h=a['actual_source_obligations'];need(sorted(map(int,h['irreversible_command_sha256']))==list(range(2,1027)),'full actual obligation set changed');need(a['acknowledged_values_reconciled']==1025 and a['original_catalog_and_refusal_bytes_unchanged'] is True,'ACK/original integrity differs')
 need(a['source_native']['n0']['native_applied_index']==1025 and a['source_native']['n1']['native_applied_index']==0,'native facts changed')
 n=a['source_native']['n2'];need(n['native_applied_index'] is None and n['invalidated'] is True and n['factory_initialization_complete'] is False,'unknown native proof falsely raised')
 for key,text in [('prefix_export_error','requires a range snapshot'),('direct_tail_error','compacted/unavailable gap'),('source_ready_error','not fully resolved')]:need(text in a[key],'later refusal omitted')
 c=read('custody-receipt.json');need(c['files']==98 and c['bytes']==8757788 and c['manifest_sha256']=='784e609625bdc9ca3140f9e2defc762f788cbf277865e6065314b74b021c3f52','custody changed');o=read('original-comparison.json');need(o['original_retained_and_custody_large_fixture_files_unchanged'] is True and o['files']==53,'original comparison missing')
 return {'result':'VERIFIED_SCOPED_CAPACITY_CONTROLS','distinct_tests':21,'actual_obligations':1025,'full_recovery':False,'qualification':False}
if __name__=='__main__':print(json.dumps(verify(Path(sys.argv[1]) if len(sys.argv)>1 else DEFAULT),sort_keys=True))
