#!/usr/bin/env python3
"""Read-only exact-source abort regression evidence; no readiness grant."""
from pathlib import Path
import hashlib,json,sys
DEFAULT=Path(__file__).resolve().parents[1]/'docs/evidence/native-origin-abort-baseline-2026-11-11'
def need(value,message):
 if not value:raise ValueError(message)
def verify(root=DEFAULT):
 root=Path(root).resolve()
 def read(name):return json.loads((root/name).read_text())
 files=read('MANIFEST.json')['files'];need({f['path'] for f in files}=={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'manifest set differs')
 for f in files:
  p=(root/f['path']).resolve();need(p.is_relative_to(root) and not p.is_symlink(),'unsafe path');b=p.read_bytes();need(len(b)==f['bytes'] and hashlib.sha256(b).hexdigest()==f['sha256'],'digest differs')
 f=read('findings.json');source='eafa6949a78ea4e97f1ea3271ff97f2de27d20c6';need((f['source_revision'],f['baseline_outcome'],f['exit_code'])==(source,'FAIL',101),'baseline rewritten')
 for k in ['first_call_acknowledged','second_call_acknowledged','main_or_azure_cause_claim','native_quorum_restore_liveness_proven','physical_qualification','production_qualified','strong_get_tested','irretrievable_loss_proven']:need(f[k] is False,'unsupported claim: '+k)
 r=read('runs/attempt-1-native-abort-result.json');need(r['source_revision']==source and r['exit_code']==101 and r['guard_stop_reason'] is None,'run mismatch')
 a=read('actual-report.json');need(a['classification']=='NATIVE_ABORT_NEXT_ACK_REFUSED' and a['actual_abort']['state']=='aborted' and a['actual_abort']['command']['index']==1,'actual abort changed')
 need('failed prepare quorum' in a['first_error'] and 'cannot extend this actual native base' in a['second_error'],'wrong actual boundary')
 for key in ['after_abort_native','after_second_native']:need(a[key]['native_applied_index']==0 and a[key]['pending_command_index'] is None and a[key]['invalidated'] is False,'native0 fact changed')
 need(a['after_abort_raw']['resolved_through']==a['after_second_raw']['resolved_through']==1 and a['no_counter_reset_or_epoch_seed'] is True,'raw/recovery facts changed')
 c=read('custody-receipt.json');need(c['files']==52 and c['bytes']==405693 and c['manifest_sha256']=='c737ba3f64c9629150c3340506627468880f83aea037e471ea681b63faabcf90','custody changed')
 return {'result':'VERIFIED_EXPECTED_ABORT_LIVENESS_FAILURE','acknowledgments':0,'qualification':False}
if __name__=='__main__':print(json.dumps(verify(Path(sys.argv[1]) if len(sys.argv)>1 else DEFAULT),sort_keys=True))
