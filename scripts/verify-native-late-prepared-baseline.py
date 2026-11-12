#!/usr/bin/env python3
"""Offline baseline receipt checks, not HA/activation certification."""
from pathlib import Path
import hashlib,json,sys
DEFAULT=Path(__file__).resolve().parents[1]/'docs/evidence/native-late-prepared-baseline-2026-11-11'
def need(value,message):
 if not value:raise ValueError(message)
def verify(root=DEFAULT):
 root=Path(root).resolve()
 def read(name):return json.loads((root/name).read_text())
 manifest=read('MANIFEST.json');files=manifest['files'];need({x['path'] for x in files}=={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'file set differs')
 for entry in files:
  path=(root/entry['path']).resolve();need(path.is_relative_to(root) and not path.is_symlink(),'unsafe path');raw=path.read_bytes();need(len(raw)==entry['bytes'] and hashlib.sha256(raw).hexdigest()==entry['sha256'],'evidence hash differs')
 finding=read('findings.json');source='e3572fc655fe0dff9e634f56c5c01019ab8de173'
 need(finding['source_revision']==source and finding['baseline_outcome']=='FAIL' and finding['exit_code']==101,'baseline rewritten')
 for key in ['main_or_azure_cause_claim','new_application_ack_tested','strong_get_tested','activation_wire_tested','physical_qualification','production_qualified','irretrievable_loss_proven']:need(finding[key] is False,'unsupported claim: '+key)
 for name,code in [('late-strict-red',101),('smoke',0),('complete-control',0)]:
  run=read('runs/e3572fc6-'+name+'-result.json');need(run['source_revision']==source and run['exit_code']==code and run['guard_stop_reason'] is None,'wrong source/result')
 report=read('actual-report.json');need(report['classification']=='LATE_PREPARED_COMPLETION_REFUSED' and 'closed current native-ready majority unavailable' in report['completion_error'],'actual failure boundary changed');need((report['data_leader'],report['metadata_leader'],report['stopped_endpoint'])==('n0','n1','n2'),'fault scope changed')
 need(report['acknowledged_record_preserved_on_available_nodes'] is True and report['late_prepared_range_index']==1,'native data fact changed')
 actual=report['activated_before_loss'];need(set(actual)=={'n0','n2'} and all(x['stage']=='ActivatedAnchor' and x['range_index']==1 for x in actual.values()),'actual stages changed');need(actual['n0']['logical_history_sha256']==actual['n2']['logical_history_sha256'],'history disagreement')
 custody=read('custody-receipt.json');need(custody['files']==65 and custody['bytes']==1071545 and custody['manifest_sha256']=='35f0cce9233a063c9df3d5cf9578da836280d20135761366e4ec3ba8fc0bf54e','custody changed')
 return {'result':'VERIFIED_EXPECTED_BASELINE_FAILURE','source':source,'qualification':False}
if __name__=='__main__':print(json.dumps(verify(Path(sys.argv[1]) if len(sys.argv)>1 else DEFAULT),sort_keys=True))
