import json,pathlib,subprocess,time,hashlib
root=pathlib.Path('/root/bicdb-durability-s3');control=root/'host-control';source='2403deefde68869c15f02b73d40406ffdfb50148'
receipt=root/'report-native-identity-finance492-recovery-pipeline.json';assert not receipt.exists()
record={'source':source,'status':'waiting_original95_and_current_probe','steps':[],'started_unix':time.time(),'scope':'Exact492candidate process38/EIO8 then disposableKVM10 then independentverifier. Sameoriginalchecks; waitapplicationterminal/currentprobe. No sourcepush/productionpromotion/cloudspend.'}
def save():receipt.write_text(json.dumps(record,indent=2)+'\n')
def active(unit):return subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']
save()
try:
 while active('bicdb-private-composed-4e329f36-mtable95-v1'):time.sleep(5)
 application=json.loads((root/'report-private-composed-4e329f36-mtable95-v1.json').read_text());assert application['source']=='4e329f368de5fc7485971b877e905d3d17110880'
 while True:
  f=root/'report-native-identity-finance492-validation.json';progress=json.loads(f.read_text()) if f.exists() else {}
  if len(progress.get('steps',[]))>=3 and all(x.get('exit')==0 for x in progress['steps'][:3]):break
  assert active('bicdb-native-identity-finance492-validation'),progress
  time.sleep(5)
 assert progress['source']==source
 custody=json.loads((root/'native-identity-finance492-probe-custody.json').read_text());assert custody['source']==source and custody['status']=='probe_built'
 assert hashlib.file_digest(pathlib.Path(custody['binary']).open('rb'),'sha256').hexdigest()==custody['binary_sha256']
 commands=[['/usr/bin/python3',str(control/'run-native-identity-finance492-recovery.py'),'--source',source,'--build','native-identity-finance492-validation','--tag','native-identity-finance492-recovery-2403deef','--custody','native-identity-finance492-probe-custody.json','--runtime',source],['/usr/bin/python3',str(control/'run-native-identity-finance492-vm.py')],['/usr/bin/python3',str(control/'verify-native-identity-finance492-recovery.py'),'--finance492','--vm']]
 record['status']='running';save()
 for command in commands:
  step={'command':command,'started_unix':time.time()};record['steps'].append(step);save()
  step.update(exit=subprocess.call(command),finished_unix=time.time());save();assert step['exit']==0,step
 record.update(status='recovery_and_independent_verification_passed',finished_unix=time.time())
except BaseException as error:record.update(status='failed',error=repr(error),finished_unix=time.time())
save();raise SystemExit(0 if record['status']=='recovery_and_independent_verification_passed' else 1)
