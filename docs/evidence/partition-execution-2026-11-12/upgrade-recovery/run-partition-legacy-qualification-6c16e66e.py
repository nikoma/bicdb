import pathlib,json,time,subprocess,sys
root=pathlib.Path('/root/bicdb-durability-s3');out=root/'report-partition-legacy-qualification-6c16e66e.json';record={'source':'6c16e66e742bb575041aebce4086ebf434b9fe64','status':'running','started_unix':time.time(),'scope':'Actualold482immutablefixtures; default8/explicit8/entryandstartup14, copiesonly, unchangedassertions/14sourceACKs/fullrecordhashes/reopens/indexchecks/actualoldreaderbyteidentity. Separate source-scopedacceptance, not fullapp/HA.','steps':[]}
def save():out.write_text(json.dumps(record,indent=2)+'\n')
save()
for name in ['run-partition-default-6c16e66e.py','run-partition-explicit-6c16e66e.py','run-partition-entry-6c16e66e.py']:
 step={'script':name,'started_unix':time.time()};record['steps'].append(step);save();step.update(exit=subprocess.call(['python3',str(root/'host-control'/name)]),finished_unix=time.time());save()
record.update(status='legacy_controls_passed' if all(s['exit']==0 for s in record['steps']) else 'legacy_controls_failed',finished_unix=time.time());save();print(json.dumps(record));sys.exit(0 if record['status']=='legacy_controls_passed' else 1)
