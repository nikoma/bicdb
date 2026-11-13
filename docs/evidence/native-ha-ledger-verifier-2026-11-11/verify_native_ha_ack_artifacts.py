"""Read-only external replay of ACK-ledger vs saved rowsets, without opening BicDB."""
import argparse,hashlib,json,pathlib,sqlite3,time
p=argparse.ArgumentParser();p.add_argument('root',type=pathlib.Path);a=p.parse_args();r=json.loads((a.root/'result.json').read_text());assert r['status']=='passed' and r['storage_mode']=='server_paged';out={'source':r['source'],'binary_sha256':r['binary_sha256'],'result_sha256':hashlib.file_digest((a.root/'result.json').open('rb'),'sha256').hexdigest(),'engine_opened':False,'cases':[],'checked_unix':time.time()}
for c in r['cases']:
 ledger=pathlib.Path(c['ack_ledger']);assert hashlib.file_digest(ledger.open('rb'),'sha256').hexdigest()==c['ack_ledger_sha256']
 with sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True) as db:
  ack=list(db.execute('SELECT id,value FROM ack ORDER BY id'));refused={v[0] for v in db.execute("SELECT id FROM attempts WHERE status='error' AND sqlstate='25006'")};assert len(ack)==c['acknowledged'];checked=0
  for stage in c['rounds']:
   for snapshot in [stage['successor']]+stage['nodes']:
    path=pathlib.Path(snapshot['rows_file']);assert hashlib.file_digest(path.open('rb'),'sha256').hexdigest()==snapshot['rows_sha256'];rows=dict(json.loads(path.read_text()));expected=ack[:snapshot['acknowledged']];assert all(rows.get(k)==v for k,v in expected);assert -1 not in rows and not refused.intersection(rows);assert len(rows)==snapshot['observed'];checked+=1
  formats=[]
  for node in ledger.parent.glob('n*/format.json'):
   d=json.loads(node.read_text());assert d['storage_mode']=='server_paged' and 'server_paged_storage' in d['feature_flags'];formats.append({'path':str(node),'sha256':hashlib.file_digest(node.open('rb'),'sha256').hexdigest()})
  assert len(formats)==c['voters'];out['cases'].append({'voters':c['voters'],'acknowledged':len(ack),'known_readonly_refusals':len(refused),'snapshots_verified':checked,'paged_formats':formats})
out['status']='passed';path=a.root/'independent-artifact-verification.json';assert not path.exists();path.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'cases':[{k:v for k,v in c.items() if k!='paged_formats'} for c in out['cases']]}))
