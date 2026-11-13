#!/usr/bin/env python3
"""Offline SDK unit/TCP-mock evidence verification; no test rerun or network."""
import gzip,hashlib,json,pathlib,re,zipfile
HERE=pathlib.Path(__file__).resolve().parent
SOURCE='8352e3d6bcf30ca5d03576a0ef70f98cc4244fec'
def sha(v):return hashlib.sha256(v).hexdigest()
def main():
 sums=json.loads((HERE/'SHA256SUMS.json').read_text());actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='SHA256SUMS.json' and '__pycache__' not in p.parts};assert actual==set(sums)
 for name,digest in sums.items():assert sha((HERE/name).read_bytes())==digest,name
 e=json.loads((HERE/'evidence.json').read_text());r=json.loads((HERE/'request.json').read_text());g=json.loads((HERE/'github-run.json').read_text());a=json.loads((HERE/'github-artifacts.json').read_text())['artifacts']
 assert e['source']==r['source_revision']==g['headSha']==SOURCE and e['run']==g['databaseId']==43528329600 and g['conclusion']=='success'
 assert sha(gzip.decompress((HERE/'source-change.patch.gz').read_bytes()))==e['source_patch_sha256']
 assert r['inputs']['core_filter']=='event_client_' and len(r['new_regression_names'])==6
 assert not e['production_qualified'] and not e['runtime_custody_present'] and e['unit_TCP_mock_scope_only']
 assert len(a)==1 and a[0]['id']==e['artifact']==14085989657 and a[0]['workflow_run']['head_sha']==SOURCE
 archive=HERE/'focused-core-evidence.zip';assert sha(archive.read_bytes())==e['artifact_sha256']==a[0]['digest'].removeprefix('sha256:') and archive.stat().st_size==e['artifact_bytes']==a[0]['size_in_bytes']
 with zipfile.ZipFile(archive) as z:
  assert len(z.namelist())==4 and set(z.namelist())=={'source-revision.txt','selected-tests.log','tests.log','rustc.txt'}
  for m in e['members']:
   data=z.read(m['path']);assert len(data)==m['bytes'] and sha(data)==m['sha256'];assert not re.search(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----',data)
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  selected=[v[:-6] for v in z.read('selected-tests.log').decode().splitlines() if v.endswith(': test')];log=z.read('tests.log').decode();executed=re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',log,re.M)
  assert len(selected)==len(set(selected))==len(executed)==12 and set(selected)==set(r['static_selected_tests'])=={n for n,s in executed} and all(s=='ok' for n,s in executed)
  assert all(any(name.endswith('::'+new) for name in selected) for new in r['new_regression_names'])
  assert 'test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 1081 filtered out; finished in 2.67s' in log
 jobs={j['databaseId']:j for j in g['jobs']}
 for jid,steps in [(132239531943,['Build projection chaos helper','Record source and list selected Core library tests','Execute selected Core library tests']),(132239499719,['cargo check --workspace --all-targets','Workflow consumer compiles without Core defaults'])]:
  assert jobs[jid]['conclusion']=='success'
  for step in steps:assert next(s for s in jobs[jid]['steps'] if s['name']==step)['conclusion']=='success'
 for name in ['core-job.log.gz','workspace-job.log.gz']:
  data=gzip.decompress((HERE/name).read_bytes());assert SOURCE.encode() in data and not re.search(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----',data)
 print(json.dumps({'verification':'passed','source':SOURCE,'run':43528329600,'selected_executed':12,'passed':12,'failed':0,'all_six_new_regressions':True,'alltargets_consumer':'passed','scope':'SDK unit and loopback TCP mock tests only','HA_qualified':False},sort_keys=True))
if __name__=='__main__':main()
