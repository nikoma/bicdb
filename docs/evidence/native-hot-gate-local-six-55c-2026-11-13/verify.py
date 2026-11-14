#!/usr/bin/env python3
"""Verify retained local-gate CI evidence offline; no Rust build, engine or network."""
import gzip,hashlib,json,pathlib,re,zipfile
HERE=pathlib.Path(__file__).resolve().parent
SOURCE='55c50e07c0f36ee7314342b4c8653988f444a835'
def sha(v):return hashlib.sha256(v).hexdigest()
def main():
 sums=json.loads((HERE/'SHA256SUMS.json').read_text())
 actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='SHA256SUMS.json' and '__pycache__' not in p.parts};assert actual==set(sums)
 for name,digest in sums.items():assert sha((HERE/name).read_bytes())==digest,name
 e=json.loads((HERE/'evidence.json').read_text());g=json.loads((HERE/'github-run.json').read_text());a=json.loads((HERE/'github-artifacts.json').read_text())['artifacts']
 assert e['source']==g['headSha']==SOURCE and e['run']==g['databaseId']==43523702442
 assert g['status']=='completed' and g['conclusion']=='success'
 assert not e['qualification'] and not e['projection_claimed'] and not e['artifact_runtime_custody_present']
 assert len(a)==1 and a[0]['id']==e['artifact']==14083514559 and a[0]['workflow_run']['head_sha']==SOURCE
 archive=HERE/'focused-core-evidence.zip';assert sha(archive.read_bytes())==e['artifact_sha256']==a[0]['digest'].removeprefix('sha256:') and archive.stat().st_size==e['artifact_bytes']==a[0]['size_in_bytes']
 expected={'rustc.txt','selected-tests.log','source-revision.txt','tests.log'}
 with zipfile.ZipFile(archive) as z:
  assert len(z.namelist())==4 and set(z.namelist())==expected
  assert sum(v.file_size for v in z.infolist())<16*1024*1024
  for member in e['members']:
   data=z.read(member['path']);assert len(data)==member['bytes'] and sha(data)==member['sha256']
   assert not re.search(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----',data)
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  selected=[v[:-6] for v in z.read('selected-tests.log').decode().splitlines() if v.endswith(': test')]
  log=z.read('tests.log').decode();executed=re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',log,re.M)
  assert len(selected)==len(set(selected))==len(executed)==6 and selected==e['selected']
  assert set(selected)=={name for name,status in executed} and all(status=='ok' for name,status in executed)
  assert 'test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 1144 filtered out; finished in 0.00s' in log
  assert all(name.startswith('distribution_consensus::publication_version::tests::metadata_publication_version_') for name in selected)
 source=(HERE/'publication_version.rs.txt').read_bytes();assert sha(source)==e['source_file_sha256']
 assert {name.rsplit('::',1)[-1] for name in selected}==set(re.findall(r'fn (metadata_publication_version_\w+)\(',source.decode()))
 jobs={j['databaseId']:j for j in g['jobs']}
 for job_id,steps in [(132224155429,['Build projection chaos helper','Record source and list selected Core library tests','Execute selected Core library tests']),(132224127271,['cargo check --workspace --all-targets','Workflow consumer compiles without Core defaults'])]:
  job=jobs[job_id];assert job['status']=='completed' and job['conclusion']=='success'
  for name in steps:assert next(s for s in job['steps'] if s['name']==name)['conclusion']=='success'
 for name in ['core-job.log.gz','workspace-job.log.gz']:
  data=gzip.decompress((HERE/name).read_bytes());assert SOURCE.encode() in data
  assert not re.search(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----',data)
 print(json.dumps({'verification':'passed','source':SOURCE,'run':43523702442,'selected':6,'executed_passed':6,'executed_failed':0,'alltargets_and_consumer':'passed','runtime_custody':False,'scope':'local publication gate unit cases only','production_qualified':False},sort_keys=True))
if __name__=='__main__':main()
