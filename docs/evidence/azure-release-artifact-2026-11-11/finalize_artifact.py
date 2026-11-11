import hashlib,json,os,shutil,subprocess
from pathlib import Path
root=Path('/tmp/bicdb-azure-artifact-evidence')
source=Path('/tmp/bicdb-azure-artifact-source')
raw=Path('/tmp/bicdb-azure-artifact-target/release/bicdb')
revision='b05385d4b033edf0db03f3eccf4bdd86f7c8eddb'
result=json.loads((root/'result.json').read_text());assert result['exit_code']==0
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source,text=True).strip()
def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
artifact=root/'bicdb-1.0.461-beta-b05385d4'
with artifact.open('xb') as out,raw.open('rb') as src:
 shutil.copyfileobj(src,out,1024*1024)
artifact.chmod(0o700)
subprocess.run(['strip','--strip-debug',str(artifact)],check=True)
artifact.chmod(0o555)
version=subprocess.check_output([str(artifact),'--version'],text=True).strip()
assert version=='bicdb 1.0.461-beta',version
subprocess.run(['python3',str(source/'scripts/capability_manifest.py'),'generate','--binary',str(artifact),'--revision',revision,'--output',str(root/'CAPABILITIES.json')],check=True,stdout=subprocess.DEVNULL)
manifest_hash=sha(root/'CAPABILITIES.json')
check=subprocess.run(['python3',str(source/'scripts/capability_manifest.py'),'check','--manifest',str(root/'CAPABILITIES.json'),'--binary',str(artifact),'--revision',revision,'--expected-sha256',manifest_hash,'--mode','distributed_ranges','--require','general_cross_range_transactions','--level','qualified'],capture_output=True,text=True)
assert check.returncode==2 and json.loads(check.stdout)['allowed'] is False,(check.returncode,check.stdout,check.stderr)
meta={'source_revision':revision,'version':version,'build_profile':'repository release; thinLTO; line-tables-only; default CLI features','raw_binary':{'path':str(raw),'bytes':raw.stat().st_size,'sha256':sha(raw)},'exercised_binary':{'path':str(artifact),'bytes':artifact.stat().st_size,'sha256':sha(artifact),'transformation':'strip --strip-debug; immutable separate copy'},'capability_manifest_sha256':manifest_hash,'qualified_multi_range_admission_check':{'exit_code':check.returncode,'result':json.loads(check.stdout)},'dynamic_libraries':subprocess.check_output(['ldd',str(artifact)],text=True),'elf_header':subprocess.check_output(['readelf','-h',str(artifact)],text=True),'production_qualified':False,'published_release':False,'deployed':False}
(root/'artifact.json').write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps({k:meta[k] for k in ['source_revision','version','exercised_binary','capability_manifest_sha256','qualified_multi_range_admission_check']}))
