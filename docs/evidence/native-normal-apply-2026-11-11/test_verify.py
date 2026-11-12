"""Semantic negatives replace hardlinks atomically; original evidence is read-only."""
import gzip,hashlib,importlib.util,json,os,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('normal_apply_verifier',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def raw_write(path,data):
 tmp=path.with_name(path.name+'.replacement');tmp.write_bytes(data);os.replace(tmp,path)
def write(path,obj):raw_write(path,(json.dumps(obj,indent=2,sort_keys=True)+'\n').encode())
def manifest(root):
 write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class IntegrityNegatives(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
 def tearDown(self):self.tmp.cleanup()
 def modify_claim(self,key,value):
  path=self.root/'claims.json';c=json.loads(path.read_text());c[key]=value;write(path,c);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_cohorts_cannot_be_combined(self):self.modify_claim('cohorts_combined_into_total',True)
 def test_prior_origin_suite_cannot_be_relabelled_current(self):self.modify_claim('prior_origin_revision',v.REV)
 def test_no_production_or_strong_claim(self):self.modify_claim('strong_get_tested',True)
 def test_successful_database_custody_cannot_be_invented(self):self.modify_claim('successful_database_archive_retained',True)
 def test_compile_failure_cannot_be_omitted(self):
  p=self.root/'claims.json';c=json.loads(p.read_text());c['runs']=[r for r in c['runs'] if r['name']!='first-coherence'];write(p,c);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_resource_pause_cannot_be_hidden_by_rehash(self):
  p=self.root/'runs/actual-mtls-v2/resources.jsonl.gz';rows=[json.loads(x) for x in gzip.decompress(p.read_bytes()).decode().splitlines()];rows[0]['paused']=True;raw_write(p,gzip.compress(('\n'.join(json.dumps(x) for x in rows)+'\n').encode(),mtime=0));manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_broken_pipe_diagnostics_cannot_be_hidden(self):
  p=self.root/'runs/actual-mtls-v2/stderr.gz';text=gzip.decompress(p.read_bytes()).replace(b'Broken pipe',b'Hidden error');raw_write(p,gzip.compress(text,mtime=0));manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_source_patch_tamper_cannot_be_hidden_by_manifest(self):
  p=self.root/'source/primary/engine.patch.gz';raw_write(p,gzip.compress(gzip.decompress(p.read_bytes())+b'\n# changed\n',mtime=0));manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
