import gzip,hashlib,importlib.util,json,os,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent;spec=importlib.util.spec_from_file_location('fsync_verifier',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def raw_write(path,b):
 tmp=path.with_name(path.name+'.replacement');tmp.write_bytes(b);os.replace(tmp,path)
def write(path,x):raw_write(path,(json.dumps(x,indent=2,sort_keys=True)+'\n').encode())
def manifest(root):write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class IntegrityNegatives(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
 def tearDown(self):self.tmp.cleanup()
 def claim(self,key,value):
  p=self.root/'claims.json';c=json.loads(p.read_text());c[key]=value;write(p,c);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_zero_match_filter_cannot_be_counted(self):self.claim('zero_match_counted_as_pass',True)
 def test_missing_normal_mtls_cases_cannot_be_invented(self):self.claim('normal_apply_mtls_specific_tests',2)
 def test_source_finding_cannot_become_observed_ack_loss(self):self.claim('acknowledged_data_loss_observed',True)
 def test_strong_read_or_physical_qualification_cannot_be_claimed(self):self.claim('strong_get_tested',True)
 def test_retained_root_cannot_invent_external_ledger(self):self.claim('external_ack_ledger_retained',True)
 def test_original_compile_failure_cannot_be_omitted(self):
  p=self.root/'claims.json';c=json.loads(p.read_text());c['runs']=[r for r in c['runs'] if r['name']!='native-fsync-v1'];write(p,c);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_resource_pause_cannot_be_hidden_by_manifest(self):
  p=self.root/'runs/native-fsync-v2/resources.jsonl.gz';rows=[json.loads(x) for x in gzip.decompress(p.read_bytes()).decode().splitlines()];rows[0]['paused']=True;raw_write(p,gzip.compress(('\n'.join(json.dumps(x) for x in rows)+'\n').encode(),mtime=0));manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_source_patch_change_is_detected(self):
  p=self.root/'source/primary/engine.patch.gz';raw_write(p,gzip.compress(gzip.decompress(p.read_bytes())+b'\n# altered\n',mtime=0));manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
