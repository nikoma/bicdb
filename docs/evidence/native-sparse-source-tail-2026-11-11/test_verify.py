import sys
sys.dont_write_bytecode=True
import gzip,hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('sparse_evidence_verifier',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def write(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
def manifest(root):
 write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class IntegrityNegatives(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root)
 def tearDown(self):self.tmp.cleanup()
 def rejected(self):
  manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_production_claim_is_rejected_after_manifest_rehash(self):
  p=self.root/'claims.json';j=json.loads(p.read_text());j['production_qualified']=True;write(p,j);self.rejected()
 def test_lagging_decision_does_not_become_materialized_data(self):
  p=self.root/'actual-result.json';j=json.loads(p.read_text());j['behind_native_after_sparse']['native_applied_index']=1026;write(p,j);self.rejected()
 def test_unknown_original_call_does_not_become_new_ack(self):
  p=self.root/'claims.json';j=json.loads(p.read_text());j['original_unknown_tail_is_new_ack']=True;write(p,j);self.rejected()
 def test_original_failed_fixture_cannot_be_omitted(self):
  p=self.root/'claims.json';j=json.loads(p.read_text());j['runs']=[r for r in j['runs'] if r['name']!='copied-sparse-actual1025'];write(p,j);self.rejected()
 def test_resource_pause_cannot_be_hidden_by_rehash(self):
  p=self.root/'runs/two-range-domain/resources.jsonl.gz';rows=[json.loads(s) for s in gzip.decompress(p.read_bytes()).decode().splitlines()];rows[0]['paused']=True;p.write_bytes(gzip.compress(('\n'.join(json.dumps(s) for s in rows)+'\n').encode(),mtime=0));self.rejected()
 def test_source_patch_change_is_detected_after_manifest_rehash(self):
  p=self.root/'engine.patch.gz';p.write_bytes(gzip.compress(gzip.decompress(p.read_bytes())+b'\n',mtime=0));self.rejected()
if __name__=='__main__':unittest.main()
