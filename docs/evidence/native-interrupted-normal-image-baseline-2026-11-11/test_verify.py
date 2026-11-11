import gzip,hashlib,importlib.util,json,os,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('interrupted_normal_receipt',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def write(p,obj):
 t=p.with_name(p.name+'.tmp');t.write_text(json.dumps(obj,sort_keys=True,indent=2)+'\n');os.replace(t,p)
def manifest(root):write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class Negatives(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
 def tearDown(self):self.tmp.cleanup()
 def claims(self,fn):
  p=self.root/'claims.json';x=json.loads(p.read_text());fn(x);write(p,x);manifest(self.root)
 def test_characterization_pass_not_recovery_success(self):
  self.claims(lambda c:c.__setitem__('cold_recovery_success',True))
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_unknown_is_not_zero(self):
  p=self.root/'projections/cold-result.json';x=json.loads(p.read_text());x['target_actual_native']['native_applied_index']=0;write(p,x);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_unknown2_cannot_inflate_ack_ledger(self):
  self.claims(lambda c:c.__setitem__('ledger_acknowledged_records',2))
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_resource_interruption_must_remain_recorded(self):
  self.claims(lambda c:c.__setitem__('runs',[r for r in c['runs'] if not r['name'].endswith('-v2')]))
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_intent_not_cleared(self):
  self.claims(lambda c:c.__setitem__('target_intent_retained',False))
  with self.assertRaises(AssertionError):v.verify(self.root)
 def test_rehashed_disk_stop_cannot_become_completed_validation(self):
  p=self.root/'runs/coherent-interrupted-normal-cold-v2/result.json';x=json.loads(p.read_text());x['guard_stop_reason']=None;x['exit_code']=0;write(p,x);manifest(self.root)
  with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
