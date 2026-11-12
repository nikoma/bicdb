import hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'verify-native-late-prepared-baseline.py';s=importlib.util.spec_from_file_location('late_baseline',p);v=importlib.util.module_from_spec(s);s.loader.exec_module(v)
class LatePreparedReceiptTests(unittest.TestCase):
 def test_authentic_receipt(self):self.assertFalse(v.verify()['qualification'])
 def tamper(self,change):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'bundle';shutil.copytree(v.DEFAULT,root);p=root/'findings.json';value=json.loads(p.read_text());change(value);p.write_text(json.dumps(value));m=json.loads((root/'MANIFEST.json').read_text())
   for x in m['files']:
    raw=(root/x['path']).read_bytes();x.update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
   (root/'MANIFEST.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):v.verify(root)
 def test_rehashed_failure_cannot_become_success(self):self.tamper(lambda x:x.update(baseline_outcome='PASS',exit_code=0))
 def test_rehashed_local_run_cannot_be_physical(self):self.tamper(lambda x:x.update(physical_qualification=True))
 def test_rehashed_release_cannot_be_new_ack(self):self.tamper(lambda x:x.update(new_application_ack_tested=True))
if __name__=='__main__':unittest.main()
