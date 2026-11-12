from pathlib import Path
import hashlib,importlib.util,json,shutil,tempfile,unittest
p=Path(__file__).resolve().parents[1]/'verify-native-complete-obligation-evidence.py';s=importlib.util.spec_from_file_location('complete_obligation',p);v=importlib.util.module_from_spec(s);s.loader.exec_module(v)
class CompleteObligationEvidenceTests(unittest.TestCase):
 def test_authentic(self):self.assertEqual(v.verify()['actual_obligations'],1025)
 def tamper(self,change):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'bundle';shutil.copytree(v.DEFAULT,root);p=root/'findings.json';j=json.loads(p.read_text());change(j);p.write_text(json.dumps(j));m=json.loads((root/'MANIFEST.json').read_text())
   for f in m['files']:
    b=(root/f['path']).read_bytes();f.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
   (root/'MANIFEST.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):v.verify(root)
 def test_rehashed_scope_cannot_be_full_repair(self):self.tamper(lambda x:x.update(full_repair_demonstrated=True))
 def test_rehashed_unknown_cannot_be_ready(self):self.tamper(lambda x:x.update(n2_native_history='KNOWN_READY'))
 def test_rehashed_run_cannot_be_qualified(self):self.tamper(lambda x:x.update(physical_qualified=True,production_qualified=True))
if __name__=='__main__':unittest.main()
