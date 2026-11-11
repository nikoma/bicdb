from pathlib import Path
import hashlib,importlib.util,json,shutil,tempfile,unittest
p=Path(__file__).resolve().parents[1]/'verify-native-generic-repair-baseline.py';s=importlib.util.spec_from_file_location('native_repair',p);v=importlib.util.module_from_spec(s);s.loader.exec_module(v)
class GenericRepairEvidenceTests(unittest.TestCase):
 def test_authentic(self):self.assertEqual(v.verify()['actual_acks'],2)
 def tamper(self,change):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'bundle';shutil.copytree(v.DEFAULT,root);p=root/'findings.json';j=json.loads(p.read_text());change(j);p.write_text(json.dumps(j));m=json.loads((root/'MANIFEST.json').read_text())
   for f in m['files']:
    b=(root/f['path']).read_bytes();f.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
   (root/'MANIFEST.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):v.verify(root)
 def test_rehashed_failure_cannot_pass(self):self.tamper(lambda x:x.update(baseline_outcome='PASS',exit_code=0))
 def test_rehashed_unknown_cannot_be_native_floor(self):self.tamper(lambda x:x.update(native_after=1,native_after_meaning='KNOWN'))
 def test_rehashed_embedded_case_cannot_be_production_rpc(self):self.tamper(lambda x:x.update(production_rpc_dynamically_exercised=True))
if __name__=='__main__':unittest.main()
