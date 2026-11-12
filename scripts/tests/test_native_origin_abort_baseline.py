from pathlib import Path
import hashlib,importlib.util,json,shutil,tempfile,unittest
p=Path(__file__).resolve().parents[1]/'verify-native-origin-abort-baseline.py';s=importlib.util.spec_from_file_location('native_abort',p);v=importlib.util.module_from_spec(s);s.loader.exec_module(v)
class NativeAbortEvidenceTests(unittest.TestCase):
 def test_authentic(self):self.assertEqual(v.verify()['acknowledgments'],0)
 def tamper(self,change):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'bundle';shutil.copytree(v.DEFAULT,root);p=root/'findings.json';j=json.loads(p.read_text());change(j);p.write_text(json.dumps(j));m=json.loads((root/'MANIFEST.json').read_text())
   for f in m['files']:
    b=(root/f['path']).read_bytes();f.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
   (root/'MANIFEST.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):v.verify(root)
 def test_rehashed_failure_cannot_pass(self):self.tamper(lambda x:x.update(baseline_outcome='PASS',exit_code=0))
 def test_rehashed_restoration_cannot_claim_ack(self):self.tamper(lambda x:x.update(second_call_acknowledged=True,native_quorum_restore_liveness_proven=True))
 def test_rehashed_case_cannot_be_physical(self):self.tamper(lambda x:x.update(physical_qualification=True))
if __name__=='__main__':unittest.main()
