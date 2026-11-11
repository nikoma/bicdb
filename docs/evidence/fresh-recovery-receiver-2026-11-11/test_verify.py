import importlib.util,json,hashlib,shutil,tempfile,unittest,sys
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('receiver_verify',ROOT/'verify.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class VerificationTests(unittest.TestCase):
    def clone(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name)/'bundle';shutil.copytree(ROOT,p);return p
    def seal(self,p):
        rows=[{'path':f.relative_to(p).as_posix(),'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'bytes':f.stat().st_size} for f in sorted(p.rglob('*')) if f.is_file() and f!=p/'MANIFEST.json'];(p/'MANIFEST.json').write_text(json.dumps(rows,indent=2)+'\n')
    def test_retained_results(self):self.assertEqual(module.verify(ROOT)['unique_local_tests'],44)
    def test_omitted_manifest_file_rejected(self):
        p=self.clone();rows=json.loads((p/'MANIFEST.json').read_text());rows=[r for r in rows if r['path']!='claims.json'];(p/'MANIFEST.json').write_text(json.dumps(rows));self.assertRaises(AssertionError,module.verify,p)
    def test_resealed_forged_count_rejected(self):
        p=self.clone();c=json.loads((p/'claims.json').read_text());c['runs'][0]['executions']=2;(p/'claims.json').write_text(json.dumps(c));self.seal(p);self.assertRaises(AssertionError,module.verify,p)
    def test_resealed_source_digest_rejected(self):
        p=self.clone();rows=json.loads((p/'source-files.json').read_text());rows[0]['sha256']='0'*64;(p/'source-files.json').write_text(json.dumps(rows));self.seal(p);self.assertRaises(AssertionError,module.verify,p)
if __name__=='__main__':unittest.main()
