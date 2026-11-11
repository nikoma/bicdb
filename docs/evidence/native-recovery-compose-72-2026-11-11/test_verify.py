import importlib.util,json,hashlib,shutil,tempfile,unittest,sys
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('prepared_verify',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
class Cases(unittest.TestCase):
    def clone(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name)/'bundle';shutil.copytree(ROOT,p);return p
    def seal(self,p):
        rows=[{'path':x.relative_to(p).as_posix(),'bytes':x.stat().st_size,'sha256':hashlib.sha256(x.read_bytes()).hexdigest()} for x in sorted(p.rglob('*')) if x.is_file() and x!=p/'MANIFEST.json'];(p/'MANIFEST.json').write_text(json.dumps(rows))
    def test_retained(self):self.assertEqual(v.verify(ROOT)['distinct_focused_tests'],72)
    def test_manifest_omission(self):
        p=self.clone();m=json.loads((p/'MANIFEST.json').read_text());(p/'MANIFEST.json').write_text(json.dumps([r for r in m if r['path']!='claims.json']));self.assertRaises(AssertionError,v.verify,p)
    def test_resealed_false_complete_claim(self):
        p=self.clone();c=json.loads((p/'claims.json').read_text());c['recovery_complete_exercised']=True;(p/'claims.json').write_text(json.dumps(c));self.seal(p);self.assertRaises(AssertionError,v.verify,p)
    def test_resealed_source_hash(self):
        p=self.clone();s=json.loads((p/'source-files.json').read_text());s[0]['sha256']='0'*64;(p/'source-files.json').write_text(json.dumps(s));self.seal(p);self.assertRaises(AssertionError,v.verify,p)
    def test_resealed_tally(self):
        p=self.clone();c=json.loads((p/'claims.json').read_text());c['runs'][0]['count']+=1;(p/'claims.json').write_text(json.dumps(c));self.seal(p);self.assertRaises(AssertionError,v.verify,p)
if __name__=='__main__':unittest.main()
