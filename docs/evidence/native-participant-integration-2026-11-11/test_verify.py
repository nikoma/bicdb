import importlib.util,json,hashlib,shutil,tempfile,unittest,sys
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent;spec=importlib.util.spec_from_file_location('native_integration_verify',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
class Cases(unittest.TestCase):
    def clone(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name)/'bundle';shutil.copytree(ROOT,p);return p
    def seal(self,p):(p/'MANIFEST.json').write_text(json.dumps([{'path':x.relative_to(p).as_posix(),'bytes':x.stat().st_size,'sha256':hashlib.sha256(x.read_bytes()).hexdigest()} for x in sorted(p.rglob('*')) if x.is_file() and x!=p/'MANIFEST.json'],indent=2)+'\n')
    def test_valid_groups(self):self.assertEqual(len(v.verify(ROOT)['groups']),3)
    def test_omitted_critical_file(self):
        p=self.clone();m=json.loads((p/'MANIFEST.json').read_text());(p/'MANIFEST.json').write_text(json.dumps([r for r in m if r['path']!='claims.json']));self.assertRaises(AssertionError,v.verify,p)
    def test_resealed_wrong_revision(self):
        p=self.clone();c=json.loads((p/'claims.json').read_text());c['groups'][0]['source_revision']='0'*40;(p/'claims.json').write_text(json.dumps(c));self.seal(p);self.assertRaises(AssertionError,v.verify,p)
    def test_resealed_false_qualification(self):
        p=self.clone();c=json.loads((p/'claims.json').read_text());c['native_activation_tested']=True;(p/'claims.json').write_text(json.dumps(c));self.seal(p);self.assertRaises(AssertionError,v.verify,p)
if __name__=='__main__':unittest.main()
