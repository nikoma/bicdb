import gzip,hashlib,importlib.util,json,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('completion_evidence',ROOT/'verify.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
def seal(p):
    (p/'MANIFEST.json').write_text(json.dumps([{'path':f.relative_to(p).as_posix(),'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in sorted(p.rglob('*')) if f.is_file() and f!=p/'MANIFEST.json'],indent=2)+'\n')
class Integrity(unittest.TestCase):
    def case(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name)/'copy';shutil.copytree(ROOT,p);return p
    def test_manifest_requires_all_files(self):
        p=self.case();m=json.loads((p/'MANIFEST.json').read_text());m.pop();(p/'MANIFEST.json').write_text(json.dumps(m))
        with self.assertRaises(AssertionError):mod.verify(p)
    def test_resealed_inflated_count_refuses(self):
        p=self.case();f=p/'claims.json';c=json.loads(f.read_text());c['distinct_tests']=67;f.write_text(json.dumps(c));seal(p)
        with self.assertRaises(AssertionError):mod.verify(p)
    def test_resealed_failed_run_refuses(self):
        p=self.case();f=p/'runs/cad84207-completion/result.json';d=json.loads(f.read_text());d['exit_code']=101;f.write_text(json.dumps(d));seal(p)
        with self.assertRaises(AssertionError):mod.verify(p)
    def test_wrong_worktree_compile_refuses(self):
        p=self.case();f=p/'runs/cad84207-completion/stderr.gz';s=gzip.decompress(f.read_bytes()).replace(b'/tmp/bicdb-native-completion-participants/crates/bicdb-core',b'/tmp/wrong-worktree/crates/bicdb-core');f.write_bytes(gzip.compress(s,mtime=0));seal(p)
        with self.assertRaises(AssertionError):mod.verify(p)
if __name__=='__main__':unittest.main()
