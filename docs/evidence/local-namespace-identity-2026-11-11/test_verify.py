import gzip,hashlib,importlib.util,json,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('namespace_evidence_verify',ROOT/'verify.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
def seal(p):
 provenance=p/'raw-provenance.json';rows=json.loads(provenance.read_text())
 for r in rows:
  b=(p/r['retained_path']).read_bytes()
  if r['compressed']:b=gzip.decompress(b)
  r['original_bytes']=len(b);r['original_sha256']=hashlib.sha256(b).hexdigest()
 provenance.write_text(json.dumps(rows,indent=2)+'\n')
 (p/'MANIFEST.json').write_text(json.dumps([{'path':f.relative_to(p).as_posix(),'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in sorted(p.rglob('*')) if f.is_file() and f!=p/'MANIFEST.json'],indent=2)+'\n')
class Integrity(unittest.TestCase):
 def case(self):
  t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name)/'copy';shutil.copytree(ROOT,p);return p
 def test_manifest_requires_all_files(self):
  p=self.case();m=json.loads((p/'MANIFEST.json').read_text());m.pop();(p/'MANIFEST.json').write_text(json.dumps(m))
  with self.assertRaises(AssertionError):mod.verify(p)
 def test_inflated_count_refuses(self):
  p=self.case();f=p/'claims.json';c=json.loads(f.read_text());c['broad_core_passed']=535;f.write_text(json.dumps(c));seal(p)
  with self.assertRaises(AssertionError):mod.verify(p)
 def test_erased_failed_setup_refuses(self):
  p=self.case();f=p/'raw/v1/integration-ha_state_crash-result.json';c=json.loads(f.read_text());c['exit_code']=0;f.write_text(json.dumps(c));seal(p)
  with self.assertRaises(AssertionError):mod.verify(p)
 def test_missing_probe_environment_refuses(self):
  p=self.case();f=p/'raw/v1/all-core-units-result.json';c=json.loads(f.read_text());c['environment_override']={};f.write_text(json.dumps(c));seal(p)
  with self.assertRaises(AssertionError):mod.verify(p)
 def test_wrong_compiler_worktree_refuses(self):
  p=self.case();f=p/'raw/v1/namespace-identity-focus.stderr.gz';b=gzip.decompress(f.read_bytes()).replace(b'/tmp/bicdb-local-namespace-public/crates/bicdb-core',b'/tmp/wrong-worktree/crates/bicdb-core');f.write_bytes(gzip.compress(b,mtime=0));seal(p)
  with self.assertRaises(AssertionError):mod.verify(p)
if __name__=='__main__':unittest.main()
