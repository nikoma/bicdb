import hashlib,importlib.util,json,shutil,sys,tempfile,unittest
from pathlib import Path
from unittest import mock
sys.dont_write_bytecode=True
HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('birth_receipt_verify',HERE/'verify.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def write(path,data):
 path.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
def manifest(root):
 write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.relative_to(root).as_posix()!='MANIFEST.json'])
class Integrity(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)/'bundle';shutil.copytree(HERE,self.root)
 def tearDown(self):self.temp.cleanup()
 def claims(self,edit):
  p=self.root/'claims.json';c=json.loads(p.read_text());edit(c);write(p,c);manifest(self.root)
 def reject(self):
  with self.assertRaises((AssertionError,ValueError,KeyError)):module.verify(self.root)
 def test_final_count_cannot_include_overlapping_earlier_passes(self):
  self.claims(lambda c:c.update(distinct_passing_tests=44,passing_executions=44));self.reject()
 def test_birth_checked_candidate_cannot_become_strong_value_permission(self):
  self.claims(lambda c:c.update(enrolled_read_result_grants_value_or_absence=True,strong_get_tested=True));self.reject()
 def test_earlier_factory_failure_cannot_be_relabelled_final_pass(self):
  self.claims(lambda c:next(r for r in c['runs'] if r['name']=='native-enrollment-v2').update(classification='final_exact_source_passing',exit_code=0));self.reject()
 def test_missing_manifest_coverage_is_rejected(self):
  p=self.root/'MANIFEST.json';rows=json.loads(p.read_text());write(p,[r for r in rows if r['path']!='claims.json']);self.reject()
 def test_pressure_pause_cannot_be_hidden(self):
  self.claims(lambda c:next(r for r in c['runs'] if r['name']=='native-enrollment-v2').update(pause_episodes=0));self.reject()
 def test_new_factory_roots_cannot_be_invented_from_existing_r2_custody(self):
  self.claims(lambda c:c.update(new_factory_fixture_roots_retained=True));self.reject()
 def test_primary_source_identity_cannot_be_changed_to_later_integration(self):
  self.claims(lambda c:c.update(source_revision='d875b466adaefb5e08f0575bf257287d39ea35c2'));self.reject()
 def test_read_fixture_failures_cannot_be_removed_from_claims(self):
  self.claims(lambda c:next(r for r in c['runs'] if r['name']=='native-read-enrolled-v1').update(failed_test_ids=[]));self.reject()
 def test_default_verification_does_not_read_private_archive(self):
  original=Path.read_bytes
  def guarded(path):
   if str(path).startswith('/root/.local/state/bicdb/'):raise AssertionError('unexpected private archive read')
   return original(path)
  with mock.patch.object(Path,'read_bytes',guarded):result=module.verify(self.root)
  self.assertFalse(result['private_custody_verified'])
if __name__=='__main__':unittest.main()
