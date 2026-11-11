import gzip,hashlib,importlib.util,json,os,shutil,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('activation_receipt_verifier',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def manifest(root):
    rows=[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'];write(root/'MANIFEST.json',rows)
def write(path,value):
    tmp=path.with_name(path.name+'.tmp');tmp.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');os.replace(tmp,path)
class TamperTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
    def tearDown(self):self.tmp.cleanup()
    def test_claiming_a_later_ack_is_rejected(self):
        path=self.root/'claims.json';c=json.loads(path.read_text());c['new_application_write_after_activation_tested']=True;write(path,c);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_a_lost_failed_case_is_rejected(self):
        path=self.root/'claims.json';c=json.loads(path.read_text());c['runs']=[r for r in c['runs'] if r['name']!='activated-rplusone'];write(path,c);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_edited_pass_count_is_rejected(self):
        path=self.root/'claims.json';c=json.loads(path.read_text());c['distinct_passing_tests']=11;write(path,c);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_resource_pause_cannot_be_hidden_by_manifest_rehash(self):
        path=self.root/'runs/selected-key-decision/resources.jsonl.gz';rows=[json.loads(s) for s in gzip.decompress(path.read_bytes()).decode().splitlines()];rows[0]['paused']=True
        tmp=path.with_name(path.name+'.tmp');tmp.write_bytes(gzip.compress(('\n'.join(json.dumps(r) for r in rows)+'\n').encode(),mtime=0));os.replace(tmp,path);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_old_anchor_cannot_claim_current_follower_catchup(self):
        path=self.root/'followup-1cd/claims.json';c=json.loads(path.read_text());c['late_follower_caught_up_r2']=True;write(path,c);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
