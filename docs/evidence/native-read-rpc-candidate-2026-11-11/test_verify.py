import gzip,hashlib,importlib.util,json,os,shutil,sys,tempfile,unittest
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('native_rpc_receipt',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def write(path,obj):
    tmp=path.with_name(path.name+'.tmp');tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n');os.replace(tmp,path)
def manifest(root):write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class NegativeCases(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
    def tearDown(self):self.tmp.cleanup()
    def change(self,fn):
        p=self.root/'claims.json';c=json.loads(p.read_text());fn(c);write(p,c);manifest(self.root)
    def test_no_strong_permission(self):
        self.change(lambda c:c.__setitem__('strong_get_tested',True))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_factual_candidate_cannot_become_admitted(self):
        self.change(lambda c:c.__setitem__('candidate_is_unadmitted',False))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_prior_executions_cannot_inflate_final_distinct_count(self):
        self.change(lambda c:c.__setitem__('passing_tests',20))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_pending_counter_has_no_pass_claim(self):
        self.change(lambda c:c['pending_test_only_followup'].__setitem__('tested',True))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_prior_source_receipt_required(self):
        self.change(lambda c:c.__setitem__('runs',[c['runs'][-1]]))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_rehashed_resource_pause_rejected(self):
        p=self.root/'runs/read-rpc-v2/resources.jsonl.gz';rows=[json.loads(s) for s in gzip.decompress(p.read_bytes()).decode().splitlines()];rows[0]['paused']=True
        tmp=p.with_name(p.name+'.tmp');tmp.write_bytes(gzip.compress(('\n'.join(json.dumps(r) for r in rows)+'\n').encode(),mtime=0));os.replace(tmp,p);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
