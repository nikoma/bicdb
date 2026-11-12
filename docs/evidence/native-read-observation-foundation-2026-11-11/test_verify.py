import gzip,hashlib,importlib.util,json,os,shutil,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('native_read_evidence',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def write(path,obj):
    tmp=path.with_name(path.name+'.tmp');tmp.write_text(json.dumps(obj,sort_keys=True,indent=2)+'\n');os.replace(tmp,path)
def manifest(root):
    write(root/'MANIFEST.json',[{'path':p.relative_to(root).as_posix(),'bytes':len(p.read_bytes()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'])
class NegativeCases(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'bundle';shutil.copytree(ROOT,self.root,copy_function=os.link)
    def tearDown(self):self.tmp.cleanup()
    def change(self,fn):
        p=self.root/'claims.json';c=json.loads(p.read_text());fn(c);write(p,c);manifest(self.root)
    def test_observation_cannot_claim_strong_read(self):
        self.change(lambda c:c.__setitem__('strong_get_tested',True))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_retained_roots_cannot_invent_an_external_ledger(self):
        self.change(lambda c:c.__setitem__('external_ack_ledger_written',True))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_missing_failed_attempt_is_rejected(self):
        self.change(lambda c:c.__setitem__('runs',[r for r in c['runs'] if r['name']!='read-foundation-v2']))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_earlier_passes_do_not_inflate_successful_revision(self):
        self.change(lambda c:c.__setitem__('passing_tests',17))
        with self.assertRaises(AssertionError):v.verify(self.root)
    def test_manifest_rehash_does_not_hide_resource_pause(self):
        p=self.root/'runs/read-foundation-v3/resources.jsonl.gz';rows=[json.loads(s) for s in gzip.decompress(p.read_bytes()).decode().splitlines()];rows[0]['paused']=True
        tmp=p.with_name(p.name+'.tmp');tmp.write_bytes(gzip.compress(('\n'.join(json.dumps(r) for r in rows)+'\n').encode(),mtime=0));os.replace(tmp,p);manifest(self.root)
        with self.assertRaises(AssertionError):v.verify(self.root)
if __name__=='__main__':unittest.main()
