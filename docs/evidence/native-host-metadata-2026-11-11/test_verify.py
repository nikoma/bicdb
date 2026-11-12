#!/usr/bin/env python3
"""Evidence corruption checks; these do not test the database."""
import gzip,hashlib,importlib.util,json,shutil,tempfile,unittest,sys
sys.dont_write_bytecode=True
from pathlib import Path
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('bundle_verify',ROOT/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def rehash(d):
 rows=[]
 for p in sorted(d.rglob('*')):
  if p.is_file() and p.name!='MANIFEST.json':
   b=p.read_bytes();rows.append({'path':p.relative_to(d).as_posix(),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
 (d/'MANIFEST.json').write_text(json.dumps(rows,indent=2)+'\n')
class Evidence(unittest.TestCase):
 def test_original(self):v.verify(ROOT)
 def corrupt(self,fn,rehash_manifest=True):
  with tempfile.TemporaryDirectory() as t:
   d=Path(t)/'bundle';shutil.copytree(ROOT,d);fn(d)
   if rehash_manifest:rehash(d)
   with self.assertRaises((AssertionError,ValueError,KeyError,TypeError)):v.verify(d)
 def test_unmanifested_file(self):self.corrupt(lambda d:(d/'extra').write_text('x'),False)
 def test_changed_raw_bytes(self):self.corrupt(lambda d:(d/'README.md').write_text('changed'),False)
 def test_false_qualification(self):
  def f(d):p=d/'claims.json';x=json.loads(p.read_text());x['production_qualified']=True;p.write_text(json.dumps(x))
  self.corrupt(f)
 def test_source_relabel(self):
  def f(d):p=next((d/'runs').glob('*/result.json'));x=json.loads(p.read_text());x['source_revision']='0'*40;p.write_text(json.dumps(x))
  self.corrupt(f)
 def test_missing_test(self):
  def f(d):p=d/'claims.json';x=json.loads(p.read_text());x['distinct_passing_tests']+=1;p.write_text(json.dumps(x))
  self.corrupt(f)
 def test_failed_run_hidden(self):
  def f(d):p=d/'claims.json';x=json.loads(p.read_text());next(r for r in x['runs'] if r['exit_code']!=0)['exit_code']=0;p.write_text(json.dumps(x))
  self.corrupt(f)
 def test_source_patch_corrupted(self):
  def f(d):p=d/'final-source.patch.gz';p.write_bytes(gzip.compress(gzip.decompress(p.read_bytes())+b'\nx',mtime=0))
  self.corrupt(f)
if __name__=='__main__':unittest.main()
