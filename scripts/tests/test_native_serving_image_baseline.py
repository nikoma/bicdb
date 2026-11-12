import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

SCRIPT=Path(__file__).resolve().parents[1]/'verify-native-serving-image-baseline.py'
spec=importlib.util.spec_from_file_location('serving_image_baseline',SCRIPT)
verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)


class BaselineEvidence(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)/'evidence';shutil.copytree(verifier.DEFAULT,self.root)

    def change(self,path,value):
        p=self.root/path;p.write_text(json.dumps(value,indent=2)+'\n')
        manifest=json.loads((self.root/'manifest.json').read_text())
        manifest[path]=hashlib.sha256(p.read_bytes()).hexdigest()
        (self.root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')

    def test_actual_expected_red_is_read_only(self):
        before={str(p.relative_to(self.root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(verifier.verify(self.root)['status'],'VERIFIED_EXPECTED_RED')
        after={str(p.relative_to(self.root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before,after)

    def test_rehashed_failed_run_cannot_be_passing(self):
        path='runs/attempt-2-serving-repair-result.json'
        result=json.loads((self.root/path).read_text());result['exit_code']=0;self.change(path,result)
        with self.assertRaises(AssertionError):verifier.verify(self.root)

    def test_rehashed_current_main_attribution_is_refused(self):
        summary=json.loads((self.root/'summary.json').read_text())
        summary['current_main_or_azure_attribution']=True;self.change('summary.json',summary)
        with self.assertRaises(AssertionError):verifier.verify(self.root)

    def test_rehashed_physical_qualification_is_refused(self):
        summary=json.loads((self.root/'summary.json').read_text())
        summary['qualification']['physically_tested']=True;self.change('summary.json',summary)
        with self.assertRaises(AssertionError):verifier.verify(self.root)

    def test_rehashed_cold_snapshot_claim_is_refused(self):
        summary=json.loads((self.root/'summary.json').read_text())
        summary['independent_cold_snapshot_a_retained']=True;self.change('summary.json',summary)
        with self.assertRaises(AssertionError):verifier.verify(self.root)


if __name__=='__main__':unittest.main()
