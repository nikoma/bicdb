import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("native_r0_verifier", ROOT / "scripts/verify-native-r0-target-evidence.py")
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)
EVIDENCE = ROOT / "docs/evidence/native-r0-target-install-2026-11-11"


def rehash(root, name):
    path = root / "manifest.json"
    manifest = json.loads(path.read_text())
    data = (root / name).read_bytes()
    for item in manifest["files"]:
        if item["path"] == name:
            item.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            break
    else:
        raise AssertionError("missing original manifest entry")
    path.write_text(json.dumps(manifest))


class NativeR0EvidenceTests(unittest.TestCase):
    def test_original_focused_scope_and_sources_replay(self):
        result = VERIFIER.verify(EVIDENCE)
        self.assertEqual(result["tests"], 7)
        self.assertFalse(result["physical_qualified"])

    def altered(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name) / "evidence"
        shutil.copytree(EVIDENCE, root)
        return root

    def test_rehashed_physical_qualification_upgrade_is_refused(self):
        root = self.altered()
        path = root / "validation.json"
        value = json.loads(path.read_text())
        value["physical_testing"] = True
        path.write_text(json.dumps(value))
        rehash(root, path.name)
        with self.assertRaisesRegex(ValueError, "qualification upgrade"):
            VERIFIER.verify(root)

    def test_rehashed_original_failed_run_cannot_be_reclassified_pass(self):
        root = self.altered()
        path = root / "focused-initial.result.json"
        value = json.loads(path.read_text())
        value["exit_code"] = 0
        path.write_text(json.dumps(value))
        rehash(root, path.name)
        with self.assertRaisesRegex(ValueError, "retained run result changed"):
            VERIFIER.verify(root)

    def test_rehashed_cached_executable_output_is_not_compilation_proof(self):
        root = self.altered()
        path = root / "focused-third.log.gz"
        text = gzip.decompress(path.read_bytes()).decode()
        text = "\n".join(line for line in text.splitlines() if "Compiling bicdb-core" not in line)
        path.write_bytes(gzip.compress(text.encode(), mtime=0))
        rehash(root, path.name)
        with self.assertRaisesRegex(ValueError, "actual candidate-worktree compilation"):
            VERIFIER.verify(root)


if __name__ == "__main__":
    unittest.main()
