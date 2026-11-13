#!/usr/bin/env python3
"""Six independent payload/receipt/extra-file tamper refusals; no engine open."""
import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo", type=Path, default=Path.cwd())
args = parser.parse_args()
bundle = Path(__file__).resolve().parent
cases = ["github-core-job.log.gz", "source-from-main488.patch.gz", "source-inputs.json.gz", "inclusive-stage-counts.json", "receipt.json", "unexpected-file"]
for name in cases:
    with tempfile.TemporaryDirectory(prefix="bicdb-fd85-tamper-") as directory:
        target = Path(directory) / "bundle"
        shutil.copytree(bundle, target)
        path = target / name
        if name == "unexpected-file":
            path.write_bytes(b"unrecorded")
        else:
            data = bytearray(path.read_bytes())
            data[len(data) // 2] ^= 1
            path.write_bytes(data)
        result = subprocess.run([sys.executable, str(bundle / "verify.py"), "--repo", str(args.repo.resolve()), "--bundle", str(target)], capture_output=True)
        if result.returncode == 0:
            raise AssertionError("tamper was accepted: " + name)
print("PASS: 6 independent tamper refusals")
