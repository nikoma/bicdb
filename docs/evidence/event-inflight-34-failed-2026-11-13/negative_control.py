#!/usr/bin/env python3
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
root = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix="bicdb-failed34-negative-") as tmp:
    copy = Path(tmp) / "packet"
    shutil.copytree(root, copy)
    target = copy / "artifact14091141775.zip"
    data = bytearray(target.read_bytes())
    data[len(data) // 2] ^= 1
    target.write_bytes(data)
    result = subprocess.run([sys.executable, "-B", str(copy / "verify.py")], capture_output=True, text=True)
    if result.returncode == 0 or "file hash: artifact14091141775.zip" not in result.stderr:
        raise SystemExit("FAIL: changed original artifact was not rejected")
print("PASS: changed failed-run artifact rejected; original packet untouched")
