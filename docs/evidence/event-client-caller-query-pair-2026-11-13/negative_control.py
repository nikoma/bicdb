#!/usr/bin/env python3
"""Tamper an original ZIP in a temporary copy; require offline rejection."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix="bicdb-caller-query-negative-") as tmp:
    copy = Path(tmp) / "packet"
    shutil.copytree(root, copy)
    target = copy / "b18/original-focused-core.zip"
    data = bytearray(target.read_bytes())
    data[len(data) // 2] ^= 1
    target.write_bytes(data)
    result = subprocess.run([sys.executable, "-B", str(copy / "verify.py")], capture_output=True, text=True)
    if result.returncode == 0 or "file hash: b18/original-focused-core.zip" not in result.stderr:
        raise SystemExit("FAIL: changed original artifact was not rejected by its hash")
print("PASS: changed original ZIP rejected; immutable source packet untouched")
