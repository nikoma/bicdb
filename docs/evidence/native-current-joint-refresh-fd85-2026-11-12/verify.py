#!/usr/bin/env python3
"""Verify recorded bytes and reconstruct selected source in a temporary Git index.

No checkout, compiler, engine open, network request, or source-HEAD change.
The public base commit must already exist in the supplied repository.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

RECEIPT_SHA256 = "aa789d29ff6114260c11663fedefda1d67f2ba92832e91c458b2538040b9935d"
SOURCE = "fd85a24392abf8a273479f8637e040523c053109"
BASE = "d1d816afe0446b2a614c38004738d3d1c8bbc547"

def require(condition, message):
    if not condition:
        raise ValueError(message)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    args = parser.parse_args()
    bundle = args.bundle.resolve()
    receipt_bytes = (bundle / "receipt.json").read_bytes()
    require(hashlib.sha256(receipt_bytes).hexdigest() == RECEIPT_SHA256, "receipt checksum mismatch")
    receipt = json.loads(receipt_bytes)
    allowed = set(receipt["files"]) | {"receipt.json", "README.md", "verify.py", "tamper_tests.py"}
    require({p.name for p in bundle.iterdir()} == allowed, "unexpected or missing bundle file")
    for name, expected in receipt["files"].items():
        require(Path(name).name == name, "nonlocal payload path")
        payload = (bundle / name).read_bytes()
        require(len(payload) == expected["bytes"], "payload size mismatch: " + name)
        require(hashlib.sha256(payload).hexdigest() == expected["sha256"], "payload checksum mismatch: " + name)
    require(receipt["source_revision"] == SOURCE and receipt["public_base_revision"] == BASE, "wrong source/base")
    require((bundle / "source-revision.txt").read_text().strip() == SOURCE, "artifact source mismatch")
    raw = gzip.decompress((bundle / "github-core-job.log.gz").read_bytes())
    require(hashlib.sha256(raw).hexdigest() == "b3e5b12d1934d7e032987a8d1822da0a9b1a5938ec4aab96337aad517fd7a265", "raw job digest mismatch")
    log = gzip.decompress((bundle / "tests.log.gz").read_bytes()).decode()
    require("0 passed; 1 failed; 0 ignored; 0 measured; 932 filtered out" in log and "83.78s" in log, "recorded outcome mismatch")
    require(receipt["result"]["failed"] == 1 and not receipt["result"]["activation_ack4_strong_r4_reached"], "failure classification changed")
    inputs = json.loads(gzip.decompress((bundle / "source-inputs.json.gz").read_bytes()))
    require(inputs["source_revision"] == SOURCE and inputs["base_revision"] == BASE, "input manifest source mismatch")
    def git(*arguments, env=None, data=None):
        return subprocess.check_output(["git", "-C", str(args.repo), *arguments], env=env, input=data, stderr=subprocess.PIPE)
    common = Path(git("rev-parse", "--git-common-dir").decode().strip())
    if not common.is_absolute():
        common = (args.repo / common).resolve()
    git("cat-file", "-e", BASE + "^{commit}")
    with tempfile.TemporaryDirectory(prefix="bicdb-fd85-source-") as directory:
        temp = Path(directory)
        objects = temp / "objects"
        objects.mkdir()
        env = os.environ.copy()
        env.update(GIT_INDEX_FILE=str(temp / "index"), GIT_OBJECT_DIRECTORY=str(objects), GIT_ALTERNATE_OBJECT_DIRECTORIES=str(common / "objects"))
        git("read-tree", BASE, env=env)
        git("apply", "--cached", "--whitespace=nowarn", "-", env=env, data=gzip.decompress((bundle / "source-from-main488.patch.gz").read_bytes()))
        actual = []
        for entry in git("ls-files", "--stage", "-z", "--", *inputs["selectors"], env=env).split(b"\0"):
            if not entry:
                continue
            metadata, path = entry.split(b"\t", 1)
            mode, blob, stage = metadata.split()
            require(stage == b"0", "unmerged reconstructed input")
            actual.append((path.decode(), mode.decode(), blob.decode()))
        expected = [(entry["path"], entry["mode"], entry["git_blob"]) for entry in inputs["inputs"]]
        require(actual == expected, "reconstructed selected source differs from manifest")
        blobs = git("cat-file", "--batch", env=env, data=("\n".join(entry["git_blob"] for entry in inputs["inputs"]) + "\n").encode())
        offset = 0
        for entry in inputs["inputs"]:
            end = blobs.index(b"\n", offset)
            blob, kind, count = blobs[offset:end].split()
            count = int(count)
            payload = blobs[end + 1:end + 1 + count]
            offset = end + count + 2
            require(blob.decode() == entry["git_blob"] and kind == b"blob", "wrong reconstructed blob")
            require(count == entry["bytes"] and hashlib.sha256(payload).hexdigest() == entry["sha256"], "source content mismatch: " + entry["path"])
        require(offset == len(blobs), "unexpected trailing blob data")
    require(len(expected) == receipt["source_input_files"], "source input count mismatch")
    require(sum(entry["bytes"] for entry in inputs["inputs"]) == receipt["source_input_bytes"], "source input byte count mismatch")
    print(json.dumps({"verified": True, "source": SOURCE, "public_base": BASE, "source_inputs": len(expected), "source_bytes": receipt["source_input_bytes"], "recorded_result": "FAILED 0 passed / 1 failed", "engine_opened": False, "compiled": False}, sort_keys=True))

if __name__ == "__main__":
    main()
