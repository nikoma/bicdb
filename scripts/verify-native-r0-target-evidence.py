#!/usr/bin/env python3
"""Read-only integrity/scope replay of the retained R0 software component run."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re


def verify(root):
    root = Path(root).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    expected = set()
    for item in manifest["files"]:
        relative = Path(item["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe evidence path")
        name = str(relative)
        if name in expected:
            raise ValueError("duplicate evidence path")
        expected.add(name)
        data = (root / relative).read_bytes()
        if len(data) != item["bytes"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError("evidence file integrity mismatch: " + name)
    actual = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file() and p.name != "manifest.json"}
    if actual != expected:
        raise ValueError("manifest does not cover the exact exported evidence set")
    source = json.loads((root / "source-inputs.json").read_text())
    final = json.loads((root / "validation.json").read_text())
    if source["source_commit"] != final["source_commit"]:
        raise ValueError("source identity differs from final result")
    for item in source["files"]:
        with gzip.open(root / item["export_path"], "rb") as stream:
            data = stream.read(item["source_bytes"] + 1)
        if len(data) != item["source_bytes"] or hashlib.sha256(data).hexdigest() != item["source_sha256"]:
            raise ValueError("source snapshot integrity mismatch")
    for claim in ["nonzero_R_supported", "current_voter_repair_source_producer_implemented", "physical_testing", "production_qualified", "deployed", "network_TLS_exercised", "operator_binary_produced"]:
        if final.get(claim) is not False:
            raise ValueError("unsupported scope/qualification upgrade: " + claim)
    if final["tests_passed"] != 7 or final["tests_failed_final"] != 0 or final["status"] != "FOCUSED_SOFTWARE_COMPONENT_PASS":
        raise ValueError("unexpected final component result")
    for stage, exit_code in [("initial", 101), ("second", 101), ("third", 0)]:
        result = json.loads((root / f"focused-{stage}.result.json").read_text())
        if result["exit_code"] != exit_code or result["resource_stop_reason"] is not None:
            raise ValueError("retained run result changed")
    result = json.loads((root / "focused-third.result.json").read_text())
    if result["source_commit"] != final["source_commit"]:
        raise ValueError("successful run source differs")
    log = gzip.decompress((root / "focused-third.log.gz").read_bytes()).decode()
    if not re.search(r"Compiling bicdb-core v1\.0\.466-beta \(/tmp/bicdb-native-range-image-install/crates/bicdb-core\)", log):
        raise ValueError("no actual candidate-worktree compilation proof")
    if "test result: ok. 7 passed; 0 failed; 0 ignored" not in log:
        raise ValueError("successful focused test output missing")
    samples = [json.loads(line) for line in gzip.decompress((root / "focused-third.resources.jsonl.gz").read_bytes()).splitlines()]
    if not samples or min(sample["available_bytes"] for sample in samples) != result["min_available_bytes"]:
        raise ValueError("RAM minimum does not match retained monitor samples")
    return {"status": "PASS", "tests": 7, "source_commit": final["source_commit"], "scope": "software empty-genesis R0 only", "physical_qualified": False, "production_qualified": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path, default=Path(__file__).resolve().parents[1] / "docs/evidence/native-r0-target-install-2026-11-11")
    print(json.dumps(verify(parser.parse_args().directory), sort_keys=True))
