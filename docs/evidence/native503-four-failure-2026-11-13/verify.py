#!/usr/bin/env python3
"""Offline integrity/source/failed-suite checks. Never opens a BicDB database."""
import argparse
import gzip
import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path

REPORT_SHA = "997e86fb6431cb7072593bd17b3f8a980d7e41c3a966ea13e39548846c423f09"
LOG_SHA = "8af0ca1ea94ef88ae64a0ed4101d907d044072acd20ff8ce2c22d4149da09e54"
PRIVATE_BLOCK = re.compile(rb"-----BEGIN (?:(?:RSA|EC|OPENSSH|ENCRYPTED|DSA) )?PRIVATE KEY-----")


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_bytes())


def git_object_id(kind, data):
    return hashlib.sha1(kind.encode() + b" " + str(len(data)).encode() + b"\0" + data).hexdigest()


def commit_tree(root, commit):
    raw = (root / "proofs/git/commits" / (commit + ".txt")).read_bytes()
    require(git_object_id("commit", raw) == commit, "Git commit object identity: " + commit)
    match = re.match(rb"tree ([0-9a-f]{40})\n", raw)
    require(match is not None, "Git commit tree missing")
    return match[1].decode()


def tree_entries(root, sha):
    raw = (root / "proofs/git/trees" / (sha + ".bin")).read_bytes()
    require(git_object_id("tree", raw) == sha, "Git tree object identity: " + sha)
    entries = {}
    offset = 0
    while offset < len(raw):
        space = raw.index(b" ", offset)
        nul = raw.index(b"\0", space + 1)
        require(nul + 21 <= len(raw), "Truncated Git tree entry")
        name = raw[space + 1:nul].decode()
        require(name not in entries, "Duplicate Git tree member")
        entries[name] = (raw[offset:space].decode(), raw[nul + 1:nul + 21].hex())
        offset = nul + 21
    return entries


def member_blob(root, tree, path):
    parts = Path(path).parts
    require(parts and all(p not in (".", "..") for p in parts), "Unsafe source path")
    for index, part in enumerate(parts):
        entries = tree_entries(root, tree)
        require(part in entries, "Missing Git source membership: " + path)
        mode, tree = entries[part]
        if index + 1 < len(parts):
            require(mode in ("40000", "040000"), "Expected Git directory")
        else:
            require(mode in ("100644", "100755"), "Expected ordinary source blob")
    return tree


def verify(root):
    manifest = read_json(root / "manifest.json")
    expected = {entry["path"]: entry for entry in manifest["files"]}
    actual = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), "Symlink in evidence packet")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    require(actual == set(expected) | {"manifest.json"}, "Packet file set differs from manifest")
    for name, entry in expected.items():
        path = Path(name)
        require(not path.is_absolute() and ".." not in path.parts, "Unsafe manifest path")
        data = (root / name).read_bytes()
        require(len(data) == entry["bytes"] and digest(data) == entry["sha256"], "Hash/size mismatch: " + name)
    report_raw = (root / "forensics-original.json").read_bytes()
    require(digest(report_raw) == REPORT_SHA, "Frozen forensic report changed")
    report = json.loads(report_raw)
    with gzip.open(root / "raw-job.log.gz", "rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    require(len(raw) <= 8 * 1024 * 1024, "Raw log exceeds packet bound")
    require(digest(raw) == LOG_SHA == report["raw_log"]["sha256"], "Lossless original log mismatch")
    require(len(raw) == report["raw_log"]["bytes"], "Original log byte count")
    text = raw.decode()
    lines = text.splitlines()
    byte_lines = raw.splitlines(keepends=True)
    require(len(lines) == report["raw_log"]["lines"], "Original log line count")
    summary = re.findall(r"test result: FAILED\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9.]+)s", text)
    require(len(summary) == 1, "Exactly one failed Rust test summary required")
    passed, failed, ignored, measured, filtered = map(int, summary[0][:5])
    duration = float(summary[0][5])
    require((passed, failed, ignored, measured, filtered) == (208, 4, 1, 0, 877), "Unexpected actual suite counts")
    require(f"running {passed + failed + ignored} tests" in text, "Selected count inconsistent with actual log")
    require((passed, failed, ignored, filtered, duration) == (report["result"]["passed"], report["result"]["failed"], report["result"]["ignored"], report["result"]["filtered"], report["result"]["duration_seconds"]), "Report differs from actual Rust summary")
    headers = [(number, m[1]) for number, line in enumerate(lines, 1) if (m := re.search(r"---- (.+) stdout ----$", line))]
    require(len(headers) == failed, "Actual failed stdout boundaries missing")
    require([name for _, name in headers] == [case["test"] for case in report["cases"]], "Failed case identities changed")
    boundaries = read_json(root / "case-boundaries.json")
    require(len(boundaries) == failed, "Expected four exact case sections")
    for boundary, (number, name), case in zip(boundaries, headers, report["cases"]):
        require(boundary["test"] == name and boundary["start_line_in_full_log"] == number, "Failure section start differs")
        start, end = boundary["start_line_in_full_log"], boundary["end_line_in_full_log"]
        section = b"".join(byte_lines[start - 1:end])
        require(gzip.decompress((root / boundary["section_file"]).read_bytes()) == section, "Failure section not exact original bytes")
        require(digest(section) == boundary["uncompressed_sha256"], "Failure section hash")
        require(case["classification"].startswith("FAILED"), "Failed finding was promoted")
        require(f'{case["panic"]["path"]}:{case["panic"]["line"]}:' in section.decode(), "Original panic source boundary absent")
    identity = read_json(root / "source-identity.json")
    checkouts = re.findall(r"git log -1 --format=%H\r?\n[^\n]*?\b([0-9a-f]{40})\s*\n", text)
    require(checkouts == [identity["actual_checkout_sha"]], "Actual checkout SHA not proven by raw git log output")
    head_tree = commit_tree(root, identity["run_head_sha"])
    actual_tree = commit_tree(root, identity["actual_checkout_sha"])
    main_tree = commit_tree(root, identity["main_comparison_sha"])
    require(identity["actual_checkout_sha"] != identity["run_head_sha"], "Distinct source/checkout identities erased")
    require(head_tree == actual_tree == identity["run_head_tree"] == identity["actual_checkout_tree"], "PR checkout is not the same verified complete tree")
    require(main_tree == identity["main_comparison_tree"], "Main comparison tree mismatch")
    for pin in report["source_pins"]:
        source = (root / "sources" / pin["path"]).read_bytes()
        require(len(source) == pin["bytes"] and digest(source) == pin["sha256"], "Source pin content mismatch")
        blob = git_object_id("blob", source)
        require(blob == pin["blob"] == member_blob(root, head_tree, pin["path"]) == member_blob(root, actual_tree, pin["path"]), "Source blob is not a member of tested tree")
        require((member_blob(root, main_tree, pin["path"]) == blob) == pin["byte_identical_to_main629"], "Main source comparison claim mismatch")
    job, run = read_json(root / "job-terminal.json"), read_json(root / "run-observation.json")
    require(job["id"] == report["job"] and job["run_id"] == report["run"] == run["id"], "Run/job identity mismatch")
    require(job["status"] == "completed" and job["conclusion"] == "failure", "Original failed job status lost")
    require(job["head_sha"] == run["head_sha"] == identity["run_head_sha"] == report["source"], "API head differs from preserved source")
    require(run["repository"]["private"] is False, "Public repository access metadata differs")
    matches = []
    for name in expected:
        data = (root / name).read_bytes()
        if name.endswith(".gz"):
            data = gzip.decompress(data)
        if PRIVATE_BLOCK.search(data):
            matches.append(name)
    require(not matches, "Recognized private-key-block marker in packet: " + ", ".join(matches))
    return {"status": "passed", "selected": passed + failed + ignored, "passed_tests": passed,
            "failed_tests": failed, "ignored_not_executed": ignored, "test_seconds_not_RTO": duration,
            "actual_checkout": identity["actual_checkout_sha"], "run_head": identity["run_head_sha"],
            "same_verified_tree": head_tree, "source_pins": len(report["source_pins"]),
            "private_block_literal_matches": matches, "no_secret_absence_guarantee": True,
            "database_opened": False, "runtime_or_physical_qualification": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="detect one deliberately mutated packet copy")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    result = verify(root)
    if args.self_test:
        with tempfile.TemporaryDirectory(prefix="bicdb-native503-tamper-") as temporary:
            copied = Path(temporary) / "packet"
            shutil.copytree(root, copied)
            target = copied / "job-terminal.json"
            target.write_bytes(target.read_bytes() + b"\n")
            try:
                verify(copied)
            except ValueError as error:
                require("Hash/size mismatch: job-terminal.json" in str(error), "Unexpected tamper-test failure")
            else:
                raise ValueError("Deliberate mutation was not detected")
        result["tamper_test"] = "passed"
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
