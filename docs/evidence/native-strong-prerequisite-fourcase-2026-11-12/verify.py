#!/usr/bin/env python3
"""Verify retained bytes and bounded results; never open/replay a database."""
import argparse
import copy
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
SOURCE = "7b55059fd610b40189afd239cf8e9ea7b2986ad4"
RUN = 43330054124


def sha(data):
    return hashlib.sha256(data).hexdigest()


def validate(result, core, compile_meta, events):
    assert result["source_revision"] == SOURCE and result["run_id"] == RUN
    assert result["core_job_id"] == 131572255168 and result["compile_job_id"] == 131572220029
    assert result["core"] == dict(passed=0, failed=4, ignored=0, filtered=955, seconds=78.27)
    assert "0 passed; 4 failed; 0 ignored; 0 measured; 955 filtered out; finished in 78.27s" in core
    assert result["test_threads"] == 4 and len(result["cases"]) == 4
    assert len({case["test"] for case in result["cases"]}) == 4
    assert all(case["outcome"] == "FAILED" and not case["postcat_cut_reached"]
               and not case["CurrentJointRecovery_continuation_reached"] for case in result["cases"])
    assert sum("at DataQuorum:" in c["actual_refusal"] for c in result["cases"]) == 3
    assert sum("at Canceled:" in c["actual_refusal"] for c in result["cases"]) == 1
    assert core.count("NATIVE_STRONG_TRACE ") == result["trace_events"] == 802
    assert core.count("NATIVE_PROBE_TRACE ") == result["recovery_probe_trace_events"] == 0
    assert not result["physical_qualification"] and not result["operator_or_autonomous_recovery_qualification"]
    assert not result["native_engine_opened_or_replayed_by_receipt"]
    assert not result["portable_packet_contains_databases_or_private_keys"]
    assert not result["historical_eedf_fourcase_failure_replaced_or_waived"]
    assert result["all_targets"] == result["isolated_no_default_consumer"] == "PASS"
    assert compile_meta["conclusion"] == "success"
    assert all(step["conclusion"] == "success" for step in compile_meta["steps"]
               if step["name"] in ["cargo check --workspace --all-targets", "Workflow consumer compiles without Core defaults"])
    # Recompute all stage distributions from actual raw events, without summing nested spans.
    durations = {}
    for line in core.splitlines():
        if "NATIVE_STRONG_TRACE " not in line or "event=end" not in line:
            continue
        stage = re.search(r"\bstage=(\S+)", line).group(1)
        duration = re.search(r"\belapsed_us=(\d+)", line)
        if duration:
            durations.setdefault(stage, []).append(int(duration.group(1)))
    import statistics
    for stage, values in durations.items():
        assert events["stages"][stage] == dict(ends=len(values), min_us=min(values),
            median_us=statistics.median(values), max_us=max(values))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo")
    parser.add_argument("--private-zip")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    manifest = json.loads((HERE / "MANIFEST.json").read_text())
    for item in manifest["files"]:
        path = PurePosixPath(item["path"])
        assert not path.is_absolute() and ".." not in path.parts
        raw = (HERE / str(path)).read_bytes()
        assert len(raw) == item["bytes"] and sha(raw) == item["sha256"], str(path)
    result = json.loads((HERE / "result.json").read_text())
    core = gzip.decompress((HERE / "core.log.gz").read_bytes()).decode()
    for kind in ["core", "compile"]:
        raw = gzip.decompress((HERE / f"{kind}.log.gz").read_bytes())
        index = json.loads((HERE / f"{kind}-index.json").read_text())
        assert len(raw) == index["raw_bytes"] and sha(raw) == index["raw_sha256"]
        assert index["source_revision"] == SOURCE and index["run_id"] == RUN
    compile_meta = json.loads((HERE / "compile-job.json").read_text())
    events = json.loads((HERE / "trace-profile.json").read_text())
    validate(result, core, compile_meta, events)
    selected = gzip.decompress((HERE / "selected-tests.log.gz").read_bytes()).decode()
    names = {line[:-6] for line in selected.splitlines() if line.endswith(": test")}
    assert names == {case["test"] for case in result["cases"]}
    assert (HERE / "source-revision.txt").read_text().strip() == SOURCE
    custody = json.loads((HERE / "private-custody-receipt.json").read_text())
    assert custody["zip_sha256"] == result["private_artifact_sha256"]
    assert len(custody["manifests"]) == 4 and custody["verified_files"] == 300
    assert custody["verified_bytes"] == 18711064
    assert all(not m["missing_files"] and m["verified_files"] == m["expected_files"] == 75
               and not m["engine_reopened"] and not m["atomic_powerloss_snapshot_or_replay_verified"]
               and m["source_revision"] == SOURCE for m in custody["manifests"])
    if args.repo:
        got = subprocess.check_output(["git", "-C", args.repo, "rev-parse", SOURCE], text=True).strip()
        assert got == SOURCE
        protocol = subprocess.check_output(["git", "-C", args.repo, "show", SOURCE + ":crates/bicdb-core/src/distribution.rs"], text=True)
        assert "pub const CLUSTER_DATA_PROTOCOL_VERSION: u32 = 28;" in protocol
        package = subprocess.check_output(["git", "-C", args.repo, "show", SOURCE + ":crates/bicdb-core/Cargo.toml"], text=True)
        assert 'version = "1.0.490-beta"' in package
    if args.private_zip:
        archive = Path(args.private_zip).read_bytes()
        assert sha(archive) == custody["zip_sha256"]
        with zipfile.ZipFile(args.private_zip) as z:
            assert z.read("source-revision.txt").decode().strip() == SOURCE
            for m in custody["manifests"]:
                raw = z.read(m["path"])
                assert sha(raw) == m["manifest_sha256"]
                actual = json.loads(raw)
                for item in actual["files"]:
                    payload = z.read(str(PurePosixPath(m["path"]).parent / item["path"]))
                    assert len(payload) == item["bytes"] and sha(payload) == item["sha256"]
        print("PASS: private ZIP and all 300 manifest payloads, bytes only/no engine open")
    if args.self_test:
        mutations = [lambda r: r.update(source_revision="0" * 40),
                     lambda r: r["core"].update(passed=4, failed=0),
                     lambda r: r.update(physical_qualification=True),
                     lambda r: r.update(native_engine_opened_or_replayed_by_receipt=True),
                     lambda r: r["cases"][0].update(postcat_cut_reached=True),
                     lambda r: r.update(historical_eedf_fourcase_failure_replaced_or_waived=True)]
        for mutate in mutations:
            bad = copy.deepcopy(result)
            mutate(bad)
            try:
                validate(bad, core, compile_meta, events)
            except AssertionError:
                continue
            raise AssertionError("semantic tamper was accepted")
        print("PASS: six semantic tamper controls")
    print("PASS: portable bytes, exact 0/4 boundary, trace distributions and compiler scope")


if __name__ == "__main__":
    main()
