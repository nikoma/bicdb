#!/usr/bin/env python3
"""Offline verification of immutable FAILED34 evidence. No engine or network."""
import gzip
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parent
HEAD = "91713c0fed694505086f5351a2b0df755b7c6ffa"
RUNTIME = "1ce471aa315b93f58c00ddce5c0577505df472ab"
ARTIFACTS = {
    14091141775: "2d87f9051135c77ec96e6dce81dfbfa9f388130de8d91b4b08786f9a7b6da8bc",
    14090654425: "4edd485a9b2e8e12210a68c51ec541afce05763a3adddf295e11f109e78b0215",
}


def require(condition, label):
    if not condition:
        raise ValueError(label)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load(name):
    return json.loads((ROOT / name).read_text())


def verify():
    manifest = load("manifest.json")
    actual = {str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file() and p.name != "manifest.json"}
    require(actual == set(manifest["files"]), "packet file set")
    for name, item in manifest["files"].items():
        data = (ROOT / name).read_bytes()
        require(len(data) == item["bytes"] and sha(data) == item["sha256"], "file hash: " + name)
    run, request, retained = load("run-terminal.json"), load("request.json"), load("artifact-verification.json")
    require(run["id"] == 43538783755 and run["head_sha"] == HEAD and run["conclusion"] == "failure", "failed run identity")
    require(request["head"] == HEAD and request["runtime_source"] == RUNTIME and request["dispatch_count"] == 1 and
            request["test_threads"] == 1, "request identity")
    binding = load("source-binding.json")
    require(binding["runtime"] == RUNTIME and binding["qualifier"] == HEAD and binding["qualifier_Core_identical"] and
            binding["Core_subtree"] == "700ec073513e7ac7cc46df10bb0dc292abc215cd", "recorded Core identity")
    require(sha((ROOT / "guard-build-memory.py").read_bytes()) == request["build_guard_sha256"], "guard identity")
    artifacts = {a["id"]: a for a in load("artifacts.json")["artifacts"]}
    pressure_count = 0
    for artifact_id, digest in ARTIFACTS.items():
        raw = (ROOT / f"artifact{artifact_id}.zip").read_bytes()
        require(sha(raw) == digest and artifacts[artifact_id]["digest"] == "sha256:" + digest, "original artifact")
        with zipfile.ZipFile(ROOT / f"artifact{artifact_id}.zip") as z:
            require(not any("custody" in n for n in z.namelist()), "unexpected custody")
            for name in z.namelist():
                data = z.read(name)
                identity = retained["artifact_members"][f"{artifact_id}:{name}"]
                require(len(data) == identity["bytes"] and sha(data) == identity["sha256"], "member " + name)
                if name.endswith(".jsonl"):
                    rows = [json.loads(line) for line in data.decode().splitlines()]
                    require(rows[-1]["event"] == "completed" and rows[-1]["guard_returncode"] == 0 and
                            rows[-1]["child_returncode"] == 0 and rows[-1]["final_group_signal_before_leader_reap"], "guard completion")
                    require(not any(r["event"] in {"resource_guard_interrupted_build", "monitor_failed", "caller_interrupted"}
                                    for r in rows), "guard interrupted")
                    pressure_count += 1
                if name == "tests.log":
                    tests = data.decode()
                if name == "selected-tests.log":
                    selected = [line[:-6] for line in data.decode().splitlines() if line.endswith(": test")]
                if name == "source-revision.txt":
                    require(data.decode().strip() == HEAD, "artifact source")
                if name == "requested-filters.txt":
                    require(data.decode().splitlines() == ["event_client_", "actual_rpc_evidence_"], "fixed OR filters")
    require(pressure_count == 4, "four guarded commands")
    require(len(selected) == 34 and sorted(n.rsplit("::", 1)[-1] for n in selected) == sorted(request["static_names"]), "exact34 selection")
    executed = re.findall(r"^test (\S+) \.\.\. (ok|FAILED)$", tests, re.M)
    require(len(executed) == 34 and sorted(n for n, _ in executed) == sorted(selected), "exact34 execution")
    failed = [n for n, result in executed if result == "FAILED"]
    require(failed == retained["failed"] and len(failed) == 1 and failed[0].endswith(
        "::event_client_sibling_cleanup_evidence_failure_cannot_hide_behind_actual_mock_ack"), "sole failed control")
    require(retained["whole_result"] in tests and "33 passed; 1 failed; 0 ignored;" in retained["whole_result"], "whole failed result")
    observations = [json.loads(line.split("event_client_inflight_budget_observation ", 1)[1]) for line in tests.splitlines()
                    if line.startswith("event_client_inflight_budget_observation ")]
    require(observations == load("actual-observations.json")["inflight"] and len(observations) == 6, "complete six observations")
    cleanup = next(o for o in observations if o["probe"] == "CleanupEvidence")
    require(cleanup["applications"] == 2 and cleanup["mock_acks"] == 1 and cleanup["cleanup_faults"] == 1, "reached seam")
    require(cleanup["result"].startswith("Err(RpcEvidenceFailure { may_have_dispatched: true,") and
            "unit observer refuses sibling cancellation record" not in cleanup["result"], "observed original-detail failure")
    records = cleanup["rpc_records"]
    ack = next(r for r in records if r["operation"] == "event_append" and r["phase"] == "validated_transport_result_ok")
    canceled = next(r for r in records if r["operation"] == "event_append" and r["phase"] == "partial_or_rejected_response_frame")
    require(ack["phase_unix_ns"] < canceled["phase_unix_ns"] and canceled["may_have_dispatched"], "ACK before sibling cancellation")
    inspected = load("decoded-frame-inspection.json")
    frames, total_records = [], 0
    for o in observations:
        for r in o["rpc_records"]:
            total_records += 1
            if r["raw_frame"] is None:
                continue
            data = bytes(r["raw_frame"])
            require(not any(marker.encode() in data for marker in inspected["credential_marker_list"]), "decoded credential literal")
            frames.append((o["probe"], r["attempt_id"], r["phase"], len(data), sha(data)))
    expected = [(r["probe"], r["attempt_id"], r["phase"], r["frame_bytes"], r["frame_sha256"]) for r in inspected["frames"]]
    require(frames == expected and total_records == 30 and len(frames) == 14 and tests.count('"raw_frame":') == total_records,
            "complete raw-frame accounting")
    root_receipt = load("root-independent-original-verification.json")
    require(root_receipt["sha256"] == ARTIFACTS[14091141775] and root_receipt["source"] == HEAD and
            root_receipt["selected"] == 34 and root_receipt["observations"] == observations, "independent original check")
    jobs = load("jobs-terminal.json")["jobs"]
    require(next(j for j in jobs if j["id"] == 132274164014)["conclusion"] == "success" and
            next(j for j in jobs if j["id"] == 132274186176)["conclusion"] == "failure", "job classifications")
    for name in ["core-job.log", "workspace-job.log"]:
        require(sha(gzip.decompress((ROOT / (name + ".gz")).read_bytes())) == retained["file_sha256"][name], "complete job log")
    require(retained["failure_stage"].startswith("first assertion") and not retained["guard_exit74_monitor_failure"] and
            not retained["guard_exit75_resource_interruption"], "failure stage vs resource classification")
    print("PASS: immutable FAILED34 (33/1), reached cleanup/detail defect, all four guards0; no HA qualification")


if __name__ == "__main__":
    verify()
