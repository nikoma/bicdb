#!/usr/bin/env python3
"""Offline immutable pair verification; no engine, network or third-party modules."""
import gzip
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parent
EXPECTED = {
    "b18": (43532705713, "560d8a873e65a7931226a0e38fc460f38c058ae6", 14088672023,
            "1f8ac7a8e42c98abb0be65ba128591daf1389579ee40ff0468ed46b72d6e1309", "failure", 132254119527, 132254084400),
    "505": (43532727697, "8ec10012cf7aa9f746bc503fca104de1e896f418", 14088981350,
            "31b2b0629ff60b41eb52767de37820a7676a178bbdfeb51c136cbeff461599ff", "success", 132254221204, 132254159409),
}
PREFIX = "distribution_transport::event_client_retry_tests::caller_query_budget_tests::"
NAMES = [PREFIX + "event_client_caller_query_budget_expired_refuses_before_tcp_dispatch",
         PREFIX + "event_client_caller_query_budget_live_scope_accepts_actual_mock_ack"]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def verify():
    manifest = read_json(ROOT / "manifest.json")
    actual = {str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file()
              and p.name != "manifest.json"}
    require(actual == set(manifest["files"]), "packet file set changed")
    for name, info in manifest["files"].items():
        data = (ROOT / name).read_bytes()
        require(len(data) == info["bytes"] and sha(data) == info["sha256"], "file hash: " + name)
    require(sha((ROOT / "source/caller_query_budget_tests.rs").read_bytes()) ==
            "a7e9f0b65a5ea9db0c3cc699ff30eaca282add7353001cc1cc6349e24432fd80", "fixture changed")
    require(sha((ROOT / "source/tests.yml").read_bytes()) ==
            "48c255ae145e842d8e6667e915a951ebf170190ec9617b303cb08372ca7f4c47", "workflow changed")
    pair = read_json(ROOT / "pair-terminal-verification.json")
    for label, (run_id, head, artifact_id, digest, conclusion, core_id, workspace_id) in EXPECTED.items():
        base = ROOT / label
        run = read_json(base / "run-terminal.json")
        request = read_json(base / "request.json")
        jobs = read_json(base / "jobs-terminal.json")["jobs"]
        retained = read_json(base / "artifact-verification.json")
        require(run["id"] == run_id and run["head_sha"] == head and
                run["status"] == "completed" and run["conclusion"] == conclusion, label + " run identity")
        require(request["head"] == head and request["dispatch_count"] == 1 and
                request["inputs"] == {"core_filter": "event_client_caller_query_budget_"}, label + " request")
        core = next(j for j in jobs if j["id"] == core_id)
        workspace = next(j for j in jobs if j["id"] == workspace_id)
        require(core["conclusion"] == conclusion, label + " Core result")
        require(workspace["conclusion"] == "success", label + " workspace")
        for name in ["cargo check --workspace --all-targets", "Workflow consumer compiles without Core defaults"]:
            require(next(s for s in workspace["steps"] if s["name"] == name)["conclusion"] == "success",
                    label + " " + name)
        require(next(s for s in core["steps"] if s["name"] == "Record source and list selected Core library tests")
                ["conclusion"] == "success", label + " selected list compile")
        require(next(s for s in core["steps"] if s["name"] == "Execute selected Core library tests")
                ["conclusion"] == conclusion, label + " execute status")
        artifact = next(a for a in read_json(base / "artifacts.json")["artifacts"] if a["id"] == artifact_id)
        raw = (base / "original-focused-core.zip").read_bytes()
        require(sha(raw) == digest and artifact["digest"] == "sha256:" + digest, label + " original ZIP")
        with zipfile.ZipFile(base / "original-focused-core.zip") as archive:
            require(set(archive.namelist()) == {"source-revision.txt", "rustc.txt", "selected-tests.log", "tests.log"},
                    label + " four text members")
            require(archive.read("source-revision.txt").decode().strip() == head, label + " artifact source")
            selected = sorted(l[:-6] for l in archive.read("selected-tests.log").decode().splitlines()
                              if l.endswith(": test"))
            require(selected == NAMES, label + " exact two selected names")
            tests = archive.read("tests.log").decode()
            observations = [json.loads(l.split("event_client_caller_query_budget_observation ", 1)[1])
                            for l in tests.splitlines() if l.startswith("event_client_caller_query_budget_observation ")]
            require(len(observations) == 2, label + " observation count")
            by_expiry = {o["expired_scope"]: o for o in observations}
            require(set(by_expiry) == {True, False}, label + " expiry controls")
            require(by_expiry[False] == {"accepted_tcp_connections": 1, "application_frames": 1,
                    "expired_scope": False, "result": "Ok(true)"}, label + " healthy ACK")
            expected = {"accepted_tcp_connections": 1, "application_frames": 1,
                        "expired_scope": True, "result": "Ok(true)"} if label == "b18" else {
                        "accepted_tcp_connections": 0, "application_frames": 0,
                        "expired_scope": True, "result": "Err(QueryTimedOut)"}
            require(by_expiry[True] == expected, label + " actual expired result/network counts")
            summary = next(l for l in tests.splitlines() if l.startswith("test result: "))
            required = "FAILED. 1 passed; 1 failed; 0 ignored;" if label == "b18" else "ok. 2 passed; 0 failed; 0 ignored;"
            require(required in summary, label + " whole result")
            require(len(re.findall(r"^test " + re.escape(PREFIX) + r"\S+ \.\.\. (?:ok|FAILED)$", tests, re.M)) == 2,
                    label + " two executed tests")
            require(retained["test_result"] == summary and retained["actual_observations_before_assertions"] == observations,
                    label + " original receipt")
            require(pair[label]["head"] == head and pair[label]["outcome"] == summary and
                    pair[label]["observations"] == observations, label + " pair receipt")
        for name in ["core-job.log", "workspace-job.log"]:
            require(sha(gzip.decompress((base / (name + ".gz")).read_bytes())) == retained["file_hashes"][name],
                    label + " full log bytes")
        patch = gzip.decompress((base / "test-only-source.patch.gz").read_bytes())
        require(patch.startswith(("commit " + head).encode()) and b"caller_query_budget_tests.rs" in patch,
                label + " test-only patch")
    require(pair["CI_retries"] == 0 and pair["HA_qualification"] is False and
            pair["root_cause_of_unrelated_original_first_APP68"] == "not established", "scope limits")
    print("PASS: identical paired tests; b18 confirmed expired-scope dispatch, synchronous505 refusal; no HA qualification")


if __name__ == "__main__":
    verify()
