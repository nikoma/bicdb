#!/usr/bin/env python3
"""Check retained job identity, complete logs and synthetic fault artifacts.

Integrity verification does not rerun the engine or qualify any release.
"""
import gzip
import hashlib
import importlib.util
import json
import pathlib
import re
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent


def require(condition, message):
    if not condition:
        raise ValueError(message)


def relative(name):
    path = pathlib.PurePosixPath(name)
    require(not path.is_absolute() and ".." not in path.parts, "unsafe path")
    return ROOT.joinpath(*path.parts)


def load_helper(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), ROOT / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    require(manifest["format"] == 1, "unknown manifest format")
    require(manifest["qualification"] == "NOT QUALIFIED", "qualification changed")
    require(manifest["production_gate_changed"] is False, "admission changed")
    require(manifest["physical_four_to_five_tested"] is False, "physical claim")
    require(manifest["throughput_gain_tested"] is False, "capacity claim")
    files = {entry["path"]: entry for entry in manifest["files"]}
    require(len(files) == len(manifest["files"]), "duplicate artifact path")
    for name, entry in files.items():
        raw = relative(name).read_bytes()
        require(len(raw) == entry["bytes"], f"size differs: {name}")
        require(hashlib.sha256(raw).hexdigest() == entry["sha256"],
                f"checksum differs: {name}")
    counts = []
    for job in manifest["jobs"]:
        require(all(job[key] in files for key in ("index", "metadata", "log")),
                "unlisted job evidence")
        index = json.loads(relative(job["index"]).read_text())
        metadata = json.loads(relative(job["metadata"]).read_text())
        require(metadata["head_sha"] == index["source_revision"] == job["source"],
                "job source differs")
        require(metadata["id"] == index["job_id"] == job["job_id"],
                "job identity differs")
        require(metadata["run_id"] == index["run_id"] == job["run_id"],
                "run identity differs")
        require(metadata["status"] == index["job_status"] == "completed",
                "nonterminal job")
        require(metadata["conclusion"] == index["job_conclusion"] == job["conclusion"],
                "job conclusion differs")
        compressed = relative(job["log"]).read_bytes()
        require(hashlib.sha256(compressed).hexdigest() == index["compressed_sha256"],
                "compressed log differs")
        raw = gzip.decompress(compressed)
        require(len(raw) == index["raw_bytes"], "incomplete raw log")
        require(hashlib.sha256(raw).hexdigest() == index["raw_sha256"],
                "raw log differs")
        text = re.sub(r"\x1b\[[0-9;]*m", "", raw.decode())
        summaries = [line for line in text.splitlines() if re.match(
            r"^\S+Z test result: (?:ok|FAILED)\. \d+ passed;", line)]
        require(summaries == index["test_summaries"] == job["test_summaries"],
                "test summary differs")
        observations = re.findall(r"^\S+Z test (.+) \.\.\. (ok|FAILED|ignored[^\n]*)$",
                                  text, re.MULTILINE)
        expected = [(entry["test"], entry["result"])
                    for entry in index["test_observations"]]
        require(observations == expected, "named test outcomes differ")
        if job["label"].startswith("remote-old-target-"):
            require(not summaries and "error[E0609]" in text,
                    "compile failure relabeled as runtime evidence")
        if job["source"].startswith("156a586b"):
            require(not summaries and "error[E0502]" in text,
                    "activation compile failure relabeled as runtime evidence")
        counts.append((job["label"], summaries or ["NO TEST RESULT: compile failure"]))
    projector = load_helper("project-native-remote-evidence.py")
    for artifact in manifest["artifacts"]:
        require(artifact["path"] in files and artifact["database_custody"] is False,
                "artifact custody changed")
        with zipfile.ZipFile(relative(artifact["path"])) as archive:
            require(archive.testzip() is None, "damaged ZIP")
            require(archive.read("source-revision.txt").decode().strip()
                    == artifact["source"], "artifact source differs")
            for entry in archive.infolist():
                path = pathlib.PurePosixPath(entry.filename)
                require(not path.is_absolute() and ".." not in path.parts,
                        "unsafe ZIP member")
                require(entry.file_size <= 64 * 1024 * 1024, "oversized ZIP member")
                require(path.suffix in {".txt", ".log", ".json", ".jsonl"},
                        "unexpected artifact type; inspect before publication")
            if artifact.get("kind") == "public_projection":
                require(artifact["projection_manifest"] in files, "unlisted projection manifest")
                projection = json.loads(relative(artifact["projection_manifest"]).read_text())
                require(projection["source_commit"] == artifact["source"], "projection source")
                require(projection["original_zip_sha256"] == artifact["original_zip_sha256"],
                        "original archive hash differs")
                require(projection["projection_fixture_db_custody_present"] is False,
                        "public projection contains fixture database custody")
                require(projection["projection_zip_sha256"] == files[artifact["path"]]["sha256"],
                        "projection archive hash differs")
                included = {entry["path"]: entry for entry in projection["included_members"]}
                omitted = {entry["path"] for entry in projection["omitted_members"]}
                require(not set(included).intersection(omitted), "included/omitted overlap")
                require(set(archive.namelist()) == set(included), "projected member set differs")
                for name, entry in included.items():
                    require(projector.allowed(name) and "/custody/" not in name,
                            "projected member is outside logical whitelist")
                    body = archive.read(name)
                    require(len(body) == entry["bytes"]
                            and hashlib.sha256(body).hexdigest() == entry["sha256"],
                            "projected logical member differs")
    origin = load_helper("verify-native-origin-evidence.py")
    remote = load_helper("verify-native-remote-projection.py")
    for artifact in manifest["artifacts"]:
        path = relative(artifact["path"])
        digest = files[artifact["path"]]["sha256"]
        parser = artifact.get("parser", "origin-journals")
        if parser == "origin-journals":
            report = origin.verify(path, artifact["source"], digest)
            require(report["recorded_successful_strong_violations"] == 0,
                    "recorded Strong violation differs from stated finite observations")
            print(artifact["source"][:12] + ": independently checked "
                  + str(report["journal_count"]) + " origin journals; original result "
                  + " | ".join(report["test_execution_summaries"]))
        elif parser in {"registered-image", "test-results"}:
            count = 10 if parser == "registered-image" else 2
            report = remote.verify(path, digest, artifact["source"], count)
            print(artifact["source"][:12] + ": independently checked "
                  + str(report["case_count"]) + " remote logical case directories; "
                  + " | ".join(report["test_summaries"]))
    for label, summaries in counts:
        print(label + ": " + " | ".join(summaries))
    projections = sum(a.get("kind") == "public_projection" for a in manifest["artifacts"])
    print(f"Verified {len(files)} files, {len(counts)} jobs, "
          f"{len(manifest['artifacts']) - projections} complete synthetic archives "
          f"and {projections} public projections. "
          "No engine replay or qualification performed.")


if __name__ == "__main__":
    verify()
