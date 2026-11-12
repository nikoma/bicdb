#!/usr/bin/env python3
"""Verify retained CI bytes and recorded results; does not execute BicDB."""
import hashlib
import json
from pathlib import Path
import zipfile


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def main():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / "manifest.json").read_text())
    for case in manifest["cases"]:
        archive = root / case["archive"]
        require(hashlib.sha256(archive.read_bytes()).hexdigest() == case["artifact_sha256"],
                f"archive digest mismatch: {archive.name}")
        with zipfile.ZipFile(archive) as saved:
            files = {item.filename for item in saved.infolist() if not item.is_dir()}
            require(files == {item["path"] for item in case["files"]}, "archive membership differs")
            for item in case["files"]:
                raw = saved.read(item["path"])
                require(len(raw) == item["bytes"] and
                        hashlib.sha256(raw).hexdigest() == item["sha256"],
                        f"member digest mismatch: {item['path']}")
            require(saved.read("source-revision.txt").decode().strip() == case["source_revision"],
                    "source identity mismatch")
            results = [name for name in files if name.endswith("/bounded-prefix-result.json")]
            require(len(results) == 1, "expected one bounded-prefix report")
            directory = results[0].rsplit("/", 1)[0]
            result = json.loads(saved.read(results[0]))
            recovery = json.loads(saved.read(directory + "/majority-recovery-result.json"))
            require(result["covered_frontier"] == 1, "unexpected matched frontier")
            require(result["actual_ack"]["match_index"] == case["expected_ack"], "unexpected ACK")
            require(result["leader_advanced"] is case["expected_premature_commit"],
                    "unexpected leader commit decision")
            require(result["c_status"]["commit_index"] == case["expected_c_commit"],
                    "unexpected C commit index")
            require(recovery["reopen_verified"] is True, "missing strict-reopen result")
            envelope_count = 0
            for name in files:
                if not name.endswith(".json"):
                    continue
                document = json.loads(saved.read(name))
                if "checksum_sha256" not in document or "state" not in document:
                    continue
                # The captured envelopes preserve serde struct field order.
                encoded = json.dumps(document["state"], ensure_ascii=False,
                                     separators=(",", ":")).encode()
                require(hashlib.sha256(encoded).hexdigest() == document["checksum_sha256"],
                        f"state envelope checksum mismatch: {name}")
                envelope_count += 1
                if "/before-recovery-" in name and name.endswith("-c.json"):
                    require(document["state"]["commit_index"] == case["expected_c_commit"],
                            "C pre-recovery state differs from its report")
                if name.endswith("/cluster-metadata-consensus.json"):
                    require(document["state"]["commit_index"] == 4,
                            "final committed state is not index 4")
                    require(document["state"]["committed_topology"]["generation"] == 3,
                            "final topology generation differs")
            require(envelope_count == 6, "expected six real pre/post state envelopes")
            print(f"{case['label']}: custody verified; matched=1 ACK={case['expected_ack']} "
                  f"premature_commit={case['expected_premature_commit']}; six state checksums verified")


if __name__ == "__main__":
    main()
