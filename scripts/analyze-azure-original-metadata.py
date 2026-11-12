#!/usr/bin/env python3
"""Read original private archives; emit only allowlisted metadata observations."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def canonical(value):
    # Original serde envelopes preserve struct and ordered-map field order.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def analyze(archives, receipt):
    expected = {item["node"]: item["original_archive_sha256"] for item in receipt["nodes"]}
    rows = []
    for node in range(5):
        archive = archives / f"node-{node}.tar.gz"
        with archive.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        require(digest == expected[node], f"original archive digest mismatch: node {node}")
        matched = []
        with tarfile.open(archive, "r|gz") as saved:
            for entry in saved:
                if not entry.isfile() or not entry.name.endswith("/cluster-metadata-consensus.json"):
                    continue
                require(entry.size <= 32 * 1024 * 1024, "metadata member exceeds analysis bound")
                raw = saved.extractfile(entry).read()
                envelope = json.loads(raw)
                state = envelope["state"]
                require(hashlib.sha256(canonical(state)).hexdigest() == envelope["checksum_sha256"],
                        f"metadata checksum mismatch: node {node}")
                require(state["commit_index"] == state["base_index"], "unexpected uncompacted commit")
                topology = state["committed_topology"]
                matched.append({
                    "node": node, "archive_sha256": digest,
                    "member": entry.name, "metadata_sha256": hashlib.sha256(raw).hexdigest(),
                    "state_checksum_valid": True, "current_term": state["current_term"],
                    "base_index": state["base_index"], "base_term": state["base_term"],
                    "commit_index": state["commit_index"],
                    "topology_generation": topology["generation"],
                    "topology_sha256": hashlib.sha256(canonical(topology)).hexdigest(),
                    "role": state["role"], "leader_id": state["leader_id"],
                    "log_length": len(state["log"]),
                    "log_entries": [{"index": item["index"], "term": item["term"],
                                     "generation": item["topology"]["generation"],
                                     "barrier": item.get("leadership_barrier")}
                                    for item in state["log"]],
                })
        require(len(matched) == 1, f"expected exactly one metadata state: node {node}")
        rows.extend(matched)
    conflicts = []
    comparisons = []
    for index, left in enumerate(rows):
        for right in rows[index + 1:]:
            if left["commit_index"] <= 0 or left["commit_index"] != right["commit_index"]:
                continue
            agreement = (left["base_term"] == right["base_term"] and
                         left["topology_sha256"] == right["topology_sha256"])
            comparison = {"nodes": [left["node"], right["node"]],
                          "index": left["commit_index"], "agreement": agreement}
            comparisons.append(comparison)
            if not agreement:
                conflicts.append(comparison)
    return {
        "runtime_source": receipt["runtime_source"], "runtime_version": receipt["runtime_version"],
        "scope": "original pre-diagnostic sequential stopped archives; read-only metadata derivation",
        "archive_custody": "passed", "metadata_envelope_checksums": "passed",
        "rows": rows, "same_positive_committed_index_comparisons": comparisons,
        "same_positive_committed_index_conflicts": conflicts,
        "historical_causality": "inconclusive", "row_visibility": "not_tested",
        "quorum_signatures": "not_tested", "production_qualification": "not_tested",
        "limitations": ["non-simultaneous stopped captures",
                        "no intermediate or request-time metadata retained by this extraction",
                        "absence of final conflict cannot rule out an earlier transient false commit",
                        "different committed indices cannot be compared as identical snapshots",
                        "uncommitted suffix differences alone do not establish a safety violation"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", required=True, type=Path)
    parser.add_argument("--receipt", type=Path, default=Path(__file__).resolve().parents[1] /
                        "docs/evidence/azure-cold-catalog-reconciliation-2026-11-11/summary.json")
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", type=Path)
    destination.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = analyze(args.archives, json.loads(args.receipt.read_text()))
    if args.check:
        require(result == json.loads(args.check.read_text()), "derived metadata report differs")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Five archive digests and metadata checksums verified; "
          f"{len(result['same_positive_committed_index_comparisons'])} equal-index comparisons, "
          f"{len(result['same_positive_committed_index_conflicts'])} conflicts; causality inconclusive")


if __name__ == "__main__":
    main()
