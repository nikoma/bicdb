#!/usr/bin/env python3
"""Verify retained frozen463 handoff receipts without replaying the engine."""
import argparse
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def event(report, name):
    return next(row for row in report["events"] if row["cut"] == name)


def voters(descriptor):
    return {r["node_id"] for r in descriptor["replicas"] if r["role"] == "voter"}


def entry(observation, index):
    ranges = observation["catalog"]["catalog"]["ranges"]
    return next(iter(ranges.values()))["entries"][str(index)]


def topology_catalog(report):
    result = {}
    for row in report["metadata_exchanges"]:
        candidates = []
        if row["kind"] == "proposal":
            candidates.append(row["entry"]["topology"])
        if row["kind"] == "append":
            request = row["request"]
            if request.get("snapshot"):
                candidates.append(request["snapshot"]["topology"])
            candidates.extend(e["topology"] for e in request["entries"])
        for topology in candidates:
            generation = topology["generation"]
            if generation in result:
                assert canonical(result[generation]) == canonical(topology)
            result[generation] = topology
    return result


def validate(report):
    ack = next(e for e in report["events"] if e["cut"] == "acknowledged_commit" and e["key"] == "late")
    ready = event(report, "real_final_catchup_ready_before_late_ack")
    old = event(report, "late_ack_old_quorum_after_final_copy")
    promoted = event(report, "promoted_four_voters_only_two_available")
    final = event(report, "final_new_authority_observation")
    index = report["late_range_index"]
    expected = report["late_payload"]
    expected_hash = hashlib.sha256(canonical(expected).encode()).hexdigest()
    assert ack["payload"] == expected and ack["payload_sha256"] == expected_hash
    assert report["late_payload_sha256"] == expected_hash
    assert ready["unix_ns"] <= ack["invoked_unix_ns"] <= ack["acknowledged_unix_ns"] <= old["unix_ns"]
    assert report["source_db_commit_seq_after_late_ack"] > report["copied_source_commit_sequence_at_ready"]
    assert index > report["seed_range_index"]
    assert voters(old["nodes"][0]["committed_range"]) == {"n0", "n1", "n2"}
    commands = [entry(old["nodes"][i], index) for i in [0, 1]]
    assert all(e["state"] in ["applied", "quorum_committed"] for e in commands)
    assert commands[0]["command"] == commands[1]["command"]
    assert commands[0]["command"]["index"] == index
    assert commands[0]["command"]["mutations"][0]["record"]["metadata"] == expected
    assert old["nodes"][0]["late"] == old["nodes"][1]["late"] == expected
    assert voters(promoted["nodes"][3]["committed_range"]) == {"n0", "n1", "n2", "n3"}
    assert event(report, "promoted_four_voter_quorum_guard")["recovery"].startswith("Err(")
    assert promoted["nodes"][0]["metadata"]["commit_index"] < promoted["nodes"][3]["metadata"]["commit_index"]
    assert voters(final["nodes"][3]["committed_range"]) == {"n1", "n2", "n3"}
    assert not final["nodes"][1]["available_for_data_rpc"]
    assert all(final["nodes"][i]["available_for_data_rpc"] for i in [2, 3])
    assert final["nodes"][0]["late"] is None  # Actual source cleanup happened.
    assert final["nodes"][1]["late"] == expected  # A durable old copy remains.
    assert entry(final["nodes"][0], index)["command"] == commands[0]["command"]
    assert report["new_leader_recovery_succeeded"]
    assert event(report, "new_configuration_fresh_recovery")["recovery"].startswith("Ok(")
    # Reconstruct complete per-store topologies from retained actual metadata
    # entries/snapshots + each store's observed committed generation. No engine
    # state/counter is synthesized or rewritten. Range equality is cross-checked.
    catalog = topology_catalog(report)
    topology_rows = []
    for cut in [ready, old, promoted, final]:
        stores = []
        for observation in cut["nodes"]:
            status = observation["metadata"]
            assert set(status["voters"]) == {f"n{i}" for i in range(5)}
            topology = catalog[status["topology_generation"]]
            descriptor = next(r for r in topology["ranges"].values() if r["id"] == observation["committed_range"]["id"])
            assert descriptor == observation["committed_range"]
            stores.append({"node": observation["node"], "observed_status": status, "topology": topology})
        topology_rows.append({"cut": cut["cut"], "stores": stores})
    # A real promotion proposal was committed using replies from n3/n4 plus
    # leader n2 before source0 learned the committed transition.
    proposal_position = next(i for i, r in enumerate(report["metadata_exchanges"])
                             if r["kind"] == "proposal" and any(x["target"] == "n3" and x["phase"] == "promoted" for x in r["entry"]["topology"]["relocations"].values()))
    proposal = report["metadata_exchanges"][proposal_position]
    assert proposal["unix_ns"] > ack["acknowledged_unix_ns"]
    fresh = {"n2"}
    for row in report["metadata_exchanges"][proposal_position + 1:]:
        if row["kind"] != "append":
            continue
        response = row["response"]
        if response["success"] and response["match_index"] >= proposal["entry"]["index"]:
            fresh.add(response["node_id"])
        if row["leader_commit_advanced"]:
            break
    assert fresh == {"n2", "n3", "n4"}
    assert not report["irretrievable_data_loss_claimed"] and not report["azure_cause_proven"]
    return topology_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--require-preserved", action="store_true")
    parser.add_argument("--write-topologies", type=Path)
    args = parser.parse_args()
    control = json.loads((args.directory / "control.json").read_text())
    cleanup = json.loads((args.directory / "cleanup-first.json").read_text())
    topologies = {"derivation": "complete topologies reconstructed from actual retained metadata entries/snapshots and actual per-store committed-generation/status; direct observed range equality checked", "control": validate(control), "cleanup_first": validate(cleanup)}
    assert control["new_leader_local_value"] == control["late_payload"]
    final = event(cleanup, "final_new_authority_observation")
    assert cleanup["new_leader_local_value"] is None
    assert all(final["nodes"][i]["late"] is None and final["nodes"][i]["range_progress"]["resolved_through"] < cleanup["late_range_index"] for i in [2, 3])
    if args.write_topologies:
        args.write_topologies.write_text(json.dumps(topologies, separators=(",", ":")) + "\n")
    result = {"witness_validity": "PASS", "healthy_control": "PASS", "four_voter_quorum_guard": "PASS", "handoff_ack_visibility": "FAILED_CORRECTNESS", "old_node1_data_and_node0_catalog_retained": True, "irretrievable_data_loss_proven": False, "azure_cause_proven": False, "physical_or_production_qualification": False}
    print(json.dumps(result))
    if args.require_preserved:
        assert cleanup["new_leader_local_value"] == cleanup["late_payload"], "acknowledged value absent on ready new authority after legitimate cleanup-first handoff"


if __name__ == "__main__":
    main()
