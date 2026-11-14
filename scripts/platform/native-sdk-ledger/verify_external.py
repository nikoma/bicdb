#!/usr/bin/env python3
"""Read-only receipt/index checker. Integrity is not authenticity or HA proof.

Never narrows the goal's ACK set to wrapper-delivered ACKs. Incomplete evidence
retains every planned write as a possible target. Requires an independently
pinned snapshot index; never creates or repairs that index or original bytes.
"""
import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import collector as C
import verify as V


FILES = {"collector-manifest.json", "collector-receipts.jsonl", "controller-witnesses.jsonl",
         "journal.jsonl", "producer-receipts.jsonl", "storage-contract.json", *C.INPUT_LIMITS}


def lines(path, maximum, row_bound, count_bound, expected):
    total = 0
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for number in range(1, count_bound + 2):
            raw = file.readline(row_bound + 1)
            if not raw:
                V.require({"bytes": total, "sha256": digest.hexdigest()} == expected,
                          "actually consumed snapshot stream differs from pinned bytes/hash")
                return
            total += len(raw)
            digest.update(raw)
            V.require(total <= maximum and number <= count_bound and len(raw) <= row_bound
                      and raw.endswith(b"\n"), "receipt/witness truncated or byte/count bound")
            yield raw[:-1]


def hash_file(path, maximum):
    digest, size = hashlib.sha256(), 0
    V.require(not path.is_symlink() and path.is_file(), "snapshot file must be regular, not symlink")
    with path.open("rb") as file:
        while raw := file.read(65536):
            size += len(raw)
            V.require(size <= maximum, "snapshot file byte bound")
            digest.update(raw)
    return {"bytes": size, "sha256": digest.hexdigest()}


def check(index_path, root):
    root = Path(root)
    index = V.decode(V.bounded(index_path, C.MAX_HEADER))
    required = {"format", "run_id", "producer_id", "session_id", "source_sha", "binary_sha256",
                "member_node_id", "member_certificate_sha256", "collector_certificate_sha256",
                "ledger_client_certificate_sha256", "controller_certificate_sha256", "files", "completion"}
    V.require(set(index) == required and type(index["format"]) is int and index["format"] == 1,
              "external immutable index fields")
    V.require(all(V.valid_uuid(index[k]) for k in ("run_id", "producer_id", "session_id")), "index identity")
    V.require(all(V.HEX.fullmatch(index[k]) for k in ("binary_sha256", "member_certificate_sha256",
              "collector_certificate_sha256", "ledger_client_certificate_sha256", "controller_certificate_sha256")),
              "index fingerprints")
    V.require(len({index[k] for k in ("ledger_client_certificate_sha256", "controller_certificate_sha256",
                                    "collector_certificate_sha256", "member_certificate_sha256")}) == 4,
              "separate instrumentation identities")
    V.require(isinstance(index["source_sha"], str) and C.re.fullmatch(r"[0-9a-f]{40}", index["source_sha"])
              and isinstance(index["member_node_id"], str) and 0 < len(index["member_node_id"]) <= 4096,
              "source/member index types")
    completion = index["completion"]
    wanted = FILES | ({"completion-witness.json"} if completion is not None else set())
    V.require(set(index["files"]) == wanted, "all original snapshot files pinned")
    consumed = {}
    for name in sorted(wanted):
        maximum = C.MAX_TOTAL if name == "journal.jsonl" else (
            C.INPUT_LIMITS.get(name) or (64 * 1024**2 if name.endswith(".jsonl") else C.MAX_HEADER))
        actual = hash_file(root / name, maximum)
        V.require(index["files"][name] == actual, "immutable snapshot file hash/size: " + name)
        consumed[name] = actual
    def read_pinned(name, maximum):
        raw = V.bounded(root / name, maximum)
        V.require({"bytes": len(raw), "sha256": C.sha(raw)} == consumed[name],
                  "actually consumed JSON input differs from pinned bytes/hash: " + name)
        return raw
    manifest = V.decode(read_pinned("collector-manifest.json", C.MAX_HEADER))
    V.require(type(manifest.get("format")) is int and manifest["format"] == 1
              and manifest.get("qualification_status") == "not_qualified"
              and manifest.get("throughput_benchmark") is False
              and set(manifest["inputs"]) == set(C.INPUT_LIMITS), "original collector manifest scope")
    storage = V.decode(read_pinned("storage-contract.json", C.MAX_HEADER))
    V.require(storage.get("filesystem") in {"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs"}
              and storage.get("mount_preflight") == "local_filesystem_type_only"
              and storage.get("actual_backing_storage_and_flush_behavior_qualified") is False,
              "collector local-storage preflight missing; no flush/power-loss qualification")
    for key in ("run_id", "producer_id", "session_id", "source_sha", "binary_sha256", "member_node_id",
                "member_certificate_sha256", "collector_certificate_sha256", "ledger_client_certificate_sha256"):
        V.require(manifest[key] == index[key], "manifest/index identity: " + key)
    for name in C.INPUT_LIMITS:
        V.require(manifest["inputs"][name] == consumed[name], "exact original input: " + name)
    ledger_config = V.decode(read_pinned("ledger-config.json", C.MAX_HEADER))
    V.require(ledger_config.get("producer_id") == index["producer_id"]
              and ledger_config.get("session_id") == index["session_id"]
              and ledger_config.get("server_certificate_sha256") == index["collector_certificate_sha256"]
              and ledger_config.get("key_free_inputs_reviewed") is True, "actual original ledger configuration")
    receipts, row_receipts, witness_receipts = {}, {}, {}
    offset, previous, next_row = 0, None, 1
    input_receipts, started = set(), False
    for raw in lines(root / "collector-receipts.jsonl", 64 * 1024**2, C.MAX_HEADER, 70500,
                     consumed["collector-receipts.jsonl"]):
        r = V.decode(raw)
        number = len(receipts) + 1
        action = r.get("action")
        extras = {"start": set(), "input": {"name"}, "row": {
                  "sequence", "record_sha256", "previous_record_sha256", "durable_end_offset"},
                  "witness": {"witness_sequence"}}.get(action)
        V.require(extras is not None and set(r) == C.COMMON | extras | {
                  "collector_certificate_sha256", "client_certificate_sha256", "receipt_sequence"},
                  "collector receipt exact fields")
        V.require(r["receipt_sequence"] == number and type(r["receipt_sequence"]) is int
                  and type(r["format"]) is int and r["format"] == 1
                  and all(r[k] == index[k] for k in ("run_id", "producer_id", "session_id"))
                  and r["collector_certificate_sha256"] == index["collector_certificate_sha256"]
                  and type(r["payload_bytes"]) is int and 0 < r["payload_bytes"] <= C.MAX_PAYLOAD
                  and V.HEX.fullmatch(r["payload_sha256"]), "receipt identity/order/bounds")
        V.require(r["client_certificate_sha256"] == index[
                  "controller_certificate_sha256" if action == "witness" else "ledger_client_certificate_sha256"],
                  "actual pinned peer attribution")
        if action == "start":
            V.require(not started and (r["payload_bytes"], r["payload_sha256"]) == (
                consumed["collector-manifest.json"]["bytes"], consumed["collector-manifest.json"]["sha256"]),
                "start receipt exact original manifest")
            started = True
        elif action == "input":
            name = r["name"]
            V.require(started and name in C.INPUT_LIMITS and name not in input_receipts
                      and (r["payload_bytes"], r["payload_sha256"]) == (
                          consumed[name]["bytes"], consumed[name]["sha256"]), "exact input receipt order")
            input_receipts.add(name)
        elif action == "row":
            V.require(started and input_receipts == set(C.INPUT_LIMITS) and r["sequence"] == next_row
                      and type(r["sequence"]) is int and r["previous_record_sha256"] == previous
                      and V.HEX.fullmatch(r["record_sha256"])
                      and r["durable_end_offset"] == offset + r["payload_bytes"], "exact row receipt end offset")
            row_receipts[next_row] = r
            next_row += 1
            offset, previous = r["durable_end_offset"], r["record_sha256"]
        else:
            n = r["witness_sequence"]
            V.require(type(n) is int and n == len(witness_receipts) + 1, "witness receipt order")
            witness_receipts[n] = r
        receipts[number] = raw
    V.require(started and input_receipts == set(C.INPUT_LIMITS), "missing external original input receipt")
    producer_observed, previous_receipt = set(), 0
    for raw in lines(root / "producer-receipts.jsonl", 64 * 1024**2, C.MAX_HEADER, 70500,
                     consumed["producer-receipts.jsonl"]):
        r = V.decode(raw)
        n = r.get("receipt_sequence")
        V.require(type(n) is int and n > previous_receipt and receipts.get(n) == raw
                  and r["action"] != "witness", "producer receipt differs/reorders original collector response")
        producer_observed.add(n)
        previous_receipt = n
    rows, actual_offset = 0, 0
    for raw in lines(root / "journal.jsonl", C.MAX_TOTAL, V.MAX_LINE, V.MAX_ROWS, consumed["journal.jsonl"]):
        rows += 1
        if rows == 1:
            started_row = V.decode(raw)
            description = started_row.get("data", {}).get("external_sink", {})
            V.require(started_row.get("kind") == "run_started"
                      and description.get("mode") == "external_fsync_required"
                      and all(description.get(k) == index[k] for k in ("run_id", "producer_id", "session_id"))
                      and description.get("collector_certificate_sha256") == index["collector_certificate_sha256"]
                      and description.get("ledger_client_certificate_sha256") == index["ledger_client_certificate_sha256"]
                      and description.get("io_timeout_ms") == ledger_config.get("io_timeout_ms")
                      and description.get("throughput_benchmark") is False, "actual external sink header binding")
        line = raw + b"\n"
        actual_offset += len(line)
        r = row_receipts.get(rows)
        V.require(r is not None and r["payload_sha256"] == C.sha(line)
                  and r["record_sha256"] == C.sha(raw) and r["payload_bytes"] == len(line)
                  and r["durable_end_offset"] == actual_offset, "actual journal bytes/receipt offset mismatch")
    V.require(rows == len(row_receipts) and offset == actual_offset, "missing receipt/journal tail")
    witnesses = {}
    for raw in lines(root / "controller-witnesses.jsonl", 64 * 1024**2, 4 * C.MAX_HEADER, 256,
                     consumed["controller-witnesses.jsonl"]):
        w = V.decode(raw)
        n = len(witnesses) + 1
        original = bytes.fromhex(w["original_witness_hex"])
        r = witness_receipts.get(n)
        V.require(w["sequence"] == n and r is not None and r["payload_sha256"] == C.sha(original)
                  and r["payload_bytes"] == len(original)
                  and w["controller_certificate_sha256"] == index["controller_certificate_sha256"],
                  "original controller witness bytes/peer/receipt")
        witnesses[n] = V.decode(original)
    V.require(len(witnesses) == len(witness_receipts), "missing independent witness tail")
    completion_path, exit_status = None, None
    if completion is not None:
        V.require(set(completion) == {"controller_witness_sequence", "command_sha256", "exit_status"},
                  "completion binding fields")
        w = witnesses.get(completion["controller_witness_sequence"], {})
        completion_raw = read_pinned("completion-witness.json", C.MAX_HEADER)
        V.require(w.get("kind") == "process_exit" and w.get("output_complete") is True
                  and w.get("timed_out") is False and w.get("command_sha256") == completion["command_sha256"]
                  and C.sha(C.encode(w["observed_command"])) == completion["command_sha256"]
                  and type(completion["exit_status"]) is int and type(w.get("exit_status")) is int
                  and w.get("exit_status") == completion["exit_status"]
                  and bytes.fromhex(w["stdout_hex"]) == completion_raw, "actual observed command completion binding")
        completion_path, exit_status = root / "completion-witness.json", completion["exit_status"]
    report = V.verify(root / "plan.json", root / "journal.jsonl", completion_path, exit_status)
    V.require(report["journal_input"] == {"bytes_read": consumed["journal.jsonl"]["bytes"],
              "read_bytes_sha256": consumed["journal.jsonl"]["sha256"], "within_total_bound": True},
              "verifier consumed actual pinned journal")
    claims = report["run_manifest_claims"]
    V.require(claims.get("plan_sha256") == consumed["plan.json"]["sha256"]
              and claims.get("build_source_claim") == index["source_sha"]
              and claims.get("client_binary_sha256") == index["binary_sha256"]
              and claims.get("caller_member_identity") == index["member_node_id"], "actual consumed plan/source/member")
    for name in ("config.json", "topology.json"):
        V.require(report["archived_inputs"].get(name, {}).get("sha256") == consumed[name]["sha256"],
                  "SDK verifier consumed actual pinned original input: " + name)
    plan = V.decode(read_pinned("plan.json", C.INPUT_LIMITS["plan.json"]))
    writes = [c["logical_id"] for c in plan["operations"] if c["operation"] == "append"]
    not_observed = [r["sequence"] for r in row_receipts.values()
                    if r["receipt_sequence"] not in producer_observed]
    missing_delivery = [number for number, raw in receipts.items()
                        if V.decode(raw)["action"] != "witness" and number not in producer_observed]
    return {"format": 1, "qualification_status": "not_qualified", "snapshot_integrity": "verified",
            "consumed_files": consumed, "sdk_evidence": report,
            "collector_storage_preflight": storage,
            "collector_row_receipts": rows, "producer_unproven_receipt_delivery_rows": not_observed,
            "producer_unproven_receipt_delivery_sequences": missing_delivery,
            "controller_witnesses": witnesses,
            "external_receipt_chain_complete": not missing_delivery,
            "functional_evidence_complete": bool(writes) and not missing_delivery and report["complete_evidence"],
            "required_reconciliation_original_write_ids": writes,
            "known_sdk_ok_write_ids": sorted(report["visible_sdk_acks"]),
            "possible_success_raw_replies": report["raw_database_success_replies"],
            "all_attempts_and_uncertainties_covered": report["complete_evidence"],
            "wrapper_ack_set_never_narrows_goal_ack_corpus": True,
            "missing_explicit_ack_record_proves_no_sdk_ok": False,
            "independent_witness_scope": "authenticated controller observed local child; remote process exit/fault effect needs independent confirmation",
            "source_binary_and_witness_authenticity_independently_proven": False,
            "all_required_values_reconciled": False,
            "throughput_RPO_RTO_physical_or_production_qualified": False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--index", required=True, type=Path)
    p.add_argument("--snapshot", required=True, type=Path)
    a = p.parse_args()
    try:
        report = check(a.index, a.snapshot)
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError, RecursionError) as error:
        print(json.dumps({"qualification_status": "not_qualified", "snapshot_integrity": "invalid_or_incomplete",
                          "functional_evidence_complete": False, "error": str(error)[:512]}))
        return 2
    print(json.dumps(report, sort_keys=True, default=lambda value:
                     {"json_decimal": str(value)} if isinstance(value, Decimal)
                     else (_ for _ in ()).throw(TypeError("unknown report type"))))
    return 0 if report["functional_evidence_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
