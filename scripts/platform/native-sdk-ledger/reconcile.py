#!/usr/bin/env python3
"""Bounded read-only reconciliation plans and exact cross-batch coverage.

This consumes format-1 stock-probe artifacts; it never contacts BicDB. Source
and restore provenance remain external facts. No write/retry plan is emitted.
"""
import argparse
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import verify as V

MAX_SOURCES = 16
MAX_TOTAL_JOURNALS = 2 * 1024**3
MAX_TOTAL_PLANS = 64 * 1024**2
MAX_TARGETS = 8192
MAX_PAYLOAD = 32768
MAX_PAYLOADS = 64 * 1024**2
MAX_OUTPUT = 128 * 1024**2
NAMESPACE = uuid.UUID("47d73bea-53a4-4659-855b-80021fedccae")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_file(path, bound):
    count, state = 0, hashlib.sha256()
    with path.open("rb") as source:
        while True:
            data = source.read(min(65536, bound - count + 1))
            if not data:
                break
            count += len(data)
            V.require(count <= bound, "input journal byte budget exceeded")
            state.update(data)
    return state.hexdigest(), count


def path_from(base, value):
    V.require(isinstance(value, str) and 0 < len(value.encode()) <= 4096, "bounded input path required")
    path = Path(value)
    return path if path.is_absolute() else base / path


def identity(value):
    V.require(isinstance(value, dict) and set(value) == {"build_source_claim", "client_binary_sha256"},
              "explicit source/binary binding required")
    V.require(isinstance(value["build_source_claim"], str)
              and __import__("re").fullmatch("[0-9a-fA-F]{40}", value["build_source_claim"])
              and isinstance(value["client_binary_sha256"], str)
              and V.HEX.fullmatch(value["client_binary_sha256"]), "invalid source/binary identity")


def check_inputs(directory, expected_cluster, expected_bootstrap):
    raw_config = V.bounded(directory / "config.json", 1024**2)
    raw_topology = V.bounded(directory / "topology.json", 16 * 1024**2)
    config, topology = V.decode(raw_config), V.decode(raw_topology)
    for actual in (config, topology):
        V.require(actual.get("cluster_id") == expected_cluster, "cross-cluster source refused")
        bootstrap = actual.get("range_image_bootstrap_id")
        V.require(V.valid_uuid(bootstrap) and uuid.UUID(bootstrap) == uuid.UUID(expected_bootstrap),
                  "missing/different bootstrap refused")
    V.require(config.get("transport", {}).get("dev_localhost_plaintext") is False,
              "explicit authenticated TLS configuration required")
    return {"config_sha256": V.digest(raw_config), "topology_sha256": V.digest(raw_topology)}


def match_client(report, expected):
    actual = report["run_manifest_claims"]
    if not actual:
        return "unknown: source run_started unavailable"
    for key, value in expected.items():
        V.require(actual.get(key) == value, "recorded source/binary disagrees with pinned identity")
    return "matches recorded claims; build authenticity external"


def require_consumed_journal(report, expected_hash, expected_size, label):
    consumed = report.get("journal_input", {})
    V.require(type(consumed.get("bytes_read")) is int and consumed["bytes_read"] == expected_size
              and consumed.get("read_bytes_sha256") == expected_hash
              and consumed.get("within_total_bound") is True,
              f"verifier consumed a different or incomplete {label} journal")


def require_consumed_plan(report, expected_hash, label):
    claims = report.get("run_manifest_claims", {})
    if not claims:
        # A rejected/missing header cannot bind a plan or establish completeness.
        # Keep its original planned writes as possible targets, never drop them.
        V.require(report.get("complete_evidence") is False,
                  f"complete {label} evidence lacks a verified plan binding")
        return
    V.require(claims.get("plan_sha256") == expected_hash,
              f"verifier consumed a different {label} plan")


def plan_values(raw):
    """One pass over operations; preserve each exact value span, no Decimal dump."""
    operations_raw = V.raw_at(raw, ["operations"])
    text = operations_raw.decode()
    decoder = json.JSONDecoder(object_pairs_hook=V.pairs)
    at, values = 1, {}
    while at < len(text):
        while at < len(text) and text[at] in " \n\t\r,":
            at += 1
        if at == len(text) or text[at] == "]":
            break
        _, end = decoder.raw_decode(text, at)
        command_raw = text[at:end].encode()
        command = V.decode(command_raw)
        if command["operation"] == "append":
            values[command["logical_id"]] = V.raw_at(command_raw, ["value"])
        at = end
    return values


def prefix_rows(directory, report):
    """Logical raw-line hashes and canonical observed value spans only."""
    values, logical, previous = {}, {}, None
    with (directory / "journal.jsonl").open("rb") as source:
        for number in range(1, report["valid_records"] + 1):
            line = source.readline(V.MAX_LINE + 1)
            V.require(line.endswith(b"\n") and len(line) <= V.MAX_LINE, "source changed during prefix read")
            V.require(V.decode(V.raw_at(line[:-1], ["previous_record_sha256"])) == previous,
                      "source prefix chain changed during inspection")
            previous = V.digest(line[:-1])
            kind = V.decode(V.raw_at(line[:-1], ["kind"]))
            if kind not in {"logical_attempt", "sdk_ack_observed", "logical_terminal"}:
                continue
            row = V.decode(line)
            data = row["data"]
            name = data["command"]["logical_id"] if kind == "logical_attempt" else data["logical_id"]
            logical.setdefault(name, {})[kind] = {"sequence": number, "raw_record_sha256": V.digest(line[:-1])}
            if kind == "logical_attempt" and data["command"]["operation"] == "append":
                values[name] = V.raw_at(line[:-1], ["data", "command", "value"])
    V.require(previous == report["last_valid_record_sha256"], "source prefix changed after verification")
    return values, logical


def load_spec(index):
    raw = V.bounded(index, 1024**2)
    spec = V.decode(raw)
    V.require(isinstance(spec, dict) and set(spec) == {
        "format", "cluster_id", "bootstrap_id", "read_client", "sources"}, "reconciliation index fields")
    V.require(spec["format"] == 1 and isinstance(spec["cluster_id"], str) and spec["cluster_id"]
              and V.valid_uuid(spec["bootstrap_id"]), "cluster/bootstrap binding required")
    V.require(isinstance(spec["sources"], list) and 1 <= len(spec["sources"]) <= MAX_SOURCES,
              "source count must be1..16")
    read = spec["read_client"]
    V.require(isinstance(read, dict) and set(read) == {"identity", "directory", "config_sha256", "topology_sha256"},
              "read client identity and input archives required")
    identity(read["identity"])
    input_hashes = check_inputs(path_from(index.parent, read["directory"]), spec["cluster_id"], spec["bootstrap_id"])
    V.require(all(input_hashes[k] == read[k] for k in input_hashes), "read input hash mismatch")
    return raw, spec


def derive(index):
    index = Path(index)
    index_raw, spec = load_spec(index)
    bundle = V.digest(index_raw)
    sources, writes, targets, payloads = [], [], {}, {}
    total_journals = total_plans = payload_bytes = 0
    run_ids = set()
    for ordinal, source in enumerate(spec["sources"]):
        V.require(isinstance(source, dict) and set(source) == {
            "directory", "identity", "plan_sha256", "journal_sha256", "completion_witness",
            "completion_witness_sha256", "exit_status"}, "source index fields")
        identity(source["identity"])
        directory = path_from(index.parent, source["directory"])
        inputs = check_inputs(directory, spec["cluster_id"], spec["bootstrap_id"])
        raw_plan = V.bounded(directory / "plan.json", V.MAX_PLAN)
        total_plans += len(raw_plan)
        V.require(total_plans <= MAX_TOTAL_PLANS and V.digest(raw_plan) == source["plan_sha256"],
                  "source plan hash/aggregate byte bound")
        journal_hash, size = sha_file(directory / "journal.jsonl", MAX_TOTAL_JOURNALS - total_journals)
        total_journals += size
        V.require(journal_hash == source["journal_sha256"], "source journal hash mismatch")
        witness = None
        if source["completion_witness"] is not None:
            witness = path_from(index.parent, source["completion_witness"])
            V.require(V.digest(V.bounded(witness, 1024**2)) == source["completion_witness_sha256"],
                      "source completion witness hash mismatch")
        else:
            V.require(source["completion_witness_sha256"] is None and source["exit_status"] is None,
                      "unlinked completion/exit evidence")
        report = V.verify(directory / "plan.json", directory / "journal.jsonl", witness, source["exit_status"])
        require_consumed_journal(report, journal_hash, size, "source")
        require_consumed_plan(report, source["plan_sha256"], "source")
        binding = match_client(report, source["identity"])
        plan = V.decode(raw_plan)
        run = str(uuid.UUID(plan["run_id"]))
        V.require(run not in run_ids, "duplicate run UUID across sources; do not collapse histories")
        run_ids.add(run)
        observed_values, rows = prefix_rows(directory, report)
        final_hash, final_size = sha_file(directory / "journal.jsonl", size)
        V.require(final_hash == journal_hash and final_size == size, "source journal changed during inspection")
        original_values = plan_values(raw_plan)
        summaries = {"source_ordinal": ordinal, "run_id": run, "identity": source["identity"],
                     "plan_sha256": source["plan_sha256"], "journal_sha256": journal_hash,
                     "completion_witness_sha256": source["completion_witness_sha256"],
                     "exit_status": source["exit_status"], "archived_inputs": inputs, "client_binding": binding,
                     "complete_source_evidence": report["complete_evidence"], "integrity": report["integrity"],
                     "defects": report["defects"], "evidence_gaps": report["evidence_gaps"],
                     "untrusted_tail_logical_observations": report["untrusted_tail_logical_observations"],
                     "untrusted_tail_omitted": report["untrusted_tail_observations_omitted_at_explicit_cap"]}
        sources.append(summaries)
        for command in plan["operations"]:
            if command["operation"] != "append":
                continue
            name = command["logical_id"]
            source_id = f"{run}/{name}"
            attempted = name in report["attempted"]
            unattempted = report["complete_evidence"] and name in report["unattempted"]
            raw_value = observed_values.get(name, original_values[name])
            V.require(len(raw_value) <= MAX_PAYLOAD, "raw lexical value exceeds32KiB; no partial output")
            payload_hash = V.digest(raw_value)
            record = {"source_identity": source_id, "source_ordinal": ordinal, "logical_id": name,
                      "namespace": command["namespace"], "key": command["key"],
                      "canonical_record_id": command["namespace"] + "/" + command["key"],
                      "original_plan_value_raw_sha256": V.digest(original_values[name]),
                      "expected_value_raw_sha256": payload_hash,
                      "expected_value_span_origin": "trusted logical_attempt" if name in observed_values else "original plan",
                      "actual_attempt_observed": attempted,
                      "confirmed_unattempted": unattempted,
                      "possibly_attempted": not unattempted,
                      "visible_sdk_ack": name in report["visible_sdk_acks"],
                      "ack_ledger_fsync_return_linked": name in report["fsync_return_linked_sdk_acks"],
                      "ack_ledger_fsync_return_unproven": name in report["fsync_return_unproven_sdk_acks"],
                      "failed": name in report["failed"], "unfinished": name in report["unfinished"],
                      "not_observed_in_valid_prefix": not attempted,
                      "original_write_error_unresolved": name in report["unresolved_writes"],
                      "terminal_outcome": report["terminal_outcomes"].get(name, {}).get("outcome"),
                      "logical_raw_records": rows.get(name, {}),
                      "sdk_attempts": [rpc for rpc in report["sdk_rpc_attempts"] if rpc["logical_id"] == name],
                      "target_id": None}
            if not unattempted:
                key = (command["namespace"], command["key"], payload_hash)
                target_id = V.digest(encoded(key))
                if target_id not in targets:
                    V.require(len(targets) < MAX_TARGETS, "target cap exceeded; no subset output")
                    payload_bytes += len(raw_value)
                    V.require(payload_bytes <= MAX_PAYLOADS, "total lexical payload byte cap exceeded")
                    targets[target_id] = {"target_id": target_id, "namespace": key[0], "key": key[1],
                                          "canonical_record_id": key[0] + "/" + key[1],
                                          "expected_value_raw_sha256": payload_hash,
                                          "comparison_digest": V.typed_digest(V.decode(raw_value)),
                                          "source_identities": [], "has_linked_source_ack": False,
                                          "has_visible_source_ack": False}
                    payloads[target_id] = raw_value
                targets[target_id]["source_identities"].append(source_id)
                targets[target_id]["has_linked_source_ack"] |= record["ack_ledger_fsync_return_linked"]
                targets[target_id]["has_visible_source_ack"] |= record["visible_sdk_ack"]
                record["target_id"] = target_id
            writes.append(record)
    ordered = sorted(targets.values(), key=lambda target: (target["namespace"], target["key"], target["target_id"]))
    conflicts = []
    by_key = {}
    for target in ordered:
        by_key.setdefault(target["canonical_record_id"], []).append(target)
    for key, group in by_key.items():
        if len(group) > 1:
            conflicts.append({"canonical_record_id": key, "target_ids": [t["target_id"] for t in group],
                              "classification": "conflicting_payload_histories" if len({t["comparison_digest"] for t in group}) > 1
                              else "different_exact_lexical_encodings_inconclusive",
                              "acknowledged_target_ids": [t["target_id"] for t in group if t["has_linked_source_ack"]]})
    files, batches = {}, []
    for at in range(0, len(ordered), 1024):
        number = at // 1024
        run_id = str(uuid.uuid5(NAMESPACE, f"{bundle}/{number}"))
        phase = f"reconciliation-{bundle[:16]}-batch-{number + 1}"
        commands = []
        batch_targets = ordered[at:at + 1024]
        for target in batch_targets:
            logical_id = "strong-" + target["target_id"]
            fields = {"operation": "strong_read", "logical_id": logical_id,
                      "namespace": target["namespace"], "key": target["key"]}
            command = encoded(fields)[:-1] + b',"expected":' + payloads[target["target_id"]] + b',"acknowledged_by":null}'
            commands.append(command)
            target["batch"] = number
            target["read_logical_id"] = logical_id
        raw = b'{"run_id":' + encoded(run_id) + b',"phase":' + encoded(phase) + b',"operations":[' + b','.join(commands) + b']}'
        V.require(len(raw) <= V.MAX_PLAN, "read plan40MiB cap exceeded; no subset output")
        V.validate_plan(V.decode(raw))
        filename = f"strong-read-plan-{number + 1:04}.json"
        files[filename] = raw
        batches.append({"index": number, "filename": filename, "plan_sha256": V.digest(raw),
                        "run_id": run_id, "target_ids": [t["target_id"] for t in batch_targets]})
    manifest = {"format": 1, "input_index_sha256": bundle, "cluster_id": spec["cluster_id"],
                "bootstrap_id": str(uuid.UUID(spec["bootstrap_id"])), "read_client": spec["read_client"],
                "sources": sources, "original_writes": writes, "targets": ordered, "batches": batches,
                "conflicts": conflicts, "all_source_evidence_complete": all(s["complete_source_evidence"] for s in sources),
                "qualification_status": "not_qualified", "append_or_replay_generated": False,
                "source_write_uncertainty_resolved": False,
                "has_original_write_corpus": bool(writes), "has_eligible_target_corpus": bool(ordered),
                "no_target_reason": "no_original_writes" if not writes else
                                    "all_original_writes_confirmed_unattempted" if not ordered else None,
                "has_visible_source_ack_sample": any(w["visible_sdk_ack"] for w in writes),
                "has_linked_source_ack_sample": any(w["ack_ledger_fsync_return_linked"] for w in writes),
                "confirmed_unattempted_source_identities": [w["source_identity"] for w in writes if w["confirmed_unattempted"]],
                "unterminated_source_identities": [w["source_identity"] for w in writes if w["unfinished"]],
                "external_gates": ["restore lineage not verified by this tool", "source/binary build authenticity",
                                   "actual server/VM identity and durability", "external member-principal topology decision"],
                "bounds": {"sources": MAX_SOURCES, "total_journal_bytes": MAX_TOTAL_JOURNALS,
                           "total_plan_bytes": MAX_TOTAL_PLANS, "targets": MAX_TARGETS,
                           "raw_payload_each": MAX_PAYLOAD, "raw_payload_total": MAX_PAYLOADS,
                           "output_bytes": MAX_OUTPUT}}
    files["reconciliation-manifest.json"] = encoded(manifest) + b"\n"
    V.require(sum(map(len, files.values())) <= MAX_OUTPUT, "output byte cap exceeded; no subset output")
    return manifest, files


def generate(index, output):
    manifest, files = derive(index)  # All bounds/coverage derived BEFORE any output is created.
    output = Path(output)
    output.mkdir(mode=0o700)
    parent_fd = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    for name, raw in files.items():
        fd = os.open(output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as destination:
            destination.write(raw)
            destination.flush()
            os.fsync(destination.fileno())
    fd = os.open(output, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return {"targets": len(manifest["targets"]), "batches": len(manifest["batches"]),
            "original_write_identities": len(manifest["original_writes"]),
            "all_source_evidence_complete": manifest["all_source_evidence_complete"],
            "has_original_write_corpus": manifest["has_original_write_corpus"],
            "has_eligible_target_corpus": manifest["has_eligible_target_corpus"],
            "no_target_reason": manifest["no_target_reason"],
            "has_visible_source_ack_sample": manifest["has_visible_source_ack_sample"],
            "has_linked_source_ack_sample": manifest["has_linked_source_ack_sample"],
            "conflicts": manifest["conflicts"], "qualification_status": "not_qualified"}


def coverage(index, bundle, result_index):
    manifest, files = derive(index)
    bundle = Path(bundle)
    V.require({path.name for path in bundle.iterdir()} == set(files), "unexpected/missing bundle files")
    for name, raw in files.items():
        V.require(V.bounded(bundle / name, MAX_OUTPUT) == raw, "generated bundle differs from complete source derivation")
    result_raw = V.bounded(result_index, 1024**2)
    results = V.decode(result_raw)
    V.require(isinstance(results, dict) and set(results) == {"format", "batches"} and results["format"] == 1
              and isinstance(results["batches"], list) and len(results["batches"]) <= len(manifest["batches"]),
              "result index fields/count")
    by_batch = {}
    for item in results["batches"]:
        V.require(set(item) == {"index", "directory", "journal_sha256", "completion_witness",
                               "completion_witness_sha256", "exit_status"}, "read result binding fields")
        number = item["index"]
        V.require(type(number) is int and 0 <= number < len(manifest["batches"]) and number not in by_batch,
                  "unknown/duplicate read result batch")
        by_batch[number] = item
    verified, absent, failed, not_verified, batch_reports, observations = [], [], [], [], [], []
    aggregate_bytes = 0
    for batch in manifest["batches"]:
        number = batch["index"]
        if number not in by_batch:
            not_verified.extend(batch["target_ids"])
            batch_reports.append({"index": number, "missing": True})
            continue
        item = by_batch[number]
        directory = path_from(Path(result_index).parent, item["directory"])
        V.require(V.bounded(directory / "plan.json", V.MAX_PLAN) == files[batch["filename"]],
                  "read result used a different/subset plan")
        inputs = check_inputs(directory, manifest["cluster_id"], manifest["bootstrap_id"])
        V.require(all(inputs[k] == manifest["read_client"][k] for k in inputs), "read result input config/topology differs")
        journal_hash, size = sha_file(directory / "journal.jsonl", MAX_TOTAL_JOURNALS - aggregate_bytes)
        aggregate_bytes += size
        V.require(journal_hash == item["journal_sha256"], "read result journal hash mismatch")
        witness = None
        if item["completion_witness"] is not None:
            witness = path_from(Path(result_index).parent, item["completion_witness"])
            V.require(V.digest(V.bounded(witness, 1024**2)) == item["completion_witness_sha256"], "read witness hash mismatch")
        else:
            V.require(item["completion_witness_sha256"] is None and item["exit_status"] is None, "read witness absent")
        report = V.verify(directory / "plan.json", directory / "journal.jsonl", witness, item["exit_status"])
        require_consumed_journal(report, journal_hash, size, "read-result")
        require_consumed_plan(report, batch["plan_sha256"], "read-result")
        binding = match_client(report, manifest["read_client"]["identity"])
        complete = report["complete_evidence"]
        batch_reports.append({"index": number, "journal_sha256": journal_hash, "complete_evidence": complete,
                              "client_binding": binding, "defects": report["defects"], "evidence_gaps": report["evidence_gaps"],
                              "failed_logical_ids": report["failed"], "unfinished_logical_ids": report["unfinished"],
                              "unattempted_logical_ids": report["unattempted"]})
        for target in (t for t in manifest["targets"] if t["batch"] == number):
            terminal = report["terminal_outcomes"].get(target["read_logical_id"])
            observations.append({"target_id": target["target_id"], "read_logical_id": target["read_logical_id"],
                                 "batch_evidence_complete": complete,
                                 "terminal_observed_in_valid_prefix": terminal is not None,
                                 "terminal_outcome": terminal.get("outcome") if terminal else None,
                                 "counts_as_verified_target": complete and terminal is not None
                                 and terminal["outcome"] == "exact_match"})
            if not complete or terminal is None:
                not_verified.append(target["target_id"])
            elif terminal["outcome"] == "exact_match":
                verified.append(target["target_id"])
            elif terminal["outcome"] == "expected_value_absent":
                absent.append(target["target_id"])
                if target["has_visible_source_ack"]:
                    failed.append({"target_id": target["target_id"], "outcome": "observed_sdk_acknowledged_value_absent",
                                   "ack_ledger_fsync_return_linked": target["has_linked_source_ack"]})
            else:
                failed.append({"target_id": target["target_id"], "outcome": terminal["outcome"]})
    nonempty = manifest["has_original_write_corpus"] and manifest["has_eligible_target_corpus"]
    complete = (nonempty and not not_verified and not failed and not manifest["conflicts"]
                and manifest["all_source_evidence_complete"])
    return {"format": 1, "qualification_status": "not_qualified", "all_target_observations_covered": nonempty and not not_verified,
            "reconciliation_evidence_complete": complete,
            "has_original_write_corpus": manifest["has_original_write_corpus"],
            "has_eligible_target_corpus": manifest["has_eligible_target_corpus"],
            "no_target_reason": manifest["no_target_reason"],
            "has_visible_source_ack_sample": manifest["has_visible_source_ack_sample"],
            "has_linked_source_ack_sample": manifest["has_linked_source_ack_sample"],
            "required_target_ids": [t["target_id"] for t in manifest["targets"]],
            "verified_present_target_ids": verified, "strong_observed_absent_target_ids": absent,
            "not_verified_target_ids": not_verified, "failed_targets": failed,
            "read_terminal_observations": observations,
            "source_payload_conflicts": manifest["conflicts"], "batches": batch_reports,
            "original_unresolved_source_identities": [w["source_identity"] for w in manifest["original_writes"]
                                                       if w["original_write_error_unresolved"] or
                                                       (w["possibly_attempted"] and not w["ack_ledger_fsync_return_linked"])],
            "source_write_uncertainty_resolved": False, "external_gates": manifest["external_gates"],
            "confirmed_unattempted_source_identities": manifest["confirmed_unattempted_source_identities"],
            "unterminated_source_identities": manifest["unterminated_source_identities"],
            "restore_lineage_qualified": False, "physical_qualification": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    generate_parser = sub.add_parser("generate")
    generate_parser.add_argument("--index", required=True, type=Path)
    generate_parser.add_argument("--output", required=True, type=Path)
    check = sub.add_parser("coverage")
    check.add_argument("--index", required=True, type=Path)
    check.add_argument("--bundle", required=True, type=Path)
    check.add_argument("--results", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.operation == "generate":
            report = generate(args.index, args.output)
            status = 0 if (report["all_source_evidence_complete"] and not report["conflicts"]
                           and report["has_original_write_corpus"] and report["has_eligible_target_corpus"]) else 1
        else:
            report = coverage(args.index, args.bundle, args.results)
            status = 0 if report["reconciliation_evidence_complete"] else 1
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError, RecursionError) as error:
        report = {"qualification_status": "not_qualified", "complete_evidence": False,
                  "fatal_input_or_bound_error": str(error)[:512], "no_subset_coverage_claim": True}
        status = 2
    print(json.dumps(report, sort_keys=True))
    return status


if __name__ == "__main__":
    sys.exit(main())
