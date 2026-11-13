#!/usr/bin/env python3
"""Classify source-pinned native-origin test journals without rerunning a DB.

Reads ZIP members in place; never extracts DBs, treats a timeout as rollback,
converts an unavailable read to absence, or calls a fixture clock hardware RTO.
Every logical call and physical RPC phase is retained in the derived report;
raw-frame hashes reference the immutable original ZIP for exact bytes.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import pathlib
import re
import sys
import zipfile

MAX_ARCHIVE = 256 * 1024 * 1024
MAX_MEMBER = 64 * 1024 * 1024
MAX_TOTAL = 1024 * 1024 * 1024
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_sha(value):
    # serde_json's default map representation sorts string keys. The fixtures
    # use JSON integers/strings/arrays; all original payload/hash bytes remain.
    return sha(json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":")).encode())


def frame_projection(raw):
    if raw is None:
        return None
    require(isinstance(raw, list) and all(type(b) is int and 0 <= b <= 255 for b in raw),
            "RPC raw frame is not a bounded byte array")
    data = bytes(raw)
    result = {"bytes": len(data), "sha256": sha(data), "decoded": None}
    if len(data) < 4:
        result["frame_decode_note"] = "retained frame has no complete length prefix"
        return result
    if int.from_bytes(data[:4], "big") != len(data) - 4:
        result["frame_decode_note"] = "retained frame length prefix differs from available bytes"
        return result
    try:
        result["decoded"] = json.loads(data[4:])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        result["frame_decode_note"] = str(error)
    return result



def case_result(test_log, directory, results):
    # --show-output sections retain both passed and failed cases. Associate
    # the actual journal directory printed by that section; no scenario table.
    sections = re.split(r"---- ([^\n]+) stdout ----", test_log)
    matches = []
    marker = directory.rstrip("/").rsplit("/", 1)[-1]
    for index in range(1, len(sections), 2):
        if marker in sections[index + 1]:
            name = sections[index].strip()
            matches.append({"test": name, "result": results.get(name, "not recorded")})
    return matches


def classify_case(member, data, expected_source, test_log, results):
    events = [json.loads(line) for line in data.decode().splitlines() if line.strip()]
    require(events, f"empty journal: {member}")
    sequences = [event.get("journal_sequence") for event in events]
    require(sequences == list(range(1, len(events) + 1)),
            f"missing/duplicated/reordered journal sequence: {member}")
    identity = [e for e in events if e.get("kind") == "execution_identity"]
    require(len(identity) == 1 and identity[0].get("source") == expected_source,
            f"execution identity is not the pinned source: {member}")
    intents = {}
    replies = {}
    attempts = {}
    rpc_events = []
    warnings = []
    for event in events:
        kind = event.get("kind")
        if kind == "logical_intent":
            context = event["context"]
            key = context["diagnostic_business_id"]
            require(key not in intents, f"duplicate logical intent: {key}")
            if context.get("payload") is not None and context.get("payload_sha256"):
                require(json_sha(context["payload"]) == context["payload_sha256"],
                        f"full expected payload hash mismatch: {member}:{event['journal_sequence']}")
            intents[key] = event
        elif kind == "logical_public_reply":
            key = event["diagnostic_business_id"]
            require(key not in replies, f"duplicate public reply: {key}")
            replies[key] = event
        elif kind == "actual_rpc":
            actual = event["actual"]
            context = event.get("logical_context") or {}
            logical = context.get("diagnostic_business_id")
            projected = {k: v for k, v in actual.items() if k != "raw_frame"}
            projected["raw_frame"] = frame_projection(actual.get("raw_frame"))
            projected["source_journal_sequence"] = event["journal_sequence"]
            projected["source_unix_ns"] = event.get("unix_ns")
            projected["logical_context"] = context
            rpc_events.append(projected)
            attempt_key = (logical, actual.get("attempt_id"))
            attempts.setdefault(attempt_key, []).append(projected)
    require(not set(replies).difference(intents), f"public reply without intent: {member}")
    for logical, _attempt in attempts:
        if logical not in intents:
            warnings.append(f"RPC context has no retained logical intent: {logical}")
    operations = []
    acknowledged = {}
    acknowledged_payloads = {}
    append_ordinals = collections.Counter()
    last_append_payload = {}
    expected = {}
    false_absences = []
    wrong_payloads = []
    fixture_payload_mismatches = []
    for logical, intent in intents.items():
        context = intent["context"]
        key = context.get("event_id") or context.get("key")
        if context.get("operation") == "append":
            expected[key] = context.get("payload")
        reply = replies.get(logical)
        if reply is not None:
            require(reply["journal_sequence"] > intent["journal_sequence"], "public reply precedes its invocation")
        outcome = (reply or {}).get("outcome", {})
        classification = outcome.get("classification", "NO_PUBLIC_REPLY_RETAINED")
        logical_attempts = []
        for (parent, attempt), phases in attempts.items():
            if parent != logical:
                continue
            final = phases[-1]
            response_frames = [p["raw_frame"]["decoded"] for p in phases
                               if p.get("raw_frame") and p.get("phase") == "response_frame_before_decode"
                               and isinstance(p["raw_frame"].get("decoded"), dict)]
            # Report failures and dispatch uncertainty separately. A rejected
            # RPC is not evidence that another retry or the business call
            # rolled back. A server Ok is not an external application ACK.
            logical_attempts.append({
                "attempt_id": attempt,
                "first_sequence": phases[0]["source_journal_sequence"],
                "last_sequence": final["source_journal_sequence"],
                "terminal_phase": final.get("phase"),
                "transport_success": final.get("phase") == "validated_transport_result_ok",
                "transport_failed": final.get("phase") == "validated_transport_result_error",
                "may_have_dispatched": any(p.get("may_have_dispatched") for p in phases),
                "wire_response_count": len(response_frames),
                "wire_error_response_count": sum(isinstance(f.get("response"), dict)
                                                   and "Err" in f["response"] for f in response_frames),
                "phases": phases,
            })
        logical_attempts.sort(key=lambda a: a["first_sequence"])
        row = {"diagnostic_business_id": logical, "context": context,
               "intent_sequence": intent["journal_sequence"],
               "reply_sequence": (reply or {}).get("journal_sequence"),
               "api_receipt_elapsed_ns": (reply or {}).get("api_receipt_elapsed_ns"),
               "classification": classification, "outcome": outcome,
               "attempt_count": len(logical_attempts),
               "physical_retry_count": max(0, len(logical_attempts) - 1),
               "attempts": logical_attempts}
        if context.get("operation") == "append":
            append_ordinals[key] += 1
            row["logical_append_attempt_ordinal_for_event"] = append_ordinals[key]
            row["is_repeated_append_of_same_payload"] = (key in last_append_payload
                and last_append_payload[key] == context.get("payload"))
            last_append_payload[key] = context.get("payload")
        operations.append(row)
        if classification == "USER_ACK":
            require(context.get("operation") == "append", "USER_ACK on a non-append intent")
            require(outcome.get("full_payload") == context.get("payload"),
                    "USER_ACK payload differs from its complete intent")
            if key not in acknowledged or reply["journal_sequence"] < acknowledged[key]:
                acknowledged[key] = reply["journal_sequence"]
                acknowledged_payloads[key] = context.get("payload")
        # A read overlapping an ACK may have linearized before it. Only an
        # actual ACK received before THIS read invocation establishes this
        # read-after-ACK relation. Intent insertion order alone is insufficient.
        prior_ack = key in acknowledged and acknowledged[key] < intent["journal_sequence"]
        if classification == "STRONG_ABSENT" and prior_ack:
            false_absences.append({"event_id": key, "ack_sequence": acknowledged[key],
                                   "read_intent_sequence": intent["journal_sequence"],
                                   "absence_sequence": reply["journal_sequence"]})
        if classification == "STRONG_PRESENT":
            if prior_ack and outcome.get("full_payload") != acknowledged_payloads[key]:
                wrong_payloads.append({"event_id": key, "sequence": reply["journal_sequence"],
                                       "read_intent_sequence": intent["journal_sequence"],
                                       "ack_sequence": acknowledged[key],
                                       "expected": acknowledged_payloads[key],
                                       "actual": outcome.get("full_payload"),
                                       "basis": "immutable event USER_ACK received before read invocation"})
            if key in expected and outcome.get("full_payload") != expected[key]:
                fixture_payload_mismatches.append({"event_id": key,
                    "sequence": reply["journal_sequence"], "expected": expected[key],
                    "actual": outcome.get("full_payload"),
                    "basis": "fixture intended payload only; not independently an ACK/commit proof"})
    operations.sort(key=lambda o: o["intent_sequence"])
    observed_after_uncertain = []
    for operation in operations:
        if operation["classification"] != "NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN":
            continue
        key = operation["context"].get("event_id")
        later = [o for o in operations if o["context"].get("event_id") == key
                 and o["classification"] == "STRONG_PRESENT"
                 and o["intent_sequence"] > (operation["reply_sequence"] or 0)]
        if later:
            observed_after_uncertain.append({"event_id": key,
                                            "uncertain_reply_sequence": operation["reply_sequence"],
                                            "later_present_reply_sequences": [o["reply_sequence"] for o in later],
                                            "classification": "NO_EXTERNAL_ACK_THEN_OBSERVED_PRESENT; NOT_ROLLBACK"})
    custody = []
    for event in events:
        if event.get("kind") != "actual_successor_cancellation_custody_gate":
            continue
        actual = event.get("actual") or {}
        failed = actual.get("actual_failed_acquisition") or {}
        complete = actual.get("fired") is True and failed.get("actual_turn_canceled") is True and bool(failed.get("actual_role_error"))
        preserved = failed.get("prior_session_retained") is True and failed.get("successor_job_created") is False
        custody.append({"journal_sequence": event["journal_sequence"], "actual": actual,
                        "classification": "passed" if complete and preserved else "failed" if complete else "inconclusive",
                        "scope": "actual canceled acquisition custody only; not HA/physical qualification"})
    terminal = [e for e in events if e.get("kind") in
                ("required_one_peer_loss_terminal", "test_terminal_after_owned_stop_attempts")]
    health = [e for e in events if e.get("kind") == "health_round"]
    counts = dict(collections.Counter(o["classification"] for o in operations))
    return {"journal": member, "journal_sha256": sha(data), "event_count": len(events),
            "execution_identity": identity[0],
            "rust_case_results": case_result(test_log, member.rsplit("/", 1)[0], results),
            "terminal_records": terminal, "health_rounds": health,
            "clock_scope": "fixture elapsed receipts only; the fixture can stop with <8s reserved for four calls; no hardware RTO",
            "operation_classification_counts": counts,
            "logical_operation_count": len(operations),
            "physical_attempt_count": len(attempts),
            "physical_rpc_phase_count": len(rpc_events),
            "rpc_context_warnings": warnings,
            "unassociated_rpc_attempts": [{"logical_business_id": logical, "attempt_id": attempt, "phases": phases}
                                          for (logical, attempt), phases in attempts.items() if logical not in intents],
            "acknowledged_event_ids": sorted(acknowledged),
            "successful_strong_false_absences_after_ack": false_absences,
            "successful_strong_wrong_full_payloads": wrong_payloads,
            "fixture_intended_payload_mismatches": fixture_payload_mismatches,
            "no_ack_then_observed_present": observed_after_uncertain,
            "custody_probes": custody,
            "operations": operations,
            "other_journal_events": [e for e in events if e.get("kind") not in
                                     ("logical_intent", "logical_public_reply", "actual_rpc")],
            "scope": "recorded in-process native/TLS test observations; no retained DB custody, power-loss replay, VM result or production qualification"}


def verify(archive, expected_source, expected_sha, expect_tests=None):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_source), "expected source must be a full commit SHA")
    require(re.fullmatch(r"[0-9a-f]{64}", expected_sha), "expected ZIP SHA256 must be complete")
    require(archive.stat().st_size <= MAX_ARCHIVE, "archive exceeds bounded verifier size")
    digest = sha(archive.read_bytes())
    require(digest == expected_sha, "ZIP SHA256 differs from expected immutable artifact")
    with zipfile.ZipFile(archive) as zipped:
        members = zipped.infolist()
        require(sum(m.file_size for m in members) <= MAX_TOTAL, "ZIP uncompressed size exceeds bound")
        require(all(m.file_size <= MAX_MEMBER for m in members), "ZIP member exceeds bound")
        require(len({m.filename for m in members}) == len(members), "duplicate ZIP member names")
        names = {m.filename for m in members}
        def member(name):
            require(name in names, f"missing required artifact member: {name}")
            return zipped.read(name)
        source = member("source-revision.txt").decode().strip()
        require(source == expected_source, "artifact source revision is not expected commit")
        selected_log = ANSI.sub("", member("selected-tests.log").decode())
        test_log = ANSI.sub("", member("tests.log").decode())
        selected = [line[:-6] for line in selected_log.splitlines() if line.endswith(": test")]
        require(selected and len(set(selected)) == len(selected), "empty or duplicate selected test list")
        if expect_tests is not None:
            require(len(selected) == expect_tests, f"selected {len(selected)} tests, expected {expect_tests}")
        results = dict(re.findall(r"^test (\S+) \.\.\. (ok|FAILED|ignored)$", test_log, re.M))
        require(set(results).issubset(set(selected)), "executed result outside selected test list")
        summaries = re.findall(r"^test result: (.+)$", test_log, re.M)
        require(summaries, "test execution summary missing; selection/build is not execution")
        journals = [m.filename for m in members if m.filename.endswith("/journal.jsonl")]
        require(journals, "artifact has no retained native evidence journals")
        cases = [classify_case(name, zipped.read(name), source, test_log, results) for name in sorted(journals)]
    violations = sum(len(c["successful_strong_false_absences_after_ack"])
                     + len(c["successful_strong_wrong_full_payloads"]) for c in cases)
    return {"format_version": 1, "verifier": "verify-native-origin-evidence.py",
            "verifier_sha256": sha(pathlib.Path(__file__).read_bytes()),
            "source_commit": source, "archive": str(archive), "archive_sha256": digest,
            "selected_tests": selected, "executed_test_results": results,
            "missing_execution_results": sorted(set(selected).difference(results)),
            "test_execution_summaries": summaries,
            "journal_count": len(cases), "recorded_successful_strong_violations": violations,
            "evidence_integrity": "passed",
            "qualification": "not established by this verifier; failing/negative cases remain separate",
            "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=pathlib.Path)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expect-tests", type=int)
    parser.add_argument("--report", required=True, type=pathlib.Path)
    args = parser.parse_args()
    try:
        report = verify(args.archive, args.expected_source, args.expected_sha256, args.expect_tests)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError, zipfile.BadZipFile) as error:
        print(f"evidence verification failed: {error}", file=sys.stderr)
        return 2
    summary = {key: report[key] for key in ("source_commit", "archive_sha256", "journal_count",
               "test_execution_summaries", "recorded_successful_strong_violations", "evidence_integrity")}
    summary["report"] = str(args.report)
    print(json.dumps(summary, sort_keys=True))
    # A failed database test remains faithfully reported, not a verifier
    # integrity failure. A successful-read violation is separately nonzero.
    return 1 if report["recorded_successful_strong_violations"] else 0


if __name__ == "__main__":
    sys.exit(main())
