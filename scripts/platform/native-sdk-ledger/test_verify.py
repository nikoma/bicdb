#!/usr/bin/env python3
"""Synthetic format controls only. No BicDB process/network/native commit occurs."""

import copy
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("ledger_verify", Path(__file__).with_name("verify.py"))
V = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(V)
RUN = "019a80b8-6594-7000-8000-000000000001"
RPC = "019a80b8-6594-7000-8000-000000000002"


def encode(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def framed(value):
    raw = encode(value)
    return list(len(raw).to_bytes(4, "big") + raw)


class Fixture:
    def __init__(self, value=None, read=False):
        self.plan = {"run_id": RUN, "phase": "synthetic-control", "operations": [
            {"operation": "append", "logical_id": "w", "namespace": "probe", "key": "k", "value": value}]}
        if read:
            self.plan["operations"].append({"operation": "strong_read", "logical_id": "r",
                                            "namespace": "probe", "key": "k", "expected": value,
                                            "acknowledged_by": "w"})
        self.plan_raw = encode(self.plan)
        self.records, self.raw, self.reservation = [], [], 0
        self.add("run_started", {"phase": self.plan["phase"], "operations": len(self.plan["operations"]),
                                  "plan_sha256": V.digest(self.plan_raw), "client_binary_sha256": "1" * 64,
                                  "build_source_claim": "a" * 40, "caller_member_identity": "driver",
                                  "config_sha256": V.digest(b"{}"), "topology_sha256": V.digest(b"{}"),
                                  "qualification_status": "not_qualified", "throughput_benchmark": False,
                                  "rpc_evidence_limits": {"max_records": 65536, "max_bytes": 1073741824}})

    def add(self, kind, data):
        row = {"format": 1, "run_id": RUN, "sequence": len(self.raw) + 1, "kind": kind,
               "previous_record_sha256": V.digest(self.raw[-1]) if self.raw else None,
               "unix_ns": 1, "monotonic_elapsed_ns": len(self.raw), "data": copy.deepcopy(data)}
        raw = encode(row)
        self.records.append(row)
        self.raw.append(raw)
        return {"sequence": row["sequence"], "record_sha256": V.digest(raw)}

    def attempt(self, name):
        command = next(op for op in self.plan["operations"] if op["logical_id"] == name)
        value = command.get("value", command.get("expected"))
        self.add("logical_attempt", {"command": command, "payload_sha256": V.digest(encode(value)),
                                     "canonical_record_id": command["namespace"] + "/" + command["key"],
                                     "payload_hash_encoding": "serde_json::to_vec(Value) UTF-8"})

    def rpc(self, name, fail=False, found=True, value=None):
        read = name == "r"
        operation = "native_event_get" if read else "event_append"
        request_body = ({"NativeEventGet": {"mode": "strong", "business_request_id": RUN,
                                            "challenge": RPC, "namespace": "probe", "key": "k"}}
                        if read else {"ApplicationEvent": {"range_id": 1, "range_epoch": 1,
                                                            "operation": {k: self.plan["operations"][0][k]
                                                                          for k in ("operation", "namespace", "key", "value")}}})
        request = {"protocol_version": 1, "cluster_id": "test", "request_id": 1,
                   "caller_node_id": "driver", "destination_node_id": "node", "request": request_body}
        body = ({"NativeEventGet": {"business_request_id": RUN, "challenge": RPC,
                                    "result": {"kind": "value", "value": {"presence": "present", "value": value}
                                               if found else {"presence": "absent"}}}}
                if read else {"ApplicationEvent": {"result": "appended", "inserted": True}})
        response = {"protocol_version": 1, "cluster_id": "test", "request_id": 1,
                    "source_node_id": "node", "response": {"Ok": body}}
        evidence = {"attempt_id": str(__import__("uuid").UUID(int=2 if not read else 3)),
                    "journal_sequence": 0, "phase_unix_ns": 1, "elapsed_ns": 0,
                    "phase": "invoked", "operation": operation, "caller": "driver", "destination": "node",
                    "topology_generation": None, "address": None, "request_id": None, "raw_frame": None,
                    "bytes_written": 0, "may_have_dispatched": False, "error_chain": []}

        def emit(phase, raw=None):
            self.reservation += 1
            evidence.update(phase=phase, raw_frame=raw, journal_sequence=self.reservation,
                            elapsed_ns=self.reservation)
            self.add("sdk_rpc", {"logical_id": name, "actual_sdk_evidence": evidence})

        emit("invoked")
        evidence.update(topology_generation=1, address="127.0.0.1:1", request_id=1)
        emit("request_frame_before_dispatch", framed(request))
        evidence.update(bytes_written=len(framed(request)), may_have_dispatched=True)
        if fail:
            evidence["error_chain"] = ["socket refused"]
            emit("exchange_error_before_cancellation_override")
            emit("validated_transport_result_error")
        else:
            emit("response_frame_before_decode", framed(response))
            emit("validated_transport_result_ok")

    def terminal(self, name, outcome, detail):
        self.add("logical_terminal", {"logical_id": name, "outcome": outcome, "detail": detail,
                                      "sdk_elapsed_ns": 2, "client_elapsed_before_terminal_fsync_ns": 3})

    def write(self, fail=False, fence=False):
        self.attempt("w")
        self.rpc("w", fail=fail)
        if fail:
            self.terminal("w", "write_error_unresolved", {"error": {"debug": "CommitOutcomeUnknown"},
                                                          "observer_fenced": fence,
                                                          "timeout_is_rollback": False, "outer_write_retry": False})
        else:
            reply = {"inserted": True}
            receipt = self.add("sdk_ack_observed", {"logical_id": "w", "reply": reply, "sdk_elapsed_ns": 2})
            self.terminal("w", "acknowledged", {"sdk_reply": reply, "durable_ack_receipt": receipt})

    def read(self, found=True, value=None, prior=True):
        self.attempt("r")
        self.rpc("r", found=found, value=value)
        expected = self.plan["operations"][1]["expected"]
        outcome = ("exact_match" if found and V.equal(value, expected) else
                   "acknowledged_value_absent" if not found and prior else
                   "expected_value_absent" if not found else "payload_mismatch")
        self.terminal("r", outcome, {"sdk_reply": {"found": found, "value": value}, "prior_ack_observed": prior})

    def complete(self):
        terminals = [r["data"] for r in self.records if r["kind"] == "logical_terminal"]
        counts = dict(V.Counter(t["outcome"] for t in terminals))
        done = len(terminals) == len(self.plan["operations"])
        success = done and all(k in {"acknowledged", "exact_match"} for k in counts)
        fence = any(t["detail"].get("observer_fenced") is True for t in terminals)
        summary = {"outcomes": counts, "plan_completed": done, "planned_operations": len(self.plan["operations"]),
                   "terminal_operations": len(terminals), "all_operations_succeeded": success,
                   "observer_fenced": fence, "unattempted_logical_ids": [op["logical_id"] for op in self.plan["operations"][len(terminals):]],
                   "qualification_status": "not_qualified", "post_convergence_capacity_demonstrated": False}
        receipt = self.add("run_completed", summary)
        self.witness = {"summary": summary, "completion_receipt": receipt}
        self.exit_status = 2 if fence else 0 if success else 1

    def rebuild(self, relink_acks=False):
        records = self.records
        self.raw, self.records = [], []
        receipts = {}
        for row in records:
            data = copy.deepcopy(row["data"])
            if relink_acks and row["kind"] == "logical_terminal" and data["outcome"] == "acknowledged":
                data["detail"]["durable_ack_receipt"] = receipts[data["logical_id"]]
            receipt = self.add(row["kind"], data)
            if row["kind"] == "sdk_ack_observed":
                receipts[data["logical_id"]] = receipt

    def verify(self, witness=True, suffix=b"", max_line=V.MAX_LINE):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name)
            (path / "plan.json").write_bytes(self.plan_raw)
            (path / "config.json").write_bytes(b"{}")
            (path / "topology.json").write_bytes(b"{}")
            (path / "journal.jsonl").write_bytes(b"\n".join(self.raw) + b"\n" + suffix)
            witness_path = None
            if witness and hasattr(self, "witness"):
                witness_path = path / "stdout.json"
                witness_path.write_bytes(encode(self.witness))
            return V.verify(path / "plan.json", path / "journal.jsonl", witness_path,
                            self.exit_status if witness_path else None, max_line)


class Controls(unittest.TestCase):
    def test_complete_write_and_present_null(self):
        f = Fixture(read=True)
        f.write()
        f.read(value=None)
        f.complete()
        r = f.verify()
        self.assertTrue(r["complete_evidence"], r)
        self.assertEqual(r["fsync_return_linked_sdk_acks"], ["w"])
        self.assertEqual(r["unresolved_writes"], [])
        self.assertEqual(r["qualification_status"], "not_qualified")

    def test_omitted_optional_ack_in_plan_matches_serialized_null(self):
        f = Fixture(read=True)
        del f.plan["operations"][1]["acknowledged_by"]
        f.plan_raw = encode(f.plan)
        f.records[0]["data"]["plan_sha256"] = V.digest(f.plan_raw)
        f.rebuild()
        f.write()
        f.attempt("r")
        f.records[-1]["data"]["command"]["acknowledged_by"] = None
        f.raw[-1] = encode(f.records[-1])
        f.rpc("r", value=None)
        f.terminal("r", "exact_match", {"sdk_reply": {"found": True, "value": None}, "prior_ack_observed": False})
        f.complete()
        self.assertTrue(f.verify()["complete_evidence"])

    def test_plan_uuid_spelling_is_not_rewritten_or_false_rejected(self):
        f = Fixture()
        f.plan["run_id"] = RUN.replace("-", "").upper()
        f.plan_raw = encode(f.plan)
        f.records[0]["data"]["plan_sha256"] = V.digest(f.plan_raw)
        f.rebuild(); f.write(); f.complete()
        self.assertTrue(f.verify()["complete_evidence"])

    def test_absent_null_is_failure_not_exact_match(self):
        f = Fixture(read=True)
        f.write()
        f.read(found=False)
        f.complete()
        r = f.verify()
        self.assertTrue(r["complete_evidence"], r)
        self.assertEqual(r["failed"], ["r"])
        self.assertEqual(r["terminal_outcomes"]["r"]["outcome"], "acknowledged_value_absent")

    def test_bool_and_integer_are_not_same_payload(self):
        self.assertFalse(V.equal(True, 1))
        self.assertFalse(V.equal({"a": True}, {"a": 1}))

    def test_arbitrary_precision_raw_span_is_not_reserialized(self):
        raw = b'{"outer": {"text":"\xe2\x82\xac", "value":1.2300e+999}}'
        self.assertEqual(V.raw_at(raw, ["outer", "value"]), b"1.2300e+999")
        self.assertNotEqual(V.decode(raw)["outer"]["value"], float("inf"))

    def test_trailing_ack_visibility_is_not_fsync_return(self):
        f = Fixture()
        f.write()
        f.raw.pop(); f.records.pop()  # actual ACK row remains, terminal not formed
        r = f.verify(False)
        self.assertEqual(r["visible_sdk_acks"].keys(), {"w"})
        self.assertEqual(r["fsync_return_unproven_sdk_acks"], ["w"])
        self.assertEqual(r["unresolved_writes"], ["w"])
        self.assertEqual(r["unfinished"], ["w"])

    def test_successor_confirms_prior_ack_but_not_trailing_completion(self):
        f = Fixture(); f.write(); f.complete()
        r = f.verify(False)
        self.assertFalse(r["complete_evidence"])
        self.assertEqual(r["fsync_return_linked_sdk_acks"], ["w"])

    def test_torn_tail_retains_prefix_attempts_and_ack(self):
        f = Fixture(); f.write()
        r = f.verify(False, b'{"format":1')
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["attempted"].keys(), {"w"})
        self.assertEqual(r["fsync_return_linked_sdk_acks"], ["w"])

    def test_changed_raw_hash_link_detected(self):
        f = Fixture(); f.write()
        f.raw[-1] = f.raw[-1].replace(b'"previous_record_sha256":"', b'"previous_record_sha256":"0')
        r = f.verify(False)
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["unfinished"], ["w"])

    def test_payload_altered_even_with_rebuilt_chain(self):
        f = Fixture({"v": 1}); f.write()
        f.records[1]["data"]["command"]["value"] = {"v": 2}
        f.rebuild()
        self.assertEqual(f.verify(False)["integrity"], "invalid")

    def test_wrong_ack_receipt_rejected(self):
        f = Fixture(); f.write()
        f.records[-1]["data"]["detail"]["durable_ack_receipt"]["record_sha256"] = "0" * 64
        f.rebuild()
        self.assertEqual(f.verify(False)["integrity"], "invalid")

    def test_missing_phase_and_reservation_do_not_hide_attempt(self):
        f = Fixture(); f.write(); f.complete()
        f.records = [r for r in f.records if not (r["kind"] == "sdk_rpc" and
                     r["data"]["actual_sdk_evidence"]["phase"] == "validated_transport_result_ok")]
        f.rebuild()
        r = f.verify(False)
        self.assertFalse(r["complete_evidence"])
        self.assertEqual(r["attempted"].keys(), {"w"})
        self.assertTrue(any("unfinished_rpc" in gap for gap in r["evidence_gaps"]))

    def test_per_attempt_reservation_regression_is_rejected(self):
        f = Fixture(read=True); f.write(); f.read(value=None); f.complete()
        # Reservation gaps cannot be sorted away or confused with ledger order.
        f.records[3]["data"]["actual_sdk_evidence"]["journal_sequence"] = 99
        f.rebuild()
        self.assertFalse(f.verify(False)["complete_evidence"])

    def test_parallel_callback_persistence_order_need_not_equal_reservation_order(self):
        f = Fixture(); f.write()
        original = copy.deepcopy(f.records[2:6])
        other = copy.deepcopy(original)
        for index, row in enumerate(original):
            row["data"]["actual_sdk_evidence"]["journal_sequence"] = [1, 6, 7, 8][index]
        for index, row in enumerate(other):
            evidence = row["data"]["actual_sdk_evidence"]
            evidence["attempt_id"] = RUN
            evidence["journal_sequence"] = index + 2
        f.records[2:6] = other + original
        f.rebuild(relink_acks=True)
        r = f.verify(False)
        self.assertEqual(r["integrity"], "valid_prefix", r)
        self.assertFalse(any("missing_sdk_reservation_sequences" in g for g in r["evidence_gaps"]))
        self.assertEqual(r["rpc_attempt_counts"], {"w": 2})

    def test_corrupt_prefix_does_not_hide_later_visible_ack(self):
        f = Fixture(); f.write(); f.complete()
        f.records[3]["previous_record_sha256"] = "0" * 64
        f.raw[3] = encode(f.records[3])
        r = f.verify(False)
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["visible_sdk_acks"], {})
        self.assertTrue(any(ob.get("kind") == "sdk_ack_observed" and ob.get("logical_id") == "w"
                            for ob in r["untrusted_tail_logical_observations"]))
        self.assertEqual(r["unresolved_writes"], ["w"])

    def test_unresolved_write_and_unattempted_read_preserved_on_fence(self):
        f = Fixture(read=True); f.write(fail=True, fence=True); f.complete()
        r = f.verify()
        self.assertTrue(r["complete_evidence"], r)
        self.assertEqual(r["failed"], ["w"])
        self.assertEqual(r["unresolved_writes"], ["w"])
        self.assertEqual(r["unattempted"], ["r"])

    def test_rpc_unknown_uuid_cannot_be_reattributed(self):
        f = Fixture(); f.write()
        f.records[3]["data"]["actual_sdk_evidence"]["attempt_id"] = RUN
        f.rebuild()
        self.assertEqual(f.verify(False)["integrity"], "invalid")

    def test_late_database_success_not_external_ack(self):
        f = Fixture(); f.attempt("w"); f.rpc("w")
        f.terminal("w", "write_error_unresolved", {"error": {"debug": "QueryTimedOut"},
                                                  "observer_fenced": False,
                                                  "timeout_is_rollback": False, "outer_write_retry": False})
        f.complete()
        r = f.verify()
        self.assertEqual(r["visible_sdk_acks"], {})
        self.assertEqual(r["unresolved_writes"], ["w"])
        self.assertEqual(len(r["raw_database_success_replies"]), 1)

    def test_ack_without_matching_rpc_reply_is_evidence_gap(self):
        f = Fixture(); f.write(); f.complete()
        response_row = f.records[4]["data"]["actual_sdk_evidence"]
        response = V.frame(response_row["raw_frame"])
        response["response"]["Ok"]["ApplicationEvent"]["inserted"] = False
        response_row["raw_frame"] = framed(response)
        f.rebuild(relink_acks=True)
        r = f.verify(False)
        self.assertTrue(any("logical_success_lacks_validated_operation_rpc" in g for g in r["evidence_gaps"]))

    def test_duplicate_json_members_refused(self):
        with self.assertRaises(V.Invalid):
            V.decode(b'{"value":null,"value":true}')

    def test_unknown_format_and_qualification_claim_refused(self):
        for field, value in (("format", 2),):
            f = Fixture(); f.records[0][field] = value
            f.raw[0] = encode(f.records[0])
            self.assertEqual(f.verify(False)["integrity"], "invalid")
        f = Fixture(); f.records[0]["data"]["qualification_status"] = "qualified"; f.rebuild()
        self.assertEqual(f.verify(False)["integrity"], "invalid")

    def test_wrong_stdout_or_exit_not_completion_proof(self):
        f = Fixture(); f.write(); f.complete()
        f.exit_status = 2
        self.assertEqual(f.verify()["integrity"], "invalid")

    def test_input_line_bound_is_fail_closed(self):
        f = Fixture(); f.write()
        r = f.verify(False, max_line=100)
        self.assertEqual(r["integrity"], "invalid")

    def test_bad_ack_reference_plan_rejected(self):
        f = Fixture(read=True)
        f.plan["operations"][1]["acknowledged_by"] = "missing"
        with self.assertRaises(V.Invalid):
            V.validate_plan(f.plan)

    def test_partial_write_keeps_no_terminal_uncertain(self):
        f = Fixture(); f.attempt("w")
        r = f.verify(False)
        self.assertEqual(r["unfinished"], ["w"])
        self.assertEqual(r["unresolved_writes"], ["w"])
        self.assertFalse(r["complete_evidence"])

    def test_sdk_quota_sequence_is_not_silently_truncated(self):
        f = Fixture(); f.write()
        f.records[2]["data"]["actual_sdk_evidence"]["journal_sequence"] = 65537
        f.rebuild()
        r = f.verify(False)
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["unfinished"], ["w"])

    def test_duplicate_phase_is_not_complete_coverage(self):
        f = Fixture(); f.write()
        f.records[3]["data"]["actual_sdk_evidence"]["phase"] = "invoked"
        f.rebuild()
        self.assertEqual(f.verify(False)["integrity"], "invalid")

    def test_wrong_typed_read_challenge_is_not_evidence_of_match(self):
        f = Fixture(read=True); f.write(); f.read(value=None); f.complete()
        response_row = next(row["data"]["actual_sdk_evidence"] for row in f.records
                            if row["kind"] == "sdk_rpc" and row["data"]["logical_id"] == "r"
                            and row["data"]["actual_sdk_evidence"]["phase"] == "response_frame_before_decode")
        response = V.frame(response_row["raw_frame"])
        response["response"]["Ok"]["NativeEventGet"]["challenge"] = RUN
        response_row["raw_frame"] = framed(response)
        f.rebuild(relink_acks=True)
        r = f.verify(False)
        self.assertTrue(any(g.get("logical_success_lacks_validated_operation_rpc") == "r"
                            for g in r["evidence_gaps"]))

    def test_malformed_nested_response_fails_closed_with_prefix(self):
        f = Fixture(); f.write()
        row = f.records[4]["data"]["actual_sdk_evidence"]
        response = V.frame(row["raw_frame"]); response["response"] = []
        row["raw_frame"] = framed(response); f.rebuild()
        r = f.verify(False)
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["attempted"].keys(), {"w"})

    def test_nonobject_terminal_detail_cannot_escape_fail_closed_report(self):
        f = Fixture(); f.write()
        f.records[-1]["data"]["outcome"] = "write_error_unresolved"
        f.records[-1]["data"]["detail"] = "error"
        f.rebuild()
        r = f.verify(False)
        self.assertEqual(r["integrity"], "invalid")
        self.assertEqual(r["unfinished"], ["w"])

    def test_byte_quota_enforced_in_sdk_serialization(self):
        f = Fixture(); f.attempt("w"); f.rpc("w")
        verifier = V.Verifier(f.plan_raw)
        verifier.row(f.raw[0]); verifier.row(f.raw[1])
        verifier.rpc_bytes_lower_bound = 1073741824
        with self.assertRaisesRegex(V.Invalid, "byte quota"):
            verifier.row(f.raw[2])

    def test_missing_reservation_is_explicit_even_with_valid_phase_order(self):
        f = Fixture(); f.write()
        for row in f.records:
            if row["kind"] == "sdk_rpc":
                row["data"]["actual_sdk_evidence"]["journal_sequence"] += 1
        f.rebuild(relink_acks=True)
        r = f.verify(False)
        self.assertTrue(any(g.get("missing_sdk_reservation_sequences") == [1] for g in r["evidence_gaps"]))

    def test_exact_raw_hash_includes_previous_link_and_field_order(self):
        f = Fixture()
        row = f.records[0]
        f.raw[0] = json.dumps(row, indent=1).encode()
        f.add("logical_attempt", {"command": f.plan["operations"][0],
                                  "canonical_record_id": "probe/k", "payload_sha256": V.digest(b"null"),
                                  "payload_hash_encoding": "serde_json::to_vec(Value) UTF-8"})
        # Single-line whitespace is allowed; raw bytes, not a canonical dump, bind it.
        f.raw[0] = f.raw[0].replace(b"\n", b" ")
        f.records[1]["previous_record_sha256"] = V.digest(f.raw[0]); f.raw[1] = encode(f.records[1])
        r = f.verify(False)
        self.assertEqual(r["integrity"], "valid_prefix")


if __name__ == "__main__":
    unittest.main()
