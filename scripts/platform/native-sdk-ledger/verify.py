#!/usr/bin/env python3
"""Bounded, offline format-1 verifier. Hash integrity is not authenticity/HA proof.

Use the original plan, not a reconstructed plan. Input JSON is never reserialized
to verify record or payload hashes. A successor proves only that the preceding
append's fsync returned in this producer's execution. A visible final row does
not prove that row's own fsync returned. --completion-witness is an independently
captured stdout JSON object; its provenance and exit status remain external facts.
"""

import argparse
import hashlib
import json
import re
import sys
import uuid
from collections import Counter
from decimal import Decimal
from pathlib import Path

MAX_BYTES = 2 * 1024**3
MAX_PLAN = 40 * 1024**2
MAX_LINE = 64 * 1024**2
MAX_ROWS = 70000  # 65536 SDK + <=3072 logical +2 header/completion producer bound
PHASES = {
    "invoked", "request_frame_before_dispatch", "response_frame_before_decode",
    "partial_or_rejected_response_frame", "exchange_error_before_cancellation_override",
    "validated_transport_result_ok", "validated_transport_result_error",
    "return_boundary_error_after_evidence",
}
HEX = re.compile(r"[0-9a-f]{64}\Z")


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON member")
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=pairs, parse_float=Decimal,
                      parse_constant=lambda _: (_ for _ in ()).throw(Invalid("nonfinite JSON")))


def integer(value):
    return type(value) is int and value >= 0


def equal(left, right):
    # Python's True == 1 must never turn a mismatched business payload into a pass.
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(equal(v, right[k]) for k, v in left.items())
    if isinstance(left, list):
        return len(left) == len(right) and all(equal(a, b) for a, b in zip(left, right))
    if isinstance(left, Decimal):
        return left.as_tuple() == right.as_tuple()
    return left == right


def raw_at(raw, path):
    """Locate a value's exact UTF-8 span without normalizing numeric spellings."""
    text = raw.decode("utf-8")
    decoder = json.JSONDecoder(object_pairs_hook=pairs)

    def skip(at):
        while at < len(text) and text[at] in " \t\r\n":
            at += 1
        return at

    at = skip(0)
    for wanted in path:
        require(text[at] == "{", "payload path is not an object")
        at = skip(at + 1)
        while True:
            key, end = decoder.raw_decode(text, at)
            at = skip(end)
            require(text[at] == ":", "missing JSON colon")
            at = skip(at + 1)
            if key == wanted:
                break
            _, at = decoder.raw_decode(text, at)
            at = skip(at)
            require(text[at] == ",", "payload path missing")
            at = skip(at + 1)
    _, end = decoder.raw_decode(text, at)
    return text[at:end].encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def typed_digest(value):
    """Type-aware comparison digest, never a replacement for raw payload SHA."""
    state = hashlib.sha256()

    def visit(node):
        if isinstance(node, dict):
            state.update(b"object[")
            for key in sorted(node):
                visit(key)
                visit(node[key])
            state.update(b"]")
        elif isinstance(node, list):
            state.update(b"array[")
            for item in node:
                visit(item)
            state.update(b"]")
        else:
            raw = (str(node.as_tuple()) if isinstance(node, Decimal) else repr(node)).encode()
            state.update(type(node).__name__.encode() + b":" + str(len(raw)).encode() + b":" + raw)
    visit(value)
    return state.hexdigest()


def bounded(path, maximum):
    with Path(path).open("rb") as source:
        raw = source.read(maximum + 1)
    require(len(raw) <= maximum, "input byte bound exceeded")
    return raw


def valid_uuid(value):
    try:
        return isinstance(value, str) and uuid.UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def validate_plan(plan):
    require(set(plan) == {"run_id", "phase", "operations"}, "plan fields")
    require(valid_uuid(plan["run_id"]) and isinstance(plan["phase"], str)
            and 0 < len(plan["phase"].encode()) <= 128, "plan identity")
    operations = plan["operations"]
    require(isinstance(operations, list) and 1 <= len(operations) <= 1024, "plan count")
    ids, writes, keys = {}, {}, set()
    for command in operations:
        require(isinstance(command, dict), "plan command")
        kind = command.get("operation")
        fields = {"operation", "logical_id", "namespace", "key"}
        require(kind in {"append", "strong_read"}, "plan operation")
        allowed = fields | ({"value"} if kind == "append" else {"expected", "acknowledged_by"})
        required = allowed - ({"acknowledged_by"} if kind == "strong_read" else set())
        require(required <= command.keys() <= allowed, "command fields")
        for key in ("logical_id", "namespace", "key"):
            require(isinstance(command[key], str) and 0 < len(command[key].encode()) <= 4096,
                    "command identity")
        require(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", command["namespace"])
                and re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", command["key"]), "SDK event name bounds")
        name = command["logical_id"]
        require(name not in ids, "duplicate logical identity")
        if kind == "append":
            key = (command["namespace"], command["key"])
            require(key not in keys, "duplicate write key")
            keys.add(key)
            writes[name] = command
        elif command.get("acknowledged_by") is not None:
            prior = writes.get(command["acknowledged_by"])
            require(prior is not None and prior["namespace"] == command["namespace"]
                    and prior["key"] == command["key"] and equal(prior["value"], command["expected"]),
                    "ACK reference is not earlier exact append")
        normalized = dict(command)
        if kind == "strong_read":
            normalized.setdefault("acknowledged_by", None)
        ids[name] = normalized
    return ids


def frame(value):
    require(isinstance(value, list) and len(value) <= MAX_LINE
            and all(type(v) is int and 0 <= v <= 255 for v in value), "raw frame bytes")
    raw = bytes(value)
    require(len(raw) >= 4 and int.from_bytes(raw[:4], "big") == len(raw) - 4,
            "complete frame length")
    return decode(raw[4:])


class Verifier:
    def __init__(self, plan_raw):
        self.plan_raw = plan_raw
        self.plan = decode(plan_raw)
        self.commands = validate_plan(self.plan)
        self.attempts, self.acks, self.terminals, self.rpcs = {}, {}, {}, {}
        self.rows, self.hashes, self.reservations = [], {}, set()
        self.defects, self.gaps, self.raw_success = [], [], []
        self.started = self.completed = None
        self.previous = None
        self.elapsed = 0
        self.rpc_bytes_lower_bound = 0
        self.archived_inputs = {}
        self.untrusted_tail = []
        self.tail_counts = Counter()
        self.tail_omitted = 0

    def row(self, raw):
        row = decode(raw)
        require(isinstance(row, dict) and set(row) == {
            "format", "run_id", "sequence", "kind", "previous_record_sha256",
            "unix_ns", "monotonic_elapsed_ns", "data"}, "journal row fields")
        require(row["format"] == 1 and type(row["format"]) is int, "format")
        require(valid_uuid(row["run_id"]) and uuid.UUID(row["run_id"]) == uuid.UUID(self.plan["run_id"]),
                "run identity changed")
        number = row["sequence"]
        require(type(number) is int and number == len(self.rows) + 1, "journal sequence")
        require(row["previous_record_sha256"] == self.previous, "raw hash link")
        require(integer(row["unix_ns"]) and integer(row["monotonic_elapsed_ns"])
                and row["monotonic_elapsed_ns"] >= self.elapsed, "journal clocks")
        require(self.completed is None, "record after completion")
        data, kind = row["data"], row["kind"]
        require(isinstance(data, dict), "record data")
        if kind == "run_started":
            require(not self.rows, "start must be first and unique")
            require(data.get("plan_sha256") == digest(self.plan_raw)
                    and data.get("phase") == self.plan["phase"]
                    and data.get("operations") == len(self.commands), "start/plan binding")
            require(data.get("qualification_status") == "not_qualified"
                    and data.get("throughput_benchmark") is False, "unsupported qualification claim")
            require(HEX.fullmatch(data.get("client_binary_sha256", ""))
                    and re.fullmatch(r"[0-9a-fA-F]{40}", data.get("build_source_claim", "")),
                    "binary/source identity")
            require(data.get("rpc_evidence_limits") == {"max_records": 65536, "max_bytes": 1073741824},
                    "unknown observer limits")
            self.started = data
        else:
            require(self.started is not None, "missing run_started")
            if kind == "logical_attempt":
                command = data["command"]
                name = command["logical_id"]
                require(name in self.commands and equal(command, self.commands[name]), "attempt/plan mismatch")
                require(name not in self.attempts, "outer operation replay")
                require(not any(t["detail"].get("observer_fenced") is True for t in self.terminals.values()),
                        "operation after sticky observer fence")
                expected_next = self.plan["operations"][len(self.attempts)]["logical_id"]
                require(name == expected_next and len(self.terminals) == len(self.attempts),
                        "logical operation overlap/order")
                field = "value" if command["operation"] == "append" else "expected"
                payload = raw_at(raw, ["data", "command", field])
                require(data.get("payload_hash_encoding") == "serde_json::to_vec(Value) UTF-8"
                        and len(payload) <= 32768 and data.get("payload_sha256") == digest(payload),
                        "exact raw payload hash")
                require(data.get("canonical_record_id") == command["namespace"] + "/" + command["key"],
                        "canonical record identity")
                self.attempts[name] = {"sequence": number, "payload_sha256": data["payload_sha256"],
                                       "operation": command["operation"], "namespace": command["namespace"],
                                       "key": command["key"], "canonical_record_id": data["canonical_record_id"]}
            elif kind == "sdk_rpc":
                self.rpc(data, raw_at(raw, ["data", "actual_sdk_evidence"]))
            elif kind == "sdk_ack_observed":
                name = data["logical_id"]
                require(name in self.attempts and name not in self.acks and name not in self.terminals
                        and self.commands[name]["operation"] == "append", "ACK attribution/order")
                require(set(data["reply"]) == {"inserted"} and type(data["reply"]["inserted"]) is bool,
                        "ACK reply type")
                self.acks[name] = {"sequence": number, "record_sha256": digest(raw), "reply": data["reply"]}
            elif kind == "logical_terminal":
                self.terminal(data)
            elif kind == "run_completed":
                require(len(self.attempts) == len(self.terminals), "completion with unfinished attempt")
                counts = dict(Counter(t["outcome"] for t in self.terminals.values()))
                done = len(self.terminals) == len(self.commands)
                success = done and all(k in {"acknowledged", "exact_match"} for k in counts)
                fenced = any(t["detail"].get("observer_fenced") is True for t in self.terminals.values())
                require(data.get("outcomes") == counts and data.get("plan_completed") is done
                        and data.get("planned_operations") == len(self.commands)
                        and data.get("terminal_operations") == len(self.terminals)
                        and data.get("all_operations_succeeded") is success
                        and data.get("observer_fenced") is fenced
                        and data.get("unattempted_logical_ids") == list(self.commands)[len(self.attempts):]
                        and data.get("qualification_status") == "not_qualified"
                        and data.get("post_convergence_capacity_demonstrated") is False,
                        "completion summary disagrees with retained operations")
                self.completed = data
            else:
                raise Invalid("unknown journal kind")
        self.previous = digest(raw)
        self.hashes[number] = self.previous
        self.elapsed = row["monotonic_elapsed_ns"]
        self.rows.append({"sequence": number, "kind": kind})

    def rpc(self, data, raw_evidence):
        name = data["logical_id"]
        require(name in self.attempts and name not in self.terminals, "RPC outside original operation")
        evidence = data["actual_sdk_evidence"]
        expected = {"attempt_id", "journal_sequence", "phase_unix_ns", "elapsed_ns", "phase", "operation",
                    "caller", "destination", "topology_generation", "address", "request_id", "raw_frame",
                    "bytes_written", "may_have_dispatched", "error_chain"}
        require(set(evidence) == expected, "unknown/missing SDK evidence fields")
        rid, sequence, phase = evidence["attempt_id"], evidence["journal_sequence"], evidence["phase"]
        require(valid_uuid(rid) and type(sequence) is int and 1 <= sequence <= 65536
                and sequence not in self.reservations and phase in PHASES, "RPC identity/reservation/phase")
        require(integer(evidence["phase_unix_ns"]) and integer(evidence["elapsed_ns"])
                and integer(evidence["bytes_written"]) and type(evidence["may_have_dispatched"]) is bool
                and isinstance(evidence["error_chain"], list)
                and all(isinstance(v, str) for v in evidence["error_chain"]), "RPC field types")
        require(evidence["caller"] == self.started.get("caller_member_identity"), "RPC caller changed")
        raw_frame = evidence["raw_frame"]
        require(raw_frame is None or (isinstance(raw_frame, list) and len(raw_frame) <= MAX_LINE
                and all(type(v) is int and 0 <= v <= 255 for v in raw_frame)), "raw frame type/bytes")
        # SDK counts serde bytes with journal_sequence charged at max_records,
        # plus one newline, before reservation. Preserve the original field order.
        charged = re.sub(rb'("journal_sequence"\s*:\s*)[0-9]+', rb'\g<1>65536', raw_evidence)
        require(self.rpc_bytes_lower_bound + len(charged) + 1 <= 1073741824,
                "SDK evidence exceeds configured byte quota")
        if phase == "invoked":
            require(rid not in self.rpcs and evidence["raw_frame"] is None
                    and evidence["may_have_dispatched"] is False, "RPC invocation")
            rpc = {"logical_id": name, "evidence": [], "request": None, "response": None}
        else:
            rpc = self.rpcs.get(rid)
        require(rpc is not None and rpc["logical_id"] == name, "RPC lacks original invocation")
        earlier = rpc["evidence"]
        if earlier:
            first, last = earlier[0], earlier[-1]
            require(sequence > last["journal_sequence"] and evidence["elapsed_ns"] >= last["elapsed_ns"]
                    and evidence["bytes_written"] >= last["bytes_written"]
                    and (not last["may_have_dispatched"] or evidence["may_have_dispatched"]),
                    "RPC monotonic fields")
            require(all(evidence[k] == first[k] for k in ("operation", "caller", "destination")),
                    "RPC identity changes")
            require(last["phase"] != "return_boundary_error_after_evidence", "RPC after return boundary")
            if last["phase"].startswith("validated_transport_result_"):
                require(phase == "return_boundary_error_after_evidence", "RPC after terminal transport")
            require(phase not in [e["phase"] for e in earlier], "duplicate RPC phase")
            allowed = {
                "invoked": {"request_frame_before_dispatch", "validated_transport_result_error"},
                "request_frame_before_dispatch": {"response_frame_before_decode", "partial_or_rejected_response_frame",
                                                  "exchange_error_before_cancellation_override", "validated_transport_result_error"},
                "response_frame_before_decode": {"exchange_error_before_cancellation_override",
                                                  "validated_transport_result_ok", "validated_transport_result_error"},
                "partial_or_rejected_response_frame": {"exchange_error_before_cancellation_override", "validated_transport_result_error"},
                "exchange_error_before_cancellation_override": {"validated_transport_result_error"},
                "validated_transport_result_ok": {"return_boundary_error_after_evidence"},
                "validated_transport_result_error": {"return_boundary_error_after_evidence"},
            }
            require(phase in allowed.get(last["phase"], set()), "RPC phase order")
        if phase == "request_frame_before_dispatch":
            require(len(earlier) == 1 and rpc["request"] is None, "request phase order")
            request = frame(evidence["raw_frame"])
            require(request["request_id"] == evidence["request_id"]
                    and request["caller_node_id"] == evidence["caller"]
                    and request["destination_node_id"] == evidence["destination"], "request frame identity")
            command = self.commands[name]
            body = request["request"]
            operation = evidence["operation"]
            if operation == "event_append":
                require(command["operation"] == "append", "write RPC attributed to read")
                actual = body["ApplicationEvent"]["operation"]
                require(equal(actual, {k: command[k] for k in ("operation", "namespace", "key", "value")}),
                        "APP bytes differ from original write")
            elif operation == "native_event_get":
                actual = body["NativeEventGet"]
                require(command["operation"] == "strong_read" and actual["mode"] == "strong"
                        and actual["namespace"] == command["namespace"] and actual["key"] == command["key"],
                        "Strong request mismatch/downgrade")
            else:
                require(operation in {"topology_discovery", "metadata_snapshot_discovery"},
                        "unexpected management/other RPC")
                require(body == {"topology_discovery": "FetchTopology",
                                 "metadata_snapshot_discovery": "FetchBootstrapSnapshot"}[operation],
                        "discovery frame mismatch")
            rpc["request"] = {k: request[k] for k in ("protocol_version", "cluster_id", "request_id",
                                                     "caller_node_id", "destination_node_id")}
            rpc["request"]["expected_native_destination"] = request.get("expected_native_destination")
            if operation == "native_event_get":
                rpc["request"]["read_correlation"] = {
                    k: body["NativeEventGet"][k] for k in ("business_request_id", "challenge")}
        elif phase in {"response_frame_before_decode", "partial_or_rejected_response_frame"}:
            require(rpc["request"] is not None and rpc["response"] is None, "response without unique request")
            if phase == "response_frame_before_decode":
                response = frame(evidence["raw_frame"])
                require(isinstance(response, dict) and isinstance(response.get("response"), dict),
                        "response envelope shape")
                # This is raw database reply DATA, not an application ACK. It may
                # subsequently fail identity, cancellation or evidence validation.
                ok = response.get("response", {}).get("Ok", {})
                if isinstance(ok, dict) and ok.get("ApplicationEvent", {}).get("result") == "appended":
                    self.raw_success.append({"attempt_id": rid, "logical_id": name,
                                             "source_node_id": response.get("source_node_id"),
                                             "request_id": response.get("request_id"),
                                             "classification": "unvalidated_raw_database_success"})
                projection = {k: response.get(k) for k in ("protocol_version", "cluster_id", "request_id", "source_node_id")}
                projection["has_ok"] = "Ok" in response["response"]
                projection["actual_native_source"] = response.get("actual_native_source")
                if isinstance(ok, dict) and "ApplicationEvent" in ok:
                    projection["app_reply"] = ok["ApplicationEvent"]
                if isinstance(ok, dict) and "NativeEventGet" in ok:
                    reply = ok["NativeEventGet"]
                    result = reply.get("result", {})
                    value = result.get("value", {})
                    present = value.get("presence") == "present"
                    absent = value == {"presence": "absent"}
                    projection["native_reply"] = {
                        "business_request_id": reply.get("business_request_id"), "challenge": reply.get("challenge"),
                        "kind": result.get("kind"), "typed_presence": present or absent,
                        "found": present, "has_value": "value" in value,
                        "value_comparison_digest": typed_digest(value.get("value"))}
                rpc["response"] = projection
        elif phase == "validated_transport_result_ok":
            require(rpc["request"] is not None and rpc["response"] is not None, "OK lacks full frames")
            request, response = rpc["request"], rpc["response"]
            require(all(request[k] == response[k] for k in ("protocol_version", "cluster_id", "request_id"))
                    and response["source_node_id"] == request["destination_node_id"]
                    and response["has_ok"], "validated response correlation")
            if request.get("expected_native_destination") is not None:
                require(equal(request["expected_native_destination"], response.get("actual_native_source")),
                        "validated response native birth correlation")
            require(evidence["error_chain"] == [], "transport success with error chain")
        if phase not in {"request_frame_before_dispatch", "response_frame_before_decode",
                         "partial_or_rejected_response_frame"}:
            # Later return/error phases may retain a response frame when receive
            # evidence itself failed. Do not reject that valid SDK custody.
            require(evidence["raw_frame"] is None or isinstance(evidence["raw_frame"], list), "raw frame type")
        # Full raw-frame arrays and discovery bodies stay in the original file.
        retained = {k: v for k, v in evidence.items() if k not in {"raw_frame", "error_chain"}}
        retained["journal_record_sequence"] = len(self.rows) + 1
        retained["error_chain_entries"] = len(evidence["error_chain"])
        retained["raw_frame_bytes"] = len(raw_frame) if raw_frame is not None else 0
        earlier.append(retained)
        self.rpcs[rid] = rpc
        self.reservations.add(sequence)
        self.rpc_bytes_lower_bound += len(charged) + 1

    def terminal(self, data):
        name, outcome, detail = data["logical_id"], data["outcome"], data["detail"]
        require(isinstance(detail, dict), "terminal detail object")
        require(name in self.attempts and name not in self.terminals, "terminal attribution/order")
        command = self.commands[name]
        if command["operation"] == "append":
            require(outcome in {"acknowledged", "write_error_unresolved"}, "write outcome")
            if outcome == "acknowledged":
                ack = self.acks.get(name)
                require(ack is not None and equal(detail["sdk_reply"], ack["reply"])
                        and detail["durable_ack_receipt"] == {k: ack[k] for k in ("sequence", "record_sha256")},
                        "ACK terminal receipt/reply mismatch")
            else:
                require(name not in self.acks and detail.get("timeout_is_rollback") is False
                        and detail.get("outer_write_retry") is False and "error" in detail,
                        "unresolved write reclassified")
        elif outcome != "read_error":
            reply = detail["sdk_reply"]
            require(set(reply) == {"found", "value"} and type(reply["found"]) is bool
                    and (reply["found"] or reply["value"] is None), "typed read presence")
            prior = command.get("acknowledged_by") in self.acks
            expected = ("exact_match" if reply["found"] and equal(reply["value"], command["expected"])
                        else "acknowledged_value_absent" if not reply["found"] and prior
                        else "expected_value_absent" if not reply["found"] else "payload_mismatch")
            require(outcome == expected and detail.get("prior_ack_observed") is prior,
                    "Strong outcome/ACK classification")
        else:
            require("error" in detail, "read error evidence")
        require(integer(data["sdk_elapsed_ns"])
                and integer(data["client_elapsed_before_terminal_fsync_ns"]), "latency samples")
        self.terminals[name] = data

    def report(self, witness=None, exit_status=None):
        gaps = list(self.gaps)
        if self.reservations:
            missing = sorted(set(range(1, max(self.reservations) + 1)) - self.reservations)
            if missing:
                gaps.append({"missing_sdk_reservation_sequences": missing})
        elif self.attempts:
            gaps.append({"missing_all_sdk_evidence": True})
        rpc_report = []
        successful_by_name = {}
        for rid, rpc in self.rpcs.items():
            evidence = rpc["evidence"]
            phases = [v["phase"] for v in evidence]
            finished = any(v.startswith("validated_transport_result_") for v in phases)
            if not finished:
                gaps.append({"unfinished_rpc": rid, "last_phase": phases[-1]})
            ok = "validated_transport_result_ok" in phases and "return_boundary_error_after_evidence" not in phases
            if ok:
                successful_by_name.setdefault(rpc["logical_id"], []).append(rpc)
            rpc_report.append({"attempt_id": rid, "logical_id": rpc["logical_id"], "phases": phases,
                               "reservation_sequences": [e["journal_sequence"] for e in evidence],
                               "journal_record_sequences": [e["journal_record_sequence"] for e in evidence],
                               "may_have_dispatched": evidence[-1]["may_have_dispatched"]})
        for name, terminal in self.terminals.items():
            if terminal["outcome"] in {"acknowledged", "exact_match", "payload_mismatch",
                                       "acknowledged_value_absent", "expected_value_absent"}:
                needed = "event_append" if self.commands[name]["operation"] == "append" else "native_event_get"
                candidates = [r for r in successful_by_name.get(name, []) if r["evidence"][0]["operation"] == needed]
                matched = False
                for rpc in candidates:
                    if needed == "event_append":
                        reply = rpc["response"].get("app_reply", {})
                        matched |= reply.get("result") == "appended" and equal(
                            {"inserted": reply.get("inserted")}, terminal["detail"]["sdk_reply"])
                    else:
                        reply = rpc["response"].get("native_reply", {})
                        request = rpc["request"]["read_correlation"]
                        terminal_reply = terminal["detail"]["sdk_reply"]
                        matched |= (reply.get("business_request_id") == request.get("business_request_id")
                                    and reply.get("challenge") == request.get("challenge")
                                    and reply.get("kind") == "value" and reply.get("typed_presence")
                                    and (not reply["found"] or reply["has_value"])
                                    and reply["found"] is terminal_reply["found"]
                                    and reply["value_comparison_digest"] == typed_digest(terminal_reply["value"]))
                if not matched:
                    gaps.append({"logical_success_lacks_validated_operation_rpc": name})
        if self.completed is None:
            gaps.append({"missing_run_completed": True})
        completion_witness = False
        if witness is not None:
            last = self.rows[-1] if self.rows else None
            expected_exit = (2 if self.completed and self.completed["observer_fenced"] else
                             0 if self.completed and self.completed["all_operations_succeeded"] else 1)
            require(self.completed is not None and equal(witness.get("summary"), self.completed)
                    and witness.get("completion_receipt") == {"sequence": last["sequence"],
                                                             "record_sha256": self.previous}
                    and exit_status == expected_exit, "external completion witness/exit mismatch")
            completion_witness = True
        elif exit_status is not None:
            gaps.append({"exit_status_without_linked_completion_witness": exit_status})
        if not completion_witness:
            gaps.append({"trailing_record_fsync_return_unproven": len(self.rows) or None})
        confirmed = [name for name, ack in self.acks.items()
                     if ack["sequence"] < len(self.rows) or completion_witness]
        unfinished = [name for name in self.attempts if name not in self.terminals]
        unresolved = [name for name in self.attempts if self.commands[name]["operation"] == "append"
                      and name not in confirmed]
        failures = [name for name, terminal in self.terminals.items()
                    if terminal["outcome"] not in {"acknowledged", "exact_match"}]
        return {"format": 1, "qualification_status": "not_qualified", "run_id": self.plan["run_id"],
                "integrity": "invalid" if self.defects else "valid_prefix",
                "complete_evidence": not self.defects and not gaps,
                "valid_records": len(self.rows), "last_valid_record_sha256": self.previous,
                "defects": self.defects, "evidence_gaps": gaps,
                "completion_witness_linked": completion_witness,
                "external_witness_authenticity_independently_verified": False,
                "run_manifest_claims": {k: self.started.get(k) for k in (
                    "build_source_claim", "client_binary_sha256", "core_version", "debug_assertions",
                    "config_sha256", "topology_sha256", "plan_sha256", "caller_member_identity",
                    "authorization_contract", "client_path", "typed_read_mode", "request_timeout")}
                    if self.started else {},
                "archived_inputs": self.archived_inputs,
                "journal_input": getattr(self, "input_read", {}),
                "attempted": self.attempts, "visible_sdk_acks": self.acks,
                "fsync_return_linked_sdk_acks": confirmed,
                "fsync_return_unproven_sdk_acks": sorted(set(self.acks) - set(confirmed)),
                "terminal_outcomes": self.terminals, "failed": failures,
                "unfinished": unfinished, "unresolved_writes": unresolved,
                "unattempted": [name for name in self.commands if name not in self.attempts],
                "unattempted_label": "not observed in valid prefix; later effects unknown if integrity invalid",
                "untrusted_tail_logical_observations": self.untrusted_tail,
                "untrusted_tail_kind_counts": dict(self.tail_counts),
                "untrusted_tail_observations_omitted_at_explicit_cap": self.tail_omitted,
                "retries": "no outer replay permitted; SDK retries retained as distinct RPC attempts",
                "rpc_attempt_counts": dict(Counter(rpc["logical_id"] for rpc in self.rpcs.values())),
                "rpc_attempt_counts_by_operation": {name: dict(Counter(
                    rpc["evidence"][0]["operation"] for rpc in self.rpcs.values() if rpc["logical_id"] == name))
                    for name in self.attempts},
                "sdk_rpc_attempts": rpc_report, "raw_database_success_replies": self.raw_success,
                "sdk_record_quota": 65536, "sdk_record_reservations_observed": len(self.reservations),
                "sdk_byte_quota": 1073741824, "sdk_charge_bytes_observed": self.rpc_bytes_lower_bound,
                "latencies_are_physical_RPO_RTO_or_capacity_proof": False,
                "cross_run_reconciliation_complete": False}


def verify(plan_path, journal_path, witness_path=None, exit_status=None, max_line=MAX_LINE):
    require(1 <= max_line <= MAX_LINE, "line bound must be 1..64MiB")
    verifier = Verifier(bounded(plan_path, MAX_PLAN))
    total = 0
    rejected = False
    physical_rows = 0
    file_hash = hashlib.sha256()
    with Path(journal_path).open("rb") as source:
        while True:
            try:
                line = source.readline(min(max_line, max(0, MAX_BYTES - total)) + 1)
            except OSError as error:
                verifier.defects.append({"journal_read_error": str(error)[:512]})
                break
            if not line:
                break
            total += len(line)
            physical_rows += 1
            file_hash.update(line)
            try:
                require(total <= MAX_BYTES and len(line) <= max_line and physical_rows <= MAX_ROWS,
                        "journal/line/record bound exceeded")
                require(line.endswith(b"\n"), "torn/nonterminated trailing row")
                if not rejected:
                    verifier.row(line[:-1])
                    continue
            except (ValueError, KeyError, TypeError, IndexError, AttributeError, RecursionError) as error:
                verifier.defects.append({"line": len(verifier.rows) + 1, "error": str(error)[:512],
                                         "rejected_line_sha256": digest(line),
                                         "rejected_line_bytes": len(line)})
                rejected = True
                if total > MAX_BYTES or len(line) > max_line or physical_rows > MAX_ROWS:
                    break
            # Never regain trusted-prefix status after any invalid/torn row.
            # Retain logical observations from the rest separately, including
            # ACKs/failures that an intact-looking later line cannot authenticate.
            try:
                untrusted = decode(line)
                kind = untrusted.get("kind", "unknown")
                if kind not in {"logical_attempt", "sdk_ack_observed", "logical_terminal", "run_completed", "sdk_rpc", "run_started"}:
                    kind = "unknown"
                verifier.tail_counts[kind] += 1
                if kind in {"logical_attempt", "sdk_ack_observed", "logical_terminal", "run_completed"}:
                    data = untrusted.get("data", {})
                    observation = {"kind": kind, "raw_line_sha256": digest(line[:-1]),
                                   "claimed_sequence": untrusted.get("sequence") if integer(untrusted.get("sequence")) else None}
                    if kind == "logical_attempt":
                        command = data.get("command", {})
                        observation.update(logical_id=command.get("logical_id"),
                                           operation=command.get("operation"), payload_sha256=data.get("payload_sha256"))
                    else:
                        observation.update(logical_id=data.get("logical_id"), outcome=data.get("outcome"))
                    for key in ("logical_id", "operation", "payload_sha256", "outcome"):
                        if key in observation and observation[key] is not None and (
                                not isinstance(observation[key], str) or len(observation[key]) > 4096):
                            observation[key] = None
                            observation["invalid_or_oversized_untrusted_fields"] = True
                    if len(verifier.untrusted_tail) < 4096:
                        verifier.untrusted_tail.append(observation)
                    else:
                        verifier.tail_omitted += 1
            except (ValueError, TypeError, AttributeError, RecursionError):
                verifier.tail_counts["unparseable"] += 1
    verifier.input_read = {"bytes_read": total, "read_bytes_sha256": file_hash.hexdigest(),
                           "within_total_bound": total <= MAX_BYTES}
    witness = None
    if witness_path:
        try:
            witness = decode(bounded(witness_path, 1024 * 1024))
        except (OSError, ValueError, RecursionError) as error:
            verifier.defects.append({"completion_witness_input_error": str(error)[:512]})
    for filename, key, limit in (("config.json", "config_sha256", 1024**2),
                                 ("topology.json", "topology_sha256", 16 * 1024**2)):
        try:
            actual = digest(bounded(Path(journal_path).with_name(filename), limit))
            matched = verifier.started is not None and verifier.started.get(key) == actual
            verifier.archived_inputs[filename] = {"sha256": actual, "manifest_hash_matches": matched}
            if not matched:
                verifier.defects.append({"archived_input_hash_mismatch": filename})
        except (OSError, ValueError) as error:
            verifier.gaps.append({"archived_input_unavailable": filename, "error": str(error)[:512]})
    try:
        return verifier.report(witness, exit_status)
    except (ValueError, KeyError, TypeError, IndexError, AttributeError) as error:
        verifier.defects.append({"completion_witness_error": str(error)[:512]})
        return verifier.report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, help="defaults to plan.json next to journal")
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--completion-witness", type=Path)
    parser.add_argument("--exit-status", type=int)
    parser.add_argument("--max-line-bytes", type=int, default=MAX_LINE)
    args = parser.parse_args()
    try:
        report = verify(args.plan or args.journal.with_name("plan.json"), args.journal,
                        args.completion_witness, args.exit_status, args.max_line_bytes)
    except (OSError, ValueError, TypeError, RecursionError) as error:
        print(json.dumps({"qualification_status": "not_qualified", "integrity": "invalid",
                          "complete_evidence": False, "input_error": str(error)[:512]}, sort_keys=True))
        return 2
    # Preserve arbitrary-precision JSON decimal observations as explicit typed
    # lexical DATA in the report; never round them through a binary float.
    print(json.dumps(report, sort_keys=True,
                     default=lambda value: {"json_decimal": str(value)} if isinstance(value, Decimal)
                     else (_ for _ in ()).throw(TypeError("unknown report type"))))
    return 2 if report["integrity"] == "invalid" else 0 if report["complete_evidence"] else 1


if __name__ == "__main__":
    sys.exit(main())
