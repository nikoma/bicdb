#!/usr/bin/env python3
"""Witness receipt/schema fencing controls. Synthetic endpoint, NOT TLS/DB proof.

The prior real mTLS/chain controls are separate. No fake network/identity here
is authority: this tests the actual controller's malformed-reply refusal.
"""
import struct
import unittest
from decimal import Decimal
from unittest.mock import patch

import collector as C
import controller as W
import verify as V
import test_external as T

CLIENT = "2" * 64
SERVER = C.sha(b"synthetic-peer-DER")


def data():
    payload = C.encode({"kind": "process_launch", "observed_command": ["synthetic-only"]})
    h = {"format": 1, "action": "witness", "run_id": T.RUN, "producer_id": T.PRODUCER,
         "session_id": T.SESSION, "payload_sha256": C.sha(payload), "payload_bytes": len(payload)}
    r = {**h, "client_certificate_sha256": CLIENT, "collector_certificate_sha256": SERVER,
         "receipt_sequence": 8, "witness_sequence": 2}
    return h, r


class WitnessSchemaControls(unittest.TestCase):
    def test_valid_exact_receipt_preserves_legitimate_interleaved_counters(self):
        h, r = data()
        self.assertEqual(W.require_witness_receipt(r, h, CLIENT, SERVER, 3, 1), (8, 2))

    def test_every_required_field_missing_or_unknown_null_is_refused(self):
        h, r = data()
        for field in r:
            bad = {k: v for k, v in r.items() if k != field}
            with self.assertRaises(ValueError): W.require_witness_receipt(bad, h, CLIENT, SERVER, 0, 0)
        for field in ("previous_record_sha256", "unknown_field", "sequence", "name"):
            with self.assertRaises(ValueError):
                W.require_witness_receipt({**r, field: None}, h, CLIENT, SERVER, 0, 0)

    def test_boolean_decimal_null_and_stale_counters_are_refused(self):
        h, r = data()
        for field in ("format", "payload_bytes", "receipt_sequence", "witness_sequence"):
            for value in (True, Decimal(r[field]), None):
                with self.assertRaises(ValueError):
                    W.require_witness_receipt({**r, field: value}, h, CLIENT, SERVER, 0, 0)
        with self.assertRaises(ValueError): W.require_witness_receipt(r, h, CLIENT, SERVER, 8, 0)
        with self.assertRaises(ValueError): W.require_witness_receipt(r, h, CLIENT, SERVER, 0, 2)

    def test_witness_and_global_counters_are_bounded_by_u64_wire_domain(self):
        h, r = data()
        for field in ("receipt_sequence", "witness_sequence"):
            for oversized in (2**64, 2**100):
                with self.assertRaises(ValueError):
                    W.require_witness_receipt({**r, field: oversized}, h, CLIENT, SERVER, 0, 0)
        boundary = {**r, "receipt_sequence": W.U64_MAX, "witness_sequence": W.U64_MAX}
        self.assertEqual(W.require_witness_receipt(boundary, h, CLIENT, SERVER, 0, 0),
                         (W.U64_MAX, W.U64_MAX))

    def test_wrong_session_hash_or_peer_is_refused(self):
        h, r = data()
        for field in ("session_id", "payload_sha256", "client_certificate_sha256", "collector_certificate_sha256"):
            with self.assertRaises(ValueError):
                W.require_witness_receipt({**r, field: "wrong"}, h, CLIENT, SERVER, 0, 0)

    def test_duplicate_raw_reply_fields_are_refused_before_validation(self):
        _, r = data()
        duplicate = b'{"format":1,' + C.encode(r)[1:]
        with self.assertRaises(ValueError): V.decode(duplicate)

    def test_actual_record_method_fences_after_incomplete_endpoint_receipt(self):
        h, reply = data()
        del reply["receipt_sequence"]  # This was accepted before the new schema check.
        wire = C.encode(reply)
        class Endpoint:
            def __init__(self): self.response = struct.pack("!I", len(wire)) + wire
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def settimeout(self, _): pass
            def connect(self, _): pass
            def do_handshake(self): pass
            def getpeercert(self, **_): return b"synthetic-peer-DER"
            def send(self, raw): return len(raw)
            def recv(self, count):
                result, self.response = self.response[:count], self.response[count:]
                return result
        endpoint = Endpoint()
        class Context:
            def wrap_socket(self, *_, **__): return endpoint
        sink = W.WitnessClient.__new__(W.WitnessClient)
        sink.config = {k: h[k] for k in ("run_id", "producer_id", "session_id")}
        sink.config.update(io_timeout_ms=1000, server_name="localhost", server_certificate_sha256=SERVER)
        sink.host, sink.port = "127.0.0.1", 1
        sink.context, sink.client_sha, sink.failed = Context(), CLIENT, False
        sink.last_receipt = sink.last_witness = 0
        with patch.object(W.socket, "socket", return_value=endpoint) as connected:
            with self.assertRaisesRegex(ValueError, "exact witness receipt field set"):
                sink.record({"kind": "process_launch", "observed_command": ["synthetic-only"]})
            self.assertTrue(sink.failed)
            self.assertEqual((sink.last_receipt, sink.last_witness), (0, 0))
            with self.assertRaisesRegex(ValueError, "fenced"):
                sink.record({"kind": "process_launch", "observed_command": ["synthetic-only"]})
            self.assertEqual(connected.call_count, 1, "sticky fence must precede another endpoint connection")

    def test_actual_record_framing_consumes_original_window_before_connect(self):
        h, _ = data()
        sink = W.WitnessClient.__new__(W.WitnessClient)
        sink.config = {k: h[k] for k in ("run_id", "producer_id", "session_id")}
        sink.config.update(io_timeout_ms=1000, server_name="localhost", server_certificate_sha256=SERVER)
        sink.host, sink.port, sink.failed = "127.0.0.1", 1, False
        sink.last_receipt = sink.last_witness = 0
        clock, original_encode = [0.0], C.encode
        def slow_framing(value):
            clock[0] = 2.0
            return original_encode(value)
        with patch.object(W.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(C, "encode", side_effect=slow_framing), \
             patch.object(W.socket, "socket") as connected:
            with self.assertRaises(TimeoutError):
                sink.record({"kind": "process_launch", "observed_command": ["synthetic-only"]})
            self.assertTrue(sink.failed)
            connected.assert_not_called()


if __name__ == "__main__":
    unittest.main()
