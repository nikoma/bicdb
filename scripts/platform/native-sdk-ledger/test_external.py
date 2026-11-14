#!/usr/bin/env python3
"""New local persistence/protocol controls, NOT BicDB/native/physical HA tests.

Synthetic SDK rows use unchanged original fixtures. TLS test makes real
loopback mTLS exchanges with ephemeral test-only certificates, not DB keys.
"""
import argparse
import copy
import contextlib
import io
import json
import os
from pathlib import Path
import queue
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import collector as C
import controller as W
import test_verify as T
import verify as V
import verify_external as E

RUN = T.RUN
PRODUCER = "019a80b8-6594-7000-8000-000000000010"
SESSION = "019a80b8-6594-7000-8000-000000000011"
CLIENT, CONTROLLER, SERVER, MEMBER = (c * 64 for c in "2345")


class Harness:
    def __init__(self, path, value=None, unfinished=False):
        self.path = path
        self.ledger_config = {"producer_id": PRODUCER, "session_id": SESSION,
                              "server_certificate_sha256": SERVER, "key_free_inputs_reviewed": True,
                              "io_timeout_ms": 1000}
        self.f = T.Fixture(value)
        self.f.records[0]["data"]["external_sink"] = {
            "mode": "external_fsync_required", "run_id": RUN, "producer_id": PRODUCER,
            "session_id": SESSION, "collector_certificate_sha256": SERVER,
            "ledger_client_certificate_sha256": CLIENT, "io_timeout_ms": 1000,
            "throughput_benchmark": False}
        self.f.rebuild()
        if unfinished:
            self.f.attempt("w")
            self.f.rpc("w")  # Possible successful API gap, deliberately no explicit ACK row.
        else:
            self.f.write()
            self.f.complete()
        self.inputs = {"config.json": b"{}", "topology.json": b"{}",
                       "plan.json": self.f.plan_raw, "ledger-config.json": C.encode(self.ledger_config)}
        self.manifest = {"format": 1, "run_id": RUN, "producer_id": PRODUCER, "session_id": SESSION,
                         "source_sha": "a" * 40, "binary_sha256": "1" * 64,
                         "member_node_id": "driver", "member_certificate_sha256": MEMBER,
                         "ledger_client_certificate_sha256": CLIENT,
                         "collector_certificate_sha256": SERVER, "qualification_status": "not_qualified",
                         "throughput_benchmark": False,
                         "inputs": {n: {"bytes": len(raw), "sha256": C.sha(raw)} for n, raw in self.inputs.items()}}
        self.store = C.Store(path, RUN, PRODUCER, CLIENT, CONTROLLER, SERVER)
        self.producer_receipts, self.closed = [], False

    def close(self):
        if not self.closed:
            self.store.close()
            self.closed = True

    def header(self, action, payload, **extra):
        return {"format": 1, "action": action, "run_id": RUN, "producer_id": PRODUCER,
                "session_id": SESSION, "payload_sha256": C.sha(payload), "payload_bytes": len(payload), **extra}

    def post(self, action, payload, peer=CLIENT, **extra):
        r = self.store.process(self.header(action, payload, **extra), payload, peer)
        if action != "witness":
            self.producer_receipts.append(C.encode(r))
        return r

    def start(self):
        self.post("start", C.encode(self.manifest))
        for name, payload in self.inputs.items():
            self.post("input", payload, name=name)

    def rows(self):
        for row, raw in zip(self.f.records, self.f.raw):
            self.post("row", raw + b"\n", sequence=row["sequence"],
                      previous_record_sha256=row["previous_record_sha256"])

    def snapshot(self, complete=True):
        completion = None
        if complete:
            raw = C.encode(self.f.witness)
            command = ["synthetic-fixture-only"]
            witness = {"kind": "process_exit", "observed_command": command,
                       "command_sha256": C.sha(C.encode(command)), "output_complete": True,
                       "timed_out": False, "exit_status": 0, "stdout_hex": raw.hex()}
            r = self.post("witness", C.encode(witness), CONTROLLER)
            completion = {"controller_witness_sequence": r["witness_sequence"],
                          "command_sha256": witness["command_sha256"], "exit_status": 0}
            (self.path / "completion-witness.json").write_bytes(raw)
        self.close()
        (self.path / "producer-receipts.jsonl").write_bytes(b"\n".join(self.producer_receipts) + b"\n")
        files = E.FILES | ({"completion-witness.json"} if complete else set())
        index = {k: self.manifest[k] for k in ("format", "run_id", "producer_id", "session_id", "source_sha",
                 "binary_sha256", "member_node_id", "member_certificate_sha256",
                 "collector_certificate_sha256", "ledger_client_certificate_sha256")}
        index.update(controller_certificate_sha256=CONTROLLER, completion=completion,
                     files={n: E.hash_file(self.path / n, C.MAX_TOTAL) for n in files})
        self.index = self.path.parent / (self.path.name + "-index.json")
        self.index.write_bytes(C.encode(index))
        return self.index


class PersistenceControls(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = Harness(Path(self.tmp.name) / "run")
        self.addCleanup(self.h.close)

    def test_exact_original_rows_external_receipts_and_completion(self):
        self.h.start(); self.h.rows()
        report = E.check(self.h.snapshot(), self.h.path)
        self.assertTrue(report["functional_evidence_complete"], report)
        self.assertEqual(report["required_reconciliation_original_write_ids"], ["w"])
        self.assertEqual(report["known_sdk_ok_write_ids"], ["w"])
        self.assertFalse(report["all_required_values_reconciled"])
        self.assertEqual(report["qualification_status"], "not_qualified")

    def test_actual_fsync_precedes_returned_row_receipt(self):
        self.h.start()
        calls = []
        def sync(fd):
            calls.append(fd)
            os.fsync(fd)
        self.h.store.sync = sync
        raw = self.h.f.raw[0] + b"\n"
        r = self.h.post("row", raw, sequence=1, previous_record_sha256=None)
        self.assertEqual(calls, [self.h.store.journal.fileno(), self.h.store.receipts.fileno()])
        self.assertEqual(r["durable_end_offset"], len(raw))
        self.assertEqual((self.h.path / "journal.jsonl").read_bytes(), raw)

    def test_payload_write_failure_fences_without_receipt(self):
        self.h.start()
        count = self.h.store.receipt_sequence
        with patch.object(C, "exact_write", side_effect=OSError("injected actual write error")):
            with self.assertRaises(OSError):
                self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)
        self.assertTrue(self.h.store.poisoned)
        self.assertEqual(self.h.store.receipt_sequence, count)

    def test_partial_actual_write_then_failure_is_retained_not_acked(self):
        self.h.start()
        original = C.exact_write
        def partial(file, raw, check=lambda: None):
            if file is self.h.store.journal:
                original(file, raw[:7], check)
                raise OSError("injected partial write")
            original(file, raw, check)
        with patch.object(C, "exact_write", side_effect=partial):
            with self.assertRaises(OSError):
                self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)
        self.assertEqual(len((self.h.path / "journal.jsonl").read_bytes()), 7)
        self.assertEqual(self.h.store.sequence, 0)
        self.assertTrue(self.h.store.poisoned)

    def test_row_fsync_failure_fences_without_success(self):
        self.h.start()
        self.h.store.sync = lambda _: (_ for _ in ()).throw(OSError("injected fsync error"))
        with self.assertRaises(OSError):
            self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)
        self.assertEqual(self.h.store.sequence, 0)
        self.assertTrue(self.h.store.poisoned)

    def test_deadline_crossing_actual_row_fsync_cannot_publish_receipt(self):
        self.h.start()
        raw = self.h.f.raw[0] + b"\n"
        clock, calls = [0.0], []
        def sync(fd):
            os.fsync(fd)
            calls.append(fd)
            clock[0] = 2.0
        self.h.store.sync = sync
        with patch.object(C.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(TimeoutError):
                self.h.store.process(self.h.header("row", raw, sequence=1, previous_record_sha256=None),
                                     raw, CLIENT, end=1.0)
        self.assertEqual(calls, [self.h.store.journal.fileno()])
        self.assertTrue(self.h.store.poisoned)
        self.assertEqual(self.h.store.sequence, 0)
        self.assertEqual((self.h.path / "journal.jsonl").read_bytes(), raw)

    def test_receipt_fsync_failure_preserves_row_without_producer_ack(self):
        self.h.start()
        def sync(fd):
            if fd == self.h.store.receipts.fileno():
                raise OSError("injected receipt fsync failure")
            os.fsync(fd)
        self.h.store.sync = sync
        with self.assertRaises(OSError):
            self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)
        self.assertEqual(self.h.store.sequence, 1)
        self.assertTrue(self.h.store.poisoned)
        self.assertEqual(len(self.h.producer_receipts), 5)  # start + four original inputs only.

    def test_lost_receipt_does_not_become_observed_delivery(self):
        self.h.start(); self.h.rows()
        del self.h.producer_receipts[-1]
        report = E.check(self.h.snapshot(), self.h.path)
        self.assertFalse(report["external_receipt_chain_complete"])
        self.assertFalse(report["functional_evidence_complete"])
        self.assertTrue(report["producer_unproven_receipt_delivery_rows"])
        self.assertEqual(report["required_reconciliation_original_write_ids"], ["w"])

    def test_sdk_ok_gap_keeps_raw_success_in_required_ack_corpus(self):
        self.h.close()
        h = Harness(Path(self.tmp.name) / "gap", unfinished=True)
        self.addCleanup(h.close)
        h.start(); h.rows()
        report = E.check(h.snapshot(complete=False), h.path)
        self.assertEqual(report["known_sdk_ok_write_ids"], [])
        self.assertTrue(report["possible_success_raw_replies"])
        self.assertEqual(report["required_reconciliation_original_write_ids"], ["w"])
        self.assertFalse(report["functional_evidence_complete"])
        self.assertFalse(report["missing_explicit_ack_record_proves_no_sdk_ok"])

    def test_wrong_identity_fences_not_recoverable_by_later_good_peer(self):
        with self.assertRaises(ValueError):
            self.h.post("start", C.encode(self.h.manifest), CONTROLLER)
        self.assertTrue(self.h.store.poisoned)
        with self.assertRaises(ValueError): self.h.start()

    def test_db_member_and_ledger_roles_cannot_reuse_identity(self):
        with self.assertRaises(ValueError):
            C.Store(Path(self.tmp.name) / "same-peer", RUN, PRODUCER, CLIENT, CONTROLLER, CLIENT)
        self.assertFalse((Path(self.tmp.name) / "same-peer").exists())
        self.h.manifest["member_certificate_sha256"] = CONTROLLER
        with self.assertRaises(ValueError): self.h.start()
        self.assertTrue(self.h.store.poisoned)

    def test_conflicting_duplicate_never_rewrites_original(self):
        self.h.start()
        raw = self.h.f.raw[0] + b"\n"
        self.h.post("row", raw, sequence=1, previous_record_sha256=None)
        with self.assertRaises(ValueError):
            self.h.post("row", raw, sequence=1, previous_record_sha256=None)
        self.assertEqual((self.h.path / "journal.jsonl").read_bytes(), raw)
        self.assertTrue(self.h.store.poisoned)

    def test_quota_fences_without_truncation(self):
        self.h.start()
        self.h.store.max_bytes = self.h.store.total + 1
        with self.assertRaises(ValueError):
            self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)
        self.assertEqual((self.h.path / "journal.jsonl").read_bytes(), b"")
        self.assertTrue(self.h.store.poisoned)

    def test_rpc_row_before_original_inputs_refuses(self):
        self.h.post("start", C.encode(self.h.manifest))
        with self.assertRaises(ValueError):
            self.h.post("row", self.h.f.raw[0] + b"\n", sequence=1, previous_record_sha256=None)

    def test_restart_never_reopens_uncertain_directory(self):
        self.h.close()
        with self.assertRaises(FileExistsError):
            C.Store(self.h.path, RUN, PRODUCER, CLIENT, CONTROLLER, SERVER)

    def test_plan_input_hash_mismatch_fences(self):
        self.h.post("start", C.encode(self.h.manifest))
        with self.assertRaises(ValueError):
            self.h.post("input", b"{}", name="plan.json")
        self.assertTrue(self.h.store.poisoned)

    def test_private_key_pem_never_accepted_as_input(self):
        raw = C.encode({"bad": "-----BEGIN PRIVATE KEY-----test"})
        self.h.inputs["config.json"] = raw
        self.h.manifest["inputs"]["config.json"] = {"bytes": len(raw), "sha256": C.sha(raw)}
        self.h.post("start", C.encode(self.h.manifest))
        with self.assertRaises(ValueError): self.h.post("input", raw, name="config.json")
        self.assertFalse((self.h.path / "config.json").exists())

    def test_forged_offset_even_with_repinned_receipt_file_refuses(self):
        self.h.start(); self.h.rows()
        index = self.h.snapshot()
        path = self.h.path / "collector-receipts.jsonl"
        receipts = [V.decode(raw) for raw in path.read_bytes().splitlines()]
        next(r for r in receipts if r["action"] == "row")["durable_end_offset"] += 1
        path.write_bytes(b"\n".join(C.encode(r) for r in receipts) + b"\n")
        pin = V.decode(index.read_bytes())
        pin["files"][path.name] = E.hash_file(path, C.MAX_TOTAL)
        index.write_bytes(C.encode(pin))
        with self.assertRaises(ValueError): E.check(index, self.h.path)

    def test_producer_receipt_identity_tamper_refuses(self):
        self.h.start(); self.h.rows()
        index = self.h.snapshot()
        p = self.h.path / "producer-receipts.jsonl"
        p.write_bytes(p.read_bytes().replace(CLIENT.encode(), MEMBER.encode()))
        with self.assertRaises(ValueError): E.check(index, self.h.path)

    def test_actually_consumed_witness_bytes_must_match_prior_pin(self):
        self.h.start(); self.h.rows()
        index = self.h.snapshot()
        original = E.hash_file
        def changed_after_hash(path, maximum):
            result = original(path, maximum)
            if path.name == "controller-witnesses.jsonl":
                path.write_bytes(path.read_bytes().replace(b'{', b'{ ', 1))
            return result
        with patch.object(E, "hash_file", side_effect=changed_after_hash):
            with self.assertRaises(ValueError): E.check(index, self.h.path)

    def test_independent_launch_can_precede_producer_start(self):
        r = self.h.post("witness", C.encode({"kind": "process_launch", "observed_command": ["test"]}), CONTROLLER)
        self.assertEqual(r["witness_sequence"], 1)
        self.h.start(); self.h.rows()
        self.assertTrue(E.check(self.h.snapshot(), self.h.path)["functional_evidence_complete"])

    def test_exact_decimal_controller_witness_is_not_reserialized(self):
        payload = b'{"kind":"process_launch","timeout_seconds":0.5000000}'
        r = self.h.post("witness", payload, CONTROLLER)
        self.assertEqual(r["payload_sha256"], C.sha(payload))
        raw = V.decode((self.h.path / "controller-witnesses.jsonl").read_bytes())
        self.assertEqual(bytes.fromhex(raw["original_witness_hex"]), payload)

    def test_missing_input_receipt_delivery_cannot_establish_complete_evidence(self):
        self.h.start(); self.h.rows()
        del self.h.producer_receipts[1]
        report = E.check(self.h.snapshot(), self.h.path)
        self.assertFalse(report["functional_evidence_complete"])
        self.assertFalse(report["external_receipt_chain_complete"])
        self.assertTrue(report["producer_unproven_receipt_delivery_sequences"])

    def test_boolean_format_is_not_accepted_as_integer_one(self):
        self.h.manifest["format"] = True
        with self.assertRaises(ValueError): self.h.start()
        self.assertTrue(self.h.store.poisoned)

    def test_producer_identity_cannot_write_independent_exit_witness(self):
        self.h.start()
        with self.assertRaises(ValueError):
            self.h.post("witness", C.encode({"kind": "process_exit"}), CLIENT)
        self.assertTrue(self.h.store.poisoned)

    def test_incomplete_independent_exit_cannot_link_completion(self):
        self.h.start(); self.h.rows()
        index = self.h.snapshot()
        pin = V.decode(index.read_bytes())
        pin["completion"]["exit_status"] = 255
        index.write_bytes(C.encode(pin))
        with self.assertRaises(ValueError): E.check(index, self.h.path)


class DeadlineControls(unittest.TestCase):
    def test_network_filesystem_is_refused_even_when_paths_exist(self):
        with patch.object(V, "bounded", return_value=b"1 0 0:1 / / rw - nfs server rw\n"):
            with self.assertRaises(ValueError): C.local_storage_contract(Path.cwd())

    def test_memory_filesystem_is_not_a_durable_sink(self):
        with patch.object(V, "bounded", return_value=b"1 0 0:1 / / rw - tmpfs tmpfs rw\n"):
            with self.assertRaises(ValueError): C.local_storage_contract(Path.cwd())

    def test_partial_read_progress_never_renews_original_deadline(self):
        clock = [0.0]
        class Slow:
            def settimeout(self, value): self.timeout = value
            def recv(self, count):
                clock[0] += 0.4
                return b"x"
        sock = Slow()
        with patch.object(C.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(TimeoutError): C.Window(1.0).read(sock, 4)
        self.assertGreaterEqual(clock[0], 1.0)

    def test_write_progress_never_renews_original_deadline(self):
        clock = [0.0]
        class Slow:
            def settimeout(self, value): pass
            def send(self, raw):
                clock[0] += 0.6
                return 1
        with patch.object(C.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(TimeoutError): C.Window(1.0).write(Slow(), b"xxxx")

    def test_torn_header_refuses_no_inferred_success(self):
        class Torn:
            def settimeout(self, value): pass
            def recv(self, count): return b""
        with self.assertRaises(EOFError): C.receive(Torn(), C.Window(C.time.monotonic() + 1))


class ActualControllerChildControls(unittest.TestCase):
    def test_actual_child_stdout_and_exit_are_retained_exactly(self):
        with tempfile.TemporaryDirectory() as directory:
            result = W.observe([sys.executable, "-c", "print('actual-child-output')"], 3, Path(directory))
            self.assertEqual(result["exit_status"], 0)
            self.assertTrue(result["output_complete"])
            self.assertEqual(bytes.fromhex(result["stdout_hex"]), b"actual-child-output\n")
            self.assertIn("actual_local_child_command_only", result["witness_scope"])

    def test_actual_child_deadline_kills_own_group_and_retains_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            result = W.observe([sys.executable, "-c", "import time;time.sleep(10)"], 0.05, Path(directory))
            self.assertTrue(result["timed_out"])
            self.assertFalse(result["output_complete"])
            self.assertIsNotNone(result["exit_status"])
            self.assertLess(result["exit_status"], 0)

    def test_actual_child_output_quota_never_silently_truncates_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            result = W.observe([sys.executable, "-c", "print('x'*100000)"], 3, Path(directory))
            self.assertFalse(result["output_complete"])
            self.assertEqual(result["stdout_bytes"], 16384)
            self.assertEqual(len((Path(directory)/"stdout.bin").read_bytes()), 16384)


@unittest.skipUnless(shutil.which("openssl"), "openssl required for ephemeral mTLS control")
class ActualTlsControl(unittest.TestCase):
    def test_real_pinned_mtls_collector_witness_and_wrong_pin_fence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def command(*argv):
                subprocess.run(["openssl", *argv], cwd=root, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True, timeout=10)
            command("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                    "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=ephemeral-test-ca")
            for name in ("server", "controller", "producer"):
                command("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                        "-keyout", name + ".key", "-out", name + ".csr", "-subj", "/CN=" + name)
                (root / "extensions").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
                command("x509", "-req", "-in", name + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                        "-CAcreateserial", "-out", name + ".pem", "-days", "1", "-extfile", "extensions")
            fingerprints = {name: C.sha(ssl.PEM_cert_to_DER_cert((root / (name + ".pem")).read_text()))
                            for name in ("server", "controller", "producer")}
            args = argparse.Namespace(listen="127.0.0.1:0", cert=str(root / "server.pem"),
                     key=str(root / "server.key"), ca=str(root / "ca.pem"), output=str(root / "collected"),
                     run_id=RUN, producer_id=PRODUCER, client_sha256=fingerprints["producer"],
                     controller_sha256=fingerprints["controller"], max_bytes=C.MAX_TOTAL,
                     io_timeout_ms=1000, max_runtime_seconds=4)
            ready, result = queue.Queue(), queue.Queue()
            diagnostics = io.StringIO()
            def run():
                try:
                    with contextlib.redirect_stderr(diagnostics):
                        result.put(C.serve(args, ready.put))
                except BaseException as error: result.put(error)
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            host, port = ready.get(timeout=3)
            config = {"address": f"{host}:{port}", "server_name": "localhost",
                      "server_certificate_sha256": fingerprints["server"], "ca_path": str(root / "ca.pem"),
                      "cert_path": str(root / "controller.pem"), "key_path": str(root / "controller.key"),
                      "run_id": RUN, "producer_id": PRODUCER, "session_id": SESSION, "io_timeout_ms": 1000}
            sink = W.WitnessClient(config)
            r = sink.record({"kind": "process_launch", "observed_command": ["synthetic-no-db"]})
            self.assertEqual(r["witness_sequence"], 1)
            self.assertEqual(r["client_certificate_sha256"], fingerprints["controller"])
            self.assertTrue((root / "collected" / "controller-witnesses.jsonl").read_bytes())
            wrong = W.WitnessClient({**config, "server_certificate_sha256": "f" * 64})
            with self.assertRaises(ValueError):
                wrong.record({"kind": "process_exit"})
            self.assertTrue(wrong.failed)
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result.get_nowait(), 2)
            failure = json.loads(diagnostics.getvalue())
            self.assertTrue(failure["collector_fenced"])
            self.assertEqual(failure["phase"], "read_request")
            self.assertEqual(failure["receipt_bytes_sent"], 0)
            self.assertIn("unknown_on_error", failure["persistence_outcome"])


if __name__ == "__main__":
    unittest.main()
