#!/usr/bin/env python3
"""Functional-test collector. Own local storage only; never database authority.

One request per pinned mTLS connection, one absolute network deadline. A receipt
is sent only after payload AND receipt-journal fsync. Kernel fsync is not
preemptible; use an independent process supervisor. Restart never resumes an
existing output directory. All uncertain/partial files remain evidence.
"""
import argparse
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import ssl
import struct
import sys
import time
import uuid

import verify as V
from local_storage import local_storage_contract, open_local_storage

MAX_HEADER = 65536
MAX_PAYLOAD = 64 * 1024**2
MAX_TOTAL = 2 * 1024**3
INPUT_LIMITS = {"config.json": 1024**2, "topology.json": 16 * 1024**2,
                "plan.json": 40 * 1024**2, "ledger-config.json": MAX_HEADER}
COMMON = {"format", "action", "run_id", "producer_id", "session_id", "payload_sha256",
          "payload_bytes"}


def encode(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def new_file(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb", buffering=0)


def new_file_at(directory_fd, name):
    V.require(Path(name).name == name and name not in {"", ".", ".."}, "bounded collector basename")
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
    return os.fdopen(fd, "wb", buffering=0)


def first_leaf_der(path):
    """Exact first certificate DER in a bounded PEM bundle; never hash a chain."""
    raw = V.bounded(path, 1024**2)
    found = re.search(rb"-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", raw, re.S)
    V.require(found is not None, "missing first PEM certificate")
    body = re.sub(rb"\s+", b"", found[1])
    der = base64.b64decode(body, validate=True)
    V.require(der, "empty first PEM certificate")
    return der


def directory_sync(path, sync=os.fsync):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        sync(fd)
    finally:
        os.close(fd)


def exact_write(file, raw, check=lambda: None):
    view = memoryview(raw)
    while view:
        check()
        count = file.write(view)
        if not count:
            raise OSError("partial collector write made no progress")
        view = view[count:]


class Store:
    """Single-threaded actual collector state. Test seams inject persistence only."""
    def __init__(self, root, run_id, producer_id, client_sha, controller_sha, server_sha,
                 max_bytes=MAX_TOTAL, sync=os.fsync):
        V.require(V.valid_uuid(run_id) and V.valid_uuid(producer_id), "collector identity")
        V.require(all(V.HEX.fullmatch(s) for s in (client_sha, controller_sha, server_sha))
                  and len({client_sha, controller_sha, server_sha}) == 3, "distinct pinned ledger peers")
        V.require(0 < max_bytes <= MAX_TOTAL, "collector byte quota")
        self.root, self.run_id, self.producer_id = Path(root), run_id, producer_id
        self.client_sha, self.controller_sha, self.server_sha = client_sha, controller_sha, server_sha
        self.sync, self.max_bytes = sync, max_bytes
        self.poisoned, self.session, self.manifest = False, None, None
        self.inputs, self.sequence, self.offset, self.previous = set(), 0, 0, None
        self.total, self.receipt_sequence, self.witness_sequence = 0, 0, 0
        self.request_end = None
        self.root.mkdir(mode=0o700)  # Existing/partially written runs never reopen for append.
        directory_sync(self.root.parent, sync)
        self.directory_fd, storage_contract = open_local_storage(self.root)
        opened = []
        try:
            for attr, name in (("journal", "journal.jsonl"), ("receipts", "collector-receipts.jsonl"),
                               ("witnesses", "controller-witnesses.jsonl")):
                file = new_file_at(self.directory_fd, name)
                opened.append(file)
                setattr(self, attr, file)
                sync(file.fileno())
            sync(self.directory_fd)
            self._file("storage-contract.json", encode(storage_contract))
        except BaseException:
            for file in opened:
                file.close()
            os.close(self.directory_fd)
            self.directory_fd = None
            raise

    def close(self):
        for file in (self.journal, self.receipts, self.witnesses):
            file.close()
        if self.directory_fd is not None:
            os.close(self.directory_fd)
            self.directory_fd = None

    def fence(self):
        self.poisoned = True

    def process(self, header, payload, peer_sha, end=None):
        try:
            self.request_end = end
            self.check_time()
            receipt = self._process(header, payload, peer_sha)
            self.check_time()
            return receipt
        except BaseException:
            self.fence()
            raise

    def check_time(self):
        if self.request_end is not None and time.monotonic() >= self.request_end:
            raise TimeoutError("original collector request deadline expired")

    def persist(self, file, raw):
        self.check_time()
        exact_write(file, raw, self.check_time)
        self.check_time()
        self.sync(file.fileno())
        self.check_time()  # Never turn a late kernel fsync into a successful receipt.

    def _process(self, h, payload, peer_sha):
        V.require(not self.poisoned, "collector fenced after uncertainty")
        V.require(self.receipt_sequence < 70500, "collector receipt count quota")
        action = h.get("action")
        extra = {"input": {"name"}, "row": {"sequence", "previous_record_sha256"},
                 "start": set(), "witness": set()}.get(action)
        V.require(extra is not None and set(h) == COMMON | extra, "collector header fields")
        V.require(type(h["format"]) is int and h["format"] == 1
                  and h["run_id"] == self.run_id and h["producer_id"] == self.producer_id
                  and V.valid_uuid(h["session_id"]), "collector request identity")
        V.require(type(h["payload_bytes"]) is int and h["payload_bytes"] == len(payload)
                  and len(payload) <= MAX_PAYLOAD and h["payload_sha256"] == sha(payload),
                  "exact payload length/hash")
        V.require(peer_sha == (self.controller_sha if action == "witness" else self.client_sha),
                  "wrong pinned instrumentation identity")
        V.require(len(payload) <= self.max_bytes - self.total, "collector total byte quota")
        receipt = {k: h[k] for k in COMMON}
        receipt.update(collector_certificate_sha256=self.server_sha,
                       client_certificate_sha256=peer_sha)
        if action == "start":
            V.require(self.manifest is None and len(payload) <= MAX_HEADER
                      and self.session in (None, h["session_id"]), "duplicate/oversized start")
            manifest = V.decode(payload)
            required = {"format", "run_id", "producer_id", "session_id", "inputs", "source_sha",
                        "binary_sha256", "member_node_id", "member_certificate_sha256",
                        "ledger_client_certificate_sha256", "collector_certificate_sha256",
                        "qualification_status", "throughput_benchmark"}
            V.require(set(manifest) == required and type(manifest["format"]) is int and manifest["format"] == 1
                      and all(manifest[k] == h[k] for k in ("run_id", "producer_id", "session_id"))
                      and manifest["qualification_status"] == "not_qualified"
                      and manifest["throughput_benchmark"] is False, "start manifest identity/scope")
            V.require(manifest["ledger_client_certificate_sha256"] == self.client_sha
                      and manifest["collector_certificate_sha256"] == self.server_sha
                      and V.HEX.fullmatch(manifest["member_certificate_sha256"])
                      and manifest["member_certificate_sha256"] not in {
                          self.client_sha, self.controller_sha, self.server_sha}
                      and V.HEX.fullmatch(manifest["binary_sha256"])
                      and isinstance(manifest["member_node_id"], str)
                      and 0 < len(manifest["member_node_id"]) <= 4096
                      and re.fullmatch(r"[0-9a-f]{40}", manifest["source_sha"]),
                      "manifest source/member/separate ledger identity")
            V.require(set(manifest["inputs"]) == set(INPUT_LIMITS), "input manifest set")
            for name, item in manifest["inputs"].items():
                V.require(set(item) == {"bytes", "sha256"} and type(item["bytes"]) is int
                          and 0 < item["bytes"] <= INPUT_LIMITS[name]
                          and V.HEX.fullmatch(item["sha256"]), "bounded exact input manifest")
            self._file("collector-manifest.json", payload)
            self.session, self.manifest = h["session_id"], manifest
        else:
            if action == "witness" and self.session is None:
                self.session = h["session_id"]  # Independent launch can precede producer start.
            V.require(self.session is not None and h["session_id"] == self.session, "wrong session")
            if action == "input":
                V.require(self.manifest is not None, "input before start")
                name = h["name"]
                V.require(name in INPUT_LIMITS and name not in self.inputs, "duplicate/unknown input")
                item = self.manifest["inputs"][name]
                V.require(len(payload) == item["bytes"] and sha(payload) == item["sha256"],
                          "input differs from pinned manifest")
                V.decode(payload)  # Structured inputs only; never upload PEM/private-key files.
                V.require(b"PRIVATE KEY-----" not in payload, "private-key material forbidden")
                self._file(name, payload)
                self.inputs.add(name)
                receipt["name"] = name
            elif action == "row":
                V.require(self.inputs == set(INPUT_LIMITS), "original inputs must precede every RPC")
                V.require(payload.endswith(b"\n") and b"\n" not in payload[:-1], "one complete original row")
                row = V.decode(payload[:-1])
                V.require(type(h["sequence"]) is int and h["sequence"] == self.sequence + 1
                          and h["previous_record_sha256"] == self.previous
                          and type(row["format"]) is int and row["format"] == 1 and row["run_id"] == self.run_id
                          and type(row["sequence"]) is int and row["sequence"] == h["sequence"]
                          and row["previous_record_sha256"] == self.previous,
                          "original row sequence/hash chain")
                self.persist(self.journal, payload)
                self.sequence += 1
                self.offset += len(payload)
                self.previous = sha(payload[:-1])
                receipt.update(sequence=self.sequence, record_sha256=self.previous,
                               previous_record_sha256=h["previous_record_sha256"],
                               durable_end_offset=self.offset)
            else:
                V.require(len(payload) <= MAX_HEADER, "bounded independent witness")
                V.require(self.witness_sequence < 256, "independent witness count quota")
                witness = V.decode(payload)
                V.require(isinstance(witness, dict) and witness.get("kind") in {
                    "process_launch", "process_exit", "fault_command", "fault_result", "wrapper_delivery"},
                    "independent controller witness kind")
                self.witness_sequence += 1
                raw = encode({"sequence": self.witness_sequence, "received_unix_ns": time.time_ns(),
                              "controller_certificate_sha256": peer_sha,
                              "original_witness_hex": payload.hex()}) + b"\n"
                self.persist(self.witnesses, raw)
                receipt["witness_sequence"] = self.witness_sequence
        self.total += len(payload)
        self.receipt_sequence += 1
        receipt["receipt_sequence"] = self.receipt_sequence
        # A stored receipt alone does not prove producer received this response.
        self.persist(self.receipts, encode(receipt) + b"\n")
        return receipt

    def _file(self, name, raw):
        self.check_time()
        with new_file_at(self.directory_fd, name) as file:
            self.persist(file, raw)
        self.check_time()
        self.sync(self.directory_fd)
        self.check_time()


class Window:
    def __init__(self, end):
        self.end = end
        self.read_bytes = 0
        self.written_bytes = 0
        self.request_header_DATA = None

    def timeout(self, stream):
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("original instrumentation IO deadline expired")
        stream.settimeout(remaining)

    def read(self, stream, count):
        result = bytearray()
        while len(result) < count:
            self.timeout(stream)
            raw = stream.recv(min(65536, count - len(result)))
            if not raw:
                raise EOFError("partial collector frame")
            self.read_bytes += len(raw)
            result.extend(raw)
        return bytes(result)

    def write(self, stream, raw):
        view = memoryview(raw)
        while view:
            self.timeout(stream)
            count = stream.send(view)
            if not count:
                raise EOFError("partial collector receipt")
            self.written_bytes += count
            view = view[count:]


def receive(stream, window):
    length, = struct.unpack("!I", window.read(stream, 4))
    V.require(0 < length <= MAX_HEADER, "collector header bound")
    header = V.decode(window.read(stream, length))
    window.request_header_DATA = header
    length, = struct.unpack("!Q", window.read(stream, 8))
    V.require(length <= MAX_PAYLOAD and header.get("payload_bytes") == length, "collector payload bound")
    return header, window.read(stream, length)


def serve(args, ready=None):
    host, port = args.listen.rsplit(":", 1)
    host = host.strip("[]")
    ipaddress.ip_address(host)  # No resolver-dependent deadline.
    V.require(1 <= args.io_timeout_ms <= 30000, "instrumentation timeout bound")
    certificate = first_leaf_der(args.cert)
    server_sha = sha(certificate)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=args.ca)
    context.load_cert_chain(args.cert, args.key)
    store = Store(args.output, args.run_id, args.producer_id, args.client_sha256,
                  args.controller_sha256, server_sha, args.max_bytes)
    listener = None
    try:
        listener = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET)
        listener.bind((host, int(port)))
        listener.listen(8)
        listener.settimeout(1)
        if ready is not None:
            ready(listener.getsockname())  # Test-only transport harness; no DB authority.
    except BaseException:
        if listener is not None:
            listener.close()
        store.close()  # Retained real directory FD must not leak on startup refusal.
        raise
    end = time.monotonic() + args.max_runtime_seconds
    try:
        while time.monotonic() < end and not store.poisoned:
            try:
                sock, _ = listener.accept()
            except TimeoutError:
                continue
            window = Window(min(end, time.monotonic() + args.io_timeout_ms / 1000))
            phase, header = "tls_handshake", None
            try:
                with sock:
                    window.timeout(sock)
                    with context.wrap_socket(sock, server_side=True, do_handshake_on_connect=False) as stream:
                        window.timeout(stream)
                        stream.do_handshake()
                        peer = sha(stream.getpeercert(binary_form=True))
                        phase = "read_request"
                        header, payload = receive(stream, window)
                        phase = "persist_request"
                        receipt = store.process(header, payload, peer, window.end)
                        raw = encode(receipt)
                        phase = "send_receipt"
                        window.write(stream, struct.pack("!I", len(raw)) + raw)
            except (OSError, ValueError, KeyError, TypeError, EOFError, AttributeError, RecursionError) as error:
                store.fence()  # Even lost response after fsync stops this run; no automatic retry.
                if header is None:
                    header = window.request_header_DATA
                # Bounded DATA only, never frame/key contents. The independent
                # runner must durably capture stderr; a broken collector disk
                # cannot certify its own failure-log persistence.
                identity = {k: str(header[k])[:160] for k in (
                    "action", "run_id", "producer_id", "session_id", "sequence", "name",
                    "payload_sha256", "payload_bytes") if isinstance(header, dict) and k in header}
                print(json.dumps({"collector_fenced": True, "phase": phase,
                      "error_type": type(error).__name__, "error": str(error)[:512],
                      "request_identity_DATA": identity, "application_bytes_received": window.read_bytes,
                      "receipt_bytes_sent": window.written_bytes,
                      "persistence_outcome": "unknown_on_error; never rollback",
                      "stderr_durability_requires_independent_capture": True}), file=sys.stderr, flush=True)
        return 2 if store.poisoned else 0
    finally:
        listener.close()
        store.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ("listen", "cert", "key", "ca", "output", "run-id", "producer-id",
                 "client-sha256", "controller-sha256"):
        p.add_argument("--" + flag, required=True)
    p.add_argument("--io-timeout-ms", type=int, default=1000)
    p.add_argument("--max-runtime-seconds", type=int, default=1800)
    p.add_argument("--max-bytes", type=int, default=MAX_TOTAL)
    args = p.parse_args()
    V.require(1 <= args.max_runtime_seconds <= 86400, "collector lifetime bound")
    return serve(args)


if __name__ == "__main__":
    raise SystemExit(main())
