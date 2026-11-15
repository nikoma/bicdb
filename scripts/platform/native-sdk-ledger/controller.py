#!/usr/bin/env python3
"""Independent bounded command observer; separate ledger mTLS identity only.

It records the exit of the ACTUAL local child it launches. An SSH/provider
command's exit is not automatically proof of remote producer exit/power loss.
Keep that distinction in the independently reviewed run manifest. No DB keys,
network filesystem, shell evaluation, database retries or observer reentry.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import ssl
import struct
import subprocess
import time

import collector as C
import verify as V

U64_MAX = 2**64 - 1


def require_witness_receipt(receipt, header, client_sha, server_sha, last_receipt, last_witness):
    expected = C.COMMON | {"collector_certificate_sha256", "client_certificate_sha256",
                           "receipt_sequence", "witness_sequence"}
    V.require(isinstance(receipt, dict) and set(receipt) == expected, "exact witness receipt field set")
    V.require(all(V.equal(receipt[k], value) for k, value in header.items())
              and receipt["client_certificate_sha256"] == client_sha
              and receipt["collector_certificate_sha256"] == server_sha,
              "exact controller receipt identity/hash binding")
    V.require(type(receipt["receipt_sequence"]) is int and last_receipt < receipt["receipt_sequence"] <= U64_MAX
              and type(receipt["witness_sequence"]) is int and last_witness < receipt["witness_sequence"] <= U64_MAX,
              "typed monotonic witness receipt counters")
    return receipt["receipt_sequence"], receipt["witness_sequence"]


class WitnessClient:
    def __init__(self, config):
        V.require(set(config) == {"address", "server_name", "server_certificate_sha256", "ca_path",
                  "cert_path", "key_path", "run_id", "producer_id", "session_id", "io_timeout_ms"},
                  "controller configuration fields")
        host, port = config["address"].rsplit(":", 1)
        self.host, self.port = host.strip("[]"), int(port)
        ipaddress.ip_address(self.host)
        V.require(type(config["io_timeout_ms"]) is int and 1 <= config["io_timeout_ms"] <= 30000,
                  "controller network IO bound")
        self.config = config
        self.context = ssl.create_default_context(cafile=config["ca_path"])
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.context.load_cert_chain(config["cert_path"], config["key_path"])
        self.client_sha = C.sha(C.first_leaf_der(config["cert_path"]))
        self.failed = False
        self.last_receipt = 0
        self.last_witness = 0

    def record(self, witness):
        V.require(not self.failed, "controller fenced after ambiguous receipt")
        try:
            # Include witness framing/hash work in this SAME absolute window.
            window = C.Window(time.monotonic() + self.config["io_timeout_ms"] / 1000)
            payload = C.encode(witness)
            V.require(len(payload) <= C.MAX_HEADER, "controller witness byte bound")
            h = {"format": 1, "action": "witness", "payload_sha256": C.sha(payload),
                 "payload_bytes": len(payload), **{k: self.config[k] for k in
                 ("run_id", "producer_id", "session_id")}}
            window.remaining()  # No endpoint connection after a known framing expiry.
            with socket.socket(socket.AF_INET6 if ":" in self.host else socket.AF_INET) as sock:
                window.timeout(sock)
                sock.connect((self.host, self.port))
                with self.context.wrap_socket(sock, server_hostname=self.config["server_name"],
                                              do_handshake_on_connect=False) as stream:
                    window.timeout(stream)
                    stream.do_handshake()
                    V.require(C.sha(stream.getpeercert(binary_form=True)) == self.config["server_certificate_sha256"],
                              "collector pin changed")
                    raw = C.encode(h)
                    window.write(stream, struct.pack("!I", len(raw)) + raw + struct.pack("!Q", len(payload)) + payload)
                    length, = struct.unpack("!I", window.read(stream, 4))
                    V.require(0 < length <= C.MAX_HEADER, "receipt length bound")
                    receipt = V.decode(window.read(stream, length))
                    counters = require_witness_receipt(receipt, h, self.client_sha,
                               self.config["server_certificate_sha256"], self.last_receipt, self.last_witness)
                    window.timeout(stream)
                    self.last_receipt, self.last_witness = counters
                    return receipt
        except BaseException:
            self.failed = True
            raise


def retain(root, name, raw):
    with C.new_file(root / name) as file:
        C.exact_write(file, raw)
        os.fsync(file.fileno())
    C.directory_sync(root)


def observe(command, seconds, output):
    """Capture real child output/exit with one absolute command deadline."""
    V.require(command and 0 < seconds <= 86400 and len(command) <= 128
              and all(isinstance(s, str) and len(s.encode()) <= 4096 for s in command)
              and len(C.encode(command)) <= 16384, "bounded controller command")
    started = time.monotonic()
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, start_new_session=True)
    streams = {child.stdout: "stdout", child.stderr: "stderr"}
    captured = {name: bytearray() for name in streams.values()}
    incomplete, expired = False, False
    selector = selectors.DefaultSelector()
    for file in streams:
        os.set_blocking(file.fileno(), False)
        selector.register(file, selectors.EVENT_READ)
    end = started + seconds
    cleanup_end = None
    try:
        while selector.get_map() or child.poll() is None:
            now = time.monotonic()
            if cleanup_end is None and now >= end:
                expired = incomplete = True
            if incomplete and cleanup_end is None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)  # Own child process group only.
                except ProcessLookupError:
                    pass
                cleanup_end = now + 2
            if cleanup_end is not None and now >= cleanup_end:
                break
            for key, _ in selector.select(min(0.05, max(0, (cleanup_end or end) - now))):
                raw = os.read(key.fileobj.fileno(), 4096)
                if not raw:
                    selector.unregister(key.fileobj)
                    continue
                target = captured[streams[key.fileobj]]
                remaining = 16384 - len(target)
                target.extend(raw[:max(0, remaining)])
                if len(raw) > remaining:
                    incomplete = True
        if selector.get_map() or child.poll() is None:
            incomplete = True
        result = {"kind": "process_exit", "observed_child_pid": child.pid,
                  "observed_command": command, "command_sha256": C.sha(C.encode(command)),
                  "exit_status": child.poll(), "timed_out": expired,
                  "output_complete": not incomplete,
                  "stdout_hex": bytes(captured["stdout"]).hex(),
                  "stderr_sha256": C.sha(captured["stderr"]),
                  "stdout_bytes": len(captured["stdout"]), "stderr_bytes": len(captured["stderr"]),
                  "elapsed_ns": int((time.monotonic() - started) * 10**9),
                  "witness_scope": "actual_local_child_command_only; remote_process_and_fault_effect_require_independent_confirmation"}
        retain(output, "stdout.bin", bytes(captured["stdout"]))
        retain(output, "stderr.bin", bytes(captured["stderr"]))
        retain(output, "actual-exit.json", C.encode(result))
        return result
    finally:
        selector.close()
        for file in streams:
            file.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--timeout-seconds", type=float, default=120)
    p.add_argument("--purpose", choices=("workload", "fault"), default="workload")
    p.add_argument("command", nargs=argparse.REMAINDER)
    args = p.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    V.require(command, "command required")
    V.require(len(command) <= 128 and all(len(s.encode()) <= 4096 for s in command)
              and len(C.encode(command)) <= 16384, "command argument byte bound")
    args.output.mkdir(mode=0o700)
    C.directory_sync(args.output.parent)
    sink = WitnessClient(V.decode(V.bounded(args.config, C.MAX_HEADER)))
    launch = {"kind": "fault_command" if args.purpose == "fault" else "process_launch", "observed_command": command,
              "command_sha256": C.sha(C.encode(command)), "unix_ns": time.time_ns(),
              "timeout_seconds": args.timeout_seconds}
    retain(args.output, "launch.json", C.encode(launch))
    retain(args.output, "launch-receipt.json", C.encode(sink.record(launch)))
    result = observe(command, args.timeout_seconds, args.output)
    if args.purpose == "fault":
        result["kind"] = "fault_result"
        result["actual_remote_fault_effect_confirmed"] = False
    retain(args.output, "exit-receipt.json", C.encode(sink.record(result)))
    return 0 if result["output_complete"] and result["exit_status"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
