#!/usr/bin/env python3
"""Descriptor/mount consistency controls and real loopback certificate chains.

No mounts, VMs, database processes or DB keys are created. Metadata negatives
inject contradictory observations around real opened FDs; they are not actual
mount or disk faults. TLS certificates are ephemeral test identities only.
"""
import argparse
import base64
import contextlib
import copy
import io
import os
from pathlib import Path
import queue
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

import collector as C
import controller as W
import local_storage as S
import test_external as T
import verify as V


class OpenDirectoryControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_actual_directory_device_mount_id_and_root_are_bound(self):
        fd, facts = S.open_local_storage(self.root)
        try:
            actual = os.fstat(fd)
            self.assertEqual(facts["st_dev"], actual.st_dev)
            self.assertEqual(facts["st_ino"], actual.st_ino)
            self.assertEqual(facts["kernel_mount_id"], S.mount_id(fd))
            entry, = [m for m in S.mountinfo() if m["mount_id"] == S.mount_id(fd)]
            self.assertEqual(facts["mount_filesystem_root"], entry["root"])
            self.assertEqual((facts["actual_device_major"], facts["actual_device_minor"]),
                             (os.major(actual.st_dev), os.minor(actual.st_dev)))
            os.fsync(fd)
            self.assertFalse(facts["actual_backing_storage_and_flush_behavior_qualified"])
        finally:
            os.close(fd)

    def changed_entries(self, edit):
        fd = os.open(self.root, getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY)
        try:
            entries = S.mountinfo()
            target, = [m for m in entries if m["mount_id"] == S.mount_id(fd)]
            edit(entries, target)
            with patch.object(S, "mountinfo", return_value=entries):
                return S.inspect_directory(fd)
        finally:
            os.close(fd)

    def test_duplicate_actual_mount_id_refuses_ambiguity(self):
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.changed_entries(lambda entries, target: entries.append(copy.deepcopy(target)))

    def test_actual_st_dev_must_match_selected_mount_device(self):
        def mismatch(entries, target): target["minor"] += 1
        with self.assertRaisesRegex(ValueError, "st_dev"):
            self.changed_entries(mismatch)

    def test_same_path_old_overmount_cannot_override_actual_mount_id(self):
        def unrelated(entries, target):
            old = {**target, "mount_id": max(m["mount_id"] for m in entries) + 100,
                   "filesystem": "nfs"}
            entries.append(old)
        facts = self.changed_entries(unrelated)
        self.assertIn(facts["filesystem"], S.LOCAL_FILESYSTEMS)

    def test_unrelated_namespace_mount_root_does_not_replace_selected_storage(self):
        def unrelated_namespace(entries, target):
            entries.append({"mount_id": max(m["mount_id"] for m in entries) + 100,
                            "major": 0, "minor": 999, "root": "net:[1234]",
                            "mountpoint": "/unrelated/namespace", "filesystem": "nsfs"})
        facts = self.changed_entries(unrelated_namespace)
        self.assertIn(facts["filesystem"], S.LOCAL_FILESYSTEMS)

    def test_actual_mount_type_refuses_even_when_equal_path_old_entry_is_local(self):
        def actual_not_local(entries, target):
            old = {**target, "mount_id": max(m["mount_id"] for m in entries) + 100,
                   "filesystem": "ext4"}
            entries.append(old)
            target["filesystem"] = "cifs"  # Injected observation, no actual mount operation.
        with self.assertRaisesRegex(ValueError, "local durable filesystem"):
            self.changed_entries(actual_not_local)

    def test_missing_actual_mount_id_refuses(self):
        def missing(entries, target): entries.remove(target)
        with self.assertRaisesRegex(ValueError, "missing"):
            self.changed_entries(missing)

    def test_selected_root_escape_refuses(self):
        def wrong_root(entries, target): target["root"] = "/../escape"
        with self.assertRaisesRegex(ValueError, "root"):
            self.changed_entries(wrong_root)

    def test_selected_mountpoint_must_contain_actual_opened_path(self):
        def wrong_point(entries, target): target["mountpoint"] = str(self.root / "unrelated")
        with self.assertRaisesRegex(ValueError, "outside"):
            self.changed_entries(wrong_point)

    def test_fresh_mountpoint_handle_must_match_original_opened_mount(self):
        fd = os.open(self.root, getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY)
        actual_identity = S.identity
        def changed_mount(other_fd):
            device, inode, mount = actual_identity(other_fd)
            return (device, inode, mount + 1) if other_fd != fd else (device, inode, mount)
        try:
            with patch.object(S, "identity", side_effect=changed_mount):
                with self.assertRaisesRegex(ValueError, "mountpoint changed"):
                    S.inspect_directory(fd)
        finally:
            os.close(fd)

    def test_genuine_other_device_and_mount_id_are_not_local_disk_proof(self):
        if not Path("/dev/shm").is_dir():
            self.skipTest("genuine /dev/shm directory unavailable")
        fd = os.open("/dev/shm", getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY)
        here = os.open(self.root, getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY)
        try:
            if os.fstat(fd).st_dev == os.fstat(here).st_dev:
                self.skipTest("host has no distinct /dev/shm device")
            self.assertNotEqual(S.mount_id(fd), S.mount_id(here))
            with self.assertRaisesRegex(ValueError, "local durable filesystem"):
                S.inspect_directory(fd)
        finally:
            os.close(fd)
            os.close(here)

    def test_final_directory_symlink_is_refused(self):
        link = self.root / "alias"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError): S.open_local_storage(link)

    def test_collector_inputs_stay_on_held_directory_after_actual_path_replacement(self):
        h = T.Harness(self.root / "store")
        try:
            original = self.root / "held-original"
            h.path.rename(original)
            h.path.mkdir()
            h.start()
            self.assertFalse((h.path / "collector-manifest.json").exists())
            self.assertEqual(V.decode((original / "collector-manifest.json").read_bytes()), h.manifest)
            self.assertTrue(all((original / name).is_file() for name in C.INPUT_LIMITS))
            self.assertEqual(os.fstat(h.store.directory_fd).st_ino, original.stat().st_ino)
            self.assertNotEqual(h.path.stat().st_ino, original.stat().st_ino)
        finally:
            h.close()


class FirstLeafFormatControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "certificates.pem"

    def pem(self, raw):
        return b"-----BEGIN CERTIFICATE-----\n" + base64.b64encode(raw) + b"\n-----END CERTIFICATE-----\n"

    def test_exact_first_der_not_concatenated_bundle_bytes(self):
        # Deliberately synthetic framing bytes; real X509 validation is TLS's job.
        self.path.write_bytes(self.pem(b"first-der-format-control") + self.pem(b"second-format-control"))
        self.assertEqual(C.first_leaf_der(self.path), b"first-der-format-control")

    def test_malformed_first_certificate_is_not_skipped_for_later_one(self):
        self.path.write_bytes(b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n" + self.pem(b"later"))
        with self.assertRaises(ValueError): C.first_leaf_der(self.path)

    def test_empty_first_certificate_is_not_skipped(self):
        self.path.write_bytes(self.pem(b"") + self.pem(b"later"))
        with self.assertRaises(ValueError): C.first_leaf_der(self.path)

    def test_entire_pem_input_is_bounded_even_when_first_leaf_is_small(self):
        self.path.write_bytes(self.pem(b"small") + b" " * 1024**2)
        with self.assertRaises(ValueError): C.first_leaf_der(self.path)


@unittest.skipUnless(shutil.which("openssl"), "openssl required for actual ephemeral certificate-chain control")
class ActualCertificateChainControl(unittest.TestCase):
    def test_real_multicertificate_peer_chain_and_first_leaf_pins_interoperate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def command(*argv):
                subprocess.run(["openssl", *argv], cwd=root, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True, timeout=10)
            command("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                    "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=ephemeral-chain-ca")
            reference_leafs = {}
            for name in ("server", "controller", "producer"):
                command("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                        "-keyout", name + ".key", "-out", name + ".csr", "-subj", "/CN=" + name)
                (root / "extensions").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
                command("x509", "-req", "-in", name + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                        "-CAcreateserial", "-out", name + ".pem", "-days", "1", "-extfile", "extensions")
                pem = root / (name + ".pem")
                reference_leafs[name] = C.sha(ssl.PEM_cert_to_DER_cert(pem.read_text()))
                pem.write_bytes(pem.read_bytes() + (root / "ca.pem").read_bytes())
                self.assertEqual(C.sha(C.first_leaf_der(pem)), reference_leafs[name])
            args = argparse.Namespace(listen="127.0.0.1:0", cert=str(root / "server.pem"),
                     key=str(root / "server.key"), ca=str(root / "ca.pem"), output=str(root / "collected"),
                     run_id=T.RUN, producer_id=T.PRODUCER, client_sha256=reference_leafs["producer"],
                     controller_sha256=reference_leafs["controller"], max_bytes=C.MAX_TOTAL,
                     io_timeout_ms=1000, max_runtime_seconds=4)
            ready, result, diagnostics = queue.Queue(), queue.Queue(), io.StringIO()
            def run():
                try:
                    with contextlib.redirect_stderr(diagnostics):
                        result.put(C.serve(args, ready.put))
                except BaseException as error: result.put(error)
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            host, port = ready.get(timeout=3)
            # Genuine occupied port: a second startup must close its own real
            # directory/files on refusal, without affecting the first server.
            failed_start = copy.copy(args)
            failed_start.listen = f"{host}:{port}"
            failed_start.output = str(root / "bind-refused")
            descriptors_before = set(os.listdir("/proc/self/fd"))
            with self.assertRaises(OSError):
                C.serve(failed_start)
            self.assertEqual(set(os.listdir("/proc/self/fd")), descriptors_before)
            config = {"address": f"{host}:{port}", "server_name": "localhost",
                      "server_certificate_sha256": reference_leafs["server"], "ca_path": str(root / "ca.pem"),
                      "cert_path": str(root / "controller.pem"), "key_path": str(root / "controller.key"),
                      "run_id": T.RUN, "producer_id": T.PRODUCER, "session_id": T.SESSION, "io_timeout_ms": 1000}
            receipt = W.WitnessClient(config).record({"kind": "process_launch", "observed_command": ["chain-control"]})
            self.assertEqual(receipt["collector_certificate_sha256"], reference_leafs["server"])
            self.assertEqual(receipt["client_certificate_sha256"], reference_leafs["controller"])
            wrong = W.WitnessClient({**config, "server_certificate_sha256": "f" * 64})
            with self.assertRaisesRegex(ValueError, "pin"):
                wrong.record({"kind": "process_exit"})
            self.assertTrue(wrong.failed)
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result.get_nowait(), 2)


if __name__ == "__main__":
    unittest.main()
