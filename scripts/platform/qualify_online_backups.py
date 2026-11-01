#!/usr/bin/env python3
"""Run real SQL writers, external-oracle restores and Linux I/O/crash faults.

Requires psycopg 3, gcc, Linux and a built BicDB CLI. Artifacts are retained,
including a FULL-synchronous SQLite acknowledgement journal and process logs.
No production executable or source file is modified by fault injection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import threading
import time
import traceback

KEY_ENV = "BICDB_QUALIFICATION_KEY"
TERMINAL = {"complete", "failed", "cancelled"}


class Journal:
    """Expected values originate here, never from BicDB hashes or manifests."""
    def __init__(self, path, writers):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE attempts(writer INT, seq INT, delta INT, token TEXT,
                rolled_back INT, acknowledged INT DEFAULT 0, dependencies TEXT,
                outcome TEXT DEFAULT 'planned',
                PRIMARY KEY(writer,seq));
            CREATE TABLE payload(id INT PRIMARY KEY, body TEXT);
        """)
        self.writers = writers

    def heads(self):
        with self.lock:
            return self._heads()

    def attempt_heads(self):
        with self.lock:
            return dict(self.db.execute("SELECT writer,MAX(seq) FROM attempts GROUP BY writer"))

    def _heads(self):
        return dict(self.db.execute("SELECT writer,MAX(seq) FROM attempts WHERE outcome IN ('acknowledged','resolved_committed') GROUP BY writer"))

    def plan(self, writer, seq, rollback):
        delta = ((writer + 3) * (seq + 7)) % 199 - 99
        token = hashlib.sha256(f"qualified:{writer}:{seq}".encode()).hexdigest()
        with self.lock:
            self.db.execute("INSERT INTO attempts(writer,seq,delta,token,rolled_back,dependencies) VALUES(?,?,?,?,?,?)",
                            (writer, seq, delta, token, rollback, json.dumps(self._heads())))
            self.db.commit()
        return delta, token

    def acknowledge(self, writer, seq):
        with self.lock:
            self.db.execute("UPDATE attempts SET acknowledged=1,outcome='acknowledged' WHERE writer=? AND seq=?", (writer, seq))
            self.db.commit()

    def outcome(self, writer, seq, value):
        with self.lock:
            self.db.execute("UPDATE attempts SET outcome=? WHERE writer=? AND seq=?", (value, writer, seq))
            self.db.commit()

    def next_sequence(self, writer):
        with self.lock:
            return self.db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM attempts WHERE writer=?", (writer,)).fetchone()[0]

    def reconcile(self, conn):
        with self.lock:
            pending = list(self.db.execute("SELECT writer,seq,token,outcome FROM attempts WHERE outcome IN ('planned','commit_sent')"))
        resolved = []
        for writer, seq, token, outcome in pending:
            rows = conn.execute(f"SELECT token FROM ledger WHERE id={writer*10000000+seq}").fetchall()
            if rows:
                assert rows == [(token,)], "transaction ID resolved to a different payload"
                assert outcome == "commit_sent", "a transaction committed without a COMMIT attempt"
                result = "resolved_committed"
            else:
                result = "resolved_aborted"
            self.outcome(writer, seq, result)
            resolved.append({"writer": writer, "seq": seq, "transaction_id": token, "outcome": result})
        return resolved

    def count(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM attempts WHERE acknowledged=1").fetchone()[0]

    def expected(self, heads):
        expected = sqlite3.connect(":memory:")
        expected.executescript("""
            CREATE TABLE ledger(id INT PRIMARY KEY, writer INT, seq INT, delta INT, token TEXT);
            CREATE TABLE accounts(id INT PRIMARY KEY, head INT, total INT);
            CREATE TABLE slots(id INT PRIMARY KEY, value INT, body TEXT);
            CREATE TABLE counters(id INT PRIMARY KEY, n INT, total INT);
            CREATE TABLE payload(id INT PRIMARY KEY, body TEXT);
            INSERT INTO counters VALUES(1,0,0);
        """)
        with self.lock:
            attempts = list(self.db.execute("SELECT writer,seq,delta,token,outcome,rolled_back,dependencies FROM attempts ORDER BY writer,seq"))
            payload = list(self.db.execute("SELECT * FROM payload ORDER BY id"))
        expected.executemany("INSERT INTO payload VALUES(?,?)", payload)
        for writer in range(self.writers):
            expected.execute("INSERT INTO accounts VALUES(?,0,0)", (writer,))
            expected.executemany("INSERT INTO slots VALUES(?,0,'initial')", [(writer * 16 + slot,) for slot in range(16)])
        for writer, seq, delta, token, outcome, rollback, dependencies in attempts:
            if seq > heads.get(writer, 0) or rollback or outcome in ('aborted', 'resolved_aborted'):
                continue
            assert outcome in ('acknowledged', 'resolved_committed'), f"unreconciled outcome at restore cut: {writer}/{seq}"
            for dependency, predecessor in json.loads(dependencies).items():
                assert heads.get(int(dependency), 0) >= predecessor, "restore cut violates observed commit ordering"
            expected.execute("INSERT INTO ledger VALUES(?,?,?,?,?)", (writer * 10000000 + seq, writer, seq, delta, token))
            expected.execute("UPDATE accounts SET head=?,total=total+? WHERE id=?", (seq, delta, writer))
            expected.execute("UPDATE slots SET value=?,body=? WHERE id=?", (delta, token, writer * 16 + seq % 16))
            expected.execute("UPDATE counters SET n=n+1,total=total+? WHERE id=1", (delta,))
        return expected


class Campaign:
    def __init__(self, args):
        global psycopg
        import psycopg

        self.args = args
        self.root = args.work_dir.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.binary = args.binary.resolve()
        self.repo = self.root / "repository"
        self.control = self.root / "fault-control"
        self.trace = self.root / "fault-trace.log"
        self.control.write_text("")
        self.library = self.root / "fault-inject.so"
        for filename in ["qualify_online_backups.py", "backup_fault_inject.c"]:
            (self.root / filename).write_bytes(Path(__file__).with_name(filename).read_bytes())
        subprocess.run(["gcc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror",
                        "-o", str(self.library), str(Path(__file__).with_name("backup_fault_inject.c"))], check=True)
        self.env = os.environ.copy()
        self.env.update({KEY_ENV: "isolated-qualification-key", "LD_PRELOAD": str(self.library),
                         "BICDB_QUALIFY_CONTROL": str(self.control), "BICDB_QUALIFY_TRACE": str(self.trace),
                         "BICDB_QUALIFY_REPOSITORY": str(self.repo), "BICDB_QUALIFY_SOURCE": str(self.root / "db")})
        self.processes = []
        self.stop = threading.Event()
        self.errors = []
        self.writer_stats = [{"commits": 0, "rollbacks": 0, "serialization_aborts": 0, "latencies": []} for _ in range(args.writers)]
        self.inflight_lock = threading.Lock()
        self.inflight = 0
        self.peak_inflight = 0
        self.journal = Journal(self.root / "external-oracle.sqlite", args.writers)
        self.report = {"passed": False, "binary": str(self.binary), "binary_sha256": hashlib.file_digest(self.binary.open("rb"), "sha256").hexdigest(),
                       "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "fault_helper_sha256": hashlib.sha256(Path(__file__).with_name("backup_fault_inject.c").read_bytes()).hexdigest(),
                       "parameters": vars(args) | {"binary": str(self.binary), "work_dir": str(self.root)},
                       "backups": [], "restores": [], "oracle_checks": [], "faults": [], "corruption": [],
                       "fault_model": "Linux process crashes and injected libc I/O errors; not machine power cuts or storage-controller testing"}

    def emit(self, event, **values):
        print(json.dumps({"event": event, "elapsed": round(time.monotonic() - self.started, 3), **values}), flush=True)

    def run_cli(self, *args, success=True, env=None):
        result = subprocess.run([str(self.binary), *map(str, args)], env=env or self.env,
                                capture_output=True, text=True, timeout=180)
        if success:
            assert result.returncode == 0, f"{args}: {result.stderr}\n{result.stdout}"
        else:
            assert result.returncode != 0, f"unexpected success: {args}"
        return result

    def serve(self, path, primary=False):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        log = (self.root / f"server-{port}.log").open("w")
        command = [str(self.binary), "serve", str(path), "--port", str(port), "--max-connections-per-ip", "32"]
        if primary:
            limits = self.root / "limits.json"
            limits.write_text(json.dumps({"bytes_per_second": self.args.copy_rate, "max_coordination_millis": 5000}))
            command += ["--backup-repository", str(self.repo), "--backup-key-env", KEY_ENV, "--backup-limits", str(limits)]
        process = subprocess.Popen(command, env=self.env, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((process, log))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            assert process.poll() is None, f"server exited: {log.name}"
            try:
                conn = self.connect(port)
                return process, port, conn
            except psycopg.OperationalError:
                time.sleep(.05)
        raise AssertionError("server startup deadline")

    @staticmethod
    def connect(port):
        return psycopg.connect(host="127.0.0.1", port=port, user="bicdb", dbname="bicdb", autocommit=True, connect_timeout=5)

    @staticmethod
    def stop_process(process, kill=False):
        if process.poll() is None:
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def set_fault(self, kind):
        pending = self.control.with_suffix(".pending")
        pending.write_text(kind)
        os.replace(pending, self.control)
        return self.trace.stat().st_size if self.trace.exists() else 0

    def fault_hits(self, offset, kind):
        if not self.trace.exists():
            return []
        return [line for line in self.trace.read_text()[offset:].splitlines() if line.startswith(kind + " ")]

    def seed(self, conn):
        conn.execute("CREATE TABLE ledger(id BIGINT PRIMARY KEY, writer INT, seq INT, delta INT, token TEXT)")
        conn.execute("CREATE TABLE accounts(id INT PRIMARY KEY, head INT, total BIGINT)")
        conn.execute("CREATE TABLE slots(id INT PRIMARY KEY, value INT, body TEXT)")
        conn.execute("CREATE TABLE counters(id INT PRIMARY KEY, n BIGINT, total BIGINT)")
        conn.execute("CREATE TABLE payload(id INT PRIMARY KEY, body TEXT)")
        conn.execute("CREATE TABLE rollback_probe(id BIGINT PRIMARY KEY)")
        conn.execute("INSERT INTO counters VALUES(1,0,0)")
        for writer in range(self.args.writers):
            conn.execute(f"INSERT INTO accounts VALUES({writer},0,0)")
            for slot in range(16):
                conn.execute(f"INSERT INTO slots VALUES({writer*16+slot},0,'initial')")
        for start in range(0, self.args.seed_rows, 32):
            conn.execute("BEGIN")
            for i in range(start, min(start + 32, self.args.seed_rows)):
                body = hashlib.shake_256(f"payload:{i}".encode()).hexdigest(self.args.seed_bytes // 2)
                conn.execute(f"INSERT INTO payload VALUES({i},'{body}')")
                self.journal.db.execute("INSERT INTO payload VALUES(?,?)", (i, body))
            conn.execute("COMMIT")
            self.journal.db.commit()
        conn.execute("CREATE INDEX ledger_writer_seq ON ledger(writer,seq)")
        conn.execute("CREATE INDEX slots_value ON slots(value)")

    def writer(self, writer, port):
        try:
            with self.connect(port) as conn:
                seq = 0
                while not self.stop.is_set():
                    seq += 1
                    rollback = seq % 11 == 0
                    delta, token = self.journal.plan(writer, seq, rollback)
                    ident = writer * 10000000 + seq
                    started = time.monotonic()
                    with self.inflight_lock:
                        self.inflight += 1
                        self.peak_inflight = max(self.peak_inflight, self.inflight)
                    try:
                        self.transaction(conn, writer, seq, delta, token)
                        if rollback:
                            conn.execute(f"INSERT INTO rollback_probe VALUES({ident})")
                            conn.execute("ROLLBACK")
                            self.journal.outcome(writer, seq, "aborted")
                            self.writer_stats[writer]["rollbacks"] += 1
                        else:
                            self.journal.outcome(writer, seq, "commit_sent")
                            conn.execute("COMMIT")
                            self.writer_stats[writer]["latencies"].append(time.monotonic() - started)
                            self.journal.acknowledge(writer, seq)
                            self.writer_stats[writer]["commits"] += 1
                    except psycopg.errors.SerializationFailure:
                        conn.execute("ROLLBACK")
                        self.journal.outcome(writer, seq, "aborted")
                        self.writer_stats[writer]["serialization_aborts"] += 1
                        self.stop.wait(.001 * (writer + 1))
                    finally:
                        with self.inflight_lock:
                            self.inflight -= 1
                    self.stop.wait(self.args.writer_pause)
        except BaseException:
            self.errors.append(traceback.format_exc())
            self.stop.set()

    @staticmethod
    def transaction(conn, writer, seq, delta, token):
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO ledger VALUES({writer*10000000+seq},{writer},{seq},{delta},'{token}')")
        conn.execute(f"UPDATE accounts SET head={seq},total=total+({delta}) WHERE id={writer}")
        slot = writer * 16 + seq % 16
        conn.execute(f"DELETE FROM slots WHERE id={slot}")
        conn.execute(f"INSERT INTO slots VALUES({slot},{delta},'{token}')")
        conn.execute(f"UPDATE counters SET n=n+1,total=total+({delta}) WHERE id=1")

    def start_backup(self, conn, name, parent=None):
        lower = self.journal.heads()
        values = f"'{name}'" + (f",'{parent}'" if parent else "")
        start = json.loads(conn.execute(f"SELECT bicdb_backup_start({values})").fetchone()[0])
        return {"name": name, "parent": parent, "id": start["id"], "lower_heads": lower}

    def status(self, conn, job):
        return json.loads(conn.execute(f"SELECT bicdb_backup_status('{job['id']}')").fetchone()[0])[0]

    def backup(self, conn, name, parent=None, expected="complete"):
        job = self.start_backup(conn, name, parent)
        before = self.journal.count()
        during = None
        last_copy_count = None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            assert not self.errors, self.errors
            status = self.status(conn, job)
            if status["phase"] == "copying":
                during = self.journal.count() if during is None else during
                last_copy_count = self.journal.count()
                if "upper_heads" not in job:
                    job["upper_heads"] = self.journal.attempt_heads()
            if status["phase"] in TERMINAL:
                assert status["phase"] == expected, status
                job.update({"result": status.get("result"), "error": status.get("error"),
                            "acknowledged_during_job": self.journal.count() - before,
                            "acknowledged_after_copy_observed": last_copy_count - during if during is not None else 0})
                return job
            time.sleep(.025)
        raise AssertionError(f"backup deadline: {job}")

    def verify_database(self, conn, label, lower=None, upper=None):
        heads = dict(conn.execute("SELECT id,head FROM accounts ORDER BY id").fetchall())
        for writer, minimum in (self.journal.heads() if lower is None else lower).items():
            assert heads.get(int(writer), 0) >= minimum, f"acknowledged-before-capture write missing: {label}"
        if upper is not None:
            assert all(head <= upper.get(writer, 0) for writer, head in heads.items()), f"post-capture transaction leaked into backup: {label}"
        expected = self.journal.expected(heads)
        queries = ["SELECT id,writer,seq,delta,token FROM ledger ORDER BY id",
                   "SELECT id,head,total FROM accounts ORDER BY id",
                   "SELECT id,value,body FROM slots ORDER BY id",
                   "SELECT id,n,total FROM counters ORDER BY id",
                   "SELECT id,body FROM payload ORDER BY id"]
        queries += [f"SELECT id FROM ledger WHERE writer={w} AND seq>=3 AND seq<=10000000 ORDER BY id" for w in range(self.args.writers)]
        queries += ["SELECT id,value FROM slots WHERE value>=0 ORDER BY id"]
        digest = hashlib.sha256()
        for sql in queries:
            actual = conn.execute(sql).fetchall()
            wanted = expected.execute(sql).fetchall()
            assert actual == wanted, f"independent SQLite mismatch: {label}: {sql}: actual={actual[:5]} expected={wanted[:5]} counts={len(actual)}/{len(wanted)}"
            digest.update(json.dumps(wanted, separators=(",", ":")).encode())
        assert conn.execute("SELECT COUNT(*) FROM rollback_probe").fetchone()[0] == 0
        rows = expected.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        expected.close()
        result = {"label": label, "heads": heads, "ledger_rows": rows, "queries_compared": len(queries), "oracle_sha256": digest.hexdigest()}
        self.report["oracle_checks"].append(result)
        self.emit("oracle-pass", **result)
        return result

    def restore_check(self, name, lower=None, upper=None):
        target = self.root / ("restore-" + name)
        self.run_cli("backup", "restore", self.repo / (name + ".bicbackup"), target, "--key-env", KEY_ENV)
        process, _, conn = self.serve(target)
        try:
            result = self.verify_database(conn, name, lower, upper)
        finally:
            conn.close()
            self.stop_process(process)
        self.report["restores"].append(result)

    def fault_campaign(self, conn, process):
        baseline = self.backup(conn, "fault-baseline")
        self.report["backups"].append(baseline)
        for kind in ["enospc-object", "eio-file-sync", "eio-dir-sync", "eio-published-dir-sync", "eio-manifest"]:
            offset = self.set_fault(kind)
            try:
                job = self.backup(conn, kind, expected="failed")
                hits = self.fault_hits(offset, kind)
                assert hits, f"fault did not hit real I/O: {kind}"
                published = (self.repo / (kind + ".bicbackup")).exists()
                if kind not in ("eio-dir-sync", "eio-published-dir-sync"):
                    assert not published, job
                if kind == "eio-published-dir-sync":
                    assert published, "post-link directory sync fault did not leave its complete manifest"
            finally:
                self.set_fault("")
            self.verify_database(conn, kind)
            # A link can succeed before the directory fsync reports EIO. The
            # operation was not acknowledged; an artifact may be absent or
            # complete, but a visible partial artifact is never acceptable.
            if published:
                self.run_cli("backup", "verify", self.repo / (kind + ".bicbackup"), "--key-env", KEY_ENV)
                self.restore_check(kind)
            self.run_cli("backup", "verify", self.repo / "fault-baseline.bicbackup", "--key-env", KEY_ENV)
            self.report["faults"].append({"kind": kind, "hits": hits[:4], "complete_or_absent": True,
                                         "publication_acknowledged": False, "published_and_verified": published,
                                         "error": job["error"]})
            self.emit("fault-pass", kind=kind)
        for kind in ["stall-object", "stall-manifest", "stall-published"]:
            offset = self.set_fault(kind)
            self.start_backup(conn, kind)
            deadline = time.monotonic() + 180
            while not self.fault_hits(offset, kind):
                assert time.monotonic() < deadline, f"crash point not reached: {kind}"
                assert process.poll() is None
                time.sleep(.01)
            conn.close()
            self.stop_process(process, kill=True)
            self.set_fault("")
            process, _, conn = self.serve(self.root / "db", primary=True)
            self.verify_database(conn, "restart-" + kind)
            manifest = self.repo / (kind + ".bicbackup")
            if kind != "stall-published":
                assert not manifest.exists(), f"unpublished backup appeared: {kind}"
            if manifest.exists():
                self.run_cli("backup", "verify", manifest, "--key-env", KEY_ENV)
                self.restore_check(kind)
            self.report["faults"].append({"kind": kind, "sigkill": True, "published": manifest.exists(), "acknowledged_source_preserved": True})
            self.emit("fault-pass", kind=kind)
        self.restore_check("fault-baseline")
        # Explicitly force the non-atomic BicDB/SQLite acknowledgement window.
        # One COMMIT returned but its acknowledgement was never journaled;
        # another transaction never sent COMMIT. Only these uncertain IDs may
        # resolve either way. Known acknowledgements are never reclassified.
        committed_seq = self.journal.next_sequence(0)
        delta, token = self.journal.plan(0, committed_seq, False)
        self.transaction(conn, 0, committed_seq, delta, token)
        self.journal.outcome(0, committed_seq, "commit_sent")
        conn.execute("COMMIT")
        # Deliberately omit Journal.acknowledge here.
        aborted_seq = self.journal.next_sequence(1)
        delta, token = self.journal.plan(1, aborted_seq, False)
        self.transaction(conn, 1, aborted_seq, delta, token)
        self.stop_process(process, kill=True)
        conn.close()
        process, _, conn = self.serve(self.root / "db", primary=True)
        resolved = self.journal.reconcile(conn)
        assert {(r['writer'], r['seq'], r['outcome']) for r in resolved} == {
            (0, committed_seq, 'resolved_committed'), (1, aborted_seq, 'resolved_aborted')}
        self.verify_database(conn, "acknowledgement-window-restart", self.journal.heads())
        self.report["faults"].append({"kind": "commit-before-oracle-ack-sigkill", "resolved_transactions": resolved})
        after_uncertain = self.backup(conn, "after-uncertain", "fault-baseline")
        self.report["backups"].append(after_uncertain)
        self.restore_check("after-uncertain", self.journal.heads())
        # Fail a real multi-table source COMMIT durability operation. Its ID is
        # durably planned before sending COMMIT and reconciled after restart;
        # every previously acknowledged transaction must survive.
        for kind in ["eio-source-wal", "eio-page-wal"]:
            probe_seq = self.journal.next_sequence(0)
            delta, token = self.journal.plan(0, probe_seq, False)
            self.transaction(conn, 0, probe_seq, delta, token)
            self.journal.outcome(0, probe_seq, "commit_sent")
            offset = self.set_fault(kind)
            rejected = False
            try:
                conn.execute("COMMIT")
            except psycopg.Error:
                rejected = True
            finally:
                self.set_fault("")
            assert self.fault_hits(offset, kind), "source durability injection was not reached"
            assert rejected, "source commit acknowledged despite injected WAL sync failure"
            conn.close()
            self.stop_process(process, kill=True)
            process, _, conn = self.serve(self.root / "db", primary=True)
            resolved = self.journal.reconcile(conn)
            assert len(resolved) == 1 and resolved[0]["writer"] == 0 and resolved[0]["seq"] == probe_seq
            self.verify_database(conn, kind + "-restart")
            self.report["faults"].append({"kind": kind, "commit_rejected": True, "acknowledged_source_preserved": True,
                                         "resolved_transactions": resolved})
        # Stop a restore after an actual payload rename into the destination.
        target = self.root / "interrupted-restore"
        env = self.env | {"BICDB_QUALIFY_RESTORE": str(target)}
        offset = self.set_fault("stall-install")
        log = (self.root / "interrupted-restore.log").open("w")
        child = subprocess.Popen([str(self.binary), "backup", "restore", str(self.repo / "fault-baseline.bicbackup"), str(target), "--key-env", KEY_ENV], env=env, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((child, log))
        deadline = time.monotonic() + 180
        while not self.fault_hits(offset, "stall-install"):
            assert time.monotonic() < deadline and child.poll() is None, "restore installation crash point was not reached"
            time.sleep(.01)
        self.stop_process(child, kill=True)
        self.set_fault("")
        assert (target / "backup-restore.pending").exists()
        rejected = self.run_cli("inspect", target, success=False)
        assert "interrupted backup restore" in rejected.stderr + rejected.stdout
        self.run_cli("backup", "restore", self.repo / "fault-baseline.bicbackup", target, "--force", "--key-env", KEY_ENV)
        restored_process, _, restored = self.serve(target)
        try:
            result = self.verify_database(restored, "repaired-interrupted-restore",
                                          baseline["lower_heads"], baseline.get("upper_heads"))
            self.report["restores"].append(result)
        finally:
            restored.close()
            self.stop_process(restored_process)
        self.report["faults"].append({"kind": "restore-install-sigkill", "partial_target_refused": True, "fresh_restore_verified": True})
        return conn, process

    def corruption_campaign(self):
        # Prune only this isolated clone to one complete manifest, ensuring
        # every remaining object is referenced by the backup under test.
        import shutil
        clone = self.root / "corruption-repository"
        shutil.copytree(self.repo, clone)
        manifest = clone / "fault-baseline.bicbackup"
        for other in clone.glob("*.bicbackup"):
            if other != manifest:
                other.unlink()
        self.run_cli("backup", "repository-gc", clone, "--min-age-seconds", "0", "--key-env", KEY_ENV)
        target = self.root / "corruption-target"
        target.mkdir()
        (target / "sentinel").write_bytes(b"must survive failed restore")
        def reject(label):
            self.run_cli("backup", "verify", manifest, "--key-env", KEY_ENV, success=False)
            self.run_cli("backup", "restore", manifest, target, "--force", "--key-env", KEY_ENV, success=False)
            assert (target / "sentinel").read_bytes() == b"must survive failed restore"
            assert not (target / "backup-restore.pending").exists(), "corruption reached target installation"
            self.report["corruption"].append({"case": label, "verify_rejected": True, "restore_rejected_before_install": True})
        objects = sorted((clone / "objects").glob("*/*"), key=lambda p: -p.stat().st_size)[:self.args.corrupt_objects]
        assert objects
        for object_path in objects:
            intact = object_path.read_bytes()
            mutations = {"bit-flip": intact[:len(intact)//2] + bytes([intact[len(intact)//2] ^ 1]) + intact[len(intact)//2+1:],
                         "truncated": intact[:len(intact)//2], "trailing-garbage": intact + b"unexpected trailing bytes", "missing": None}
            for label, damaged in mutations.items():
                try:
                    if damaged is None:
                        object_path.unlink()
                    else:
                        object_path.write_bytes(damaged)
                    reject(f"object-{object_path.name}-{label}")
                finally:
                    object_path.write_bytes(intact)
        intact = manifest.read_bytes()
        for label, damaged in [("manifest-magic", bytes([intact[0] ^ 1]) + intact[1:]),
                               ("manifest-ciphertext", intact[:-1] + bytes([intact[-1] ^ 1])),
                               ("manifest-truncated", intact[:len(intact)//2]),
                               ("manifest-trailing", intact + b"trailing")]:
            try:
                manifest.write_bytes(damaged)
                reject(label)
            finally:
                manifest.write_bytes(intact)
        self.run_cli("backup", "verify", manifest, "--key-env", KEY_ENV)
        self.emit("corruption-pass", cases=len(self.report["corruption"]))

    def run(self):
        self.started = time.monotonic()
        try:
            self.run_cli("init", self.root / "seed")
            self.run_cli("store", "migrate-mode", self.root / "seed", self.root / "db")
            process, port, conn = self.serve(self.root / "db", primary=True)
            self.seed(conn)
            workers = [threading.Thread(target=self.writer, args=(w, port), name=f"writer-{w}") for w in range(self.args.writers)]
            for worker in workers:
                worker.start()
            load_started = time.monotonic()
            previous = None
            number = 0
            try:
                while time.monotonic() - load_started < self.args.duration:
                    tick = time.monotonic()
                    name = f"load-{number:03d}"
                    parent = previous if number % 2 else None
                    job = self.backup(conn, name, parent)
                    assert job["acknowledged_after_copy_observed"] > 0, "no transaction completed during the observed copy phase"
                    self.report["backups"].append(job)
                    self.emit("backup-pass", name=name, parent=parent, transactions_during_copy=job["acknowledged_after_copy_observed"])
                    previous = name
                    number += 1
                    while time.monotonic() < min(load_started + self.args.duration, tick + self.args.interval):
                        assert not self.errors, self.errors
                        time.sleep(.1)
            finally:
                self.stop.set()
                for worker in workers:
                    worker.join(timeout=30)
                assert all(not w.is_alive() for w in workers), "writer failed to stop at acknowledgement barrier"
            assert not self.errors, self.errors
            assert number >= self.args.minimum_backups, f"too few backups completed: {number}"
            self.report["sustained_seconds"] = time.monotonic() - load_started
            self.report["acknowledged_transactions"] = self.journal.count()
            self.report["rollback_attempts"] = self.journal.db.execute("SELECT COUNT(*) FROM attempts WHERE rolled_back=1").fetchone()[0]
            self.report["peak_inflight_transactions"] = self.peak_inflight
            self.report["writers"] = []
            for stats in self.writer_stats:
                latencies = sorted(stats["latencies"])
                assert stats["commits"] > 0, "one writer made no progress"
                self.report["writers"].append({key: value for key, value in stats.items() if key != 'latencies'} | {
                    "transaction_with_oracle_intent_p99_seconds": latencies[min(int(len(latencies)*.99),len(latencies)-1)],
                    "transaction_with_oracle_intent_max_seconds": max(latencies)})
            assert self.peak_inflight >= 2, "concurrent transactions were not exercised"
            self.verify_database(conn, "live-source", self.journal.heads())
            for job in self.report["backups"]:
                self.restore_check(job["name"], job["lower_heads"], job.get("upper_heads"))
            conn, process = self.fault_campaign(conn, process)
            self.verify_database(conn, "final-source", self.journal.heads())
            conn.close()
            self.stop_process(process)
            self.corruption_campaign()
            self.report["passed"] = True
        except BaseException:
            self.report["error"] = traceback.format_exc()
            raise
        finally:
            self.stop.set()
            self.set_fault("")
            for process, log in reversed(self.processes):
                self.stop_process(process)
                log.close()
            self.report["elapsed_seconds"] = time.monotonic() - self.started
            self.report["all_processes_stopped"] = all(p.poll() is not None for p, _ in self.processes)
            (self.root / "report.json").write_text(json.dumps(self.report, indent=2))
            self.journal.db.close()
        self.emit("campaign-pass", transactions=self.report["acknowledged_transactions"], backups=len(self.report["backups"]), restores=len(self.report["restores"]), faults=len(self.report["faults"]), corruption_cases=len(self.report["corruption"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True, help="new isolated directory; retained as evidence")
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--interval", type=float, default=40)
    parser.add_argument("--writer-pause", type=float, default=.03)
    parser.add_argument("--copy-rate", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--seed-rows", type=int, default=1024)
    parser.add_argument("--seed-bytes", type=int, default=8192)
    parser.add_argument("--minimum-backups", type=int, default=4)
    parser.add_argument("--corrupt-objects", type=int, default=12)
    args = parser.parse_args()
    assert args.duration > 0 and args.writers > 1 and args.interval > 0 and args.minimum_backups >= 2
    Campaign(args).run()


if __name__ == "__main__":
    main()
