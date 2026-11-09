#!/usr/bin/env python3
"""Independent crash/recovery reconciler for BicDB storage-durability fixes.

Drives `bicdb serve` over pgwire (own stdlib client, pgw.py) with ordinary
SQL. Clients keep their own fsynced ledgers OUTSIDE the database directory.
After each crash and recovery the ledgers are read back from disk and
reconciled against what the recovered server returns.

One invocation runs one boundary for N repetitions and appends one JSON line
per trial to --out.
"""

import argparse
import hashlib
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pgw  # noqa: E402

WRITERS = 6
KSPAN = 10_000_000


# ----------------------------------------------------------------- workload

def payload_for(row_id):
    digest = hashlib.sha256(row_id.encode()).hexdigest()
    return digest * (1 + int(digest[:4], 16) % 31)


def tok_of(w, i):
    return f"w{w}-{i}"


def k_of(w, i, j):
    return w * KSPAN + i * 3 + j


def delete_target(w, i):
    return f"{tok_of(w, i - 3)}:0" if i >= 3 and i % 5 == 0 else None


class Ledger:
    """Append-only JSON lines, fsynced per line, outside the database dir."""

    def __init__(self, path):
        self.fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    def write(self, record):
        record["t"] = time.time()
        os.write(self.fd, (json.dumps(record, sort_keys=True) + "\n").encode())
        os.fsync(self.fd)

    def close(self):
        os.close(self.fd)


def read_ledgers(ledger_dir):
    attempts, results, commit_sent, order = {}, {}, {}, {}
    for name in sorted(os.listdir(ledger_dir)):
        if not name.startswith("writer-"):
            continue
        w = int(name.split("-")[1])
        seq = []
        with open(os.path.join(ledger_dir, name)) as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                tok = rec["tok"]
                if rec["ev"] == "attempt":
                    attempts[tok] = rec
                elif rec["ev"] == "commit":
                    commit_sent[tok] = rec.get("txid")
                elif rec["ev"] == "result":
                    results[tok] = rec
                    seq.append(rec)
        order[w] = seq
    return attempts, results, commit_sent, order


def client_loop(w, port, ledger, start_i, cycle, stop, stats):
    i = start_i
    conn = None
    failures_after_fence = 0
    fenced = False
    while not stop.is_set():
        if conn is None:
            try:
                conn = pgw.Conn("127.0.0.1", port, timeout=30)
            except (pgw.ConnectionLost, OSError, pgw.PgError):
                stats["connect_fail"] += 1
                if stop.wait(0.05):
                    break
                continue
        tok = tok_of(w, i)
        target = delete_target(w, i)
        ledger.write({"ev": "attempt", "tok": tok, "w": w, "i": i, "cycle": cycle,
                      "delete": target})
        stage = "stmt"
        try:
            conn.query("BEGIN")
            txid = conn.query("SELECT txid_current()").rows[0][0]
            values = ",".join(
                f"('{tok}:{part}', '{tok}', {w}, {k_of(w, i, j)}, '{payload_for(f'{tok}:{part}')}')"
                for j, part in enumerate(["m", "0", "1"]))
            conn.query(f"INSERT INTO evr (id, txn, writer, k, payload) VALUES {values}")
            conn.query(f"UPDATE evhot SET seq = {i}, txn = '{tok}' WHERE writer = {w}")
            if target:
                conn.query(f"DELETE FROM evr WHERE id = '{target}'")
            ledger.write({"ev": "commit", "tok": tok, "txid": txid})
            stage = "commit"
            res = conn.query("COMMIT")
            if res.tag == "COMMIT":
                ledger.write({"ev": "result", "tok": tok, "status": "ok", "fenced_before": fenced})
                stats["ok"] += 1
            else:
                ledger.write({"ev": "result", "tok": tok, "status": "error", "code": "TAG",
                              "detail": str(res.tag), "stage": stage})
                stats["err"] += 1
        except pgw.PgError as error:
            ledger.write({"ev": "result", "tok": tok, "status": "error", "code": error.code,
                          "message": error.message[:300], "detail": error.detail,
                          "hint": error.hint[:200], "stage": stage})
            stats["err"] += 1
            stats.setdefault("codes", {}).setdefault(error.code, 0)
            stats["codes"][error.code] += 1
            try:
                conn.query("ROLLBACK")
            except pgw.PgError:
                pass
            except pgw.ConnectionLost:
                conn = None
            if error.code in ("08007", "58030"):
                fenced = True
                failures_after_fence += 1
                if failures_after_fence > 3:
                    break
                time.sleep(0.02)
        except pgw.ConnectionLost as error:
            ledger.write({"ev": "result", "tok": tok, "status": "lost", "stage": stage,
                          "message": str(error)[:200]})
            stats["lost"] += 1
            conn = None
            break
        i += 1
    if conn is not None:
        conn.close()
    stats["next_i"] = i


# ------------------------------------------------------------------ server

LIVE = []


def kill_live():
    for proc in LIVE:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                pass
            proc.wait()
    LIVE.clear()


class Server:
    def __init__(self, args, db, logpath, extra_env):
        env = dict(os.environ)
        for key in list(env):
            if key.startswith("BICDB_"):
                del env[key]
        env.update(args.base_env)
        env.update(extra_env)
        self.log = open(logpath, "ab")
        cmd = [args.bin, "serve", db, "--port", str(args.port),
               "--max-result-rows", "10000000", "--max-connections", "128",
               "--max-connections-per-ip", "64", "--shutdown-grace-seconds", "5"]
        self.proc = subprocess.Popen(cmd, env=env, stdout=self.log, stderr=self.log,
                                     start_new_session=True)
        LIVE.append(self.proc)

    def wait_ready(self, timeout=180):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                return False
            try:
                conn = pgw.Conn("127.0.0.1", ARGS.port, timeout=5)
                conn.query("SELECT 1")
                conn.close()
                return True
            except (OSError, pgw.ConnectionLost, pgw.PgError):
                time.sleep(0.1)
        return False

    def kill(self):
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGKILL)
        self.proc.wait()
        self.log.close()
        return self.proc.returncode

    def term(self, timeout=60):
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait()
        self.log.close()
        return self.proc.returncode


def port_free(port):
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) != 0


# --------------------------------------------------------------- tampering

MAGIC = b"BICF"


def frames(path):
    """Independent frame walker: (offset, length) of each complete frame.
    Header: 'BICF' v1 kind flags(2) len(4 LE) crc(4)."""
    data = open(path, "rb").read()
    out, pos = [], 0
    while pos + 16 <= len(data) and data[pos:pos + 4] == MAGIC:
        length = int.from_bytes(data[pos + 8:pos + 12], "little")
        if pos + 16 + length > len(data):
            break
        out.append((pos, 16 + length))
        pos += 16 + length
    return out, len(data)


def frame_logs(db):
    found = []
    for root, _dirs, files in os.walk(db):
        for name in files:
            path = os.path.join(root, name)
            try:
                with open(path, "rb") as handle:
                    if handle.read(4) == MAGIC:
                        found.append(path)
            except OSError:
                pass
    return sorted(found)


def tree_digest(db):
    digest = {}
    for root, _dirs, files in os.walk(db):
        for name in files:
            path = os.path.join(root, name)
            h = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    h.update(chunk)
            digest[os.path.relpath(path, db)] = h.hexdigest()
    return digest


def tamper_torn(db, rng, target):
    """Append a torn copy of the last complete frame to one frame log: either
    a strict prefix of it (torn write) or the whole frame with one byte flipped
    (length landed, contents did not). Bytes that were never acknowledged:
    nothing synced is modified."""
    logs = [p for p in frame_logs(db) if target in os.path.relpath(p, db)]
    logs = [p for p in logs if frames(p)[0]]
    if not logs:
        return {"applied": False, "reason": f"no frame log matching {target}"}
    path = rng.choice(logs)
    fr, size = frames(path)
    off, length = fr[-1]
    frame = bytearray(open(path, "rb").read()[off:off + length])
    if rng.random() < 0.5:
        cut = rng.randrange(1, length)
        blob, how = bytes(frame[:cut]), f"prefix {cut}/{length}"
    else:
        flip = rng.randrange(16, length) if length > 16 else rng.randrange(length)
        frame[flip] ^= 0xFF
        blob, how = bytes(frame), f"complete frame, byte {flip} flipped"
    with open(path, "ab") as handle:
        handle.write(blob)
        handle.flush()
        os.fsync(handle.fileno())
    return {"applied": True, "file": os.path.relpath(path, db), "frames": len(fr),
            "size_before": size, "how": how}


def tamper_midfile(db, rng, target):
    """Flip one byte inside a frame that is followed by at least one complete
    frame (mid-file damage)."""
    logs = [p for p in frame_logs(db) if target in os.path.relpath(p, db)]
    logs = [p for p in logs if len(frames(p)[0]) >= 2]
    if not logs:
        return {"applied": False, "reason": f"no multi-frame log matching {target}"}
    path = rng.choice(logs)
    fr, size = frames(path)
    idx = rng.randrange(0, len(fr) - 1)
    off, length = fr[idx]
    pos = off + rng.randrange(length)
    with open(path, "r+b") as handle:
        handle.seek(pos)
        byte = handle.read(1)
        handle.seek(pos)
        handle.write(bytes([byte[0] ^ 0xFF]))
        handle.flush()
        os.fsync(handle.fileno())
    return {"applied": True, "file": os.path.relpath(path, db), "frames": len(fr),
            "damaged_frame": idx, "offset": pos, "size": size}


# ---------------------------------------------------------------- verifier

def run_verifier(args, db, label, trial_dir):
    if args.mode != "paged" or not args.verifier:
        return {"ran": False}
    before = tree_digest(db)
    out = os.path.join(trial_dir, f"verify-{label}.json")
    proc = subprocess.run([args.verifier, "verify-store", db, "--read-only",
                           "--max-seconds", "300"], capture_output=True, timeout=900)
    after = tree_digest(db)
    with open(out, "wb") as handle:
        handle.write(proc.stdout + b"\n--stderr--\n" + proc.stderr)
    summary = None
    try:
        report = json.loads(proc.stdout)
        summary = {k: report.get(k) for k in ("status", "verdict", "result", "clean")
                   if k in report}
        if not summary:
            summary = {"keys": sorted(report)[:12]}
    except ValueError:
        summary = {"unparsed": proc.stdout[:300].decode(errors="replace")}
    return {"ran": True, "exit": proc.returncode, "clean": proc.returncode == 0,
            "unchanged": before == after, "summary": summary}


# --------------------------------------------------------------- reconcile

def detail_identity(detail):
    ident = {}
    for part in (detail or "").split():
        if "=" in part:
            key, value = part.split("=", 1)
            ident[key] = value
    return ident


def reconcile(args, ledger_dir, stage, resolutions_prev, capture_plans, prev_committed=None,
              control=None):
    attempts, results, commit_sent, order = read_ledgers(ledger_dir)
    conn = pgw.Conn("127.0.0.1", args.port, timeout=120)
    v = []  # violations

    def viol(kind, **info):
        if len(v) < 200:
            v.append(dict(kind=kind, **info))
        counts[kind] = counts.get(kind, 0) + 1

    counts = {}
    if control:
        apply_control(conn, control, attempts, results)
    scan = conn.query("SELECT id, txn, writer, k, payload FROM evr").rows
    count = int(conn.query("SELECT count(*) FROM evr").rows[0][0])
    rows = {}
    for rid, txn, writer, k, payload in scan:
        if writer is None or k is None:
            viol("row_unreadable", id=rid)
            continue
        if rid in rows:
            viol("duplicate_row_in_scan", id=rid)
        rows[rid] = (txn, int(writer), int(k), payload)
    if count != len(scan):
        viol("count_differs_from_scan", count=count, scan=len(scan))

    # (d) every row was written by a known transaction, with exact contents
    for rid, (txn, writer, k, payload) in rows.items():
        tok, _, part = rid.partition(":")
        att = attempts.get(tok)
        if att is None or part not in ("m", "0", "1"):
            viol("row_no_transaction_wrote", id=rid)
            continue
        j = ["m", "0", "1"].index(part)
        if (txn, writer, k, payload) != (tok, att["w"], k_of(att["w"], att["i"], j),
                                         payload_for(rid)):
            viol("row_content_mismatch", id=rid)

    # 08007 resolution
    resolutions = {}
    for tok, res in results.items():
        if res["status"] == "error" and res.get("code") == "08007":
            ident = detail_identity(res.get("detail"))
            inc, seq = ident.get("incarnation"), ident.get("commit_seq")
            if not inc or not seq or seq == "none":
                resolutions[tok] = {"answer": "no_identity", "ident": ident}
                continue
            answer = conn.query(f"SELECT bicdb_commit_outcome('{inc}', {int(seq)})").rows[0][0]
            resolutions[tok] = {"answer": answer, "incarnation": inc, "commit_seq": int(seq),
                                "transaction_id": ident.get("transaction_id")}
            if ident.get("transaction_id") not in (None, "none") and \
                    ident.get("transaction_id") != str(commit_sent.get(tok)):
                viol("08007_transaction_id_mismatch", tok=tok, detail=ident.get("transaction_id"),
                     ledger=commit_sent.get(tok))

    classes = {"ok": 0, "failed": 0, "unknown_08007": 0, "inflight_commit_sent": 0,
               "inflight_no_commit": 0}
    inflight_outcomes = {"present": 0, "absent": 0}
    committed = set()
    for tok, att in attempts.items():
        present_m = f"{tok}:m" in rows
        present_1 = f"{tok}:1" in rows
        if present_m != present_1:
            viol("partial_transaction", tok=tok, m=present_m, r1=present_1)
        res = results.get(tok)
        if att.get("setup"):
            continue
        if res is not None and res["status"] == "ok":
            classes["ok"] += 1
            if not present_m:
                viol("acknowledged_missing", tok=tok)  # (a)
        elif res is not None and res["status"] == "error" and res.get("code") != "08007":
            classes["failed"] += 1
            if present_m:
                viol("failed_present", tok=tok, code=res.get("code"))  # (b)
        elif res is not None and res["status"] == "error":
            classes["unknown_08007"] += 1
            answer = resolutions[tok]["answer"]
            if answer == "committed" and not present_m:
                viol("resolved_committed_but_absent", tok=tok)  # (c)
            elif answer == "not_committed" and present_m:
                viol("resolved_not_committed_but_present", tok=tok)
            elif answer not in ("committed", "not_committed"):
                viol("unresolved_after_reopen", tok=tok, answer=answer)
            prev = resolutions_prev.get(tok)
            if prev is not None and prev["answer"] != answer:
                viol("resolution_changed", tok=tok, before=prev["answer"], after=answer)
        elif tok in commit_sent:
            classes["inflight_commit_sent"] += 1
            inflight_outcomes["present" if present_m else "absent"] += 1
        else:
            classes["inflight_no_commit"] += 1
            if present_m:
                viol("committed_without_commit_sent", tok=tok)
        if present_m:
            committed.add(tok)

    if prev_committed is not None:
        for tok in prev_committed - committed:
            viol("committed_then_vanished", tok=tok)
        for tok in committed - prev_committed:
            if attempts[tok]["cycle"] == 1:
                viol("absent_then_appeared", tok=tok)

    # deletes, and the hot row = newest committed commit per writer
    deleted_by = {}
    for tok in committed:
        target = attempts[tok].get("delete")
        if target:
            deleted_by[target] = tok
    for tok in committed:
        rid = f"{tok}:0"
        if rid in deleted_by and rid in rows:
            viol("acknowledged_delete_resurrected", id=rid, by=deleted_by[rid])
        if rid not in deleted_by and rid not in rows:
            viol("row_missing_not_deleted", id=rid)
    hot = {}
    for hot_row in conn.query("SELECT writer, seq, txn FROM evhot").rows:
        try:
            hot[int(hot_row[0])] = (int(hot_row[1]), hot_row[2])
        except (TypeError, ValueError):
            viol("hot_row_unreadable", row=[str(x)[:80] for x in hot_row])
    for w in range(WRITERS):
        mine = [attempts[t]["i"] for t in committed if attempts[t]["w"] == w]
        expect = (max(mine), tok_of(w, max(mine))) if mine else (-1, "setup")
        if hot.get(w) != expect:
            viol("hot_row_mismatch", writer=w, got=hot.get(w), expect=expect)

    # (e) secondary and primary indexes agree with the scan
    plans = {}
    for w in range(WRITERS):
        via = {r[0] for r in conn.query(f"SELECT id FROM evr WHERE writer = {w}").rows}
        expect = {rid for rid, val in rows.items() if val[1] == w}
        if via != expect:
            viol("writer_index_differs", writer=w, missing=len(expect - via), extra=len(via - expect))
        lo, hi = w * KSPAN, (w + 1) * KSPAN
        via = {r[0] for r in conn.query(f"SELECT id FROM evr WHERE k >= {lo} AND k < {hi}").rows}
        expect = {rid for rid, val in rows.items() if lo <= val[2] < hi}
        if via != expect:
            viol("k_index_differs", writer=w, missing=len(expect - via), extra=len(via - expect))
    rng = random.Random(len(rows))
    sample_present = rng.sample(sorted(rows), min(40, len(rows)))
    absent_pool = sorted(f"{t}:m" for t in attempts if t not in committed and not attempts[t].get("setup"))
    sample_absent = rng.sample(absent_pool, min(40, len(absent_pool)))
    for rid in sample_present:
        got = conn.query(f"SELECT id FROM evr WHERE id = '{rid}'").rows
        if len(got) != 1:
            viol("pk_lookup_missing", id=rid)
        k = rows[rid][2]
        got = conn.query(f"SELECT id FROM evr WHERE k = {k}").rows
        if [r[0] for r in got] != [rid]:
            viol("unique_index_lookup_wrong", id=rid)
    for rid in sample_absent:
        if conn.query(f"SELECT id FROM evr WHERE id = '{rid}'").rows:
            viol("pk_lookup_found_absent", id=rid)
    if sample_present:
        rid = sample_present[0]
        try:
            conn.query("BEGIN")
            conn.query(f"INSERT INTO evr (id, txn, writer, k, payload) VALUES "
                       f"('probe-dup', 'probe', 99, {rows[rid][2]}, 'x')")
            conn.query("ROLLBACK")
            viol("unique_index_not_enforced", k=rows[rid][2])
        except pgw.PgError as error:
            if error.code != "23505":
                viol("unique_probe_unexpected_error", code=error.code)
            try:
                conn.query("ROLLBACK")
            except pgw.PgError:
                pass
    if capture_plans:
        for label, sql in (("writer", "SELECT id FROM evr WHERE writer = 1"),
                           ("k_range", f"SELECT id FROM evr WHERE k >= {KSPAN} AND k < {2 * KSPAN}"),
                           ("k_eq", f"SELECT id FROM evr WHERE k = {KSPAN}"),
                           ("pk", "SELECT id FROM evr WHERE id = 'w1-1:m'")):
            try:
                plans[label] = " | ".join(r[0] for r in conn.query("EXPLAIN " + sql).rows)
            except pgw.PgError as error:
                plans[label] = f"error {error.code}"

    # fail-closed rule: per client, no acknowledgment after its first durability error
    for w, seq in order.items():
        seen_fence = False
        cycle = None
        for res in seq:
            if attempts[res["tok"]]["cycle"] != cycle:
                cycle = attempts[res["tok"]]["cycle"]
                seen_fence = False  # a new server incarnation
            if res["status"] == "error" and res.get("code") in ("08007", "58030"):
                seen_fence = True
            elif res["status"] == "ok" and seen_fence:
                viol("ack_after_durability_error", writer=w, tok=res["tok"])
    conn.close()
    return {"stage": stage, "rows": len(rows), "classes": classes,
            "inflight_outcomes": inflight_outcomes,
            "resolutions": {"committed": sum(r["answer"] == "committed" for r in resolutions.values()),
                            "not_committed": sum(r["answer"] == "not_committed" for r in resolutions.values()),
                            "other": sum(r["answer"] not in ("committed", "not_committed") for r in resolutions.values())},
            "violations": counts, "violation_samples": v[:20], "plans": plans}, resolutions, committed


# ------------------------------------------------- negative controls (harness self-test)

CONTROL_EXPECT = {
    "delete_acked": {"acknowledged_missing"},
    "stray_row": {"row_no_transaction_wrote"},
    "hot_rollback": {"hot_row_mismatch"},
    "payload_corrupt": {"row_content_mismatch"},
    "resurrect_deleted": {"acknowledged_delete_resurrected"},
}


def apply_control(conn, control, attempts, results):
    """Deliberately damage the recovered database through SQL so the
    reconciler must report it (proves the checks can fail)."""
    acked = sorted(t for t, r in results.items() if r["status"] == "ok")
    tok = acked[len(acked) // 2]
    if control == "delete_acked":
        conn.query(f"DELETE FROM evr WHERE id LIKE '{tok}:%'")
    elif control == "stray_row":
        conn.query("INSERT INTO evr (id, txn, writer, k, payload) VALUES "
                   "('nobody-1:m', 'nobody-1', 0, 999999999, 'x')")
    elif control == "hot_rollback":
        conn.query("UPDATE evhot SET seq = seq - 1 WHERE writer = 0")
    elif control == "payload_corrupt":
        conn.query(f"UPDATE evr SET payload = 'corrupt' WHERE id = '{tok}:1'")
    elif control == "resurrect_deleted":
        deleted = [a["delete"] for t, a in attempts.items()
                   if a.get("delete") and results.get(t, {}).get("status") == "ok"]
        rid = deleted[0]
        dtok = rid.split(":")[0]
        w, i = attempts[dtok]["w"], attempts[dtok]["i"]
        conn.query(f"INSERT INTO evr (id, txn, writer, k, payload) VALUES "
                   f"('{rid}', '{dtok}', {w}, {k_of(w, i, 1)}, '{payload_for(rid)}')")


# ------------------------------------------------------------------- trial

def setup_schema(args, db, ledger_dir, trial_dir):
    env = {k: v for k, v in os.environ.items() if not k.startswith("BICDB_")}
    env.update(args.base_env)
    with open(os.path.join(trial_dir, "server.log"), "ab") as log:
        subprocess.run([args.bin, "init", db], env=env, stdout=log, stderr=log, check=True)
    with open(os.path.join(db, "format.json")) as handle:
        mode = json.load(handle).get("storage_mode")
    want = "server_paged" if args.mode == "paged" else "embedded_memory"
    if mode != want:
        raise RuntimeError(f"created storage_mode {mode}, wanted {want}")
    srv = Server(args, db, os.path.join(trial_dir, "server.log"), {})
    if not srv.wait_ready():
        srv.kill()
        raise RuntimeError("setup server did not start")
    conn = pgw.Conn("127.0.0.1", args.port)
    conn.query("CREATE TABLE evr (id text PRIMARY KEY, txn text NOT NULL, writer int4 NOT NULL, "
               "k int8 NOT NULL, payload text NOT NULL)")
    conn.query("CREATE INDEX evr_writer ON evr (writer)")
    conn.query("CREATE UNIQUE INDEX evr_k ON evr (k)")
    conn.query("CREATE TABLE evhot (writer int4 PRIMARY KEY, seq int8 NOT NULL, txn text NOT NULL)")
    conn.query("INSERT INTO evhot VALUES " + ",".join(f"({w}, -1, 'setup')" for w in range(WRITERS)))
    conn.close()
    rc = srv.term()
    return rc


def run_workload(args, db, ledger_dir, trial_dir, cycle, start, extra_env, rng, kind):
    srv = Server(args, db, os.path.join(trial_dir, "server.log"), extra_env)
    if not srv.wait_ready():
        rc = srv.kill()
        return {"started": False, "server_exit": rc}, start
    stop = threading.Event()
    ledgers = [Ledger(os.path.join(ledger_dir, f"writer-{w}")) for w in range(WRITERS)]
    stats = [{"ok": 0, "err": 0, "lost": 0, "connect_fail": 0} for _ in range(WRITERS)]
    threads = [threading.Thread(target=client_loop,
                                args=(w, args.port, ledgers[w], start[w], cycle, stop, stats[w]))
               for w in range(WRITERS)]
    t0 = time.time()
    for t in threads:
        t.start()
    duration = args.duration if kind != "kill" else rng.uniform(0.3, args.duration)
    outcome = "timeout"
    fence_seen_at = None
    while True:
        time.sleep(0.02)
        if srv.proc.poll() is not None:
            outcome = "server_exited"
            break
        if fence_seen_at is None and any(s.get("codes", {}).get("08007") or s.get("codes", {}).get("58030")
                                         for s in stats):
            fence_seen_at = time.time()
        if fence_seen_at is not None and time.time() - fence_seen_at > 1.0:
            outcome = "fenced"
            break
        if time.time() - t0 > duration:
            break
    elapsed = time.time() - t0
    graceful = cycle == 1 and ARGS.end == "term" and outcome == "timeout"
    if graceful:
        # stop the clients first, then a graceful shutdown (shutdown checkpoint
        # and transaction-log cut run with the fault armed)
        stop.set()
        for t in threads:
            t.join(60)
        rc = srv.term()
        outcome = "graceful_shutdown"
    else:
        rc = srv.kill()
    stop.set()
    for t in threads:
        t.join(60)
    for ledger in ledgers:
        ledger.close()
    nxt = [s.get("next_i", start[w]) + 1 for w, s in enumerate(stats)]
    agg = {"ok": sum(s["ok"] for s in stats), "err": sum(s["err"] for s in stats),
           "lost": sum(s["lost"] for s in stats), "codes": {}}
    for s in stats:
        for code, n in s.get("codes", {}).items():
            agg["codes"][code] = agg["codes"].get(code, 0) + n
    log = open(os.path.join(trial_dir, "server.log"), "rb").read()
    marker = None
    try:
        with open(os.path.join(db, "durability-failed.json")) as handle:
            marker = json.load(handle)
    except (OSError, ValueError):
        pass
    counts = {key: log.count(key.encode()) for key in
              ("auto-checkpoint", "checkpoint", "durability.write_failed", "frame_log", "panicked")}
    return {"marker": marker, "log_counts": counts, "started": True, "outcome": outcome, "elapsed_s": round(elapsed, 2), "server_exit": rc,
            "crash_point_reached": b"bicdb test crash point reached" in log,
            "write_failed_logged": log.count(b"durability.write_failed"),
            "marker_present": os.path.exists(os.path.join(db, "durability-failed.json")),
            "clients": agg}, nxt


def one_trial(args, rep):
    rng = random.Random(f"{args.name}-{rep}-{args.seed}")
    trial_dir = os.path.join(args.workroot, f"{args.name}-{rep}")
    shutil.rmtree(trial_dir, ignore_errors=True)
    os.makedirs(trial_dir)
    db = os.path.join(trial_dir, "db")
    ledger_dir = os.path.join(trial_dir, "ledger")
    os.makedirs(ledger_dir)
    rec = {"boundary": args.name, "rep": rep, "mode": args.mode, "profile": args.profile,
           "kind": args.kind, "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    setup_rc = setup_schema(args, db, ledger_dir, trial_dir)
    rec["setup_exit"] = setup_rc

    extra = {}
    if args.kind in ("failio", "crash"):
        site = rng.choice(args.sites)
        n = rng.randint(args.nmin, args.nmax)
        spec = f"{site}:{n}{'+' if args.persistent else ''}"
        if args.kind == "failio":
            extra["BICDB_TEST_FAIL_IO"] = spec
            if args.also_fail:
                extra["BICDB_TEST_FAIL_IO"] += "," + args.also_fail
        else:
            extra["BICDB_TEST_CRASH_AT"] = spec
        if args.volatile:
            extra["BICDB_TEST_WAL_VOLATILE_TAIL"] = "1"
        rec["injected"] = extra
    start = [0] * WRITERS
    cycle1, start = run_workload(args, db, ledger_dir, trial_dir, 1, start, extra, rng, args.kind)
    rec["cycle1"] = cycle1
    if args.kind == "failio":
        rec["fired"] = cycle1.get("marker_present") or bool(cycle1.get("write_failed_logged")) \
            or bool(cycle1["clients"]["codes"]) if cycle1.get("started") else False
    elif args.kind == "crash":
        rec["fired"] = cycle1.get("crash_point_reached", False)
    else:
        rec["fired"] = cycle1.get("started", False)

    if args.kind in ("torn", "midfile"):
        pristine = os.path.join(trial_dir, "pristine")
        shutil.copytree(db, pristine, symlinks=True)
        tam = (tamper_torn if args.kind == "torn" else tamper_midfile)(db, rng, args.target)
        rec["tamper"] = tam
        rec["fired"] = tam["applied"]

    if args.kind == "recoverycrash":
        # crash the RECOVERY of the crashed store at a named point, then
        # recover again without faults
        site = rng.choice(args.sites)
        n = rng.randint(args.nmin, args.nmax)
        renv = {"BICDB_TEST_CRASH_AT": f"{site}:{n}"}
        if args.volatile:
            renv["BICDB_TEST_WAL_VOLATILE_TAIL"] = "1"
        rec["injected"] = renv
        rsrv = Server(args, db, os.path.join(trial_dir, "server.log"), renv)
        came_up = rsrv.wait_ready()
        rc = rsrv.kill()
        log = open(os.path.join(trial_dir, "server.log"), "rb").read()
        rec["recovery_crash"] = {"came_up": came_up, "exit": rc,
                                 "crash_point_reached": log.count(b"bicdb test crash point reached") >= 1}
        rec["fired"] = rec["recovery_crash"]["crash_point_reached"]

    rec["verify_pre_recovery"] = run_verifier(args, db, "pre", trial_dir)
    shutil.copytree(db, os.path.join(trial_dir, "crashed-1"), symlinks=True)

    before = tree_digest(db) if args.kind == "midfile" else None
    srv = Server(args, db, os.path.join(trial_dir, "server.log"), {})
    ready = srv.wait_ready()
    rec["reopen_ok"] = ready
    if args.kind == "midfile":
        if ready:
            rec["midfile_refused"] = False
            srv.term()
        else:
            rec["midfile_refused"] = True
            srv.kill()
            after = tree_digest(db)
            rec["midfile_unchanged"] = after == before
            rec["midfile_changed_files"] = sorted(
                f for f in set(before) | set(after) if before.get(f) != after.get(f))
            rec["midfile_damaged_file_unchanged"] = (
                before.get(rec["tamper"]["file"]) == after.get(rec["tamper"]["file"])
                if rec.get("tamper", {}).get("applied") else None)
            tail = open(os.path.join(trial_dir, "server.log"), "rb").read()[-1500:]
            rec["refusal_tail"] = tail.decode(errors="replace")[-600:]
        shutil.rmtree(db)
        shutil.move(os.path.join(trial_dir, "pristine"), db)
        srv = Server(args, db, os.path.join(trial_dir, "server.log"), {})
        ready = srv.wait_ready()
        rec["reopen_after_restore_ok"] = ready
    elif args.kind == "torn":
        shutil.rmtree(os.path.join(trial_dir, "pristine"), ignore_errors=True)
    if not ready:
        srv.kill()
        rec["result"] = "REOPEN_FAILED"
        rec["server_log_tail"] = open(os.path.join(trial_dir, "server.log"), "rb").read()[-2000:].decode(errors="replace")
        return rec, trial_dir, False
    rec["marker_after_reopen"] = os.path.exists(os.path.join(db, "durability-failed.json"))
    r1, res1, com1 = reconcile(args, ledger_dir, "after_recovery_1", {}, True,
                               control=args.control or None)
    srv.term()
    rec["reconcile_1"] = r1

    # cycle 2: more writes on the recovered store, plain SIGKILL, recover again
    cycle2, start = run_workload(args, db, ledger_dir, trial_dir, 2, start, {}, rng, "kill")
    rec["cycle2"] = cycle2
    shutil.copytree(db, os.path.join(trial_dir, "crashed-2"), symlinks=True)
    rec["verify_pre_recovery_2"] = run_verifier(args, db, "pre2", trial_dir)
    srv = Server(args, db, os.path.join(trial_dir, "server.log"), {})
    if not srv.wait_ready():
        srv.kill()
        rec["result"] = "REOPEN_2_FAILED"
        rec["server_log_tail"] = open(os.path.join(trial_dir, "server.log"), "rb").read()[-2000:].decode(errors="replace")
        return rec, trial_dir, False
    if args.control:
        rec["reconcile_2"] = None
        detected = set(r1["violations"])
        rec["control_expected"] = sorted(CONTROL_EXPECT[args.control])
        rec["result"] = "PASS" if CONTROL_EXPECT[args.control] <= detected else "FAIL"
        srv.kill()
        return rec, trial_dir, rec["result"] == "PASS"
    r2, _, _ = reconcile(args, ledger_dir, "after_recovery_2", res1, False, prev_committed=com1)
    rec["reconcile_2"] = r2
    rec["clean_shutdown_exit"] = srv.term()
    rec["verify_post_recovery"] = run_verifier(args, db, "post", trial_dir)

    ok = not r1["violations"] and not r2["violations"]
    for key in ("verify_pre_recovery", "verify_post_recovery"):
        ver = rec[key]
        if ver.get("ran") and not ver.get("unchanged"):
            ok = False
    if rec["verify_post_recovery"].get("ran") and not rec["verify_post_recovery"]["clean"]:
        ok = False
    if args.kind == "midfile" and rec.get("tamper", {}).get("applied"):
        if not rec.get("midfile_refused") or not rec.get("midfile_damaged_file_unchanged"):
            ok = False
    rec["result"] = "PASS" if ok else "FAIL"
    return rec, trial_dir, ok


def main():
    global ARGS
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--bin", required=True)
    p.add_argument("--verifier")
    p.add_argument("--mode", choices=["paged", "embedded"], required=True)
    p.add_argument("--profile", choices=["stress", "default", "smallpool", "smallwal"], default="stress")
    p.add_argument("--kind", choices=["failio", "crash", "kill", "torn", "midfile", "recoverycrash"], required=True)
    p.add_argument("--sites", nargs="*", default=[])
    p.add_argument("--nmin", type=int, default=1)
    p.add_argument("--nmax", type=int, default=1)
    p.add_argument("--persistent", action="store_true")
    p.add_argument("--also-fail", default="")
    p.add_argument("--volatile", action="store_true")
    p.add_argument("--target", default="")
    p.add_argument("--reps", type=int, default=10)
    p.add_argument("--duration", type=float, default=6.0)
    p.add_argument("--port", type=int, default=56000)
    p.add_argument("--end", choices=["kill", "term"], default="kill")
    p.add_argument("--seed", default="1")
    p.add_argument("--workroot", required=True)
    p.add_argument("--keep-failures", type=int, default=3)
    p.add_argument("--out", required=True)
    p.add_argument("--control", default="", choices=["", *CONTROL_EXPECT])
    args = p.parse_args()
    ARGS = args
    env = {}
    if args.mode == "paged":
        env["BICDB_STORAGE_MODE"] = "server_paged"
    if args.profile == "smallpool":
        env["BICDB_PAGED_BUFFER_POOL_BYTES"] = str(2 * 1024 * 1024)
    if args.profile == "smallwal":
        env.update({"BICDB_PAGED_WAL_MAX_BYTES": str(512 * 1024),
                    "BICDB_PAGED_WAL_SEGMENT_BYTES": str(128 * 1024),
                    "BICDB_AUTO_COMPACT_WAL_MB": "1"})
    if args.profile == "stress":
        env.update({"BICDB_PAGED_BUFFER_POOL_BYTES": str(2 * 1024 * 1024),
                    "BICDB_PAGED_WAL_MAX_BYTES": str(512 * 1024),
                    "BICDB_PAGED_WAL_SEGMENT_BYTES": str(128 * 1024),
                    "BICDB_AUTO_COMPACT_WAL_MB": "1"})
    args.base_env = env
    if not port_free(args.port):
        sys.exit(f"port {args.port} busy")
    kept = 0
    for rep in range(args.reps):
        try:
            rec, trial_dir, ok = one_trial(args, rep)
        except Exception as error:  # harness error: recorded, not hidden
            kill_live()
            rec, trial_dir, ok = {"boundary": args.name, "rep": rep, "result": "HARNESS_ERROR",
                                  "error": repr(error)[:500]}, os.path.join(args.workroot, f"{args.name}-{rep}"), False
        with open(args.out, "a") as handle:
            handle.write(json.dumps(rec, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if ok or kept >= args.keep_failures:
            shutil.rmtree(trial_dir, ignore_errors=True)
        else:
            kept += 1
        print(f"{args.name} rep {rep}: {rec.get('result')} fired={rec.get('fired')}", flush=True)


if __name__ == "__main__":
    main()
