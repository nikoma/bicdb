"""Qualify Oracle SELECT FOR UPDATE WAIT deadlines using owned nonce tables.

Uncontended acquisition, WAIT 0, expiry, release and explicit cancellation are
separate observations. A parser failure never counts as a lock timeout.
"""
import threading
import time
import uuid

from row_lock_waits import execute, main as comparison_main


def failure(exc):
    state = getattr(exc, "sqlstate", None)
    code = getattr(exc.args[0], "code", None) if exc.args else None
    kind = "other_error"
    if code == 54:
        kind = "unavailable"
    elif code == 30006:
        kind = "lock_timeout"
    elif code == 1013 or state == "57014":
        kind = "cancelled"
    # PG transport uses a lock-not-available SQLSTATE. Keep WAIT 0 and a
    # positive deadline distinct using the expected case and measured delay.
    elif state == "55P03":
        kind = "lock_unavailable"
    return dict(outcome=kind, sqlstate=state, oracle_code=code)


def observe(factory):
    table = "BICDB_RWD_" + uuid.uuid4().hex[:16].upper()
    base = factory()
    connections = []
    created = False
    report = dict(cases=[], cleanup_errors=[], server_version=getattr(base, "version", None))
    try:
        base.autocommit = True
        execute(base, f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER)")
        created = True
        execute(base, f"INSERT INTO {table} VALUES(1,0)")

        def connection():
            conn = factory()
            conn.autocommit = False
            connections.append(conn)
            return conn

        owner, waiter = connection(), connection()

        def attempt(name, seconds, action=None):
            row = dict(id=name, wait_seconds=seconds)
            started, done = threading.Event(), threading.Event()

            def work():
                at = time.monotonic()
                started.set()
                try:
                    row.update(outcome="success", rows=execute(
                        waiter, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE WAIT {seconds}", True))
                except Exception as exc:
                    row.update(failure(exc))
                finally:
                    row["elapsed_seconds"] = round(time.monotonic() - at, 3)
                    done.set()

            thread = threading.Thread(target=work)
            thread.start()
            try:
                assert started.wait(5), "worker did not start"
                if action:
                    time.sleep(.3)
                    # Do not deliver a late cancel to the next command when
                    # an unsupported syntax has already ended this query.
                    if not done.is_set():
                        action()
                if not done.wait(seconds + 5):
                    row["watchdog_cancelled"] = True
                    waiter.cancel()
                thread.join(20)
                assert not thread.is_alive(), "row-lock worker did not finish"
                # Recovery is tested after rollback: native PG aborts a failed
                # transaction, whereas Oracle permits subsequent statements.
                waiter.rollback()
                row["connection_recovers"] = execute(
                    waiter, f"SELECT COUNT(*) FROM {table}", True) == [[1]]
                waiter.rollback()
                report["cases"].append(row)
            finally:
                if thread.is_alive():
                    waiter.cancel()
                    owner.rollback()
                    thread.join(20)
                if thread.is_alive():
                    raise RuntimeError("row-lock worker survived cancellation and owner rollback")

        attempt("uncontended", 2)
        execute(owner, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True)
        attempt("wait_zero", 0)
        attempt("deadline_expiry", 1)
        execute(owner, f"UPDATE {table} SET value=1 WHERE id=1")
        attempt("owner_release", 2, owner.commit)
        execute(owner, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True)
        attempt("explicit_cancel", 5, waiter.cancel)
        owner.rollback()
    finally:
        for conn in reversed(connections):
            try:
                conn.rollback()
                conn.close()
            except Exception as exc:
                report["cleanup_errors"].append(type(exc).__name__)
        if created:
            try:
                execute(base, f"DROP TABLE {table}")
            except Exception as exc:
                report["cleanup_errors"].append(type(exc).__name__)
        base.close()
    return report


def qualified(row):
    if row.get("watchdog_cancelled") or not row.get("connection_recovers"):
        return False
    elapsed = row["elapsed_seconds"]
    if row["id"] == "uncontended":
        return row.get("outcome") == "success" and row.get("rows") == [[0]]
    if row["id"] == "wait_zero":
        return row.get("outcome") in ("unavailable", "lock_unavailable") and elapsed < 1
    if row["id"] == "deadline_expiry":
        return row.get("outcome") in ("unavailable", "lock_timeout", "lock_unavailable") and .75 <= elapsed < 5
    if row["id"] == "owner_release":
        return row.get("outcome") == "success" and row.get("rows") == [[1]] and .2 <= elapsed < 2
    return row.get("outcome") == "cancelled" and .2 <= elapsed < 5


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b,
                  source_qualified=qualified(a), matched=qualified(a) and qualified(b))
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases),
                matched=sum(c["matched"] for c in cases),
                reference_version=source["server_version"],
                source_qualified_cases=sum(c["source_qualified"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases),
                cleanup_errors=cleanup,
                success=len(cases) == 5 and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__, references=("oracle",)))
