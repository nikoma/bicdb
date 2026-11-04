"""Distinguish per-row Oracle WAIT budgets from one whole-query deadline."""
import threading
import time
import uuid

from row_lock_deadlines import failure
from row_lock_waits import execute, main as comparison_main


def observe(factory):
    table = "BICDB_RWM_" + uuid.uuid4().hex[:16].upper()
    base = factory()
    connections = []
    created = False
    report = dict(cases=[], cleanup_errors=[], server_version=getattr(base, "version", None))
    try:
        base.autocommit = True
        execute(base, f"CREATE TABLE {table}(id INTEGER PRIMARY KEY)")
        created = True
        execute(base, f"INSERT INTO {table} VALUES(1)")
        execute(base, f"INSERT INTO {table} VALUES(2)")
        for _ in range(3):
            conn = factory()
            conn.autocommit = False
            connections.append(conn)
        first, second, waiter = connections
        for name, second_release in [("two_successive_waits", 1.2), ("second_row_expiry", 1.9)]:
            execute(first, f"SELECT id FROM {table} WHERE id=1 FOR UPDATE", True)
            execute(second, f"SELECT id FROM {table} WHERE id=2 FOR UPDATE", True)
            row = dict(id=name, wait_seconds=1, first_release_seconds=.6,
                       second_release_seconds=second_release)
            started, released = threading.Event(), threading.Event()

            def work():
                at = time.monotonic()
                started.set()
                try:
                    row.update(outcome="success", rows=execute(
                        waiter, f"SELECT id FROM {table} ORDER BY id FOR UPDATE WAIT 1", True))
                except Exception as exc:
                    row.update(failure(exc))
                finally:
                    row["elapsed_seconds"] = round(time.monotonic() - at, 3)
                    row["completed_before_second_release"] = not released.is_set()

            thread = threading.Thread(target=work)
            thread.start()
            try:
                assert started.wait(5), "worker did not start"
                time.sleep(.6)
                first.rollback()
                time.sleep(second_release - .6)
                released.set()
                second.rollback()
                thread.join(15)
                if thread.is_alive():
                    row["watchdog_cancelled"] = True
                    waiter.cancel()
                    thread.join(20)
                assert not thread.is_alive(), "multirow worker did not finish"
                waiter.rollback()
                row["connection_recovers"] = execute(waiter, f"SELECT COUNT(*) FROM {table}", True) == [[2]]
                waiter.rollback()
                report["cases"].append(row)
            finally:
                if thread.is_alive():
                    first.rollback()
                    second.rollback()
                    waiter.cancel()
                    thread.join(20)
                if thread.is_alive():
                    raise RuntimeError("multirow worker survived owner rollback and cancellation")
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
    if row["id"] == "two_successive_waits":
        return (row.get("outcome") == "success" and row.get("rows") == [[1], [2]]
                and row["elapsed_seconds"] >= 1.1 and not row["completed_before_second_release"])
    return (row.get("outcome") in ("unavailable", "lock_timeout", "lock_unavailable")
            and row["elapsed_seconds"] >= 1.4 and row["completed_before_second_release"])


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b,
                  source_qualified=qualified(a), matched=qualified(a) and qualified(b))
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases),
                matched=sum(c["matched"] for c in cases), reference_version=source["server_version"],
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 2 and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__, references=("oracle",)))
