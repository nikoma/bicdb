"""Owned-table blocking, cancellation and row-cycle comparisons over PG transport.

The ten-second owner hold exceeds the previous implicit lock retry budget.
This does not qualify metadata locks, all deadlocks or application throughput.
"""
import json
import threading
import time
import uuid


def execute(connection, sql, rows=False):
    with connection.cursor() as cursor:
        if type(connection).__module__.startswith("psycopg"):
            cursor.execute(sql, prepare=rows)
        else:
            cursor.execute(sql)
        return [[int(value) for value in row] for row in cursor.fetchall()] if rows else None


def failure(exc):
    state = getattr(exc, "sqlstate", None)
    code = getattr(exc.args[0], "code", None) if exc.args else None
    kind = "cancelled" if state == "57014" or code == 1013 else (
        "deadlock" if state == "40P01" or code == 60 else "other_error")
    return dict(outcome=kind, sqlstate=state, oracle_code=code)


def observe(factory, hold_seconds=10):
    table = "BICDB_RW_" + uuid.uuid4().hex[:16].upper()
    base = factory()
    workers = []
    report = dict(cases=[], cleanup_errors=[])
    created = False
    try:
        base.autocommit = True
        execute(base, f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER)")
        created = True
        execute(base, f"INSERT INTO {table} VALUES(1,0)")
        execute(base, f"INSERT INTO {table} VALUES(2,0)")

        def connection():
            conn = factory()
            conn.autocommit = False
            workers.append(conn)
            return conn

        owner, waiter = connection(), connection()
        execute(owner, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True)
        execute(owner, f"UPDATE {table} SET value=1 WHERE id=1")
        row = dict(id="blocking_latest_row", owner_hold_seconds=hold_seconds)
        started = threading.Event()

        def blocking():
            at = time.monotonic()
            started.set()
            try:
                row.update(outcome="success", rows=execute(waiter, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True))
            except Exception as exc:
                row.update(failure(exc))
            finally:
                waiter.rollback()
                row["elapsed_seconds"] = round(time.monotonic() - at, 3)

        thread = threading.Thread(target=blocking)
        thread.start()
        assert started.wait(5)
        time.sleep(hold_seconds)
        owner.commit()
        thread.join(20)
        assert not thread.is_alive(), "blocking worker did not finish"
        report["cases"].append(row)

        execute(owner, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True)
        row = dict(id="explicit_cancel")
        started = threading.Event()
        done = threading.Event()

        def cancel_wait():
            at = time.monotonic()
            started.set()
            try:
                row.update(outcome="success", rows=execute(waiter, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True))
            except Exception as exc:
                row.update(failure(exc))
            finally:
                waiter.rollback()
                row["elapsed_seconds"] = round(time.monotonic() - at, 3)
                done.set()

        thread = threading.Thread(target=cancel_wait)
        thread.start()
        assert started.wait(5)
        time.sleep(.25)
        waiter.cancel()
        row["completed_before_owner_release"] = done.wait(12)
        owner.rollback()
        thread.join(20)
        assert not thread.is_alive(), "cancelled worker did not finish"
        row["connection_recovers"] = execute(waiter, f"SELECT value FROM {table} WHERE id=1", True) == [[1]]
        waiter.rollback()
        report["cases"].append(row)

        execute(owner, f"SELECT value FROM {table} WHERE id=1 FOR UPDATE", True)
        execute(waiter, f"SELECT value FROM {table} WHERE id=2 FOR UPDATE", True)
        row = dict(id="row_cycle", participants=[])
        barrier = threading.Barrier(2)

        def cycle(conn, key):
            barrier.wait(timeout=5)
            try:
                result = dict(outcome="success", rows=execute(conn, f"SELECT value FROM {table} WHERE id={key} FOR UPDATE", True))
            except Exception as exc:
                result = failure(exc)
            finally:
                conn.rollback()
            row["participants"].append(result)

        threads = [threading.Thread(target=cycle, args=(owner, 2)), threading.Thread(target=cycle, args=(waiter, 1))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(20)
        assert all(not thread.is_alive() for thread in threads), "cycle worker did not finish"
        row["outcomes"] = sorted(item["outcome"] for item in row["participants"])
        report["cases"].append(row)
    finally:
        for conn in reversed(workers):
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
    if row["id"] == "blocking_latest_row":
        return row.get("outcome") == "success" and row.get("rows") == [[1]] and row["elapsed_seconds"] >= row["owner_hold_seconds"] - .25
    if row["id"] == "explicit_cancel":
        return row.get("outcome") == "cancelled" and row.get("completed_before_owner_release") and row.get("connection_recovers")
    return row.get("outcomes") == ["deadlock", "success"]


def run(source_factory, target_factory, hold_seconds=10):
    source, target = observe(source_factory, hold_seconds), observe(target_factory, hold_seconds)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=qualified(a), matched=qualified(a) and qualified(b)) for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases), matched=sum(c["matched"] for c in cases), source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup, success=len(cases) == 3 and all(c["matched"] for c in cases) and not cleanup)


def main(runner=run, description=__doc__, references=("oracle", "postgres")):
    import argparse
    import os
    from pathlib import Path
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--reference", choices=references, required=True)
    parser.add_argument("--oracle-config", type=Path)
    parser.add_argument("--oracle-disable-oob", action="store_true", help="use in-band breaks on the Oracle reference connection")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.reference == "oracle" and args.oracle_config is None:
        parser.error("--oracle-config is required with --reference oracle")

    def pg_factory(dsn):
        options = conninfo_to_dict(dsn).get("options", "") + " -c statement_timeout=15000"
        return lambda: psycopg.connect(dsn, autocommit=True, connect_timeout=10, options=options)

    if args.reference == "oracle":
        import oracledb
        config = json.loads(args.oracle_config.read_text())
        if config["user"].upper() in ("SYS", "SYSTEM"):
            raise ValueError("a dedicated non-administrative reference schema is required")
        password = Path(config["password_file"])
        if not password.is_absolute():
            password = args.oracle_config.parent / password
        def source_factory():
            conn = oracledb.connect(user=config["user"], password=password.read_text().strip(), host=config["host"], port=config["port"], service_name=config["service_name"], disable_oob=args.oracle_disable_oob)
            conn.call_timeout = 15000
            return conn
    else:
        source_factory = pg_factory(os.environ["ORACLE_CONTROL_PG_DSN"])
    target_factory = pg_factory(os.environ["BICDB_PG_DSN"])
    with target_factory() as probe:
        profile = probe.info.parameter_status("bicdb_sql_profile")
        expected = "oracle-preview-v1" if args.reference == "oracle" else "postgres"
        if profile != expected:
            raise RuntimeError("target SQL profile does not match the selected reference")
    report = runner(source_factory, target_factory)
    report["target_sql_profile"] = profile
    report["reference"] = args.reference
    if args.reference == "oracle":
        report["oracle_disable_oob"] = args.oracle_disable_oob
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ["success", "matched", "total"]}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
