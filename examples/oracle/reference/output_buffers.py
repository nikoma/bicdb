"""Compare session output buffers with Oracle through PostgreSQL transport.

Only nonce-named helper procedures are created. The reference account must be a
dedicated, non-administrative schema. Failures are evidence of missing behavior,
not permission to substitute an application-specific output no-op.
"""
import json
import uuid


def plans():
    """Explicit expectations, independently checked against the reference."""
    done = [None, 1]
    empty = [None, 0]
    return [
        ("disabled_output", [("PUT_LINE", ["ignored"])], [done]),
        ("partial_and_terminated", [("ENABLE", []), ("PUT", ["ab"]),
         ("PUT_LINE", ["cd"])], [["abcd", 0], done]),
        ("empty_line", [("ENABLE", []), ("NEW_LINE", [])], [empty, done]),
        ("null_line", [("ENABLE", []), ("PUT_LINE", [None])], [empty, done]),
        ("empty_string_line", [("ENABLE", []), ("PUT_LINE", [""])], [empty, done]),
        ("embedded_newline", [("ENABLE", []), ("PUT_LINE", ["a\nb"])],
         [["a\nb", 0], done]),
        ("unicode_line", [("ENABLE", []), ("PUT_LINE", ["café 🌲"])],
         [["café 🌲", 0], done]),
        ("unfinished_line_invisible", [("ENABLE", []), ("PUT", ["unfinished"])], [done]),
        ("read_discards_unretrieved_on_write", [("ENABLE", []),
         ("PUT_LINE", ["one"]), ("PUT_LINE", ["two"]),
         ("GET", [["one", 0]]), ("PUT_LINE", ["three"])], [["three", 0], done]),
        ("read_discards_partial_on_write", [("ENABLE", []), ("PUT", ["partial"]),
         ("GET", [done]), ("PUT_LINE", ["tail"])], [["tail", 0], done]),
        ("reenable_preserves_lines", [("ENABLE", [None]), ("PUT_LINE", ["one"]),
         ("ENABLE", [2000]), ("PUT_LINE", ["two"])], [["one", 0], ["two", 0], done]),
        ("disable_purges", [("ENABLE", []), ("PUT_LINE", ["old"]),
         ("PUT", ["partial"]), ("DISABLE", []), ("ENABLE", []),
         ("PUT_LINE", ["new"])], [["new", 0], done]),
        ("rollback_preserves_output", [("ENABLE", []),
         ("PUT_LINE", ["before rollback"]), ("ROLLBACK", [])],
         [["before rollback", 0], done]),
        ("savepoint_preserves_output", [("ENABLE", []), ("SAVEPOINT", []),
         ("PUT_LINE", ["after savepoint"]), ("ROLLBACK_TO", [])],
         [["after savepoint", 0], done]),
        ("numeric_input", [("ENABLE", []), ("PUT_LINE", [123])], [["123", 0], done]),
        ("line_byte_boundary", [("ENABLE", [None]), ("PUT", ["x" * 32763]),
         ("PUT_LINE", ["🌲"]), ("LENGTH_GET", [[32764, 32767, 0]])], [done]),
        ("line_overflow_discards_partial", [("ENABLE", [None]),
         ("PUT_LINE", ["complete"]), ("PUT", ["x" * 32000]),
         ("EXPECT_ERROR", ["PUT", ["y" * 1000], 20000, "ORU-10028"]),
         ("PUT_LINE", ["tail"])], [["complete", 0], ["tail", 0], done]),
        ("unicode_line_overflow", [("ENABLE", [None]), ("PUT", ["x" * 32764]),
         ("EXPECT_ERROR", ["PUT_LINE", ["🌲"], 20000, "ORU-10028"]),
         ("PUT_LINE", ["tail"])], [["tail", 0], done]),
        ("minimum_buffer_boundary", [("ENABLE", [0]),
         ("PUT_LINE", ["x" * 2000]),
         ("EXPECT_ERROR", ["PUT_LINE", ["z"], 20000, "ORU-10027"]),
         ("LENGTH_GET", [[2000, 2000, 0]])], [done]),
        ("buffer_overflow_preserves_complete_lines", [("ENABLE", [2000]),
         ("PUT_LINE", ["first"]),
         ("EXPECT_ERROR", ["PUT_LINE", ["x" * 2000], 20000, "ORU-10027"])],
         [["first", 0], done]),
        ("unlimited_buffer", [("ENABLE", [None]),
         ("PUT_LINE", ["x" * 21000]), ("LENGTH_GET", [[21000, 21000, 0]])], [done]),
        ("get_lines_disabled", [("LINES_GET", [3, [0, None, None, None]])], [done]),
        ("get_lines_empty", [("ENABLE", []),
         ("LINES_GET", [3, [0, None, None, None]])], [done]),
        ("get_lines_batch", [("ENABLE", []), ("PUT_LINE", ["one"]),
         ("PUT_LINE", ["two"]), ("LINES_GET", [3, [2, "one", "two", None]])], [done]),
        ("get_lines_zero", [("ENABLE", []), ("PUT_LINE", ["one"]),
         ("LINES_GET", [0, [0, None, None, None]])], [["one", 0], done]),
        ("get_lines_empty_line", [("ENABLE", []), ("NEW_LINE", []),
         ("LINES_GET", [3, [1, None, None, None]])], [done]),
        ("get_lines_discards_unretrieved_on_write", [("ENABLE", []),
         ("PUT_LINE", ["one"]), ("PUT_LINE", ["two"]), ("PUT_LINE", ["three"]),
         ("LINES_GET", [2, [2, "one", "two", None]]), ("PUT_LINE", ["new"])],
         [["new", 0], done]),
    ]


def failure(exc):
    detail = exc.args[0] if exc.args else exc
    return dict(exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None),
                oracle_code=getattr(detail, "code", None), message=str(detail).splitlines()[0])


class Backend:
    def __init__(self, connection, helper):
        self.connection = connection
        self.helper = helper
        self.oracle = not type(connection).__module__.startswith("psycopg")

    def sql(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)

    def call(self, name, args):
        with self.connection.cursor() as cursor:
            if self.oracle:
                cursor.callproc("DBMS_OUTPUT." + name, args)
            else:
                markers = ",".join("%s" for _ in args)
                cursor.execute(f"CALL DBMS_OUTPUT.{name}({markers})", args or None,
                               prepare=True)

    def get(self):
        with self.connection.cursor() as cursor:
            if self.oracle:
                line = cursor.var(str, size=32767)
                status = cursor.var(int)
                cursor.callproc(self.helper, [line, status])
                return [line.getvalue(), status.getvalue()]
            cursor.execute(f"CALL {self.helper}(NULL,NULL)", prepare=True)
            row = cursor.fetchone()
            return [row[0], int(row[1])]

    def lines(self, count):
        with self.connection.cursor() as cursor:
            if self.oracle:
                number = cursor.var(int)
                number.setvalue(0, count)
                values = [cursor.var(str, size=32767) for _ in range(3)]
                cursor.callproc(self.helper + "_LS", [number, *values])
                return [number.getvalue(), *(value.getvalue() for value in values)]
            cursor.execute(f"CALL {self.helper}_LS(%s,NULL,NULL,NULL)", [count], prepare=True)
            row = cursor.fetchone()
            return [int(row[0]), *row[1:]]


def observe(factory):
    helper = "BICDB_OUT_" + uuid.uuid4().hex[:16].upper()
    report = dict(cases=[], cleanup_errors=[])
    with factory() as connection:
        if getattr(connection, "version", None):
            report["database_version"] = connection.version
        # Each case restores the transaction explicitly. Package state must
        # survive these boundaries, but must never escape this connection.
        connection.autocommit = True
        backend = Backend(connection, helper)
        created = False
        lines_created = False
        try:
            backend.sql(f"CREATE PROCEDURE {helper}(line OUT VARCHAR2,status OUT NUMBER) "
                        "AS BEGIN DBMS_OUTPUT.GET_LINE(line,status); END;")
            created = True
            try:
                backend.sql(f"CREATE PROCEDURE {helper}_LS(n IN OUT NUMBER,"
                            "a OUT VARCHAR2,b OUT VARCHAR2,c OUT VARCHAR2) AS "
                            "v DBMS_OUTPUT.CHARARR; BEGIN DBMS_OUTPUT.GET_LINES(v,n);"
                            "a:=NULL;b:=NULL;c:=NULL;IF n>=1 THEN a:=v(1);END IF;"
                            "IF n>=2 THEN b:=v(2);END IF;IF n>=3 THEN c:=v(3);END IF;END;")
                lines_created = True
            except Exception as exc:
                report["get_lines_helper_error"] = failure(exc)
            connection.autocommit = False
            for ident, operations, expected in plans():
                row = dict(id=ident, expected=expected, observations=[], success=False, phase="reset")
                try:
                    backend.call("DISABLE", [])
                    row["phase"] = "operations"
                    for op, args in operations:
                        if op == "LINES_GET":
                            value = backend.lines(args[0])
                            row["observations"].append(dict(op=op, value=value, expected=args[1]))
                            if value != args[1]:
                                raise AssertionError("batch output differs from expectation")
                        elif op in ("GET", "LENGTH_GET"):
                            value = backend.get()
                            if op == "LENGTH_GET":
                                value = [len(value[0]), len(value[0].encode("utf-8")), value[1]]
                            row["observations"].append(dict(op=op, value=value, expected=args[0]))
                            if value != args[0]:
                                raise AssertionError("intermediate output differs from expectation")
                        elif op == "EXPECT_ERROR":
                            name, actuals, code, prefix = args
                            try:
                                backend.call(name, actuals)
                            except Exception as exc:
                                observed = failure(exc)
                                # PG transport carries Oracle errors as server
                                # messages; native Oracle carries a numeric code.
                                matched = (observed["oracle_code"] == code or
                                           f"ORA-{code:05d}" in observed["message"])
                                matched = matched and prefix in observed["message"]
                                row["observations"].append(dict(op=op, error=observed, matched=matched))
                                if not matched:
                                    raise AssertionError("unexpected output error") from exc
                                connection.rollback()
                            else:
                                raise AssertionError("expected output error was not raised")
                        elif op == "ROLLBACK":
                            connection.rollback()
                        elif op == "SAVEPOINT":
                            backend.sql("SAVEPOINT output_probe")
                        elif op == "ROLLBACK_TO":
                            backend.sql("ROLLBACK TO SAVEPOINT output_probe")
                        else:
                            backend.call(op, args)
                    row["phase"] = "final_read"
                    row["lines"] = [backend.get() for _ in expected]
                    row["success"] = row["lines"] == expected
                    if row["success"]:
                        row["phase"] = "complete"
                except Exception as exc:
                    row["error"] = failure(exc)
                finally:
                    connection.rollback()
                report["cases"].append(row)
        except Exception as exc:
            report["setup_error"] = failure(exc)
        finally:
            connection.rollback()
            connection.autocommit = True
            try:
                backend.call("DISABLE", [])
            except Exception as exc:
                # No buffer exists when the package itself is unavailable.
                report["buffer_reset_error"] = failure(exc)
            for exists, name in [(lines_created, helper + "_LS"), (created, helper)]:
                if not exists:
                    continue
                try:
                    backend.sql(f"DROP PROCEDURE {name}")
                except Exception as exc:
                    report["cleanup_errors"].append(failure(exc))
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    source_by_id = {case["id"]: case for case in source["cases"]}
    target_by_id = {case["id"]: case for case in target["cases"]}
    cases = []
    for ident, _, _ in plans():
        a, b = source_by_id.get(ident, {}), target_by_id.get(ident, {})
        cases.append(dict(id=ident, source=a, bicdb=b,
                          source_qualified=a.get("success", False),
                          matched=a.get("success", False) and b.get("success", False)))
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    qualified = all(case["source_qualified"] for case in cases) and not source.get("buffer_reset_error")
    matched = sum(case["matched"] for case in cases)
    buffers_reset = not source.get("buffer_reset_error") and not target.get("buffer_reset_error")
    return dict(format=1, application_qualified=False, reference="oracle",
                source=source, bicdb=target, cases=cases, total=len(cases), matched=matched,
                source_qualified=qualified, cleanup_errors=cleanup, buffers_reset=buffers_reset,
                success=qualified and matched == len(cases) and not cleanup and buffers_reset)


def main():
    import argparse
    import os
    from pathlib import Path
    import oracledb
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        parser.error("a dedicated non-administrative Oracle schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password

    def source():
        connection = oracledb.connect(user=config["user"], password=password.read_text().strip(),
                                      host=config["host"], port=config["port"],
                                      service_name=config["service_name"])
        connection.call_timeout = 15000
        return connection

    def target():
        dsn = os.environ["BICDB_PG_DSN"]
        options = conninfo_to_dict(dsn).get("options", "") + " -c statement_timeout=15000"
        return psycopg.connect(dsn, autocommit=True, connect_timeout=10, options=options)

    with target() as connection:
        profile = connection.info.parameter_status("bicdb_sql_profile")
        if profile != "oracle-preview-v1":
            raise RuntimeError("output buffers require an explicit Oracle preview target")
    report = run(source, target)
    report["target_sql_profile"] = profile
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ["success", "source_qualified", "matched", "total"]}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
