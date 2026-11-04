"""Ordinary UNION view definitions derive metadata without evaluating rows.

Creation, protocol description, execution errors and a fresh connection are
separate observations. This does not qualify materialized-view population.
"""
import uuid

from join_wildcards import canonical, family
from row_lock_waits import main as comparison_main


def observe(factory):
    nonce = uuid.uuid4().hex[:12].upper()
    table, valid, deferred = ("BICDB_VD_" + nonce + suffix for suffix in ("T", "V", "E"))
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle else conn.info.server_version

        def execute(connection, sql):
            with connection.cursor() as cursor:
                cursor.execute(sql)
                if not cursor.description:
                    return None
                return dict(columns=[column.name for column in cursor.description],
                            types=[family(column.type_code) for column in cursor.description],
                            rows=[[canonical(value) for value in row] for row in cursor.fetchall()])

        def describe(connection, sql, name):
            if oracle:
                with connection.cursor() as cursor:
                    cursor.parse(sql)
                    return dict(columns=[column.name for column in cursor.description],
                                types=[family(column.type_code) for column in cursor.description])
            statement = name.encode()
            parsed = connection.pgconn.prepare(statement, sql.encode())
            if parsed.status != 1:
                raise RuntimeError(parsed.error_message.decode(errors="replace"))
            result = connection.pgconn.describe_prepared(statement)
            if result.status != 1:
                raise RuntimeError(result.error_message.decode(errors="replace"))
            return dict(columns=[result.fname(i).decode() for i in range(result.nfields)],
                        types=[family(result.ftype(i)) for i in range(result.nfields)])

        def check(name, action, expected):
            row = dict(id=name, expected=expected)
            try:
                result = action()
                row.update(actual=result, qualified=result == expected)
            except Exception as exc:
                row.update(qualified=False, exception=type(exc).__name__,
                           sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
            report["cases"].append(row)

        def create(name, sql):
            execute(conn, sql)
            created.append(("VIEW", name))
            return True

        try:
            execute(conn, f"CREATE TABLE {table}(id INTEGER,v NUMERIC(10,2))")
            created.append(("TABLE", table))
            execute(conn, f"INSERT INTO {table} VALUES(1,1.25)")
            check("typed_union_created", lambda: create(valid,
                  f'CREATE VIEW {valid}("RowKey","Amount") AS SELECT id,v FROM {table} UNION ALL SELECT id+1,v+1 FROM {table}'), True)
            check("typed_union_rows", lambda: execute(conn, f"SELECT * FROM {valid} ORDER BY 1"),
                  dict(columns=["RowKey", "Amount"], types=["number", "number"],
                       rows=[[canonical(1), canonical(1.25)], [canonical(2), canonical(2.25)]]))
            check("deferred_error_created", lambda: create(deferred,
                  f'CREATE VIEW {deferred}("RowKey","Amount") AS SELECT id,CAST(1 AS NUMERIC)/0 FROM {table} UNION ALL SELECT id,v FROM {table}'), True)
            metadata = dict(columns=["RowKey", "Amount"], types=["number", "number"])
            check("deferred_error_described", lambda: describe(conn, f"SELECT * FROM {deferred}", "deferred_metadata"), metadata)

            def read_error():
                try:
                    execute(conn, f"SELECT * FROM {deferred}")
                except Exception as exc:
                    code = getattr(exc.args[0], "code", None) if exc.args else None
                    if getattr(exc, "sqlstate", None) == "22012" or code == 1476:
                        return "division_by_zero"
                    raise
                return "unexpected_success"

            check("deferred_error_on_execution", read_error, "division_by_zero")
            with factory() as fresh:
                fresh.autocommit = True
                check("fresh_connection_metadata", lambda: describe(fresh, f"SELECT * FROM {valid}", "fresh_metadata"), metadata)
        finally:
            for kind, name in reversed(created):
                try:
                    execute(conn, f"DROP {kind} {name}")
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind=kind, exception=type(exc).__name__))
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(case["matched"] for case in cases),
                cleanup_errors=cleanup, source_qualified=all(case["source_qualified"] for case in cases),
                success=len(cases) == 6 and all(case["matched"] for case in cases) and not cleanup)


def qualified(row):
    return bool(row.get("qualified", False))


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
