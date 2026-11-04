"""Typed joined scalar subqueries, empty correlations and stored view metadata.

Numeric widths and datetime precision beyond the fixture are not qualified.
All nonce objects are removed; no application schema is used.
"""
import uuid

from join_wildcards import canonical, family
from row_lock_waits import main as comparison_main


def observe(factory):
    nonce = uuid.uuid4().hex[:12].upper()
    outer, versions, values, view = ("BICDB_JS_" + nonce + suffix for suffix in ("O", "R", "T", "V"))
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
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
                row["actual"] = result
                folded = dict(result, columns=[label.lower() for label in result["columns"]])
                row["qualified"] = folded == expected
            except Exception as exc:
                row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
            report["cases"].append(row)

        try:
            for name, definition in [(outer, "id INTEGER"), (versions, "id INTEGER"),
                                     (values, "version_id INTEGER,owner_id INTEGER,amount NUMERIC(10,2),seen TIMESTAMP,marker VARCHAR(8)")]:
                execute(f"CREATE TABLE {name}({definition})")
                created.append(("TABLE", name))
            execute(f"INSERT INTO {outer} VALUES(1)")
            execute(f"INSERT INTO {outer} VALUES(2)")
            execute(f"INSERT INTO {versions} VALUES(11)")
            execute(f"INSERT INTO {values} VALUES(11,1,12.50,TIMESTAMP '2024-02-29 12:34:56.123456','match')")
            inner = f"FROM {versions} v JOIN {values} x ON v.id=x.version_id WHERE x.owner_id=o.id"
            query = f"SELECT o.id,(SELECT x.amount {inner}) AS amount,(SELECT x.seen {inner}) AS seen,(SELECT x.marker {inner}) AS marker FROM {outer} o"
            metadata = dict(columns=["id", "amount", "seen", "marker"], types=["number", "number", "timestamp", "text"])
            rows = [[canonical(1), canonical(12.5), "2024-02-29T12:34:56.123456", "match"], [canonical(2), None, None, None]]
            check("direct_description", lambda: describe(conn, query, "scalar_direct"), metadata)
            check("direct_rows", lambda: execute(query + " ORDER BY 1"), dict(metadata, rows=rows))
            check("empty_description", lambda: describe(conn, query + " WHERE o.id=99", "scalar_empty"), metadata)
            execute(f"CREATE VIEW {view} AS " + query)
            created.append(("VIEW", view))
            check("stored_view_description", lambda: describe(conn, f"SELECT * FROM {view}", "scalar_view"), metadata)
            check("stored_view_rows", lambda: execute(f"SELECT * FROM {view} ORDER BY 1"), dict(metadata, rows=rows))
            with factory() as fresh:
                fresh.autocommit = True
                check("fresh_view_empty_description", lambda: describe(fresh, f"SELECT * FROM {view} WHERE 1=0", "scalar_fresh"), metadata)
        finally:
            for kind, name in reversed(created):
                try:
                    execute(f"DROP {kind} {name}" + (" PURGE" if oracle and kind == "TABLE" else ""))
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
