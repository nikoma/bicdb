"""Prepare wildcard projections without executing their sources or sequences.

Oracle uses cursor.parse; PostgreSQL transport uses protocol Parse/Describe.
Raw labels and numeric transport families are retained independently of values.
"""
import uuid

from join_wildcards import family
from row_lock_waits import main as comparison_main


def observe(factory):
    nonce = uuid.uuid4().hex[:12].upper()
    table, sequence, view = ("BICDB_WD_" + nonce + suffix for suffix in ("T", "S", "V"))
    quoted_table, quoted_view = "BICDB_WD_" + nonce + "Q", "BICDB_WD_" + nonce + "W"
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle else conn.info.server_version
        oracle_semantics = oracle or conn.info.parameter_status("bicdb_sql_profile") in (
            "oracle-preview-v1", "oracle-hr-v1")

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall() if cursor.description else None

        try:
            execute(f"CREATE TABLE {table}(id INTEGER,marker VARCHAR(8))")
            created.append(("TABLE", table))
            execute(f"INSERT INTO {table} VALUES(1,'visible')")
            execute(f"CREATE SEQUENCE {sequence}")
            created.append(("SEQUENCE", sequence))
            execute(f"CREATE VIEW {view} AS SELECT * FROM {table}")
            created.append(("VIEW", view))
            execute(f'CREATE TABLE {quoted_table}("Key" INTEGER,"Value" VARCHAR(8))')
            created.append(("TABLE", quoted_table))
            execute(f'CREATE VIEW {quoted_view}("ResultKey","ResultValue") AS SELECT * FROM {quoted_table}')
            created.append(("VIEW", quoted_view))
            counter = f"{sequence}.NEXTVAL" if oracle_semantics else f"nextval('{sequence}')"
            star = "l.*,r.*" if oracle_semantics else "*"
            cases = [
                ("bare_table", f"SELECT * FROM {table}", ["id", "marker"], ["number", "text"]),
                ("qualified_table", f"SELECT t.* FROM {table} t", ["id", "marker"], ["number", "text"]),
                ("join_with_sequence", f"SELECT {star},{counter} AS counter FROM {table} l CROSS JOIN {table} r", ["id", "marker", "id", "marker", "counter"], ["number", "text", "number", "text", "number"]),
                ("bare_view", f"SELECT * FROM {view}", ["id", "marker"], ["number", "text"]),
                ("quoted_table", f"SELECT * FROM {quoted_table}", ["Key", "Value"], ["number", "text"]),
                ("quoted_derived", f"SELECT d.* FROM (SELECT * FROM {quoted_table}) d", ["Key", "Value"], ["number", "text"]),
                ("quoted_cte", f"WITH d AS (SELECT * FROM {quoted_table}) SELECT * FROM d", ["Key", "Value"], ["number", "text"]),
                ("quoted_declared_view", f"SELECT * FROM {quoted_view}", ["ResultKey", "ResultValue"], ["number", "text"]),
            ]
            for ordinal, (name, sql, names, types) in enumerate(cases):
                row = dict(id=name, expected_columns=names, expected_types=types)
                try:
                    if oracle:
                        with conn.cursor() as cursor:
                            cursor.parse(sql)
                            columns = [column.name for column in cursor.description]
                            actual_types = [family(column.type_code) for column in cursor.description]
                    else:
                        statement = ("wildcard_describe_" + str(ordinal)).encode()
                        prepared = conn.pgconn.prepare(statement, sql.encode())
                        if prepared.status != 1:
                            raise RuntimeError(prepared.error_message.decode(errors="replace"))
                        result = conn.pgconn.describe_prepared(statement)
                        if result.status != 1:
                            raise RuntimeError(result.error_message.decode(errors="replace"))
                        columns = [result.fname(i).decode() for i in range(result.nfields)]
                        actual_types = [family(result.ftype(i)) for i in range(result.nfields)]
                    row.update(columns=columns, types=actual_types,
                               qualified=(columns if name.startswith("quoted_") else [label.lower() for label in columns]) == names and actual_types == types)
                except Exception as exc:
                    row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
                report["cases"].append(row)
            try:
                suffix = " FROM DUAL" if oracle_semantics else ""
                value = int(execute("SELECT " + counter + suffix)[0][0])
                report["cases"].append(dict(id="sequence_unchanged_by_prepare", value=value, expected=1, qualified=value == 1))
            except Exception as exc:
                report["cases"].append(dict(id="sequence_unchanged_by_prepare", qualified=False, exception=type(exc).__name__))
        finally:
            for kind, name in reversed(created):
                try:
                    execute(f"DROP {kind} {name}")
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
                success=len(cases) == 9 and all(case["matched"] for case in cases) and not cleanup)


def qualified(row):
    return bool(row.get("qualified", False))


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
