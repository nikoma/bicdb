"""Named-query metadata for parenthesized SELECTs and limited UNION operands.

Empty and populated tables must expose the same columns. Nonce objects are
removed afterward; this does not qualify an entire driver or application.
"""
import uuid

from join_wildcards import canonical, family
from row_lock_waits import main as comparison_main


def observe(factory):
    table = "BICDB_PQ_" + uuid.uuid4().hex[:12].upper() + "T"
    report = dict(cases=[], cleanup_errors=[])
    created = False
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if not cursor.description:
                    return None
                return dict(columns=[c.name.lower() for c in cursor.description],
                            types=[family(c.type_code) for c in cursor.description],
                            rows=[[canonical(v) for v in row] for row in cursor.fetchall()])

        def describe(sql, name):
            if oracle:
                with conn.cursor() as cursor:
                    cursor.parse(sql)
                    return dict(columns=[c.name.lower() for c in cursor.description],
                                types=[family(c.type_code) for c in cursor.description])
            parsed = conn.pgconn.prepare(name.encode(), sql.encode())
            if parsed.status != 1:
                raise RuntimeError(parsed.error_message.decode(errors="replace"))
            result = conn.pgconn.describe_prepared(name.encode())
            if result.status != 1:
                raise RuntimeError(result.error_message.decode(errors="replace"))
            return dict(columns=[result.fname(i).decode().lower() for i in range(result.nfields)],
                        types=[family(result.ftype(i)) for i in range(result.nfields)])

        def check(name, action, expected):
            row = dict(id=name, expected=expected)
            try:
                actual = action()
                row.update(actual=actual, qualified=actual == expected)
            except Exception as exc:
                row.update(qualified=False, exception=type(exc).__name__,
                           sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
            report["cases"].append(row)

        try:
            execute(f"CREATE TABLE {table}(id NUMERIC(10,0),amount NUMERIC(10,2),seen TIMESTAMP,label VARCHAR(8))")
            created = True
            select = f"SELECT r.* FROM {table} r WHERE label='match'"
            queries = [f"({select})", f"/* outer */ ( ( /* inner */ {select}))",
                       f"({select} ORDER BY id FETCH FIRST 1 ROW ONLY) UNION ALL "
                       f"({select} AND id<0 ORDER BY id FETCH FIRST 1 ROW ONLY)"]
            metadata = dict(columns=["id", "amount", "seen", "label"], types=["number", "number", "timestamp", "text"])
            for index, query in enumerate(queries):
                execute(f"DELETE FROM {table}")
                check(f"{index}:empty_statement_description", lambda: describe(query, f"parenthesized_empty_{index}"), metadata)
                check(f"{index}:empty_rows", lambda: execute(query), dict(metadata, rows=[]))
                execute(f"INSERT INTO {table} VALUES(1,12.50,TIMESTAMP '2024-02-29 12:34:56','match')")
                check(f"{index}:populated_statement_description", lambda: describe(query, f"parenthesized_full_{index}"), metadata)
                expected = dict(metadata, rows=[[canonical(1), canonical(12.5), "2024-02-29T12:34:56.000000", "match"]])
                check(f"{index}:populated_rows", lambda: execute(query), expected)
        finally:
            if created:
                try:
                    execute(f"DROP TABLE {table}" + (" PURGE" if oracle else ""))
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind="TABLE", exception=type(exc).__name__))
    return report


def qualified(row):
    return bool(row.get("qualified", False))


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(c["matched"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 12 and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
