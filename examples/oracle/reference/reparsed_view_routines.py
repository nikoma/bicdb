"""Repeated routine reads of views with mixed expression types.

The same calls run against an independent reference and BicDB. Exact decimal
results must remain stable across repeated parsing; no application schema is used.
"""
from decimal import Decimal
import uuid

from row_lock_waits import main as comparison_main


def observe(factory, oracle_semantics):
    nonce = uuid.uuid4().hex[:12].upper()
    table, view, function = ("BICDB_RV_" + nonce + suffix for suffix in ("T", "V", "F"))
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle_driver = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle_driver else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall() if cursor.description else None

        try:
            execute(f"CREATE TABLE {table}(id INTEGER,amount NUMERIC(10,2),kind VARCHAR(1),label VARCHAR(8))")
            created.append(("TABLE", table))
            for values in ["1,50.35,'I','one'", "2,100.70,'I','two'", "3,161.12,'C','three'"]:
                execute(f"INSERT INTO {table} VALUES({values})")
            execute(f"CREATE VIEW {view} AS SELECT id,CASE WHEN kind='C' THEN amount * -1 ELSE amount END AS amount,"
                    f"CASE WHEN kind='C' THEN -1 ELSE 1 END AS sign,label,CAST(NULL AS VARCHAR(8)) AS note FROM {table} "
                    f"UNION ALL SELECT id+10,amount,1,label,'other' FROM {table} WHERE id<0")
            created.append(("VIEW", view))
            if oracle_semantics:
                ddl = (f"CREATE FUNCTION {function}(p_id INTEGER) RETURN NUMBER IS result NUMBER; "
                       f"BEGIN SELECT SUM(amount) INTO result FROM {view} WHERE id=p_id; RETURN result; END;")
            else:
                ddl = (f"CREATE FUNCTION {function}(p_id INTEGER) RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE result NUMERIC; "
                       f"BEGIN SELECT SUM(amount) INTO result FROM {view} WHERE id=p_id; RETURN result; END$$")
            execute(ddl)
            created.append(("FUNCTION", function))
            suffix = " FROM DUAL" if oracle_semantics else ""
            for iteration in range(60):
                for key, expected in [(1, "50.35"), (2, "100.70"), (3, "-161.12")]:
                    row = dict(id=f"{iteration}:{key}", expected=expected)
                    try:
                        values = execute(f"SELECT {function}({key}){suffix}")
                        value = values[0][0]
                        row.update(actual=str(value), qualified=len(values) == 1 and Decimal(str(value)) == Decimal(expected))
                    except Exception as exc:
                        row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
                    report["cases"].append(row)
        finally:
            for kind, name in reversed(created):
                try:
                    signature = "(INTEGER)" if kind == "FUNCTION" and not oracle_semantics else ""
                    purge = " PURGE" if kind == "TABLE" and oracle_driver else ""
                    execute(f"DROP {kind} {name}{signature}{purge}")
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind=kind, exception=type(exc).__name__))
    return report


def qualified(row):
    return bool(row.get("qualified", False))


def run(source_factory, target_factory):
    with source_factory() as probe:
        oracle = type(probe).__module__.startswith("oracledb")
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(case["matched"] for case in cases),
                cleanup_errors=cleanup, source_qualified=all(case["source_qualified"] for case in cases),
                success=len(cases) == 180 and all(case["matched"] for case in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
