"""Declared scalar-subquery types, null defaults and evaluation order.

Oracle uses NVL and PostgreSQL uses COALESCE with independent expectations.
Nonce tables are removed; these cases do not qualify arbitrary subqueries.
"""
import uuid

from join_wildcards import canonical
from row_lock_waits import main as comparison_main


def observe(factory, oracle_semantics):
    nonce = uuid.uuid4().hex[:12].upper()
    values, references = ("BICDB_SD_" + nonce + suffix for suffix in ("V", "R"))
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle_driver = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle_driver else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return [[canonical(v) for v in row] for row in cursor.fetchall()] if cursor.description else None

        def check(name, sql, expected):
            row = dict(id=name, expected=expected)
            try:
                actual = execute(sql)
                row.update(actual=actual, qualified=actual == expected)
            except Exception as exc:
                state = getattr(exc, "sqlstate", None)
                code = getattr(exc.args[0], "code", None) if exc.args else None
                row.update(exception=type(exc).__name__, sqlstate=state, oracle_code=code,
                           detail=str(exc)[:500], qualified=expected == "division_by_zero" and (code == 1476 if oracle_driver else state == "22012"))
            report["cases"].append(row)

        try:
            for table, columns in [(values, "id NUMERIC(10,0),label VARCHAR(12),amount NUMERIC(10,2),day_value DATE,seen TIMESTAMP(6)"),
                                   (references, "id INTEGER,target_id INTEGER")]:
                execute(f"CREATE TABLE {table}({columns})")
                created.append(table)
            execute(f"INSERT INTO {values} VALUES(1,'alpha',12.50,DATE '2024-02-29',TIMESTAMP '2024-02-29 12:34:56.125')")
            execute(f"INSERT INTO {values} VALUES(2,NULL,NULL,NULL,NULL)")
            for key, target in [(1, 1), (2, 2), (3, 9)]:
                execute(f"INSERT INTO {references} VALUES({key},{target})")
            fn = "NVL" if oracle_semantics else "COALESCE"
            inner = f" FROM {values} d WHERE d.id=r.target_id"
            outer = f" FROM {references} r ORDER BY r.id"
            number = lambda value: canonical(value)
            for name, expression, expected in [
                ("character", f"{fn}((SELECT d.label{inner}),'none')", ["alpha", "none", "none"]),
                ("concatenation", f"{fn}((SELECT d.label||'_x'{inner}),'none')", ["alpha_x", "_x" if oracle_semantics else "none", "none"]),
                ("nested_default", f"{fn}((SELECT {fn}(d.label,'-1')||'_x'{inner}),'none')", ["alpha_x", "-1_x", "none"]),
                ("number", f"{fn}((SELECT d.amount{inner}),7)", [number(12.5), number(7), number(7)]),
                ("date_arithmetic", f"{fn}((SELECT d.day_value{inner}),DATE '2024-03-01')-DATE '2024-02-28'", [number(1), number(2), number(2)]),
                ("timestamp_fraction", f"CASE WHEN {fn}((SELECT d.seen{inner}),TIMESTAMP '2000-01-01 00:00:07.25')=TIMESTAMP '2024-02-29 12:34:56.125' THEN 1 ELSE 0 END", [number(1), number(0), number(0)]),
            ]:
                check(name, "SELECT " + expression + outer, [[v] for v in expected])
            check("evaluation_order", f"SELECT {fn}((SELECT d.amount{inner}),1/(r.id-1)) FROM {references} r WHERE r.id=1",
                  "division_by_zero" if oracle_semantics else [[number(12.5)]])
        finally:
            for table in reversed(created):
                try:
                    execute(f"DROP TABLE {table}" + (" PURGE" if oracle_driver else ""))
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind="TABLE", exception=type(exc).__name__))
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
                cases=cases, total=len(cases), matched=sum(c["matched"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 7 and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
