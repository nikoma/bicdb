"""Set-operand scopes for typed defaults, aggregates and logical DATE.

Oracle NVL and PostgreSQL COALESCE have independent evaluation-order cases.
Nonce tables/views are removed; the fixture does not use application objects.
"""
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main


def observe(factory, oracle):
    suffix = uuid.uuid4().hex[:12].upper()
    groups, numbers, texts, view = ["BICDB_SO_" + suffix + part for part in ["G", "N", "T", "V"]]
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
                code = getattr(exc.args[0], "code", None) if exc.args else None
                state = getattr(exc, "sqlstate", None)
                row.update(exception=type(exc).__name__, detail=str(exc)[:500], oracle_code=code, sqlstate=state,
                           qualified=expected == "division_by_zero" and (code == 1476 if oracle_driver else state == "22012"))
            report["cases"].append(row)

        try:
            for table, columns in [(groups, "id NUMERIC PRIMARY KEY,happened DATE"),
                                   (numbers, "row_id NUMERIC PRIMARY KEY,group_id NUMERIC,amount NUMERIC"),
                                   (texts, "row_id NUMERIC PRIMARY KEY,amount VARCHAR(12)")]:
                execute(f"CREATE TABLE {table}({columns})")
                created.append(("TABLE", table))
            for sql in [f"INSERT INTO {groups} VALUES(1,DATE '2024-02-29')", f"INSERT INTO {groups} VALUES(2,NULL)",
                        f"INSERT INTO {numbers} VALUES(1,1,NULL)", f"INSERT INTO {numbers} VALUES(2,1,7)",
                        f"INSERT INTO {texts} VALUES(1,'abc')", f"INSERT INTO {texts} VALUES(2,NULL)"]:
                execute(sql)
            fn = "NVL" if oracle else "COALESCE"
            empty = f"SELECT id,CAST(0 AS NUMERIC) AS qty FROM {groups} WHERE id<0"
            grouped = f"SELECT g.id,SUM({fn}(n.amount,0)) AS qty FROM {groups} g LEFT JOIN {numbers} n ON g.id=n.group_id GROUP BY g.id"
            want = [[canonical(1), canonical(7)], [canonical(2), canonical(0)]]
            check("joined_aggregate", grouped + " ORDER BY 1", want)
            check("union_all", empty + " UNION ALL " + grouped + " ORDER BY 1", want)
            check("union_distinct", empty + " UNION " + grouped + " ORDER BY 1", want)
            check("ordered_page", empty + " UNION ALL " + grouped + " ORDER BY 2 DESC FETCH FIRST 1 ROW ONLY", [want[0]])
            check("nested_set", "(" + empty + " UNION ALL " + grouped + f") UNION ALL SELECT 3,5 FROM {groups} WHERE id=1 ORDER BY 1", want + [[canonical(3),canonical(5)]])
            same = f"SELECT id,CASE WHEN id=1 THEN 7 ELSE 0 END AS qty FROM {groups}"
            check("intersection", same + " INTERSECT " + grouped + " ORDER BY 1", want)
            different = f"SELECT id,CASE WHEN id=1 THEN 8 ELSE 0 END AS qty FROM {groups}"
            check("difference", different + (" MINUS " if oracle else " EXCEPT ") + grouped + " ORDER BY 1", [[canonical(1), canonical(8)]])
            execute(f"CREATE VIEW {view} AS {empty} UNION ALL {grouped}")
            created.append(("VIEW", view))
            check("stored_view", f"SELECT * FROM {view} ORDER BY id", want)
            check("stored_view_count", f"SELECT COUNT(*) FROM {view}", [[canonical(2)]])
            cte = f"WITH chosen AS (SELECT * FROM {groups}) {empty} UNION ALL " + grouped.replace(groups + " g", "chosen g") + " ORDER BY 1"
            check("cte_input", cte, want)
            check("logical_date", empty + f" UNION ALL SELECT g.id,{fn}(g.happened,DATE '2024-03-01')-DATE '2024-02-28' FROM {groups} g ORDER BY 1", [[canonical(1),canonical(1)],[canonical(2),canonical(2)]])
            check("scalar_default", empty + f" UNION ALL SELECT g.id,{fn}((SELECT {fn}(n.amount,3) FROM {numbers} n WHERE n.group_id=g.id ORDER BY row_id FETCH FIRST 1 ROW ONLY),7) FROM {groups} g ORDER BY 1", [[canonical(1),canonical(3)],[canonical(2),canonical(7)]])
            check("alias_scope", f"SELECT CAST(n.amount AS VARCHAR(12)) AS label FROM {numbers} n WHERE row_id<0 UNION ALL SELECT {fn}(n.amount,'none') FROM {texts} n ORDER BY 1", [["abc"],["none"]])
            check("evaluation_order", empty + f" UNION ALL SELECT g.id,{fn}(g.id,1/(g.id-1)) FROM {groups} g WHERE g.id=1", "division_by_zero" if oracle else [[canonical(1),canonical(1)]])
        finally:
            for kind, name in reversed(created):
                try:
                    execute(f"DROP {kind} {name}" + (" PURGE" if oracle_driver and kind == "TABLE" else ""))
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind=kind, exception=type(exc).__name__))
    return report


def qualified(row):
    return bool(row.get("qualified", False))


def run(source_factory, target_factory):
    with source_factory() as probe:
        oracle = type(probe).__module__.startswith("oracledb")
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"], matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(c["matched"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 14 and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
