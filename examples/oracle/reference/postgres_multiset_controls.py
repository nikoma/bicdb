"""Native PostgreSQL controls independent of preview collection grammar."""

PLANS = [
    ("text_all_equality", "(SELECT '01'::text AS v UNION ALL SELECT '1'::text) INTERSECT ALL SELECT '1'::text", [["1"]], None),
    ("numeric_all_scale", "SELECT '1.00'::numeric INTERSECT ALL SELECT '1.0'::numeric", [["1.00"]], None),
    ("null_all_count", "(SELECT NULL::integer UNION ALL SELECT NULL::integer) INTERSECT ALL SELECT NULL::integer", [[None]], None),
    ("union_all", "SELECT 1 AS v UNION ALL SELECT 1 ORDER BY v", [["1"], ["1"]], None),
    ("union_distinct", "SELECT 1 AS v UNION SELECT 1 UNION SELECT 2 ORDER BY v", [["1"], ["2"]], None),
    ("intersect_all", "(VALUES (1),(1),(2)) INTERSECT ALL (VALUES (1),(1),(3))", [["1"], ["1"]], None),
    ("except_all", "(VALUES (1),(1),(2)) EXCEPT ALL (VALUES (1)) ORDER BY 1", [["1"], ["2"]], None),
    ("intersect_precedence", "SELECT 1 AS v UNION SELECT 2 INTERSECT SELECT 2 ORDER BY v", [["1"], ["2"]], None),
    ("array_query_intersection", "SELECT ARRAY[1] INTERSECT SELECT ARRAY[1]", [["{1}"]], None),
    ("null_query_intersection", "SELECT NULL::integer INTERSECT SELECT NULL::integer", [[None]], None),
    ("no_oracle_multiset_grammar", "SELECT ARRAY[1] MULTISET INTERSECT ARRAY[1]", None, "42601"),
    ("no_oracle_empty_predicate", "SELECT ARRAY[1] IS EMPTY", None, "42601"),
]


def run(reference, target, schemas=None):
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  application_qualified=False, target_protocol="prepared", cases=[],
                  cleanup_errors=[], success=False)
    for name, sql, expected, error in PLANS:
        row = dict(id=name, sql=sql)
        for label, connection in (("postgres", reference), ("bicdb", target)):
            try:
                with connection.cursor() as cur:
                    cur.execute(sql, prepare=True)
                    row[label] = dict(rows=[[None if (value := cur.pgresult.get_value(i,j)) is None else value.decode("utf8")
                                             for j in range(cur.pgresult.nfields)]
                                            for i in range(cur.pgresult.ntuples)])
            except Exception as exc:
                row[label] = dict(sqlstate=getattr(exc, "sqlstate", None))
        wanted = dict(sqlstate=error) if error else dict(rows=expected)
        row["source_qualified"] = row["postgres"] == wanted
        row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
        report["cases"].append(row)
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"])
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["success"] = report["source_qualified"] and report["matched"] == report["total"]
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schemas", nargs=2, required=True)
    args = parser.parse_args()
    with psycopg.connect(os.environ["ORACLE_CONTROL_PG_DSN"], autocommit=True, connect_timeout=10) as reference:
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10) as target:
            if target.info.parameter_status("bicdb_sql_profile") != "postgres":
                raise RuntimeError("native PostgreSQL-profile target required")
            report = run(reference, target, args.schemas)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
