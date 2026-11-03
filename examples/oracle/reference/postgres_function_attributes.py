"""Independent PostgreSQL controls for stored function volatility metadata."""
import re
import uuid
from compare import canonical


def run(reference, target, schemas):
    if len(schemas) != 2 or schemas[0] == schemas[1] or not all(re.fullmatch(r"[a-z][a-z0-9_]{1,31}", s) for s in schemas):
        raise ValueError("two distinct simple schema names are required")
    prefix = "attrs_" + uuid.uuid4().hex[:10]
    names = {kind: prefix + "_" + kind for kind in ("immutable", "stable", "volatile", "default")}
    relations = {kind: (schemas[1] if kind == "default" else schemas[0]) + "." + name for kind, name in names.items()}
    report = dict(format=1, postgres_version_num=reference.info.server_version, cases=[], cleanup_errors=[],
                  application_qualified=False, scope="Native stored function declaration metadata, execution and replacement", success=False)
    created = []

    def query(connection, sql):
        with connection.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    def compare(name, sql, expected):
        row = dict(id=name)
        for label, connection in (("postgres", reference), ("bicdb", target)):
            try:
                row[label] = dict(rows=query(connection, sql))
            except Exception as error:
                row[label] = dict(sqlstate=getattr(error, "sqlstate", None))
        expected = dict(rows=[[canonical(v) for v in values] for values in expected])
        row["source_qualified"] = row["postgres"] == expected
        row["matched"] = row["source_qualified"] and row["postgres"] == row["bicdb"]
        report["cases"].append(row)

    try:
        for backend, connection in enumerate((reference, target)):
            for index, kind in enumerate(("immutable", "stable", "volatile"), 1):
                query(connection, f"CREATE FUNCTION {relations[kind]}() RETURNS INT {kind.upper()} LANGUAGE sql AS $$SELECT {index}$$")
                created.append((backend, relations[kind]))
            query(connection, f'CREATE FUNCTION {relations["default"]}("IMMUTABLE" INT) RETURNS TEXT LANGUAGE sql AS $$SELECT \'STABLE VOLATILE IMMUTABLE\'$$')
            created.append((backend, relations["default"]))
        catalog = ("SELECT p.proname,p.provolatile FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                   + "WHERE n.nspname IN ('" + schemas[0] + "','" + schemas[1] + "') AND p.proname LIKE '" + prefix + "_%' ORDER BY p.proname")
        compare("declared_and_default_volatility", catalog,
                [[names[kind], value] for kind, value in (("default", "v"), ("immutable", "i"), ("stable", "s"), ("volatile", "v"))])
        compare("function_values", f"SELECT {relations['immutable']}(),{relations['stable']}(),{relations['volatile']}(),{relations['default']}(1)", [[1, 2, 3, "STABLE VOLATILE IMMUTABLE"]])
        for connection in (reference, target):
            query(connection, f"CREATE OR REPLACE FUNCTION {relations['immutable']}() RETURNS INT STABLE LANGUAGE sql AS $$SELECT 42$$")
        compare("replacement_updates_metadata", catalog,
                [[names[kind], value] for kind, value in (("default", "v"), ("immutable", "s"), ("stable", "s"), ("volatile", "v"))])
        compare("replacement_value", f"SELECT {relations['immutable']}()", [[42]])
    finally:
        for backend, relation in reversed(created):
            try:
                query((reference, target)[backend], "DROP FUNCTION " + relation)
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=backend, error=type(error).__name__))
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"])
    report["success"] = report["matched"] == report["total"] and not report["cleanup_errors"]
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
