"""Independent PostgreSQL controls for literal data and routine call boundaries."""
from compare import canonical


def run(reference, target):
    name = "bicdb_native_dynamic_control"
    definitions = [
        (name + "_echo", "(p TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$BEGIN RETURN p;END$$"),
        (name + "_missing", "() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'missing' USING ERRCODE='P0002';END$$"),
        (name + "_catch", f"() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM {name}_missing();RETURN 42;EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 99;END$$"),
        (name + "_division", "() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN EXECUTE 'SELECT 1/0';RETURN 42;END$$"),
    ]
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  cases=[], cleanup_errors=[], success=False)
    created = []

    def query(connection, sql, values=None):
        with connection.cursor() as cursor:
            cursor.execute(sql, values)
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()]) if cursor.description else dict(status="ok")

    def compare(label, sql, values=None, error=None):
        row = dict(id=label)
        for kind, connection in [("postgres", reference), ("bicdb", target)]:
            try:
                row[kind] = query(connection, sql, values)
            except Exception as failure:
                if not getattr(failure, "sqlstate", None):
                    raise
                row[kind] = dict(error_code=failure.sqlstate)
        row["matched"] = row["postgres"] == row["bicdb"] and (
            row["postgres"] == dict(error_code=error) if error else "error_code" not in row["postgres"])
        report["cases"].append(row)

    try:
        for index, connection in enumerate((reference, target)):
            for function, declaration in definitions:
                query(connection, "CREATE FUNCTION " + function + declaration)
                created.append((index, function))
        for index, value in enumerate(["O'Reilly", "O''Reilly", "x');DELETE FROM anything;--", "ñΩ", ":1+:1"]):
            compare("bound_" + str(index), "SELECT " + name + "_echo(%s)", (value,))
            literal = "'" + value.replace("'", "''") + "'"
            compare("literal_" + str(index), "SELECT " + literal)
        compare("native_no_data_is_error", "SELECT " + name + "_missing()", error="P0002")
        compare("native_pl_handler", "SELECT " + name + "_catch()")
        compare("native_unfetched_query_executes", "SELECT " + name + "_division()", error="22012")
    finally:
        for index, function in reversed(created):
            try:
                query((reference, target)[index], "DROP FUNCTION " + function)
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=index, error=type(error).__name__))
    report["matched"] = sum(case["matched"] for case in report["cases"])
    report["total"] = len(report["cases"])
    report["success"] = report["matched"] == report["total"] and not report["cleanup_errors"]
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ["ORACLE_CONTROL_PG_DSN"], autocommit=True,
                         connect_timeout=10) as reference:
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True,
                             connect_timeout=10) as target:
            if target.info.parameter_status("bicdb_sql_profile") != "postgres":
                raise RuntimeError("a native PostgreSQL-profile target is required")
            report = run(reference, target)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
