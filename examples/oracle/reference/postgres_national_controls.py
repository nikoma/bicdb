"""Independent PostgreSQL varchar assignment and Unicode length controls."""
from compare import canonical


def run(reference, target):
    prefix = "bicdb_native_national_control"
    definitions = [(prefix + "_one", "(p TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE v VARCHAR(1);BEGIN v:=p;RETURN v;END$$"),
                   (prefix + "_two", "(p TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE v VARCHAR(2);BEGIN v:=p;RETURN v;END$$"),
                   (prefix + "_length", "(p TEXT) RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE v VARCHAR(4);BEGIN v:=p;RETURN length(v);END$$")]
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  cases=[], cleanup_errors=[], success=False)
    created = []
    def query(connection, sql, values=None):
        with connection.cursor() as cursor:
            cursor.execute(sql, values)
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()]) if cursor.description else dict(status="ok")
    try:
        for index, connection in enumerate((reference, target)):
            for name, declaration in definitions:
                query(connection, "CREATE FUNCTION " + name + declaration)
                created.append((index, name))
        for suffix, values in [("one", ["😀", "é", "A ", "ab", "Á", "", None]),
                               ("two", ["😀", "Á", "ab ", "abc", "", None]),
                               ("length", ["😀", "é", "Á", "A ", "", None])]:
            for index, value in enumerate(values):
                row = dict(id=suffix + "_" + str(index))
                for label, connection in [("postgres", reference), ("bicdb", target)]:
                    try:
                        row[label] = query(connection, f"SELECT {prefix}_{suffix}(%s)", (value,))
                    except Exception as error:
                        if not getattr(error, "sqlstate", None):
                            raise
                        row[label] = dict(error_code=error.sqlstate)
                row["matched"] = row["postgres"] == row["bicdb"]
                report["cases"].append(row)
    finally:
        for index, name in reversed(created):
            try:
                query((reference, target)[index], "DROP FUNCTION " + name)
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
