"""Independent native PostgreSQL controls for TRIM and regex isolation."""

PLANS = [
    ("trim_character_set", "SELECT TRIM('ab' FROM 'ababa')", ""),
    ("trim_empty_result", "SELECT TRIM(' ')", ""),
    ("replace_default_first", "SELECT regexp_replace('a1b2c3','[0-9]','x')", "axb2c3"),
    ("replace_global_flag", "SELECT regexp_replace('a1b2c3','[0-9]','x','g')", "axbxcx"),
    ("replace_null_flags", "SELECT regexp_replace('abc','a','x',NULL::text)", None),
    ("replace_empty_result", "SELECT regexp_replace('a','a','')", ""),
    ("qualified_default_first", "SELECT pg_catalog.regexp_replace('a1b2c3','[0-9]','x')", "axb2c3"),
    ("trim_null_input", "SELECT TRIM('ab' FROM NULL::text)", None),
]


def run(reference, target, schemas=None):
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  application_qualified=False, target_protocol="prepared", cases=[],
                  cleanup_errors=[], success=False)
    for name, sql, expected in PLANS:
        row = dict(id=name, sql=sql)
        for label, connection in (("postgres", reference), ("bicdb", target)):
            try:
                with connection.cursor() as cur:
                    cur.execute(sql, prepare=True)
                    row[label] = dict(value=cur.fetchone()[0])
            except Exception as exc:
                row[label] = dict(sqlstate=getattr(exc, "sqlstate", None), exception=type(exc).__name__)
        row["source_qualified"] = row["postgres"] == dict(value=expected)
        row["matched"] = row["source_qualified"] and row["bicdb"] == dict(value=expected)
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
