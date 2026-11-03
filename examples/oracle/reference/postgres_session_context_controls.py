"""Native PostgreSQL session and text controls independent of Oracle USERENV."""

PLANS = [('native_identity_relationship', 'SELECT CURRENT_USER = SESSION_USER', [['t']], None),
 ('current_database', 'SELECT current_database() IS NOT NULL', [['t']], None),
 ('current_schema', 'SELECT current_schema()', [['public']], None),
 ('case_sensitive_setting', "SELECT current_setting('server_version_num')::integer >= 180000", [['t']], None),
 ('native_regex_default', "SELECT regexp_replace('aa','a','x')", [['xa']], None),
 ('native_concat_null', "SELECT 'x'||NULL::text", [[None]], None),
 ('native_concat_function', "SELECT concat('x',NULL)", [['x']], None),
 ('native_text_length', "SELECT length('é')", [['1']], None)]

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
