"""Independent PostgreSQL text and binary controls for domains over arrays."""
import re
import uuid


def run(reference, target, schemas):
    if len(schemas) != 2 or schemas[0] == schemas[1] or not all(re.fullmatch(r"[a-z][a-z0-9_]{1,31}", s) for s in schemas):
        raise ValueError("two distinct simple schema names are required")
    prefix = "carrier_" + uuid.uuid4().hex[:10]
    names = {key: schemas[0] + "." + prefix + "_" + key for key in ("texts", "nested", "numbers")}
    report = dict(format=1, postgres_version_num=reference.info.server_version, cases=[], cleanup_errors=[],
                  application_qualified=False, scope="Native array-domain wire representations; logical domain OID parity is not qualified", success=False)
    created = []
    try:
        for backend, connection in enumerate((reference, target)):
            for key, base in (("texts", "text[]"), ("nested", names["texts"]), ("numbers", "numeric[]")):
                with connection.cursor() as cur:
                    cur.execute(f"CREATE DOMAIN {names[key]} AS {base}")
                created.append((backend, names[key]))
        plans = [
            ("empty", f"SELECT ARRAY[]::text[]::{names['texts']}", "{}", False),
            ("whole_null", f"SELECT NULL::{names['texts']}", None, False),
            ("null_element", f"SELECT ARRAY[NULL]::text[]::{names['texts']}", "{NULL}", False),
            ("text_quoting", f"SELECT ARRAY['a,b','a\"b','é','']::{names['texts']}", '{"a,b","a\\"b",é,""}', False),
            ("nested_domain", f"SELECT ARRAY['x',NULL]::{names['nested']}", "{x,NULL}", False),
            ("numeric_elements", f"SELECT ARRAY[1.20::numeric,NULL,-3::numeric]::{names['numbers']}", "{1.20,NULL,-3}", False),
            ("binary_nested_domain", f"SELECT ARRAY['x',NULL]::{names['nested']}",
             "00000001000000010000001900000002000000010000000178ffffffff", True),
        ]
        for name, sql, expected, binary in plans:
            row = dict(id=name, binary=binary)
            for label, connection in (("postgres", reference), ("bicdb", target)):
                try:
                    with connection.cursor(binary=binary) as cur:
                        cur.execute(sql)
                        value = cur.pgresult.get_value(0, 0)
                        value = None if value is None else value.hex() if binary else value.decode("utf8")
                        row[label] = dict(value=value)
                except Exception as exc:
                    row[label] = dict(sqlstate=getattr(exc, "sqlstate", None), error=type(exc).__name__)
            row["source_qualified"] = row["postgres"] == dict(value=expected)
            row["matched"] = row["source_qualified"] and row["postgres"] == row["bicdb"]
            report["cases"].append(row)
    finally:
        for backend, name in reversed(created):
            try:
                with (reference, target)[backend].cursor() as cur:
                    cur.execute("DROP DOMAIN " + name)
            except Exception as exc:
                report["cleanup_errors"].append(dict(backend=backend, error=type(exc).__name__))
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
