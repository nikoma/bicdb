"""Independent PostgreSQL controls for registered function return types."""
import re
import uuid


def run(reference, target, schemas):
    if len(schemas) != 2 or schemas[0] == schemas[1] or not all(re.fullmatch(r"[a-z][a-z0-9_]{1,31}", s) for s in schemas):
        raise ValueError("two distinct simple schema names are required")
    suffix = uuid.uuid4().hex[:10]
    enum_name = "MiXeD_" + suffix
    enum = schemas[0] + '."' + enum_name + '"'
    labels = "table_of_labels_" + suffix
    positive = schemas[0] + ".positive_" + suffix
    functions = {key: schemas[0] + ".return_" + key + "_" + suffix for key in ("enum", "labels", "null", "positive", "numeric", "local", "local_check", "array")}
    report = dict(format=1, postgres_version_num=reference.info.server_version, cases=[], cleanup_errors=[],
                  application_qualified=False, target_protocol="prepared", success=False,
                  scope="Native registered return coercion, quoted identities and return catalog types; no domain result-OID parity qualification")
    created = []

    def query(connection, sql):
        with connection.cursor() as cur:
            cur.execute(sql, prepare=True)
            if not cur.description:
                return []
            return [[None if (value := cur.pgresult.get_value(row, col)) is None else value.decode("utf8")
                     for col in range(cur.pgresult.nfields)] for row in range(cur.pgresult.ntuples)]

    plans = [
        ("enum", functions["enum"], f"RETURNS {enum} LANGUAGE sql AS $$SELECT 'ok'::{enum}$$"),
        ("labels", functions["labels"], f"RETURNS {labels} LANGUAGE sql AS $$SELECT ARRAY['a',NULL]$$"),
        ("null", functions["null"], f"RETURNS {labels} LANGUAGE sql AS $$SELECT NULL::{labels}$$"),
        ("positive", functions["positive"] + "(p INTEGER)", f"RETURNS {positive} LANGUAGE sql AS $$SELECT p$$"),
        ("numeric", functions["numeric"], "RETURNS NUMERIC(9,2) LANGUAGE sql AS $$SELECT 12.345::numeric$$"),
        ("local", functions["local"], f"RETURNS {labels} LANGUAGE plpgsql AS $$DECLARE v {labels}:=ARRAY['outer'];BEGIN DECLARE v integer:=7;BEGIN NULL;END;RETURN v;END;$$"),
        ("local_check", functions["local_check"], f"RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE v {positive}:=3;BEGIN v:=-1;RETURN v;END;$$"),
        ("array", functions["array"], "RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE v integer[]:=ARRAY[1];BEGIN v[3]:=7;RETURN array_to_string(v,',','NULL');END;$$"),
    ]
    try:
        for backend, connection in enumerate((reference, target)):
            for sql, cleanup in [
                (f"CREATE TYPE {enum} AS ENUM ('ok','bad')", "DROP TYPE " + enum),
                (f"CREATE DOMAIN {labels} AS text[]", "DROP DOMAIN " + labels),
                (f"CREATE DOMAIN {schemas[0]}.{labels} AS INTEGER", "DROP DOMAIN " + schemas[0] + "." + labels),
                (f"CREATE DOMAIN {positive} AS INTEGER CHECK (VALUE>0)", "DROP DOMAIN " + positive),
            ]:
                query(connection, sql)
                created.append((backend, cleanup))
            for key, name, body in plans:
                args = "" if key == "positive" else "()"
                query(connection, f"CREATE FUNCTION {name}{args} {body}")
                created.append((backend, "DROP FUNCTION " + functions[key]))
        cases = [
            ("quoted_enum_value", f"SELECT {functions['enum']}()", [["ok"]], None),
            ("table_prefix_array_domain", f"SELECT {functions['labels']}()", [["{a,NULL}"]], None),
            ("whole_null_domain", f"SELECT {functions['null']}()", [[None]], None),
            ("valid_domain_result", f"SELECT {functions['positive']}(3)", [["3"]], None),
            ("invalid_domain_result", f"SELECT {functions['positive']}(-1)", None, "23514"),
            ("native_numeric_return_modifier", f"SELECT {functions['numeric']}()", [["12.345"]], None),
            ("registered_local_array_and_shadow_restore", f"SELECT {functions['local']}()", [["{outer}"]], None),
            ("registered_local_domain_constraint", f"SELECT {functions['local_check']}()", None, "23514"),
            ("native_array_growth", f"SELECT {functions['array']}()", [["1,NULL,7"]], None),
            ("quoted_enum_return_catalog", "SELECT t.typname FROM pg_proc p JOIN pg_type t ON t.oid=p.prorettype WHERE p.proname='" + functions['enum'].split('.')[1] + "'", [[enum_name]], None),
            ("table_prefix_return_catalog", "SELECT t.typname FROM pg_proc p JOIN pg_type t ON t.oid=p.prorettype WHERE p.proname='" + functions['labels'].split('.')[1] + "'", [[labels]], None),
        ]
        for name, sql, expected, error in cases:
            row = dict(id=name)
            for label, connection in (("postgres", reference), ("bicdb", target)):
                try:
                    row[label] = dict(rows=query(connection, sql))
                except Exception as exc:
                    row[label] = dict(sqlstate=getattr(exc, "sqlstate", None))
            wanted = dict(sqlstate=error) if error else dict(rows=expected)
            row["source_qualified"] = row["postgres"] == wanted
            row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
            report["cases"].append(row)
    finally:
        for backend, sql in reversed(created):
            try:
                query((reference, target)[backend], sql)
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
