"""PostgreSQL controls for declaration-based routine security and permissions."""
import re
import uuid
from compare import canonical


def run(reference, target, schemas):
    if len(schemas) != 2 or schemas[0] == schemas[1] or not all(re.fullmatch(r"[a-z][a-z0-9_]{1,31}", s) for s in schemas):
        raise ValueError("two distinct simple schema names are required")
    prefix = "rsec_" + uuid.uuid4().hex[:10]
    role = prefix + "_caller"
    table = schemas[0] + "." + prefix + "_private"
    names = {kind: prefix + "_" + kind for kind in ("default", "invoker", "definer")}
    functions = {kind: schemas[0] + "." + name for kind, name in names.items()}
    report = dict(format=1, postgres_version_num=reference.info.server_version, cases=[], cleanup_errors=[],
                  application_qualified=False, scope="Function security declarations, body literals, caller permissions and context restoration", success=False)
    created = [dict(role=False, table=False, functions=[]), dict(role=False, table=False, functions=[])]

    def query(connection, sql):
        with connection.cursor() as cur:
            cur.execute(sql)
            return dict(rows=[[canonical(v) for v in row] for row in cur.fetchall()]) if cur.description else dict(status="ok")

    def compare(name, sql, expected=None, error=None):
        row = dict(id=name)
        for label, connection in (("postgres", reference), ("bicdb", target)):
            try:
                row[label] = query(connection, sql)
            except Exception as failure:
                row[label] = dict(sqlstate=getattr(failure, "sqlstate", None))
                if label == "bicdb":
                    row["bicdb_error_message"] = str(failure)
        expected = dict(sqlstate=error) if error else dict(rows=[[canonical(v) for v in values] for values in expected])
        row["source_qualified"] = row["postgres"] == expected
        row["matched"] = row["source_qualified"] and row["postgres"] == row["bicdb"]
        report["cases"].append(row)

    try:
        for backend, connection in enumerate((reference, target)):
            query(connection, "CREATE ROLE " + role)
            created[backend]["role"] = True
            query(connection, "GRANT USAGE ON SCHEMA " + schemas[0] + " TO " + role)
            query(connection, "CREATE TABLE " + table + "(secret INT)")
            created[backend]["table"] = True
            query(connection, "INSERT INTO " + table + " VALUES(7)")
            query(connection, "REVOKE ALL ON TABLE " + table + " FROM PUBLIC")
            for kind, attribute in (("default", ""), ("invoker", "SECURITY INVOKER"), ("definer", "SECURITY DEFINER")):
                query(connection, f"CREATE FUNCTION {functions[kind]}() RETURNS INT LANGUAGE plpgsql {attribute} AS $$DECLARE marker TEXT:='SECURITY DEFINER';v INT;BEGIN SELECT secret INTO v FROM {table};RETURN v;END$$")
                created[backend]["functions"].append(functions[kind])
                query(connection, "GRANT EXECUTE ON FUNCTION " + functions[kind] + "() TO " + role)
        catalog = ("SELECT p.proname,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                   + "WHERE n.nspname='" + schemas[0] + "' AND p.proname LIKE '" + prefix + "_%' ORDER BY p.proname")
        compare("only_explicit_definer_is_classified_as_definer", catalog,
                [[names[kind], value] for kind, value in (("default", False), ("definer", True), ("invoker", False))])
        for connection in (reference, target):
            query(connection, "SET ROLE " + role)
        compare("default_invoker_cannot_read_owner_table", "SELECT " + functions["default"] + "()", error="42501")
        compare("explicit_invoker_cannot_read_owner_table", "SELECT " + functions["invoker"] + "()", error="42501")
        compare("explicit_definer_can_read_owner_table", "SELECT " + functions["definer"] + "()", [[7]])
        compare("caller_identity_is_restored", "SELECT current_user", [[role]])
    finally:
        for backend, connection in enumerate((reference, target)):
            def cleanup(sql):
                try:
                    query(connection, sql)
                except Exception as failure:
                    report["cleanup_errors"].append(dict(backend=backend, error=type(failure).__name__))
            cleanup("RESET ROLE")
            for function in reversed(created[backend]["functions"]):
                cleanup("REVOKE EXECUTE ON FUNCTION " + function + "() FROM " + role)
                cleanup("DROP FUNCTION " + function)
            if created[backend]["table"]:
                cleanup("DROP TABLE " + table)
            if created[backend]["role"]:
                cleanup("REVOKE USAGE ON SCHEMA " + schemas[0] + " FROM " + role)
                cleanup("DROP ROLE " + role)
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
