"""Oracle collection-valued functions over the PostgreSQL driver transport."""
import json
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    local_type = identifier(prefix + "_T")
    collection = identifier(schema) + "." + local_type
    shadow = identifier("public") + "." + local_type
    functions = {key: identifier(schema) + "." + identifier(prefix + "_" + key) for key in ("VALUE", "EMPTY", "NULL")}
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, target_protocol="prepared", success=False,
                  scope="Collection-valued function declarations, owner-bound constructors and result transport; no PL/SQL methods")
    created = []

    def execute(connection, sql, label):
        with connection.cursor() as cur:
            if label == "bicdb":
                cur.execute(sql, prepare=True)
            else:
                cur.execute(sql)
            if not cur.description:
                return []
            rows = []
            for row in cur.fetchall():
                values = []
                for value in row:
                    if label == "oracle" and value is not None:
                        value = value.aslist()
                    if value is not None and not isinstance(value, list):
                        raise TypeError("registered collection result did not decode as an array")
                    values.append(None if value is None else [canonical(v) for v in value])
                rows.append(values)
            return rows

    def record(name, sql, expected, cleanup=None):
        row = dict(id=name, sql=sql)
        for backend, (label, connection) in enumerate((("oracle", reference), ("bicdb", target))):
            try:
                row[label] = dict(rows=execute(connection, sql, label))
                if cleanup:
                    created.append((backend, cleanup))
            except Exception as exc:
                detail = exc.args[0] if exc.args else None
                row[label] = dict(code=getattr(exc, "sqlstate", None) or getattr(detail, "full_code", None), exception=type(exc).__name__)
        wanted = dict(rows=expected)
        row["source_qualified"] = row["oracle"] == wanted
        row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
        report["cases"].append(row)

    try:
        record("declare_collection", f"CREATE TYPE {collection} AS TABLE OF VARCHAR2(8 BYTE)", [], "DROP TYPE " + collection)
        # A target-only owned shadow makes an accidental public-schema lookup
        # observable. Oracle resolves the unqualified type in the declaration's
        # owner; a two-byte value cannot fit this one-byte shadow.
        execute(target, f"CREATE TYPE {shadow} AS TABLE OF VARCHAR2(1 BYTE)", "bicdb")
        created.append((1, "DROP TYPE " + shadow))
        for key, value in (("VALUE", local_type + "('é',NULL)"), ("EMPTY", local_type + "()"), ("NULL", "NULL")):
            record("declare_" + key.lower(), f"CREATE FUNCTION {functions[key]} RETURN {local_type} AS BEGIN RETURN {value};END;", [], "DROP FUNCTION " + functions[key])
        from psycopg.types.array import ArrayLoader
        with target.cursor() as cur:
            cur.execute(f"SELECT CAST(NULL AS {collection}) FROM DUAL")
            oid = cur.description[0].type_code
            assert cur.fetchone()[0] is None
        loader = type("CollectionResultLoader", (ArrayLoader,), dict(base_oid=1043, delimiter=b","))
        target.adapters.register_loader(oid, loader)
        for key, expected in (("VALUE", [[['é', None]]]), ("EMPTY", [[[]]]), ("NULL", [[None]])):
            record("result_" + key.lower(), f"SELECT {functions[key]}() FROM DUAL", expected)
    finally:
        for backend, sql in reversed(created):
            try:
                execute((reference, target)[backend], sql, ("oracle", "bicdb")[backend])
            except Exception as exc:
                report["cleanup_errors"].append(dict(backend=backend, error=type(exc).__name__))
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"])
    report["success"] = report["matched"] == report["total"] and not report["cleanup_errors"]
    return report

def main():
    import argparse
    import os
    import uuid
    from pathlib import Path
    import oracledb
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        raise ValueError("a dedicated non-administrative schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(user=config["user"], password=password.read_text().strip(), host=config["host"], port=config["port"], service_name=config["service_name"]) as source:
        source.autocommit = True
        source.call_timeout = 10000
        with source.cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
            schema = cur.fetchone()[0]
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10, options="-c bicdb.sql_profile=oracle-preview-v1 -c statement_timeout=10000") as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("Oracle-preview target required")
            report = run(source, target, schema, "BICDB_CR_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
