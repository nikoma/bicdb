"""Named SQL nested-table constructors, independently checked against Oracle.

These cases qualify SQL type registration and element values. They do not
qualify PL/SQL collection methods, sparse collections, or application workflows.
"""
import json
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    names = {key: identifier(schema) + "." + identifier(prefix + "_" + key)
             for key in ("BYTES", "CHARS", "NUMS")}
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, success=False,
                  scope="Named SQL nested-table declarations and constructors; no PL/SQL method qualification")
    created = [[], []]

    def execute(connection, sql, label):
        with connection.cursor() as cur:
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
                        raise TypeError("registered collection did not decode as an array")
                    values.append(None if value is None else [canonical(v) for v in value])
                rows.append(values)
            return rows

    def record(name, sql, expected=None, error=None, create_key=None):
        row = dict(id=name, sql=sql)
        for index, (label, connection) in enumerate((("oracle", reference), ("bicdb", target))):
            try:
                row[label] = dict(rows=execute(connection, sql, label))
                if create_key is not None:
                    created[index].append(create_key)
            except Exception as exc:
                detail = exc.args[0] if exc.args else None
                code = getattr(exc, "sqlstate", None) or getattr(detail, "full_code", None)
                if label == "bicdb" and getattr(exc, "diag", None):
                    row["bicdb_sqlstate"] = code
                    try:
                        stack = json.loads(exc.diag.message_detail or "{}").get("oracle_error_stack", [])
                        if stack and isinstance(stack[0].get("code"), int):
                            code = f"ORA-{abs(stack[0]['code']):05d}"
                    except (ValueError, TypeError, AttributeError):
                        pass
                row[label] = dict(code=code, exception=type(exc).__name__)
        wanted = dict(code=error) if error else dict(rows=expected)
        outcomes = {label: {key: value for key, value in row[label].items() if key != "exception"}
                    for label in ("oracle", "bicdb")}
        row["source_qualified"] = outcomes["oracle"] == wanted
        row["matched"] = row["source_qualified"] and outcomes["bicdb"] == wanted
        report["cases"].append(row)

    try:
        for key, element in (("BYTES", "VARCHAR2(8 BYTE)"), ("CHARS", "VARCHAR2(3 CHAR)"), ("NUMS", "NUMBER")):
            record("declare_" + key.lower(), f"CREATE TYPE {names[key]} AS TABLE OF {element}", [], create_key=key)
        # The PostgreSQL transport exposes each registered logical type's OID.
        # Install standard array decoders explicitly for these domain carriers;
        # do not infer or rewrite an unregistered application result type.
        from psycopg.types.array import ArrayLoader
        for key, base_oid in (("BYTES", 1043), ("CHARS", 1043), ("NUMS", 1700)):
            if key not in created[1]:
                continue
            with target.cursor() as cur:
                cur.execute(f"SELECT CAST(NULL AS {names[key]}) FROM DUAL")
                oid = cur.description[0].type_code
                assert cur.fetchone()[0] is None
            loader = type("CollectionArrayLoader", (ArrayLoader,), dict(base_oid=base_oid, delimiter=b","))
            target.adapters.register_loader(oid, loader)
        plans = [
            ("empty", f"{names['BYTES']}()", [[]]),
            ("null_element", f"{names['BYTES']}(NULL)", [[None]]),
            ("empty_string_element", f"{names['BYTES']}('')", [[None]]),
            ("unicode", f"{names['BYTES']}('é','😀')", [["é", "😀"]]),
            ("quotes_and_delimiters", f"{names['BYTES']}('a,b','a\"b','a''b')", [["a,b", 'a"b', "a'b"]]),
            ("character_bound", f"{names['CHARS']}('😀😀😀')", [["😀😀😀"]]),
            ("whole_null", f"CAST(NULL AS {names['BYTES']})", [None]),
            ("numeric_elements", f"{names['NUMS']}(0,-3,42,NULL)", [[canonical(0), canonical(-3), canonical(42), None]]),
        ]
        for name, expression, expected in plans:
            record(name, f"SELECT {expression} FROM DUAL", [expected])
        record("byte_overflow", f"SELECT {names['BYTES']}('😀😀😀') FROM DUAL", error="ORA-22814")
    finally:
        for index in (1, 0):
            for key in reversed(created[index]):
                try:
                    with (reference, target)[index].cursor() as cur:
                        cur.execute("DROP TYPE " + names[key])
                except Exception as exc:
                    report["cleanup_errors"].append(dict(backend=index, error=type(exc).__name__))
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
            report = run(source, target, schema, "BICDB_NT_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
