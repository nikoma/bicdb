"""Oracle deterministic function declarations and execution contracts."""
import json
import re
from decimal import Decimal
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    names = {key: identifier(schema) + "." + identifier(prefix + "_" + key)
             for key in ("NUM", "ZERO", "QUOTED")}
    definitions = [
        ("numeric_declaration", "NUM", "(p NUMBER) RETURN NUMBER DETERMINISTIC AS BEGIN RETURN p+1; END;"),
        ("parameterless_declaration", "ZERO", "RETURN NUMBER DETERMINISTIC AS BEGIN RETURN 7; END;"),
        ("quoted_parameter_declaration", "QUOTED", '("DETERMINISTIC" NUMBER) RETURN NUMBER DETERMINISTIC AS BEGIN RETURN "DETERMINISTIC"+1; END;'),
    ]
    plans = [
        ("numeric_value", f"SELECT {names['NUM']}(4) FROM DUAL", [[5]]),
        ("fractional_value", f"SELECT {names['NUM']}(12.5) FROM DUAL", [[Decimal("13.5")]]),
        ("null_value", f"SELECT {names['NUM']}(NULL) FROM DUAL", [[None]]),
        ("parameterless_value", f"SELECT {names['ZERO']}() FROM DUAL", [[7]]),
        ("quoted_parameter_value", f"SELECT {names['QUOTED']}(7) FROM DUAL", [[8]]),
    ]
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, scope="Deterministic function declarations and values; no optimizer or application qualification", success=False)
    created = [[], []]

    def execute(connection, sql):
        with connection.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    def record(name, sql, expected, create_key=None):
        row = dict(id=name, sql=sql)
        for index, (label, connection) in enumerate((("oracle", reference), ("bicdb", target))):
            try:
                row[label] = dict(rows=execute(connection, sql))
                if create_key is not None and create_key not in created[index]:
                    created[index].append(create_key)
            except Exception as e:
                detail = e.args[0] if e.args else None
                row[label] = dict(code=getattr(e, "sqlstate", None) or getattr(detail, "full_code", None))
        expected = dict(rows=[[canonical(v) for v in values] for values in expected])
        row["source_qualified"] = row["oracle"] == expected
        row["matched"] = row["source_qualified"] and row["oracle"] == row["bicdb"]
        report["cases"].append(row)

    try:
        for name, key, body in definitions:
            record(name, f"CREATE OR REPLACE FUNCTION {names[key]} {body}", [], key)
        for name, sql, expected in plans:
            record(name, sql, expected)
        record("replace_declaration", f"CREATE OR REPLACE FUNCTION {names['ZERO']} RETURN NUMBER DETERMINISTIC AS BEGIN RETURN 9; END;", [], "ZERO")
        record("replacement_value", f"SELECT {names['ZERO']}() FROM DUAL", [[9]])
    finally:
        for index in (1, 0):
            for key in reversed(created[index]):
                try:
                    execute((reference, target)[index], "DROP FUNCTION " + names[key])
                except Exception as e:
                    report["cleanup_errors"].append(dict(backend=index, error=type(e).__name__))
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
            report = run(source, target, schema, "BICDB_FA_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
