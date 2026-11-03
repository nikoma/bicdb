"""Oracle dense nested-table PL/SQL contracts, independent of application names."""
import json
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    table = identifier(schema) + "." + identifier(prefix + "_TEXT")
    plans = [
        ("uninitialized_count", None, "result:=v.COUNT;", -6531),
        ("empty_count", "", "result:=v.COUNT;", 0),
        ("null_element_count", "NULL", "result:=v.COUNT;", 1),
        ("extend_default", "", "v.EXTEND;result:=v.COUNT;", 1),
        ("extend_zero", "", "v.EXTEND(0);result:=v.COUNT;", 0),
        ("extend_negative", "", "v.EXTEND(-1);result:=v.COUNT;", -6502),
        ("extend_fraction", "", "v.EXTEND(1.5);result:=v.COUNT;", 2),
        ("extend_null", "", "v.EXTEND(NULL);result:=v.COUNT;", 0),
        ("assign_before_extend", "", "v(1):='a';result:=v.COUNT;", -6533),
        ("assign_zero", "", "v.EXTEND;v(0):='a';result:=v.COUNT;", -6532),
        ("assign_byte_overflow", "", "v.EXTEND;v(1):='😀😀😀';result:=v.COUNT;", -6502),
        ("extend_copy", "'a'", "v.EXTEND(2,1);result:=v.COUNT;", 3),
        ("extend_copy_empty", "", "v.EXTEND(1,1);result:=v.COUNT;", -6533),
        ("extend_copy_zero_invalid", "", "v.EXTEND(0,99);result:=v.COUNT;", 0),
        ("extend_copy_negative", "", "v.EXTEND(-1,1);result:=v.COUNT;", -6502),
        ("assign_null_index", "", "v.EXTEND;v(NULL):='a';result:=v.COUNT;", -6502),
        ("assign_fraction_index", "", "v.EXTEND(2);v(1.5):='a';IF v(2)='a' THEN result:=2;ELSE result:=-1;END IF;", 2),
        ("read_before_extend", "", "IF v(1) IS NULL THEN result:=1;ELSE result:=-1;END IF;", -6533),
        ("count_parentheses", "", "result:=v.COUNT();", 0),
        ("extended_element_is_null", "", "v.EXTEND;IF v(1) IS NULL THEN result:=1;ELSE result:=-1;END IF;", 1),
        ("copied_element_values", "'a'", "v.EXTEND(2,1);IF v(2)='a' AND v(3)='a' THEN result:=3;ELSE result:=-1;END IF;", 3),
        ("nested_type_shadow", "'a'", "DECLARE v NUMBER:=7;BEGIN IF v<>7 THEN RETURN -1;END IF;END;result:=v.COUNT;", 1),
        ("query_alias_count_keeps_sql_scope", "'a'", "SELECT v.COUNT INTO result FROM (SELECT 7 AS COUNT FROM DUAL) v;result:=result+v.COUNT;", 8),
    ]
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, target_protocol="prepared", success=False,
                  scope="Dense named nested-table PL/SQL methods, element access and Oracle errors; no sparse collection qualification")
    created_types = []
    created_functions = []

    def query(connection, sql):
        with connection.cursor() as cur:
            if connection is target:
                cur.execute(sql, prepare=True)
            else:
                cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    def failure(exc):
        detail = exc.args[0] if exc.args else None
        return dict(code=getattr(exc, "sqlstate", None) or getattr(detail, "full_code", None), exception=type(exc).__name__)

    try:
        for backend, connection in enumerate((reference, target)):
            query(connection, f"CREATE TYPE {table} AS TABLE OF VARCHAR2(8 BYTE)")
            created_types.append(backend)
        for index, (name, initial, body, expected) in enumerate(plans):
            function = identifier(schema) + "." + identifier(prefix + f"_F{index:02d}")
            declaration = "v " + table
            if initial is not None:
                declaration += ":=" + table + "(" + initial + ")"
            ddl = f"CREATE OR REPLACE FUNCTION {function} RETURN NUMBER AS {declaration};result NUMBER;BEGIN {body}RETURN result;EXCEPTION WHEN OTHERS THEN RETURN SQLCODE;END;"
            row = dict(id=name)
            for backend, (label, connection) in enumerate((("oracle", reference), ("bicdb", target))):
                try:
                    query(connection, ddl)
                    created_functions.append((backend, function))
                    row[label] = dict(rows=query(connection, f"SELECT {function}() FROM DUAL"))
                except Exception as exc:
                    row[label] = failure(exc)
            wanted = dict(rows=[[canonical(expected)]])
            row["source_qualified"] = row["oracle"] == wanted
            row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
            report["cases"].append(row)
    finally:
        for backend, function in reversed(created_functions):
            try:
                query((reference, target)[backend], "DROP FUNCTION " + function)
            except Exception as exc:
                report["cleanup_errors"].append(dict(backend=backend, error=type(exc).__name__))
        for backend in reversed(created_types):
            try:
                query((reference, target)[backend], "DROP TYPE " + table)
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
            report = run(source, target, schema, "BICDB_NM_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
