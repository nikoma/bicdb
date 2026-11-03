"""Compare EXECUTE IMMEDIATE with Oracle on generated, removable fixtures."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    q = lambda suffix: identifier(schema) + "." + identifier(prefix + "_" + suffix)
    table = q("ROWS")
    created = []
    report = dict(format=1, oracle_version=reference.version, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)

    def execute(connection, sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()]) if cursor.description else dict(status="ok")

    def compare(case, sql, oracle_code=None, pg_code=None):
        row = dict(id=case)
        for label, connection in [("oracle", reference), ("bicdb", target)]:
            try:
                row[label] = execute(connection, sql)
            except Exception as error:
                if label == "oracle":
                    row[label] = dict(error_code=error.args[0].code)
                elif getattr(error, "sqlstate", None):
                    row[label] = dict(error_code=error.sqlstate)
                else:
                    raise
        row["matched"] = (row["oracle"] == dict(error_code=oracle_code)
                          and row["bicdb"] == dict(error_code=pg_code)) if oracle_code is not None else (
                              row["oracle"] == row["bicdb"] and "error_code" not in row["oracle"])
        report["cases"].append(row)

    numeric = [
        ("same_name_twice", "EXECUTE IMMEDIATE 'SELECT :x+:x FROM DUAL' INTO v USING 2,3;", None, None),
        ("same_number_twice", "EXECUTE IMMEDIATE 'SELECT :1+:1 FROM DUAL' INTO v USING 2,3;", None, None),
        ("missing_bind", "EXECUTE IMMEDIATE 'SELECT :x+:x FROM DUAL' INTO v USING 2;", 1008, "07001"),
        ("excess_bind", "EXECUTE IMMEDIATE 'SELECT :x FROM DUAL' INTO v USING 2,3;", 1006, "07002"),
        ("null_bind", "EXECUTE IMMEDIATE 'SELECT NVL(:1,9) FROM DUAL' INTO v USING missing;", None, None),
        ("no_into_division", "EXECUTE IMMEDIATE 'SELECT 1/0 FROM DUAL';", None, None),
        ("into_division", "EXECUTE IMMEDIATE 'SELECT 1/0 FROM DUAL' INTO v;", 1476, "22012"),
        ("no_into_invalid", "EXECUTE IMMEDIATE 'SELECT definitely_missing FROM DUAL';", 904, "42703"),
        # At a SQL function boundary Oracle maps unhandled NO_DATA_FOUND to NULL.
        ("zero_rows_sql_boundary", f"EXECUTE IMMEDIATE 'SELECT n FROM {table} WHERE n=99' INTO v;", None, None),
        ("multiple_rows", f"EXECUTE IMMEDIATE 'SELECT n FROM {table}' INTO v;", 1422, "P0003"),
        ("column_count", "EXECUTE IMMEDIATE 'SELECT 1,2 FROM DUAL' INTO v;", 932, "42804"),
        ("null_command", "EXECUTE IMMEDIATE missing_text;", 6535, "22004"),
        ("local_names_not_bound", "EXECUTE IMMEDIATE 'SELECT v FROM DUAL' INTO v;", 904, "42703"),
        ("two_statements", "EXECUTE IMMEDIATE 'SELECT 1 FROM DUAL;SELECT 2 FROM DUAL' INTO v;", 3405, "42601"),
        ("trailing_semicolon", "EXECUTE IMMEDIATE 'SELECT 1 FROM DUAL;' INTO v;", None, None),
        ("using_in", "EXECUTE IMMEDIATE 'SELECT :x FROM DUAL' INTO v USING IN 4;", None, None),
        ("unreached_branch", "IF 1=1 THEN v:=7;ELSE EXECUTE IMMEDIATE 'SELECT :1 FROM DUAL' INTO v USING 9;END IF;", None, None),
        ("table_column_not_local", f"EXECUTE IMMEDIATE 'SELECT n FROM {table} WHERE n=2' INTO v;", None, None),
        ("character_host_in_static_sql", "missing_text:='abc';SELECT COUNT(*) INTO v FROM DUAL WHERE lower('abc') LIKE lower(SUBSTR(missing_text,1,2))||'%';", None, None),
    ]
    try:
        for index, connection in enumerate((reference, target)):
            execute(connection, f"CREATE TABLE {table}(n NUMBER PRIMARY KEY)")
            created.append((index, "TABLE", table))
            for value in (1, 2):
                execute(connection, f"INSERT INTO {table} VALUES({value})")
        for index, (case, body, oracle_code, pg_code) in enumerate(numeric):
            name = q("F" + str(index))
            ddl = f"CREATE FUNCTION {name} RETURN NUMBER IS v NUMBER:=42;n NUMBER:=77;missing NUMBER;missing_text VARCHAR2(100);BEGIN {body} RETURN v;END;"
            for backend, connection in enumerate((reference, target)):
                execute(connection, ddl)
                created.append((backend, "FUNCTION", name))
            compare(case, f"SELECT {name}() FROM DUAL", oracle_code, pg_code)
        name = q("TEXT")
        ddl = f"CREATE FUNCTION {name}(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN EXECUTE IMMEDIATE 'SELECT :v FROM DUAL' INTO v USING p;RETURN v;END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "FUNCTION", name))
        for label, value in [("quotes", "O'Reilly"), ("quote_pairs", "O''Reilly"), ("sql_like_text", "x');DELETE FROM anything;--"), ("unicode", "ñΩ"), ("bind_like_text", ":1 + :1")]:
            literal = "'" + value.replace("'", "''") + "'"
            compare("bound_" + label, f"SELECT {name}({literal}) FROM DUAL")
            compare("literal_" + label, f"SELECT {literal} FROM DUAL")
        name = q("NESTED")
        echo = q("COMMAND_TEXT")
        ddl = f"CREATE FUNCTION {echo} RETURN VARCHAR2 IS BEGIN RETURN 'SELECT ''O''''Reilly'' FROM DUAL';END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "FUNCTION", echo))
        compare("command_text_layers", f"SELECT {echo}() FROM DUAL")
        ddl = f"CREATE FUNCTION {name} RETURN VARCHAR2 IS v VARCHAR2(100);BEGIN EXECUTE IMMEDIATE 'SELECT ''O''''Reilly'' FROM DUAL' INTO v;RETURN v;END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "FUNCTION", name))
        compare("nested_quote_layers", f"SELECT {name}() FROM DUAL")
        caught = q("PL_CALLER")
        ddl = f"CREATE FUNCTION {caught} RETURN NUMBER IS v NUMBER;BEGIN v:={q('F8')}();RETURN 42;EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 99;END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "FUNCTION", caught))
        compare("no_data_propagates_to_pl_caller", f"SELECT {caught}() FROM DUAL")
        chained = q("CHAINED")
        ddl = f"CREATE FUNCTION {chained}(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(200);cmd VARCHAR2(100);BEGIN cmd:='SELECT :1'||' FROM DUAL';EXECUTE IMMEDIATE cmd INTO v USING 'start:'||p||':end';RETURN v;END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "FUNCTION", chained))
        compare("chained_bind_expression", f"SELECT {chained}('O''''Reilly') FROM DUAL")
        procedure = q("WRITE")
        ddl = f"CREATE PROCEDURE {procedure}(p NUMBER) IS BEGIN EXECUTE IMMEDIATE 'INSERT INTO {table} VALUES(:1)' USING p;END;"
        for backend, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((backend, "PROCEDURE", procedure))
        for connection in (reference, target):
            connection.autocommit = False
        compare("dynamic_insert", f"CALL {procedure}(3)")
        compare("caller_sees_insert", f"SELECT n FROM {table} ORDER BY n")
        for connection in (reference, target):
            connection.rollback()
            connection.autocommit = True
        compare("parent_rollback", f"SELECT n FROM {table} ORDER BY n")
    finally:
        for connection in (reference, target):
            connection.rollback()
            connection.autocommit = True
        for backend, kind, name in reversed(created):
            try:
                execute((reference, target)[backend], f"DROP {kind} {name}")
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=backend, kind=kind, error=type(error).__name__))
    report["matched"] = sum(case["matched"] for case in report["cases"])
    report["total"] = len(report["cases"])
    report["success"] = report["matched"] == report["total"] and not report["cleanup_errors"]
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import uuid
    import oracledb
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        raise ValueError("a dedicated non-administrative schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(user=config["user"], password=password.read_text().strip(),
                          host=config["host"], port=config["port"],
                          service_name=config["service_name"]) as reference:
        reference.autocommit = True
        reference.call_timeout = 10000
        with reference.cursor() as cursor:
            cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
            schema = cursor.fetchone()[0]
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True,
                            options="-c bicdb.sql_profile=oracle-preview-v1",
                            connect_timeout=10) as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("server did not confirm the requested SQL profile")
            report = run(reference, target, schema, "BICDB_DY_" + uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
