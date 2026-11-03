"""Compare bounded AL16UTF16 routine declarations against independent Oracle."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    q = lambda suffix: identifier(schema) + "." + identifier(prefix + "_" + suffix)
    report = dict(format=1, oracle_version=reference.version, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)
    created = []

    def execute(connection, sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()]) if cursor.description else dict(status="ok")

    def compare(case, sql, overflow=False, rejected_alias=False):
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
        if rejected_alias:
            # Oracle retains an invalid compiled function; this preview rejects
            # its unsupported declaration on execution rather than accepting a
            # non-Oracle alias. Full invalid-object catalog parity is separate.
            row["matched"] = row["oracle"] == dict(error_code=6575) and row["bicdb"] == dict(error_code="0A000")
        else:
            row["matched"] = (row["oracle"] == dict(error_code=6502) and row["bicdb"] == dict(error_code="22001")) if overflow else (
                row["oracle"] == row["bicdb"] and "error_code" not in row["oracle"])
        report["cases"].append(row)

    def function(suffix, signature, declaration, body):
        name = q(suffix)
        ddl = f"CREATE FUNCTION {name}{signature} IS {declaration} BEGIN {body} END;"
        for index, connection in enumerate((reference, target)):
            execute(connection, ddl)
            created.append((index, "FUNCTION", name))
        return name

    try:
        with reference.cursor() as cursor:
            cursor.execute("SELECT value FROM nls_database_parameters WHERE parameter='NLS_NCHAR_CHARACTERSET'")
            report["national_character_set"] = cursor.fetchone()[0]
        if report["national_character_set"] != "AL16UTF16":
            raise ValueError("this contract requires an AL16UTF16 reference")
        for suffix, declaration, capacity in [
            ("V1", "NVARCHAR2(1)", 1), ("V2", "NVARCHAR2(2)", 2),
            ("C1", "NCHAR(1)", 1), ("C2", "NCHAR(2)", 2),
            ("QV", '"NVARCHAR2"(2)', 2), ("QC", '"NCHAR"(2)', 2),
            ("CD", "NCHAR", 1),
        ]:
            name = function(suffix, "(p VARCHAR2) RETURN VARCHAR2", f"v {declaration};", "v:=p;RETURN v;")
            for label, text in [("ascii", "A"), ("bmp", "é"), ("supplementary", "😀"),
                                ("combining", "Á"), ("trailing_space", "A "), ("empty", ""), ("null", None)]:
                literal = "NULL" if text is None else "'" + text.replace("'", "''") + "'"
                units = len(text.encode("utf-16-le")) // 2 if text else 0
                compare(suffix.lower() + "_" + label, f"SELECT {name}({literal}) FROM DUAL", units > capacity)
        name = function("INVALID_ALIAS", " RETURN VARCHAR2", "v NVARCHAR(2);", "v:='A';RETURN v;")
        compare("unsupported_nvarchar_alias", f"SELECT {name}() FROM DUAL", rejected_alias=True)
        for suffix, declaration, size, overflow in [
            ("MAXV", "NVARCHAR2(32767)", 16383, False),
            ("OVERV", "NVARCHAR2(32767)", 16384, True),
            ("MAXC", "NCHAR(16383)", 1, False),
        ]:
            name = function(suffix, " RETURN NUMBER", f"v {declaration};",
                            f"v:=RPAD('A',{size},'A');RETURN LENGTH(v);")
            compare(suffix.lower(), f"SELECT {name}() FROM DUAL", overflow)
        for suffix, declaration in [("VL", "NVARCHAR2(3)"), ("CL", "NCHAR(3)")]:
            name = function(suffix, "(p VARCHAR2) RETURN VARCHAR2", f"v {declaration};",
                            "v:=p;RETURN '['||v||']/ '||LENGTH(v)||'/'||LENGTHC(v)||'/'||LENGTHB(v)||'/'||LENGTH2(v)||'/'||LENGTH4(v);")
            for label, text in [("supplementary", "😀"), ("bmp", "é"), ("combining", "Á"), ("null", None)]:
                literal = "NULL" if text is None else "'" + text + "'"
                compare(suffix.lower() + "_" + label, f"SELECT {name}({literal}) FROM DUAL")
        for index, text in enumerate(["́", "́̀", "Á̀", "👩‍👩‍👧‍👦", "🇺🇸", "👍🏽", "✈️", "가", "가", "क्", "क्ष", "\r\n", "ᅡ", "ᆨ", "का", "Aः", "͏", "‍", "؀A", "a⃝", "각"]):
            name = function("U" + str(index), " RETURN VARCHAR2", "v NVARCHAR2(60);",
                            "v:='" + text + "';RETURN LENGTH(v)||'/'||LENGTHC(v)||'/'||LENGTH4(v);")
            compare("unicode_complete_" + str(index), f"SELECT {name}() FROM DUAL")
        name = function("SHADOW", " RETURN NUMBER", "v NVARCHAR2(2):='😀';n NUMBER;",
                        "DECLARE v VARCHAR2(4):='😀';BEGIN n:=LENGTH(v);END;RETURN n*10+LENGTH(v);")
        compare("nested_native_character_shadow", f"SELECT {name}() FROM DUAL")
        name = function("SQLLEN", " RETURN NUMBER", "v NVARCHAR2(2):='😀';n NUMBER;",
                        "SELECT LENGTH(v) INTO n FROM DUAL;RETURN n;")
        compare("national_host_in_static_sql", f"SELECT {name}() FROM DUAL")
        name = function("DYNAMIC", " RETURN VARCHAR2", "v NVARCHAR2(60):='O''Reilly';result VARCHAR2(100);",
                        "EXECUTE IMMEDIATE 'SELECT :1 FROM DUAL' INTO result USING v;RETURN result;")
        compare("national_host_dynamic_text_bind", f"SELECT {name}() FROM DUAL")
        name = function("DYN_LEN", " RETURN NUMBER", "v NVARCHAR2(4):='😀';result NUMBER;",
                        "EXECUTE IMMEDIATE 'SELECT LENGTH(:1) FROM DUAL' INTO result USING v;RETURN result;")
        compare("national_host_dynamic_length", f"SELECT {name}() FROM DUAL")
        for suffix, declaration, initial, condition in [
            ("FIXEQ", "NCHAR(2)", "A", "v='A'"),
            ("VAREQ", "NVARCHAR2(2)", "A ", "v='A'"),
            ("MIXEQ", "NCHAR(2)", "A", "v=w"),
            ("FIXNE", "NCHAR(2)", "A", "v!='A'"),
        ]:
            name = function(suffix, " RETURN NUMBER", f"v {declaration}:='{initial}';w NVARCHAR2(1):='A';",
                            f"IF {condition} THEN RETURN 1;ELSE RETURN 0;END IF;")
            compare(suffix.lower(), f"SELECT {name}() FROM DUAL")
        for suffix, value in [("NVL_LEN", "NVL(v,'')"), ("DECODE_LEN", "DECODE(1,1,v,'')")]:
            name = function(suffix, " RETURN NUMBER", "v NVARCHAR2(4):='😀';result NUMBER;",
                            f"SELECT LENGTH({value}) INTO result FROM DUAL;RETURN result;")
            compare(suffix.lower(), f"SELECT {name}() FROM DUAL")
        name = function("DYN_CONCAT", " RETURN NUMBER", "v NVARCHAR2(4):='😀';result NUMBER;",
                        "EXECUTE IMMEDIATE 'SELECT LENGTH(:1) FROM DUAL' INTO result USING v||'x';RETURN result;")
        compare("national_expression_dynamic_length", f"SELECT {name}() FROM DUAL")
        name = function("RET", "(p NVARCHAR2) RETURN NVARCHAR2", "v NVARCHAR2(4);", "v:=p;RETURN v;")
        compare("national_argument_and_return", f"SELECT {name}('😀') FROM DUAL")
        compare("national_return_length", f"SELECT LENGTH({name}('😀')) FROM DUAL")
        # PostgreSQL describes the physical VARCHAR codec while the versioned
        # routine retains its logical national-character assignment semantics.
        row = dict(id="prepared_national_argument_and_return")
        with reference.cursor() as cursor:
            cursor.execute(f"SELECT {name}(:1) FROM DUAL", ["😀"])
            row["oracle"] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()])
        with target.cursor() as cursor:
            cursor.execute(f"SELECT {name}(%s) FROM DUAL", ["😀"], prepare=True)
            row["bicdb"] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()])
            row["target_type_oid"] = cursor.description[0].type_code
        row["matched"] = row["oracle"] == row["bicdb"] and row["target_type_oid"] == 1043
        report["cases"].append(row)
    finally:
        for index, kind, name in reversed(created):
            try:
                execute((reference, target)[index], f"DROP {kind} {name}")
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=index, kind=kind, error=type(error).__name__))
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
            report = run(reference, target, schema, "BICDB_NC_" + uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
