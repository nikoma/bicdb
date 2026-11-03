"""Executable Oracle dictionary contracts; reports unsupported target behavior."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    table = prefix + "_T"
    view = prefix + "_V"
    q = lambda name: identifier(schema) + "." + identifier(name)
    report = dict(format=1, oracle_version=reference.version,
                  scope="Dictionary table/view rows, scalar descriptors, byte/character lengths and native descriptor contrast; no application qualification",
                  application_qualified=False, cases=[], setup_errors=[], cleanup_errors=[], success=False)
    created = []
    def query(connection, sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return [[canonical(v) for v in row] for row in cursor.fetchall()] if cursor.description else []
    try:
        with reference.cursor() as cursor:
            cursor.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER='NLS_LENGTH_SEMANTICS'")
            report["nls"] = dict(cursor.fetchall())
            cursor.execute("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'")
            report["character_set"] = cursor.fetchone()[0]
        if report["nls"] != dict(NLS_LENGTH_SEMANTICS="BYTE") or report["character_set"] != "AL32UTF8":
            raise ValueError("dictionary byte-length contract requires BYTE defaults and AL32UTF8")
        definitions = [("TABLE", table, f'CREATE TABLE {q(table)}("N" NUMBER(5,2) DEFAULT 1 NOT NULL,"B" VARCHAR2(3),"C" VARCHAR2(3 CHAR),"F" CHAR(2),"D" DATE,"T" TIMESTAMP(3))'),
                       ("VIEW", view, f'CREATE VIEW {q(view)} AS SELECT "N","B","C","D" FROM {q(table)}')]
        for backend, connection in enumerate((reference,target)):
            for kind, name, sql in definitions:
                try:
                    query(connection, sql)
                    created.append((backend,kind,q(name)))
                except Exception as error:
                    if backend == 0:
                        raise
                    report["setup_errors"].append(dict(kind=kind,code=getattr(error,"sqlstate",None)))
        plans = [
            ("owned_table", f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME='{table}'", 1),
            ("view_excluded_from_tables", f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME='{view}'", 0),
            ("table_columns", f"SELECT COLUMN_NAME,DATA_TYPE,NULLABLE,COLUMN_ID FROM USER_TAB_COLUMNS WHERE TABLE_NAME='{table}' ORDER BY COLUMN_ID", 6),
            ("view_columns", f"SELECT COLUMN_NAME,DATA_TYPE,COLUMN_ID FROM USER_TAB_COLUMNS WHERE TABLE_NAME='{view}' ORDER BY COLUMN_ID", 4),
            ("numeric_precision", f"SELECT DATA_TYPE,DATA_PRECISION,DATA_SCALE,DATA_LENGTH,NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME='{table}' AND COLUMN_NAME='N'", 1),
            ("defaults", f"SELECT COLUMN_NAME,DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME='{table}' AND COLUMN_NAME='N'", 1),
        ]
        for column in ["B","C","F","D","T"]:
            plans.append(("descriptor_"+column, f"SELECT DATA_TYPE,DATA_LENGTH,CHAR_LENGTH,CHAR_USED,DATA_PRECISION,DATA_SCALE FROM USER_TAB_COLUMNS WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{column}'", 1))
        for name, sql, count in plans:
            row = dict(id=name,sql=sql)
            for label, connection in [("oracle",reference),("bicdb",target)]:
                try:
                    row[label] = dict(rows=query(connection,sql))
                except Exception as error:
                    detail=error.args[0] if error.args else None
                    row[label]=dict(code=getattr(error,"sqlstate",None) or getattr(detail,"full_code",None))
            row["source_qualified"] = "rows" in row["oracle"] and len(row["oracle"]["rows"]) == count
            row["matched"] = row["source_qualified"] and row["oracle"] == row["bicdb"]
            report["cases"].append(row)
        report["character_assignment_cases"] = []
        for column, expected_error in [("B", "ORA-12899"), ("C", None)]:
            row = dict(id="supplementary_assignment_"+column)
            for label, connection in [("oracle", reference), ("bicdb", target)]:
                try: row[label] = dict(rows=query(connection, f"INSERT INTO {q(table)}(N,{column}) VALUES(10,'😀')"))
                except Exception as error:
                    detail=error.args[0] if error.args else None
                    row[label]=dict(code=getattr(error,"sqlstate",None) or getattr(detail,"full_code",None))
            row["source_qualified"] = row["oracle"] == (dict(code=expected_error) if expected_error else dict(rows=[]))
            row["matched"] = (row["oracle"] == dict(code=expected_error) and row["bicdb"] == dict(code="22001")) if expected_error else row["oracle"] == row["bicdb"] == dict(rows=[])
            report["character_assignment_cases"].append(row)
        # Information_schema is an independent target observation. It must not
        # be relabelled as an Oracle dictionary match.
        report["target_native_descriptors"] = query(target, f"""SELECT "column_name","data_type","character_maximum_length","character_octet_length" FROM "information_schema"."columns" WHERE "table_name"='{table}' ORDER BY "ordinal_position"
        """)
        report["target_pg_attribute_descriptors"] = query(target, f"""
            SELECT "a"."attname","a"."atttypid","a"."atttypmod"
            FROM "pg_catalog"."pg_attribute" "a"
            JOIN "pg_catalog"."pg_class" "c" ON "a"."attrelid"="c"."oid"
            WHERE "c"."relname"='{table}' AND "a"."attnum">0
            ORDER BY "a"."attnum"
        """)
    finally:
        for backend,kind,name in reversed(created):
            try: query((reference,target)[backend],f"DROP {kind} {name}")
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=backend,kind=kind,error=type(error).__name__))
    report["total"] = len(report["cases"])
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["character_assignment_matched"] = sum(row["matched"] for row in report["character_assignment_cases"])
    report["character_assignment_total"] = len(report["character_assignment_cases"])
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"] + report["character_assignment_cases"])
    report["success"] = (report["matched"] == report["total"]
                         and report["character_assignment_matched"] == report["character_assignment_total"]
                         and not report["setup_errors"] and not report["cleanup_errors"])
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
            report = run(reference, target, schema, "BICDB_DC_" + uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
