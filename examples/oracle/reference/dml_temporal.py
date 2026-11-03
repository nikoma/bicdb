"""Source comparisons for logical DATE provenance in DML predicates.

Only owned synthetic tables are modified. Complete Oracle DML, correlated
subqueries and full datetime coercion are outside this bounded contract.
"""
import json
from routine_namespaces import identifier

PLANS = [
    ("delete_timestamp_days", "DELETE FROM {T} WHERE precise+7<TIMESTAMP '2020-01-09 00:00:00'", [2]),
    ("delete_timestamp_fraction", "DELETE FROM {T} WHERE precise+0.5=DATE '2020-01-02'", [2]),
    ("delete_days", "DELETE FROM {T} WHERE created+7<DATE '2020-01-09'", [2]),
    ("delete_alias", "DELETE FROM {T} d WHERE d.created+7<DATE '2020-01-09' AND d.id=1", [2]),
    ("delete_fractional_days", "DELETE FROM {T} WHERE created+0.5<DATE '2020-01-02'", [2]),
    ("delete_inverse_days", "DELETE FROM {T} WHERE 7+created<DATE '2020-01-09'", [2]),
    ("delete_date_difference", "DELETE FROM {T} WHERE created-DATE '2020-01-01'<7", [2]),
    ("delete_null_offset", "DELETE FROM {T} WHERE created+NULL<DATE '2020-01-09'", [1, 2]),
    ("delete_typed_builtins", "DELETE FROM {T} WHERE TO_CHAR(created+7,'YYYY-MM-DD')='2020-01-08' AND DECODE(created,DATE '2020-01-01','Y','N')='Y'", [2]),
    ("update_predicate", "UPDATE {T} SET id=3 WHERE created+7<DATE '2020-01-09'", [2, 3]),
    ("delete_nvl_date", "DELETE FROM {T} WHERE NVL(created,DATE '1990-01-01')+7<DATE '2020-01-09'", [2]),
]


def run(reference, target, schema, prefix):
    name = identifier(schema) + "." + identifier(prefix + "_ROWS")
    report = dict(format=1, oracle_version=reference.version, application_qualified=False,
                  source_qualified=False, target_protocol="prepared", cases=[], cleanup_errors=[], success=False)
    created = []

    def execute(connection, sql, rows=False):
        with connection.cursor() as cur:
            if connection is target:
                cur.execute(sql, prepare=True)
            else:
                cur.execute(sql)
            return [int(row[0]) for row in cur.fetchall()] if rows else None

    try:
        for connection in (reference, target):
            execute(connection, "CREATE TABLE " + name + " (id NUMBER PRIMARY KEY,created DATE,precise TIMESTAMP(6))")
            created.append(connection)
        for case, statement, expected in PLANS:
            row = dict(id=case, sql=statement.format(T=name), expected=expected)
            for label, connection in (("oracle", reference), ("bicdb", target)):
                try:
                    execute(connection, "DELETE FROM " + name)
                    execute(connection, "INSERT INTO " + name + " VALUES(1,DATE '2020-01-01',TIMESTAMP '2020-01-01 12:00:00.123456')")
                    execute(connection, "INSERT INTO " + name + " VALUES(2,DATE '2020-01-10',TIMESTAMP '2020-01-10 12:00:00.654321')")
                    execute(connection, row["sql"])
                    row[label] = dict(ids=execute(connection, "SELECT id FROM " + name + " ORDER BY id", True))
                except Exception as exc:
                    row[label] = dict(sqlstate=getattr(exc, "sqlstate", None), exception=type(exc).__name__)
            wanted = dict(ids=expected)
            row["source_qualified"] = row["oracle"] == wanted
            row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
            report["cases"].append(row)
    finally:
        for connection in reversed(created):
            try:
                execute(connection, "DROP TABLE " + name)
            except Exception as exc:
                report["cleanup_errors"].append(dict(backend="bicdb" if connection is target else "oracle", exception=type(exc).__name__))
    report["source_qualified"] = len(report["cases"]) == len(PLANS) and all(case["source_qualified"] for case in report["cases"])
    report["matched"] = sum(case["matched"] for case in report["cases"])
    report["total"] = len(report["cases"])
    report["success"] = report["source_qualified"] and report["matched"] == report["total"] and not report["cleanup_errors"]
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
            report = run(source, target, schema, "BICDB_DML_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
