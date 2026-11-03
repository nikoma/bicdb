"""Compare SYS dictionary scalar type anchors without querying target dictionary rows."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    report = dict(format=1, oracle_version=reference.version,
                  scope="SYS.USER_TABLES/USER_TAB_COLUMNS scalar type anchors; separate from dictionary contents, visibility and application execution",
                  application_qualified=False, cases=[], cleanup_errors=[], success=False)
    q = lambda name: identifier(schema) + "." + identifier(name)

    def query(c, sql):
        with c.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    with reference.cursor() as cur:
        cur.execute("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'")
        report["character_set"] = cur.fetchone()[0]
        cur.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_LENGTH_SEMANTICS','NLS_NUMERIC_CHARACTERS','NLS_CALENDAR')")
        report["nls"] = dict(cur.fetchall())
        cur.execute("SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE FROM SYS.ALL_TAB_COLUMNS WHERE OWNER='SYS' AND TABLE_NAME IN ('USER_TABLES','USER_TAB_COLUMNS') ORDER BY TABLE_NAME,COLUMN_ID")
        descriptors = cur.fetchall()
    if (report["character_set"] != "AL32UTF8"
            or report["nls"] != dict(NLS_LENGTH_SEMANTICS="BYTE", NLS_NUMERIC_CHARACTERS=".,", NLS_CALENDAR="GREGORIAN")
            or not descriptors):
        raise ValueError("AL32UTF8, BYTE defaults, decimal-point numeric characters and Gregorian calendar are required")
    for index, (view, column, kind, width, precision, scale) in enumerate(descriptors):
        if kind not in ("VARCHAR2", "NUMBER", "DATE", "RAW", "LONG") or (kind == "NUMBER" and (precision is not None or scale is not None)):
            raise ValueError("unqualified source dictionary descriptor")
        fn = prefix + "_" + str(index)
        anchor = "SYS." + identifier(view) + "." + identifier(column) + "%TYPE"
        assignment = "DATE '2024-02-29'" if kind == "DATE" else "HEXTORAW('00FF')" if kind == "RAW" else "p"
        result = "TO_CHAR(v,'YYYY-MM-DD')" if kind == "DATE" else "RAWTOHEX(v)" if kind == "RAW" else "v"
        definition = f"CREATE FUNCTION {q(fn)}(p VARCHAR2) RETURN VARCHAR2 IS v {anchor};BEGIN v:={assignment};RETURN {result};END;"
        argument = "'12.345'" if kind == "NUMBER" else "'x'"
        row = dict(id=view + "." + column, source_type=kind, source_byte_width=int(width))
        created = []
        try:
            for backend, connection in [("oracle", reference), ("bicdb", target)]:
                try:
                    query(connection, definition); created.append((backend, connection))
                    row[backend] = dict(rows=query(connection, f"SELECT {q(fn)}({argument}) FROM DUAL"))
                except Exception as error:
                    detail = error.args[0] if error.args else None
                    row[backend] = dict(code=getattr(error, "sqlstate", None) or getattr(detail, "full_code", None))
            row["source_qualified"] = "rows" in row["oracle"] and len(row["oracle"]["rows"]) == 1
            row["matched"] = row["source_qualified"] and row["oracle"] == row["bicdb"]
            report["cases"].append(row)
        finally:
            for backend, connection in reversed(created):
                try:
                    query(connection, "DROP FUNCTION " + q(fn))
                except Exception as error:
                    report["cleanup_errors"].append(dict(backend=backend, error=type(error).__name__))
    report["matched"] = sum(c["matched"] for c in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(c["source_qualified"] for c in report["cases"])
    report["supported_scalar_matched"] = sum(c["matched"] for c in report["cases"] if c["source_type"] in ("VARCHAR2", "NUMBER", "DATE"))
    report["supported_scalar_total"] = sum(c["source_type"] in ("VARCHAR2", "NUMBER", "DATE") for c in report["cases"])
    report["success"] = report["matched"] == report["total"] and not report["cleanup_errors"]
    return report


def main():
    import argparse, json, os, uuid
    from pathlib import Path
    import oracledb, psycopg
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--oracle-config", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    config = json.loads(a.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        raise ValueError("a dedicated non-administrative schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = a.oracle_config.parent / password
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(user=config["user"], password=password.read_text().strip(), host=config["host"], port=config["port"], service_name=config["service_name"]) as source:
        source.autocommit = True; source.call_timeout = 10000
        with source.cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
            schema = cur.fetchone()[0]
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10,
                             options="-c bicdb.sql_profile=oracle-preview-v1") as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("Oracle-preview target required")
            report = run(source, target, schema, "BICDB_DA_" + uuid.uuid4().hex[:8].upper())
    a.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
