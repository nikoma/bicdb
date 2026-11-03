"""Compare PL/SQL local character units, padding, anchors and byte ceilings."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    q = lambda name: identifier(schema) + "." + identifier(name)
    table = prefix + "_T"
    report = dict(format=1, oracle_version=reference.version,
                  scope="AL32UTF8/BYTE-default local CHAR/VARCHAR2 buffers, table-column anchors, strict assignment and 32767-byte capacity; no dictionary or application qualification",
                  application_qualified=False, cases=[], cleanup_errors=[], success=False)
    created = []

    def query(c, sql):
        with c.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    def compare(case, sql, expected_error=None):
        row = dict(id=case)
        for label, connection in [("oracle", reference), ("bicdb", target)]:
            try:
                row[label] = dict(rows=query(connection, sql))
            except Exception as error:
                detail = error.args[0] if error.args else None
                row[label] = dict(code=getattr(error, "sqlstate", None) or getattr(detail, "full_code", None))
        row["source_qualified"] = (row["oracle"] == dict(code="ORA-06502") if expected_error else "rows" in row["oracle"])
        row["matched"] = row["source_qualified"] and (
            row["bicdb"] == dict(code="22001") if expected_error else row["oracle"] == row["bicdb"])
        report["cases"].append(row)

    try:
        with reference.cursor() as cur:
            cur.execute("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'")
            report["character_set"] = cur.fetchone()[0]
            cur.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER='NLS_LENGTH_SEMANTICS'")
            report["length_semantics"] = cur.fetchone()[0]
        if report["character_set"] != "AL32UTF8" or report["length_semantics"] != "BYTE":
            raise ValueError("AL32UTF8 and BYTE defaults are required")
        for backend, connection in enumerate((reference, target)):
            query(connection, f"CREATE TABLE {q(table)}(B VARCHAR2(3 BYTE),C VARCHAR2(3 CHAR))")
            created.append((backend, "TABLE", table))
        declarations = [("VB", "VARCHAR2(3 BYTE)", True), ("VC", "VARCHAR2(3 CHAR)", False),
                        ("CB", "CHAR(3 BYTE)", True), ("CC", "CHAR(3 CHAR)", False),
                        ("VD", "VARCHAR2(3)", True), ("CD", "CHAR", True),
                        ("AB", q(table) + ".B%TYPE", True), ("AC", q(table) + ".C%TYPE", False)]
        for suffix, declaration, byte_limited in declarations:
            fn = prefix + "_" + suffix
            sql = f"CREATE FUNCTION {q(fn)}(p VARCHAR2) RETURN VARCHAR2 IS v {declaration};BEGIN v:=p;RETURN v;END;"
            for backend, connection in enumerate((reference, target)):
                query(connection, sql); created.append((backend, "FUNCTION", fn))
            for name, value in [("ascii", "x"), ("accent", "é"), ("supplementary", "😀"), ("overflow_spaces", "a   "), ("empty", ""), ("null", None)]:
                literal = "NULL" if value is None else "'" + value.replace("'", "''") + "'"
                fails = name == "overflow_spaces" or (name == "supplementary" and byte_limited) or (name == "accent" and suffix == "CD")
                compare(suffix + "_" + name, f"SELECT {q(fn)}({literal}) FROM DUAL", fails)
        for suffix, declaration, value, count, fails in [
            ("MAX_ASCII", "VARCHAR2(32767 BYTE)", "a", 32767, False),
            ("MAX_UTF8", "VARCHAR2(32767 CHAR)", "é", 16383, False),
            ("OVER_UTF8", "VARCHAR2(32767 CHAR)", "é", 16384, True),
            ("PAD_ASCII", "CHAR(32767 CHAR)", "a", 1, False),
            ("PAD_UTF8", "CHAR(32767 CHAR)", "é", 1, False),
        ]:
            fn = prefix + "_" + suffix
            sql = f"CREATE FUNCTION {q(fn)} RETURN NUMBER IS v {declaration};BEGIN v:=RPAD('{value}',{count},'{value}');RETURN LENGTH(v);END;"
            for backend, connection in enumerate((reference, target)):
                query(connection, sql); created.append((backend, "FUNCTION", fn))
            compare(suffix, f"SELECT {q(fn)}() FROM DUAL", fails)
    finally:
        for backend, kind, name in reversed(created):
            try:
                query((reference, target)[backend], f"DROP {kind} {q(name)}")
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=backend, kind=kind, error=type(error).__name__))
    report["matched"] = sum(c["matched"] for c in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(c["source_qualified"] for c in report["cases"])
    report["success"] = report["matched"] == report["total"] == 53 and not report["cleanup_errors"]
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
            report = run(source, target, schema, "BICDB_CB_" + uuid.uuid4().hex[:8].upper())
    a.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
