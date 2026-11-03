"""Oracle hierarchy contracts: roots, PRIOR direction, filtering and cycles."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    table = identifier(schema) + "." + identifier(prefix + "_N")
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, scope="General hierarchy traversal and generator semantics; no collection or application qualification", success=False)
    created = []

    def query(c, sql):
        with c.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []

    plans = [
        ("depth_first_siblings", f"SELECT ID,LEVEL FROM {table} START WITH PARENT_ID IS NULL CONNECT BY PRIOR ID=PARENT_ID ORDER SIBLINGS BY ID", [[1,1],[2,2],[4,3],[3,2],[5,1]], None),
        ("subtree_root_level", f"SELECT ID,LEVEL FROM {table} START WITH ID=2 CONNECT BY PRIOR ID=PARENT_ID ORDER SIBLINGS BY ID", [[2,1],[4,2]], None),
        ("reverse_prior", f"SELECT ID,LEVEL FROM {table} START WITH ID=4 CONNECT BY PRIOR PARENT_ID=ID", [[4,1],[2,2],[1,3]], None),
        ("where_retains_descendants", f"SELECT ID,LEVEL FROM {table} WHERE ID<>2 START WITH PARENT_ID IS NULL CONNECT BY PRIOR ID=PARENT_ID ORDER SIBLINGS BY ID", [[1,1],[4,3],[3,2],[5,1]], None),
        ("connect_predicate_prunes", f"SELECT ID,LEVEL FROM {table} START WITH PARENT_ID IS NULL CONNECT BY PRIOR ID=PARENT_ID AND LEVEL<=2 ORDER SIBLINGS BY ID", [[1,1],[2,2],[3,2],[5,1]], None),
        ("cycle_error", f"SELECT ID,LEVEL FROM {table} START WITH ID=10 CONNECT BY PRIOR ID=PARENT_ID", None, "ORA-01436"),
        ("nocycle", f"SELECT ID,LEVEL,CONNECT_BY_ISCYCLE FROM {table} START WITH ID=10 CONNECT BY NOCYCLE PRIOR ID=PARENT_ID", [[10,1,0],[11,2,1]], None),
        ("generator_positive", "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=3", [[1],[2],[3]], None),
        ("generator_zero_keeps_root", "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=0", [[1]], None),
        ("generator_negative_keeps_root", "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=-1", [[1]], None),
        ("generator_null_keeps_root", "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=NULL", [[1]], None),
        ("start_with_can_remove_root", "SELECT LEVEL FROM DUAL START WITH 1=0 CONNECT BY LEVEL<=3", [], None),
        ("multiple_generator_roots", "SELECT LEVEL,COUNT(*) FROM (SELECT 1 AS N FROM DUAL UNION ALL SELECT 2 AS N FROM DUAL) CONNECT BY LEVEL<=3 GROUP BY LEVEL ORDER BY LEVEL", [[1,2],[2,4],[3,8]], None),
    ]
    try:
        for index, c in enumerate((reference, target)):
            query(c, f"CREATE TABLE {table}(ID NUMBER PRIMARY KEY,PARENT_ID NUMBER)")
            created.append(index)
            for node, parent in [(1,None),(2,1),(3,1),(4,2),(5,None),(10,11),(11,10)]:
                query(c, f"INSERT INTO {table} VALUES({node},{'NULL' if parent is None else parent})")
        for name, sql, expected, error in plans:
            row = dict(id=name, sql=sql)
            for label, c in [("oracle", reference), ("bicdb", target)]:
                try:
                    row[label] = dict(rows=query(c, sql))
                except Exception as e:
                    detail = e.args[0] if e.args else None
                    row[label] = dict(code=getattr(e, "sqlstate", None) or getattr(detail, "full_code", None))
            expected = dict(code=error) if error else dict(rows=[[canonical(v) for v in r] for r in expected])
            row["source_qualified"] = row["oracle"] == expected
            row["matched"] = row["source_qualified"] and row["oracle"] == row["bicdb"]
            report["cases"].append(row)
    finally:
        for index in reversed(created):
            try:
                query((reference, target)[index], "DROP TABLE " + table)
            except Exception as e:
                report["cleanup_errors"].append(dict(backend=index, error=type(e).__name__))
    report["matched"] = sum(r["matched"] for r in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(r["source_qualified"] for r in report["cases"])
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
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10, options="-c bicdb.sql_profile=oracle-preview-v1") as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("Oracle-preview target required")
            report = run(source, target, schema, "BICDB_HQ_" + uuid.uuid4().hex[:8].upper())
    a.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
