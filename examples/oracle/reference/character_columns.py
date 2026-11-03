"""Compare AL32UTF8 STANDARD character column capacity and fixed padding."""
import re
from compare import canonical
from routine_namespaces import identifier


def run(reference, target, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid fixture prefix")
    report = dict(format=1, oracle_version=reference.version,
                  scope="STANDARD AL32UTF8 BYTE/CHAR SQL column capacity, padding, DML recovery and PostgreSQL-driver values; routine declarations, casts and legacy metadata migration are separate qualification",
                  application_qualified=False, cases=[], cleanup_errors=[], success=False)
    created=[]
    def query(conn, sql, params=None, prepared=False):
        with conn.cursor() as cursor:
            if prepared: cursor.execute(sql, params, prepare=True)
            else: cursor.execute(sql, params)
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()]) if cursor.description else dict(status="ok")
    def compare(name, source_sql, target_sql=None, value=None, bind=False):
        row=dict(id=name)
        for label,conn,sql in [("oracle",reference,source_sql),("bicdb",target,target_sql or source_sql)]:
            try: row[label]=query(conn,sql,(value,) if bind else None,bind and label=="bicdb")
            except Exception as error:
                detail=error.args[0] if error.args else None
                row[label]=dict(code=getattr(error,"sqlstate",None) or getattr(detail,"full_code",None))
        row["matched"] = (row["oracle"] == row["bicdb"] and "code" not in row["oracle"]) or (row["oracle"]==dict(code="ORA-12899") and row["bicdb"]==dict(code="22001"))
        report["cases"].append(row)
    try:
        with reference.cursor() as cur:
            cur.execute("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'")
            report["character_set"]=cur.fetchone()[0]
        if report["character_set"] != "AL32UTF8": raise ValueError("AL32UTF8 reference required")
        for suffix,declaration in [("VB","VARCHAR2(3 BYTE)"),("VC","VARCHAR2(3 CHAR)"),("FB","CHAR(3 BYTE)"),("FC","CHAR(3 CHAR)")]:
            name=identifier(schema)+"."+identifier(prefix+"_"+suffix)
            for backend,conn in enumerate((reference,target)):
                query(conn,f"CREATE TABLE {name}(id NUMBER PRIMARY KEY,v {declaration})");created.append((backend,name))
            for index,value in enumerate(["A","é","😀","Á","é́","A  ","A   ","é ","é  ","",None,"O'Reilly"]):
                literal="NULL" if value is None else "'"+value.replace("'","''")+"'"
                for conn in (reference,target):query(conn,f"DELETE FROM {name}")
                compare(suffix+"_insert_"+str(index),f"INSERT INTO {name} VALUES(1,{literal})")
                compare(suffix+"_saved_"+str(index),f"SELECT id,v FROM {name} ORDER BY id")
            for conn in (reference,target):
                query(conn,f"DELETE FROM {name}");query(conn,f"INSERT INTO {name} VALUES(1,'A')")
            compare(suffix+"_update",f"UPDATE {name} SET v='😀' WHERE id=1")
            compare(suffix+"_update_state",f"SELECT id,v FROM {name} ORDER BY id")
            compare(suffix+"_prepared",f"UPDATE {name} SET v=:1 WHERE id=1",f"UPDATE {name} SET v=%s WHERE id=1",value="é",bind=True)
            compare(suffix+"_prepared_state",f"SELECT id,v FROM {name} ORDER BY id")
            for index,value in enumerate(["é","😀"]):
                for conn in (reference,target):query(conn,f"DELETE FROM {name}")
                row=dict(id=suffix+"_copy_"+str(index))
                try:row["oracle"]=query(reference,f"INSERT INTO {name} VALUES(1,:1)",(value,))
                except Exception as error:row["oracle"]=dict(code=error.args[0].full_code)
                try:
                    with target.cursor() as cur:
                        with cur.copy(f"COPY {name}(id,v) FROM STDIN") as copy:copy.write_row((1,value))
                    row["bicdb"]=dict(status="ok")
                except Exception as error:row["bicdb"]=dict(code=error.sqlstate)
                row["matched"]=(row["oracle"]==row["bicdb"] and "code" not in row["oracle"]) or (row["oracle"]==dict(code="ORA-12899") and row["bicdb"]==dict(code="22001"))
                report["cases"].append(row)
                compare(suffix+"_copy_state_"+str(index),f"SELECT id,v FROM {name} ORDER BY id")
        for suffix,declaration,values in [("MAXF","CHAR(2000 CHAR)",["é","é"*1000,"é"*2000]),("MAXV","VARCHAR2(4000 CHAR)",["é"*2000,"é"*2001])]:
            name=identifier(schema)+"."+identifier(prefix+"_"+suffix)
            for backend,conn in enumerate((reference,target)):
                query(conn,f"CREATE TABLE {name}(id NUMBER PRIMARY KEY,v {declaration})");created.append((backend,name))
            for index,value in enumerate(values):
                for conn in (reference,target):query(conn,f"DELETE FROM {name}")
                compare(suffix+"_insert_"+str(index),f"INSERT INTO {name} VALUES(1,:1)",f"INSERT INTO {name} VALUES(1,%s)",value=value,bind=True)
                compare(suffix+"_saved_"+str(index),f"SELECT id,v FROM {name} ORDER BY id")
    finally:
        for backend,name in reversed(created):
            try:query((reference,target)[backend],"DROP TABLE "+name)
            except Exception as error:report["cleanup_errors"].append(dict(backend=backend,error=type(error).__name__))
    report["matched"]=sum(row["matched"] for row in report["cases"]);report["total"]=len(report["cases"])
    report["success"]=report["matched"]==report["total"] and not report["cleanup_errors"]
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
            report = run(reference, target, schema, "BICDB_CC_" + uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
