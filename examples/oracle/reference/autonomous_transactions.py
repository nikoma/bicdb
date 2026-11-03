"""Compare independent transactions with Oracle using separate observer sessions.

The target uses PostgreSQL transport with the durable Oracle preview profile.
Only generated fixtures are modified. This does not qualify an application,
distributed execution, child DDL, notifications, or general PL/SQL support.
"""

import re

from compare import canonical
from routine_namespaces import identifier


def run(reference, target, reference_observer, target_observer, schema, prefix):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,24}", prefix):
        raise ValueError("invalid generated fixture prefix")
    q = lambda suffix: identifier(schema) + "." + identifier(prefix + "_" + suffix)
    table = q("ROWS")
    report = dict(format=1, reference_version=reference.version,
                  application_qualified=False, cases=[], cleanup_errors=[])
    pairs = [(reference, target), (reference_observer, target_observer)]
    created = []

    def execute(connection, sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.description:
                return dict(rows=[[canonical(value) for value in row]
                                  for row in cursor.fetchall()])
            return dict(status="ok")

    def outcome(connection, sql, oracle):
        try:
            return execute(connection, sql)
        except Exception as error:
            if oracle:
                return dict(oracle_code=error.args[0].code)
            if not getattr(error, "sqlstate", None):
                raise
            match = re.search(r"ORA-(\d{5}):", str(error))
            return dict(oracle_code=int(match[1])) if match else dict(sqlstate=error.sqlstate)

    def compare(case, sql, error=None, observer=False):
        left, right = pairs[int(observer)]
        a, b = outcome(left, sql, True), outcome(right, sql, False)
        matched = a == b and (a == dict(oracle_code=error) if error is not None
                             else "oracle_code" not in a and "sqlstate" not in a)
        report["cases"].append(dict(id=case, reference=a, bicdb=b, matched=matched))

    def both(sql):
        for connection in pairs[0]:
            execute(connection, sql)

    def transaction():
        for connection in pairs[0]:
            connection.autocommit = False

    def rollback():
        for connection in pairs[0]:
            connection.rollback()
            connection.autocommit = True

    read = f'SELECT "ID","V" FROM {table} ORDER BY "ID"'
    definitions = [("TABLE", table,
                    f'CREATE TABLE {table}("ID" NUMBER PRIMARY KEY,"V" NUMBER DEFAULT 0)')]

    def procedure(name, body, autonomous=True, declarations="", parameters=""):
        definitions.append(("PROCEDURE", q(name), f"CREATE PROCEDURE {q(name)}{parameters} IS "
                            + ("PRAGMA AUTONOMOUS_TRANSACTION;" if autonomous else "")
                            + declarations + " BEGIN " + body + " END;"))

    def function(name, body, autonomous=True, declarations="", parameters="", returns="NUMBER"):
        definitions.append(("FUNCTION", q(name), f"CREATE FUNCTION {q(name)}{parameters} RETURN {returns} IS "
                            + ("PRAGMA AUTONOMOUS_TRANSACTION;" if autonomous else "")
                            + declarations + " BEGIN " + body + " END;"))

    procedure("COMMIT", f'INSERT INTO {table}("ID") VALUES(p);COMMIT;', parameters="(p NUMBER)")
    procedure("OPEN", f'INSERT INTO {table}("ID") VALUES(9);')
    procedure("ERROR", f'INSERT INTO {table}("ID") VALUES(10);RAISE_APPLICATION_ERROR(-20100,\'before commit\');')
    procedure("AFTER", f'INSERT INTO {table}("ID") VALUES(11);COMMIT;RAISE_APPLICATION_ERROR(-20101,\'after commit\');')
    procedure("EMPTY_UPDATE", f'UPDATE {table} SET "V"=1 WHERE "ID"=999;')
    procedure("EMPTY_DELETE", f'DELETE FROM {table} WHERE "ID"=999;')
    procedure("SAVEPOINT", "SAVEPOINT point_one;")
    procedure("READ", f'SELECT COUNT(*) INTO v FROM {table};', declarations="v NUMBER;")
    procedure("LOCK", f'SELECT "ID" INTO v FROM {table} WHERE "ID"=999 FOR UPDATE;EXCEPTION WHEN NO_DATA_FOUND THEN NULL;', declarations="v NUMBER;")
    procedure("CAUGHT", f'INSERT INTO {table}("ID") VALUES(2);EXCEPTION WHEN OTHERS THEN NULL;')
    procedure("NESTED", f'INSERT INTO {table}("ID") VALUES(3);{q("COMMIT")}(4);ROLLBACK;')
    procedure("PARENT_LOCK", f'UPDATE {table} SET "V"=2 WHERE "ID"=2;COMMIT;')
    function("VALUE", f'INSERT INTO {table}("ID") VALUES(5);COMMIT;RETURN 42;')
    function("DEFAULT", f'INSERT INTO {table}("ID") VALUES(7);RETURN 42;', autonomous=False)
    function("ARGUMENT", "RETURN p;", parameters=f'(p NUMBER DEFAULT {q("DEFAULT")}())')
    procedure("CALLER", f'v:={q("ARGUMENT")}();', autonomous=False, declarations="v NUMBER;")
    function("STATUS", "RETURN SQLCODE;")
    function("HANDLER", f"RAISE_APPLICATION_ERROR(-20102,'outer');EXCEPTION WHEN OTHERS THEN RETURN {q('STATUS')}();", autonomous=False)
    try:
        for index, connection in enumerate(pairs[0]):
            connection.autocommit = True
            for kind, name, ddl in definitions:
                execute(connection, ddl)
                created.append((index, kind, name))
        transaction()
        both(f'INSERT INTO {table}("ID") VALUES(1)')
        compare("independent_commit", f'CALL {q("COMMIT")}(2)')
        compare("parent_sees_own_and_child", read)
        compare("observer_sees_child_only", read, observer=True)
        rollback()
        compare("child_survives_parent_rollback", read)
        for case, name, error in [("open_exit", "OPEN", 6519), ("error_rollback", "ERROR", 20100),
                                  ("committed_error", "AFTER", 20101), ("empty_update", "EMPTY_UPDATE", 6519),
                                  ("empty_delete", "EMPTY_DELETE", 6519), ("savepoint", "SAVEPOINT", 6519),
                                  ("empty_lock_caught", "LOCK", 6519), ("failed_insert_caught", "CAUGHT", None),
                                  ("plain_read", "READ", None), ("nested_rollback", "NESTED", None)]:
            compare(case, f'CALL {q(name)}()', error=error)
            compare(case + "_observer", read, observer=True)
        compare("sql_function", f'SELECT {q("VALUE")}() FROM DUAL')
        compare("sql_function_committed", read, observer=True)
        transaction()
        both("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        compare("serializable_before", read)
        compare("serializable_child_commit", f'CALL {q("COMMIT")}(6)')
        compare("serializable_parent_snapshot", read)
        compare("serializable_observer_current", read, observer=True)
        rollback()
        compare("new_transaction_current", read)
        transaction()
        both(f'UPDATE {table} SET "V"=1 WHERE "ID"=2')
        compare("parent_lock_deadlock", f'CALL {q("PARENT_LOCK")}()', error=60)
        # PostgreSQL transport requires ROLLBACK after the failed top-level CALL.
        rollback()
        compare("deadlock_parent_rollback", read)
        transaction()
        compare("default_in_caller", f'CALL {q("CALLER")}()')
        compare("default_parent_value", read)
        compare("default_not_independent", read, observer=True)
        rollback()
        compare("default_rolled_back", read)
        compare("shared_error_context", f'SELECT {q("HANDLER")}() FROM DUAL')
    finally:
        rollback()
        for index, kind, name in reversed(created):
            try:
                execute(pairs[0][index], f"DROP {kind} {name}")
            except Exception as error:
                report["cleanup_errors"].append(dict(backend=index, object=name,
                                                     error=type(error).__name__))
    report["matched"] = sum(row["matched"] for row in report["cases"])
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
    credentials = dict(user=config["user"], password=password.read_text().strip(),
                       host=config["host"], port=config["port"], service_name=config["service_name"])
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(**credentials) as reference, oracledb.connect(**credentials) as observer:
        reference.autocommit = observer.autocommit = True
        reference.call_timeout = observer.call_timeout = 10000
        with reference.cursor() as cursor:
            cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
            schema = cursor.fetchone()[0]
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True,
                            options="-c bicdb.sql_profile=oracle-preview-v1") as target, \
                psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True,
                                options="-c bicdb.sql_profile=oracle-preview-v1") as target_observer:
            report = run(reference, target, observer, target_observer, schema,
                         "BICDB_AT_" + uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
