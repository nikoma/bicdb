"""Observe persistent BLOB ownership and copy validity on a native Oracle reference.

Only nonce-owned tables are changed. This is a source contract probe, not a
BicDB comparison or application qualification. Each operation records its own
error, so an INSERT failure cannot be mistaken for an APPEND failure.
"""
import argparse
import json
from pathlib import Path
import uuid


CASES = [
    ("delete_then_copy", "DELETE FROM {table} WHERE id=1;", True,
     ("AB", "OK", "ORA22275", "AB", [[2, "AB"]])),
    ("delete_without_copy", "DELETE FROM {table} WHERE id=1;", False,
     ("AB", "SKIPPED", "ORA22275", "AB", [])),
    ("delete_commit_then_copy", "DELETE FROM {table} WHERE id=1; COMMIT;", True,
     ("ORA22922", "ORA22922", "ORA22922", "ORA22922", [])),
    ("delete_rollback_then_copy", "DELETE FROM {table} WHERE id=1; ROLLBACK;", True,
     ("AB", "OK", "ORA22920", "AB", [[1, "AB"], [2, "AB"]])),
    ("replace_then_copy", "UPDATE {table} SET payload=HEXTORAW('CD') WHERE id=1;", True,
     ("AB", "OK", "ORA22922", "AB", [[1, "CD"], [2, "AB"]])),
    ("replace_commit_then_copy", "UPDATE {table} SET payload=HEXTORAW('CD') WHERE id=1; COMMIT;", True,
     ("ORA22922", "ORA22922", "ORA22922", "ORA22922", [[1, "CD"]])),
    ("move_then_copy", "UPDATE {table} SET id=3 WHERE id=1;", True,
     ("AB", "OK", "OK", "AB01", [[2, "AB"], [3, "AB01"]])),
    ("move_commit_then_copy", "UPDATE {table} SET id=3 WHERE id=1; COMMIT;", True,
     ("AB", "OK", "ORA22920", "AB", [[2, "AB"], [3, "AB"]])),
]
FIELDS = ("read_before", "copy_code", "append_code", "read_after", "rows")


def observe(factory, repetitions=2):
    if not 1 <= repetitions <= 10:
        raise ValueError("repetitions must be between 1 and 10")
    import oracledb
    table = "BICDB_LO_" + uuid.uuid4().hex[:16].upper()
    report = dict(format=1, reference="oracle", cases=[], cleanup_errors=[],
                  original_objects_mutated=False, target_compared=False,
                  application_qualified=False, implementation_qualified=False)
    created = False
    with factory() as connection:
        connection.autocommit = False
        connection.call_timeout = 15000
        report["source_version"] = connection.version
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"CREATE TABLE {table}(id NUMBER PRIMARY KEY,payload BLOB)")
                created = True
                for repeat in range(repetitions):
                    for name, action, copy, expected_values in CASES:
                        cursor.execute(f"DELETE FROM {table}")
                        cursor.execute(f"INSERT INTO {table} VALUES(1,HEXTORAW('AB'))")
                        connection.commit()
                        outputs = {field: cursor.var(oracledb.STRING, 2000) for field in FIELDS[:-1]}
                        copy_statement = f"INSERT INTO {table} VALUES(2,d); :copy_code:='OK';" if copy else ":copy_code:='SKIPPED';"
                        cursor.execute(f"""DECLARE d BLOB; BEGIN
                          SELECT payload INTO d FROM {table} WHERE id=1 FOR UPDATE;
                          {action.format(table=table)}
                          BEGIN :read_before:=NVL(RAWTOHEX(DBMS_LOB.SUBSTR(d,20,1)),'NULL');
                            EXCEPTION WHEN OTHERS THEN :read_before:='ORA'||TO_CHAR(-SQLCODE); END;
                          BEGIN {copy_statement}
                            EXCEPTION WHEN OTHERS THEN :copy_code:='ORA'||TO_CHAR(-SQLCODE); END;
                          BEGIN DBMS_LOB.APPEND(d,HEXTORAW('01')); :append_code:='OK';
                            EXCEPTION WHEN OTHERS THEN :append_code:='ORA'||TO_CHAR(-SQLCODE); END;
                          BEGIN :read_after:=NVL(RAWTOHEX(DBMS_LOB.SUBSTR(d,20,1)),'NULL');
                            EXCEPTION WHEN OTHERS THEN :read_after:='ORA'||TO_CHAR(-SQLCODE); END;
                          END;""", **outputs)
                        actual = {key: value.getvalue() for key, value in outputs.items()}
                        cursor.execute(f"SELECT id,RAWTOHEX(DBMS_LOB.SUBSTR(payload,20,1)) FROM {table} ORDER BY id")
                        actual["rows"] = [[int(key), value] for key, value in cursor.fetchall()]
                        expected = dict(zip(FIELDS, expected_values, strict=True))
                        report["cases"].append(dict(id=name, repeat=repeat, actual=actual,
                                                    expected=expected, matched=actual == expected))
                        connection.rollback()
            finally:
                connection.rollback()
                if created:
                    try:
                        cursor.execute(f"DROP TABLE {table} PURGE")
                        cursor.execute("SELECT COUNT(*) FROM user_tables WHERE table_name=:name", name=table)
                        if cursor.fetchone()[0] != 0:
                            report["cleanup_errors"].append("owned table remains after DROP")
                    except Exception as error:
                        report["cleanup_errors"].append(type(error).__name__)
    report["connection_closed"] = True
    report["total"] = len(report["cases"])
    report["matched"] = sum(case["matched"] for case in report["cases"])
    report["success"] = (report["total"] == len(CASES) * repetitions
                         and report["matched"] == report["total"] and not report["cleanup_errors"])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=2, choices=range(1, 11))
    args = parser.parse_args()
    import oracledb
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        parser.error("use a dedicated non-administrative reference schema")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password

    def factory():
        return oracledb.connect(user=config["user"], password=password.read_text().strip(),
                                host=config["host"], port=config["port"],
                                service_name=config["service_name"])

    report = observe(factory, args.repetitions)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({name: report[name] for name in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
