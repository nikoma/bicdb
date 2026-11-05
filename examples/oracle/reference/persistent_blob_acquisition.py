"""Native Oracle BLOB acquisition/read-version observations, not a target test.

Uses only an owned nonce table. Ordinary reads, FOR UPDATE reads and destination
writes are compared across commit/rollback. No application objects are changed.
"""
import uuid
from persistent_blob_ownership import main as source_main

# Each action starts with row 1 = AB and no locator assigned yet. Results are
# d, e, x, subsequent APPEND error, and final table rows, respectively.
CASES = [
    ("old_locked_writer_rollback", "SELECT payload INTO d FROM {t} WHERE id=1 FOR UPDATE; COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(e,HEXTORAW('01')); COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('02')); SELECT payload INTO x FROM {t} WHERE id=1 FOR UPDATE; ROLLBACK;",
     ("AB01", "AB01", "AB01", "ORA22920", [[1, "AB01"]])),
    ("old_locked_writer_savepoint", "SELECT payload INTO d FROM {t} WHERE id=1 FOR UPDATE; COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(e,HEXTORAW('01')); COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; SAVEPOINT mark; DBMS_LOB.APPEND(d,HEXTORAW('02')); SELECT payload INTO x FROM {t} WHERE id=1 FOR UPDATE; ROLLBACK TO mark;",
     ("AB01", "AB01", "AB01", "OK", [[1, "AB0102"]])),
    ("old_snapshot_writer_rollback", "SELECT payload INTO d FROM {t} WHERE id=1; COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(e,HEXTORAW('01')); COMMIT; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('02')); SELECT payload INTO x FROM {t} WHERE id=1 FOR UPDATE; ROLLBACK;",
     ("AB01", "AB01", "AB01", "ORA22920", [[1, "AB01"]])),
    ("read_replace_commit", "SELECT payload INTO d FROM {t} WHERE id=1; e:=d; UPDATE {t} SET payload=HEXTORAW('CD') WHERE id=1; COMMIT;",
     ("AB", "AB", None, "ORA22920", [[1, "CD"]])),
    ("read_delete_commit", "SELECT payload INTO d FROM {t} WHERE id=1; e:=d; DELETE FROM {t} WHERE id=1; COMMIT;",
     ("AB", "AB", None, "ORA22920", [])),
    ("mixed_writers_commit", "SELECT payload INTO d FROM {t} WHERE id=1; SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('01')); DBMS_LOB.APPEND(e,HEXTORAW('02')); COMMIT;",
     ("AB02", "AB02", None, "ORA22920", [[1, "AB02"]])),
    ("locked_after_write_rollback", "SELECT payload INTO d FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('01')); SELECT payload INTO e FROM {t} WHERE id=1 FOR UPDATE; ROLLBACK;",
     ("AB", "AB", None, "ORA22920", [[1, "AB"]])),
    ("read_after_write_rollback", "SELECT payload INTO d FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('01')); SELECT payload INTO e FROM {t} WHERE id=1; ROLLBACK;",
     ("AB", "AB01", None, "ORA22920", [[1, "AB"]])),
    ("snapshot_destination_commit", "SELECT payload INTO d FROM {t} WHERE id=1; e:=d; SELECT payload INTO x FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('01')); COMMIT;",
     ("AB01", "AB", "AB01", "ORA22920", [[1, "AB01"]])),
    ("snapshot_independent_commit", "SELECT payload INTO d FROM {t} WHERE id=1; e:=d; SELECT payload INTO x FROM {t} WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(x,HEXTORAW('01')); COMMIT;",
     ("AB", "AB", "AB01", "ORA22920", [[1, "AB01"]])),
    ("new_value_rollback", "INSERT INTO {t} VALUES(2,HEXTORAW('CD')) RETURNING payload INTO d; e:=d; ROLLBACK;",
     ("CD", "CD", None, "ORA22920", [[1, "AB"]])),
]
FIELDS = ("d", "e", "x", "append_code", "rows")


def observe(factory, repetitions=2):
    if not 1 <= repetitions <= 10:
        raise ValueError("repetitions must be between 1 and 10")
    import oracledb
    table = "BICDB_LA_" + uuid.uuid4().hex[:16].upper()
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
                    for name, action, expected_values in CASES:
                        cursor.execute(f"DELETE FROM {table}")
                        cursor.execute(f"INSERT INTO {table} VALUES(1,HEXTORAW('AB'))")
                        connection.commit()
                        outputs = {field: cursor.var(oracledb.STRING, 2000) for field in FIELDS[:-1]}
                        reads = "\n".join(
                            f"BEGIN :{field}:=RAWTOHEX(DBMS_LOB.SUBSTR({field},20,1)); EXCEPTION WHEN OTHERS THEN :{field}:='ORA'||TO_CHAR(-SQLCODE); END;"
                            for field in FIELDS[:3])
                        cursor.execute(f"""DECLARE d BLOB;e BLOB;x BLOB;BEGIN
                          {action.format(t=table)}
                          {reads}
                          BEGIN DBMS_LOB.APPEND(d,HEXTORAW('02')); :append_code:='OK';
                            EXCEPTION WHEN OTHERS THEN :append_code:='ORA'||TO_CHAR(-SQLCODE); END;
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


if __name__ == "__main__":
    raise SystemExit(source_main(observe, __doc__))
