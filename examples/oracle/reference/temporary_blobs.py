"""Compare routine-local BLOB values and selected DBMS_LOB operations.

Creates nonce-named functions and removes them on both connections. Persistent
locators, streaming APIs, CLOBs and application qualification remain separate.
"""
import re
import uuid

from row_lock_waits import main as comparison_main


CASES = [
    ("empty", "BLOB", "d BLOB:=EMPTY_BLOB();", "RETURN d;", ""),
    ("raw_initialization", "BLOB", "d BLOB:=HEXTORAW('AB');", "RETURN d;", "AB"),
    ("raw_assignment", "BLOB", "d BLOB;", "d:=HEXTORAW('CD'); RETURN d;", "CD"),
    ("direct_raw_return", "BLOB", "", "RETURN HEXTORAW('EF');", "EF"),
    ("alias_source_write", "BLOB", "d BLOB:=HEXTORAW('AB'); e BLOB;", "e:=d; DBMS_LOB.APPEND(d,HEXTORAW('01')); RETURN e;", "AB"),
    ("alias_destination_write", "BLOB", "d BLOB:=HEXTORAW('AB'); e BLOB;", "e:=d; DBMS_LOB.APPEND(e,HEXTORAW('01')); RETURN e;", "AB01"),
    ("self_append", "BLOB", "d BLOB:=HEXTORAW('AB');", "DBMS_LOB.APPEND(d,d); RETURN d;", "ABAB"),
    ("named_append", "BLOB", "d BLOB:=HEXTORAW('AB');", "SYS.DBMS_LOB.APPEND(src_lob=>HEXTORAW('01'),dest_lob=>d); RETURN d;", "AB01"),
    ("empty_source", "BLOB", "d BLOB:=HEXTORAW('AB');", "DBMS_LOB.APPEND(d,EMPTY_BLOB()); RETURN d;", "AB"),
    ("temporary_length", "NUMBER", "d BLOB:=HEXTORAW('AB');", "DBMS_LOB.APPEND(d,HEXTORAW('0102')); RETURN DBMS_LOB.GETLENGTH(lob_loc=>d);", "3"),
    ("temporary_flag", "NUMBER", "d BLOB:=HEXTORAW('AB');", "RETURN SYS.DBMS_LOB.ISTEMPORARY(d);", "1"),
    ("empty_length", "NUMBER", "d BLOB:=EMPTY_BLOB();", "RETURN DBMS_LOB.GETLENGTH(d);", "0"),
    ("empty_flag", "NUMBER", "d BLOB:=EMPTY_BLOB();", "RETURN DBMS_LOB.ISTEMPORARY(d);", "0"),
    ("null_length", "NUMBER", "d BLOB;", "RETURN DBMS_LOB.GETLENGTH(d);", None),
    ("null_flag", "NUMBER", "d BLOB;", "RETURN DBMS_LOB.ISTEMPORARY(d);", None),
    ("empty_destination", "BLOB", "d BLOB:=EMPTY_BLOB();", "DBMS_LOB.APPEND(d,HEXTORAW('01')); RETURN d;", "ORA-14553"),
    ("null_source_empty_destination", "BLOB", "d BLOB:=EMPTY_BLOB();", "DBMS_LOB.APPEND(d,NULL); RETURN d;", "ORA-06502"),
    ("null_destination_empty_source", "BLOB", "d BLOB;", "DBMS_LOB.APPEND(d,EMPTY_BLOB()); RETURN d;", "ORA-06502"),
]


def observe(factory):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as connection:
        native = type(connection).__module__.startswith("oracledb")
        connection.autocommit = True
        report["server_version"] = connection.version if native else connection.info.server_version
        for label, result_type, declarations, body, expected in CASES:
            name = "BICDB_TL_" + uuid.uuid4().hex[:16].upper()
            created = False
            item = dict(id=label)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"CREATE FUNCTION {name} RETURN {result_type} IS {declarations} BEGIN {body} END;")
                    created = True
                    if native:
                        cursor.execute(f"SELECT {name}() FROM DUAL")
                    else:
                        cursor.execute(f"SELECT {name}() FROM DUAL", prepare=True)
                    value = cursor.fetchone()[0]
                    if hasattr(value, "read"):
                        value = value.read()
                    if isinstance(value, (bytes, bytearray, memoryview)):
                        value = bytes(value).hex().upper()
                    elif value is not None:
                        value = str(value)
                    item["actual"] = value
            except Exception as error:
                code = getattr(error.args[0], "code", None) if error.args else None
                match = re.search(r"ORA-\d{5}", str(error))
                item["actual"] = f"ORA-{code:05}" if code else match.group() if match else "other_error"
                item["sqlstate"] = getattr(error, "sqlstate", None)
                item["oracle_code"] = code
            finally:
                connection.rollback()
                if created:
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(f"DROP FUNCTION {name}")
                    except Exception as error:
                        report["cleanup_errors"].append(type(error).__name__)
            item["qualified"] = item["actual"] == expected
            report["cases"].append(item)
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b,
                  matched=a["id"] == b["id"] and a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases),
                matched=sum(case["matched"] for case in cases), cleanup_errors=cleanup,
                source_qualified=all(case["qualified"] for case in source["cases"]),
                success=len(cases) == len(CASES) and all(case["matched"] for case in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(run, __doc__, references=("oracle",)))
