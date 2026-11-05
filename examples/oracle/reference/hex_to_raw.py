"""Compare Oracle HEXTORAW character conversion and PostgreSQL binary transport.

Only scalar expressions are evaluated. No application objects or data are changed.
This does not qualify all RAW limits, LOB locators or Oracle's JDBC protocol.
"""
from decimal import Decimal

from row_lock_waits import main as comparison_main


LITERALS = [
    ("uppercase", "'00FF'", "00FF"),
    ("mixed_case", "'aBcD'", "ABCD"),
    ("one_digit", "'A'", "0A"),
    ("odd_length", "'ABC'", "0ABC"),
    ("invalid_digit", "'G0'", "invalid_hex"),
    ("internal_space", "'00 FF'", "invalid_hex"),
    ("outer_spaces", "' 00FF '", "invalid_hex"),
    ("empty", "''", None),
    ("null", "NULL", None),
    ("national", "N'00FF'", "00FF"),
    ("integer", "1", "01"),
    ("odd_integer", "123", "0123"),
    ("fraction", "2.5", "invalid_hex"),
    ("negative", "-1", "invalid_hex"),
    ("bytea_prefix", "'\\x00ff'", "invalid_hex"),
    ("hex_prefix", "'0x00ff'", "invalid_hex"),
    ("integral_numeric_scale", "1.0", "01"),
    ("text_numeric_scale", "'1.0'", "invalid_hex"),
    ("raw_conversion", "HEXTORAW('AB')", "AB"),
    ("fixed_character_padding", "CAST('AB' AS CHAR(4))", "invalid_hex"),
]


def observe(factory):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as connection:
        native = type(connection).__module__.startswith("oracledb")
        connection.autocommit = not native
        report["server_version"] = connection.version if native else connection.info.server_version

        def check(label, expression, expected, parameters=None):
            result = dict(id=label)
            try:
                with connection.cursor() as cursor:
                    sql = f"SELECT HEXTORAW({expression}) FROM DUAL"
                    if parameters is None:
                        cursor.execute(sql)
                    else:
                        cursor.execute(sql, parameters)
                    value = cursor.fetchone()[0]
                    result["actual"] = bytes(value).hex().upper() if value is not None else None
            except Exception as error:
                if native:
                    import oracledb
                    if not isinstance(error, oracledb.DatabaseError):
                        raise
                    result["oracle_code"] = error.args[0].code
                    result["actual"] = "invalid_hex" if error.args[0].code == 1465 else "other_error"
                else:
                    import psycopg
                    if not isinstance(error, psycopg.Error):
                        raise
                    result["sqlstate"] = error.sqlstate
                    result["actual"] = "invalid_hex" if error.sqlstate == "22018" and "ORA-01465" in str(error) else "other_error"
            finally:
                connection.rollback()
            result["qualified"] = result["actual"] == expected
            report["cases"].append(result)

        for label, expression, expected in LITERALS:
            check(label, expression, expected)
        parameter = ":1" if native else "%s"
        for label, value, expected in [
            ("bound_text", "ABC", "0ABC"),
            ("bound_numeric_scale", Decimal("1.0"), "01"),
            ("bound_numeric_text", "1.0", "invalid_hex"),
            ("bound_raw", b"\xAB", "AB"),
        ]:
            check(label, parameter, expected, [value])
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b,
                  matched=a["id"] == b["id"] and a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases),
                matched=sum(case["matched"] for case in cases), cleanup_errors=[],
                source_qualified=all(case["qualified"] for case in source["cases"]),
                success=len(cases) == 24 and all(case["matched"] for case in cases))


if __name__ == "__main__":
    raise SystemExit(comparison_main(run, __doc__, references=("oracle",)))
