"""Current-time type, session-zone, precision and transaction-clock comparisons.

Only connection-local settings and read-only queries are used. Absolute clock
values differ between executions; the checks compare each backend's values with
its query time window and the selected zone, not two independently read instants.
"""
import datetime as dt
import time
from zoneinfo import ZoneInfo

from row_lock_waits import main as comparison_main


def execute(conn, sql, rows=True):
    with conn.cursor() as cursor:
        if type(conn).__module__.startswith("psycopg"):
            cursor.execute(sql, prepare=rows)
        else:
            # Thin mode cannot decode a TSTZ containing an Oracle region ID.
            # Describe the original typed query without prefetching its values.
            cursor.prefetchrows = 0
            cursor.execute(sql)
        if not rows:
            return None
        types = [column.type_code for column in cursor.description]
        timezone_columns = [i for i, code in enumerate(types) if type_name(code) == "DB_TYPE_TIMESTAMP_TZ"]
        if timezone_columns:
            # This fixture generates only simple current-time projections.
            # Fetch their values in one statement, preserving the original
            # metadata separately. ISO offsets avoid driver region decoding.
            assert sql.startswith("SELECT ") and sql.endswith(" FROM DUAL")
            expressions = sql[len("SELECT "):-len(" FROM DUAL")].split(",")
            assert len(expressions) == len(types)
            for index in timezone_columns:
                expressions[index] = "TO_CHAR(" + expressions[index] + ",'YYYY-MM-DD\"T\"HH24:MI:SS.FF6 TZH:TZM')"
            cursor.execute("SELECT " + ",".join(expressions) + " FROM DUAL")
            values = cursor.fetchall()
            values = [list(row) for row in values]
            for row in values:
                for index in timezone_columns:
                    row[index] = dt.datetime.fromisoformat(row[index].replace(" ", ""))
            return values, types
        return cursor.fetchall(), types


def type_name(value):
    return getattr(value, "name", str(value))


def timestamp(value, zone):
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo(zone))
    return value.timestamp()


def observe(factory, oracle_semantics):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        oracle_driver = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle_driver else conn.info.server_version
        suffix = " FROM DUAL" if oracle_semantics else ""
        for zone in ["UTC", "Asia/Kolkata", "America/Los_Angeles"]:
            setting = "ALTER SESSION SET TIME_ZONE=" if oracle_driver else "SET TIME ZONE "
            execute(conn, setting + "'" + zone + "'", False)
            expressions = (["CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "CURRENT_DATE"]
                           + [f"{name}({precision})" for name in ["CURRENT_TIMESTAMP", "LOCALTIMESTAMP"] for precision in range(7)]
                           if oracle_semantics else ["CURRENT_TIMESTAMP", "CURRENT_DATE", "clock_timestamp()"])
            for expression in expressions:
                row = dict(id=zone + ":" + expression, expression=expression, zone=zone)
                before = time.time()
                try:
                    values, types = execute(conn, "SELECT " + expression + suffix)
                    after = time.time()
                    value = values[0][0]
                    row.update(value=value.isoformat() if isinstance(value, (dt.datetime, dt.date)) else value,
                               type=type_name(types[0]), nonnull=value is not None)
                    if expression == "CURRENT_DATE" and not oracle_semantics:
                        expected_type = "1082"
                        valid_value = isinstance(value, dt.date) and not isinstance(value, dt.datetime) and value in [
                            dt.datetime.fromtimestamp(t, ZoneInfo(zone)).date() for t in [before, after]]
                    else:
                        if oracle_semantics:
                            expected_type = (("DB_TYPE_TIMESTAMP_TZ" if expression.startswith("CURRENT_TIMESTAMP") else
                                              "DB_TYPE_DATE" if expression == "CURRENT_DATE" else "DB_TYPE_TIMESTAMP")
                                             if oracle_driver else
                                             "1184" if expression.startswith("CURRENT_TIMESTAMP") else "1114")
                        else:
                            expected_type = "1184"
                        instant = timestamp(value, zone)
                        valid_value = instant is not None and before - 5 <= instant <= after + 5
                        if expression.endswith("(0)") or expression == "CURRENT_DATE":
                            valid_value = valid_value and isinstance(value, dt.datetime) and value.microsecond == 0
                        if oracle_semantics and isinstance(value, dt.datetime) and "(" in expression:
                            precision = int(expression.rsplit("(", 1)[1][:-1])
                            valid_value = valid_value and value.microsecond % (10 ** (6 - precision)) == 0
                    row.update(expected_type=expected_type, value_qualified=bool(valid_value),
                               qualified=bool(valid_value and row["type"] == expected_type))
                except Exception as exc:
                    row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
                report["cases"].append(row)

            if oracle_semantics:
                for name in ["current_timestamp", "localtimestamp", "current_date"]:
                    for separator in [" AS ", " "]:
                        row = dict(id=zone + ":alias:" + name + ":" + separator.strip(), zone=zone)
                        try:
                            values, _ = execute(conn, "SELECT 1" + separator + name + suffix)
                            row.update(qualified=values[0][0] == 1, value=values[0][0])
                        except Exception as exc:
                            row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
                        report["cases"].append(row)
                row = dict(id=zone + ":same_statement_rounding", zone=zone)
                try:
                    values, _ = execute(conn, "SELECT CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP(0),LOCALTIMESTAMP,LOCALTIMESTAMP(0),CURRENT_DATE" + suffix)
                    a, b, rounded, local, local_rounded, date = values[0]
                    all_datetime = all(isinstance(v, dt.datetime) for v in values[0])
                    valid = False
                    if all_datetime:
                        expected = a.replace(microsecond=0) + dt.timedelta(seconds=int(a.microsecond >= 500000))
                        expected_local = local.replace(microsecond=0) + dt.timedelta(seconds=int(local.microsecond >= 500000))
                        valid = (a == b and rounded == expected and local_rounded == expected_local
                                 and date == local.replace(microsecond=0)
                                 and timestamp(a, zone) == timestamp(local, zone))
                    row.update(qualified=bool(valid), values=[v.isoformat() if isinstance(v, dt.datetime) else v for v in values[0]])
                except Exception as exc:
                    row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
                report["cases"].append(row)

            row = dict(id=zone + ":transaction_clock", zone=zone,
                       expected="advances" if oracle_semantics else "transaction_stable")
            try:
                conn.autocommit = False
                if oracle_driver:
                    execute(conn, "SET TRANSACTION READ ONLY", False)
                else:
                    conn.read_only = True
                first = execute(conn, "SELECT CURRENT_TIMESTAMP" + suffix)[0][0][0]
                time.sleep(.03)
                second = execute(conn, "SELECT CURRENT_TIMESTAMP" + suffix)[0][0][0]
                valid = isinstance(first, dt.datetime) and isinstance(second, dt.datetime)
                valid = valid and (second > first if oracle_semantics else second == first)
                row.update(qualified=bool(valid), first=str(first), second=str(second))
            except Exception as exc:
                row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
            finally:
                conn.rollback()
                conn.autocommit = True
                if not oracle_driver:
                    conn.read_only = False
            report["cases"].append(row)
    return report


def qualified(row):
    return bool(row.get("qualified", False))


def run(source_factory, target_factory):
    with source_factory() as probe:
        oracle = type(probe).__module__.startswith("oracledb")
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    total = 75 if oracle else 12
    return dict(format=1, application_qualified=False, cases=cases, reference_version=source["server_version"],
                total=len(cases), matched=sum(c["matched"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == total and all(c["matched"] for c in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
