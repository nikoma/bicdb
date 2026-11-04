"""Logical wildcard order, values and transport types through join planning.

Unquoted labels are compared case-insensitively; their raw spellings are retained.
This fixture does not qualify identifier-case or exact numeric-width metadata.
All nonce tables are removed. No application schema or data is used.
"""
import datetime
import decimal
import uuid

from row_lock_waits import main as comparison_main


def canonical(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, (int, float, decimal.Decimal)):
        return {"number": str(decimal.Decimal(str(value)).normalize())}
    return value


def family(code):
    name = getattr(code, "name", str(code))
    if name in ["20", "21", "23", "1700", "DB_TYPE_NUMBER"]:
        return "number"
    if name in ["1114", "DB_TYPE_TIMESTAMP"]:
        return "timestamp"
    if name in ["25", "1042", "1043", "DB_TYPE_VARCHAR", "DB_TYPE_CHAR"]:
        return "text"
    return name


def observe(factory, nonce):
    a, b, c = ["wc_" + part + "_" + nonce for part in ["left", "right", "third"]]
    view = "wc_view_" + nonce
    stamp = "2024-02-29T12:34:56.123456"
    number = lambda value: {"number": str(value)}
    left = [number(7), stamp, "left_07"]
    right = [number(7), number("2.5"), "right"]
    third = [number(7), "third"]
    columns = ["left_key", "event_time", "common_value", "right_key", "amount", "common_value"]
    types = ["number", "timestamp", "text", "number", "number", "text"]
    join = f"{a} l JOIN {b} r ON l.left_key=r.right_key"
    projection = "l.left_key,l.event_time,l.common_value,r.right_key,r.amount,r.common_value"
    cases = [
        ("bare_inner", f"SELECT * FROM {a} JOIN {b} ON {a}.left_key={b}.right_key", columns, types, [left + right]),
        ("aliased_inner", "SELECT * FROM " + join, columns, types, [left + right]),
        ("reverse_source_order", f"SELECT * FROM {b} r JOIN {a} l ON l.left_key=r.right_key", columns[3:] + columns[:3], types[3:] + types[:3], [right + left]),
        ("comma_join", f"SELECT * FROM {a} l,{b} r WHERE l.left_key=r.right_key", columns, types, [left + right]),
        ("cross_join", f"SELECT * FROM {a} l CROSS JOIN {b} r WHERE l.left_key=r.right_key", columns, types, [left + right]),
        ("left_join", f"SELECT * FROM {a} l LEFT JOIN {b} r ON l.left_key=r.right_key WHERE l.left_key=7", columns, types, [left + right]),
        ("right_join", f"SELECT * FROM {a} l RIGHT JOIN {b} r ON l.left_key=r.right_key", columns, types, [left + right]),
        ("qualified_stars", "SELECT l.*,r.* FROM " + join, columns, types, [left + right]),
        ("mixed_projection", "SELECT l.left_key AS first_key,*,r.amount AS last_amount FROM " + join, ["first_key"] + columns + ["last_amount"], ["number"] + types + ["number"], [[number(7)] + left + right + [number("2.5")]]),
        ("empty_result", "SELECT * FROM " + join + " WHERE 1=0", columns, types, []),
        ("derived_star", "SELECT d.* FROM (SELECT * FROM " + join + ") d", columns, types, [left + right]),
        ("cte_star", "WITH d AS (SELECT * FROM " + join + ") SELECT * FROM d", columns, types, [left + right]),
        ("explicit_control", "SELECT " + projection + " FROM " + join, columns, types, [left + right]),
        ("repeated_qualified_star", "SELECT l.*,l.* FROM " + join, columns[:3] * 2, types[:3] * 2, [left * 2]),
        ("three_inner_relations", "SELECT * FROM " + join + f" JOIN {c} t ON r.right_key=t.third_key", columns + ["third_key", "note"], types + ["number", "text"], [left + right + third]),
        ("empty_three_relations", "SELECT * FROM " + join + f" JOIN {c} t ON r.right_key=t.third_key WHERE 1=0", columns + ["third_key", "note"], types + ["number", "text"], []),
        ("positional_order", f"SELECT * FROM {a} l CROSS JOIN {b} r ORDER BY 1 DESC FETCH FIRST 1 ROWS ONLY", columns, types, [[number(32), stamp, "left_32"] + right]),
        ("derived_first_duplicate_ordinal", f"SELECT d.* FROM (SELECT * FROM {a} l CROSS JOIN {b} r) d ORDER BY 3 DESC,1 ASC FETCH FIRST 1 ROWS ONLY", columns, types, [[number(32), stamp, "left_32"] + right]),
        ("derived_second_duplicate_ordinal", f"SELECT d.* FROM (SELECT * FROM {a} l CROSS JOIN {b} r) d ORDER BY 6 ASC,1 DESC FETCH FIRST 1 ROWS ONLY", columns, types, [[number(32), stamp, "left_32"] + right]),
        ("stored_view_star", f"SELECT * FROM {view}", ["l_key","l_time","l_common","r_key","r_amount","r_common"], types, [left + right]),
        ("qualified_view_star", f"SELECT v.* FROM {view} v", ["l_key","l_time","l_common","r_key","r_amount","r_common"], types, [left + right]),
    ]
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        oracle_semantics = oracle or conn.info.parameter_status("bicdb_sql_profile") == "oracle-preview-v1"
        if oracle_semantics:
            # Oracle requires qualified wildcards beside other select items.
            entry = cases[8]
            cases[8] = (entry[0], entry[1].replace(",*,", ",l.*,r.*,"), *entry[2:])
        report["server_version"] = conn.version if oracle else conn.info.server_version
        created = []
        view_created = False
        view_error = None

        def execute(sql):
            with conn.cursor() as cursor:
                if oracle:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, prepare=True)
                if cursor.description is None:
                    return None
                return ([column.name for column in cursor.description],
                        [family(column.type_code) for column in cursor.description],
                        [[canonical(value) for value in row] for row in cursor.fetchall()])

        try:
            integer = "NUMBER(9)" if oracle_semantics else "INTEGER"
            numeric = "NUMBER(10,2)" if oracle_semantics else "NUMERIC(10,2)"
            varchar = "VARCHAR2(16)" if oracle_semantics else "VARCHAR(16)"
            for name, ddl in [(a, f"left_key {integer},event_time TIMESTAMP,common_value {varchar}"),
                              (b, f"right_key {integer},amount {numeric},common_value {varchar}"),
                              (c, f"third_key {integer},note {varchar}")]:
                execute(f"CREATE TABLE {name}({ddl})")
                created.append(name)
            for key in range(1, 33):
                execute(f"INSERT INTO {a} VALUES({key},TIMESTAMP '2024-02-29 12:34:56.123456','left_{key:02}')")
            execute(f"INSERT INTO {b} VALUES(7,2.50,'right')")
            execute(f"INSERT INTO {c} VALUES(7,'third')")
            try:
                if oracle_semantics:
                    # The reference environment returns ORA-25716 when reading
                    # the declared-column joined-wildcard form. Keep that form
                    # unqualified for Oracle; use its readable explicit form.
                    execute(f"CREATE VIEW {view} AS SELECT l.left_key AS l_key,l.event_time AS l_time,l.common_value AS l_common,r.right_key AS r_key,r.amount AS r_amount,r.common_value AS r_common FROM " + join)
                else:
                    execute(f"CREATE VIEW {view}(l_key,l_time,l_common,r_key,r_amount,r_common) AS SELECT * FROM " + join)
                view_created = True
            except Exception as exc:
                view_error = dict(exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), oracle_error=getattr(exc.args[0], "code", None) if exc.args else None)
            for name, sql, expected_columns, expected_types, expected_rows in cases:
                row = dict(id=name, expected_columns=expected_columns, expected_types=expected_types, expected_rows=expected_rows)
                if "view_star" in name and view_error:
                    row["view_creation_error"] = view_error
                try:
                    labels, actual_types, values = execute(sql)
                    row.update(columns=labels, types=actual_types, rows=values,
                               qualified=[label.lower() for label in labels] == expected_columns
                               and actual_types == expected_types and values == expected_rows)
                except Exception as exc:
                    row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), oracle_error=getattr(exc.args[0], "code", None) if exc.args else None)
                report["cases"].append(row)
        finally:
            if view_created:
                try:
                    execute("DROP VIEW " + view)
                except Exception as exc:
                    report["cleanup_errors"].append(dict(exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None)))
            for table in reversed(created):
                try:
                    execute("DROP TABLE " + table + (" PURGE" if oracle else ""))
                except Exception as exc:
                    report["cleanup_errors"].append(dict(exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None)))
    return report


def run(source_factory, target_factory):
    nonce = uuid.uuid4().hex[:10]
    source, target = observe(source_factory, nonce), observe(target_factory, nonce)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(case["matched"] for case in cases),
                cleanup_errors=cleanup, source_qualified=all(case["source_qualified"] for case in cases),
                success=len(cases) == 21 and all(case["matched"] for case in cases) and not cleanup)


def qualified(row):
    return bool(row.get("qualified", False))


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
