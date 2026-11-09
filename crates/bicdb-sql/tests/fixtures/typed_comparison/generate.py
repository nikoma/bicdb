#!/usr/bin/env python3
"""Generate the typed-comparison differential fixture from PostgreSQL 16.

Usage: generate.py <libpq dsn> > typed_comparison_pg16.json

The database should be created with LOCALE 'C' so that text ordering is
byte ordering. Every comparison pair is evaluated in every query context
(select list, WHERE, JOIN ON, CASE, derived table, CTE, view, GROUP BY,
HAVING, ORDER BY, set operation, subqueries, PL/pgSQL) and the result rows
are recorded as text; the Rust test replays the same statements.
"""
import json
import sys

import psycopg2

COLUMNS = [
    ("id", "int4"),
    ("d", "date"),
    ("ts", "timestamp"),
    ("tz", "timestamptz"),
    ("n", "numeric"),
    ("i2", "int2"),
    ("i4", "int4"),
    ("i8", "int8"),
    ("f4", "float4"),
    ("f8", "float8"),
    ("c", "char(5)"),
    ("c3", "char(3)"),
    ("vc", "varchar(10)"),
    ("tx", "text"),
    ("b", "bool"),
    ("b2", "bool"),
]

ROWS = [
    "(1, '2002-07-04', '2002-07-04 00:00', '2002-07-04 00:00+00', 2.000, 2, 2, 2, 2, 2, 'ab', 'ab', 'ab', 'ab', true, true)",
    "(2, '2002-07-04', '2002-07-04 12:00', '2002-07-03 23:00+00', 2.5, 2, 3, 3, 2.5, 2.5, 'ab', 'ab ', 'ab ', 'ab ', true, false)",
    "(3, '2002-07-05', '2002-07-04 23:59:59', '2002-07-05 00:00+00', 10, 9, 9, 10, 9.5, 10, 'b', 'a', 'a', 'abc', false, true)",
    "(4, NULL, '2002-07-04 00:00', NULL, NULL, 1, NULL, 1, NULL, 1, NULL, 'x', 'x', NULL, NULL, false)",
    "(5, '2002-07-04', NULL, '2002-07-04 04:00+00', 0.1, 0, 0, 0, 0.1, 0.1, 'ab ', 'ab', 'ab', 'abd', false, false)",
    "(6, '2002-07-03', '2002-07-04 00:00:00.000001', '2002-07-04 00:00:00.000001+00', 9007199254740993, -1, -1, 9007199254740993, -0, 9007199254740992, 'a', 'b', 'b', 'a', true, NULL)",
    "(7, 'infinity', 'infinity', 'infinity', 'NaN', 1, 1, 1, 'NaN', 'NaN', 'zz', 'zz', 'zz', 'zz', false, false)",
    "(8, '-infinity', '2002-07-04', '-infinity', -2.50, -2, -3, -3, -2.5, -2.5, '', '', '', '', true, true)",
    "(9, '1999-12-31', '2000-01-01 00:00', '1999-12-31 23:59:59-05', 7.0000000001, 7, 7, 7, 7, 7.0000000001, 'abc', 'abc', 'abc  ', 'abc', true, false)",
    "(10, '2002-07-04', '2002-07-04 00:00', '2002-07-04 00:00-04', 100, 100, 100, 100, 99.99999, 100.0, 'ABC', 'abc', 'ABC', 'ABC', true, true)",
    "(11, '2002-07-04', '2002-07-04 04:00', '2002-07-04 04:00+00', 0.10, 0, 1, 1, 1, 0.1, 'ab', 'ab', 'ab', 'ab', false, true)",
    "(12, '2024-02-29', '2024-02-29 00:00', '2024-02-29 05:00+00', 1, 1, 1, 1, 1, 1, 'q', 'q', 'q', 'q', false, false)",
]

# Pairs PostgreSQL accepts for comparison, each direction that resolves to
# a different operator or cast exercised.
PAIRS = [
    ("d", "ts"), ("ts", "d"), ("d", "tz"), ("tz", "d"), ("ts", "tz"), ("tz", "ts"),
    ("d", "d"), ("ts", "ts"),
    ("n", "i4"), ("i4", "n"), ("n", "i8"), ("n", "f8"), ("f8", "n"), ("n", "f4"),
    ("i4", "f8"), ("f8", "i4"), ("i8", "f8"), ("i2", "i8"), ("i2", "n"), ("f4", "f8"),
    ("i8", "i4"),
    ("c", "vc"), ("vc", "c"), ("c", "tx"), ("tx", "c"), ("vc", "tx"), ("c", "c3"),
    ("c3", "vc"),
    ("b", "b2"),
]
TZ_PAIRS = {("d", "tz"), ("tz", "d"), ("ts", "tz"), ("tz", "ts")}
TIME_ZONES = ["UTC", "America/New_York"]

TYPES = dict(COLUMNS)

# A second table whose columns share m's names but not their types: a
# comparison of m.x with o.x must resolve each side by its own relation.
OTHER_COLUMNS = [
    ("id", "int4"), ("d", "timestamp"), ("ts", "date"), ("tz", "date"),
    ("n", "float8"), ("f8", "numeric"), ("i4", "numeric"), ("i8", "float8"),
    ("c", "text"), ("tx", "char(5)"), ("vc", "char(5)"),
]
# o's values come from these m columns, cast to o's types.
OTHER_SOURCES = [
    ("id", None), ("ts", "timestamp"), ("d", "date"), ("ts", "date"),
    ("f8", "float8"), ("n", "numeric"), ("n", "numeric"), ("f8", "float8"),
    ("tx", "text"), ("c", "char(5)"), ("tx", "char(5)"),
]


def setup_sql():
    cols = ", ".join(f"{name} {kind}" for name, kind in COLUMNS)
    statements = [
        f"CREATE TABLE m ({cols})",
        "INSERT INTO m VALUES " + ", ".join(ROWS),
        "CREATE VIEW vm AS SELECT * FROM m",
    ]
    for a, b in PAIRS:
        statements.append(
            f"CREATE FUNCTION cmp_{a}_{b}(x {TYPES[a]}, y {TYPES[b]}) RETURNS text "
            "LANGUAGE plpgsql AS $$ BEGIN "
            "IF x = y THEN RETURN 'eq'; ELSIF x < y THEN RETURN 'lt'; "
            "ELSIF x > y THEN RETURN 'gt'; END IF; RETURN 'unknown'; END $$"
        )
        statements.append(
            f"CREATE FUNCTION rows_{a}_{b}() RETURNS text LANGUAGE plpgsql AS $$ "
            "DECLARE r record; result text := ''; BEGIN "
            f"FOR r IN SELECT id, {a} AS a, {b} AS b FROM m ORDER BY id LOOP "
            "result := result || r.id || ':' || coalesce((r.a = r.b)::text, 'null') "
            "|| ',' || coalesce((r.a < r.b)::text, 'null') || ';'; "
            "END LOOP; RETURN result; END $$"
        )
    other = ", ".join(f"{name} {kind}" for name, kind in OTHER_COLUMNS)
    statements.append(f"CREATE TABLE o ({other})")
    statements.append(
        "INSERT INTO o SELECT "
        + ", ".join(f"{name}::{kind}" if name != "id" else name for name, kind in OTHER_SOURCES)
        + " FROM m"
    )
    return statements


def contexts(a, b):
    cmp = f"{a} = {b}, {a} <> {b}, {a} < {b}, {a} <= {b}, {a} > {b}, {a} >= {b}"
    dcmp = "ca = cb, ca <> cb, ca < cb, ca <= cb, ca > cb, ca >= cb"
    return [
        ("select", f"SELECT id, {cmp} FROM m ORDER BY id"),
        ("where_eq", f"SELECT id FROM m WHERE {a} = {b} ORDER BY id"),
        ("where_lt", f"SELECT id FROM m WHERE {a} < {b} ORDER BY id"),
        ("where_ge_and", f"SELECT id FROM m WHERE id > 0 AND {a} >= {b} ORDER BY id"),
        ("case",
         f"SELECT id, CASE WHEN {a} = {b} THEN 'eq' WHEN {a} < {b} THEN 'lt' "
         f"WHEN {a} > {b} THEN 'gt' ELSE 'unknown' END FROM m ORDER BY id"),
        ("join_eq", f"SELECT x.id, y.id FROM m x JOIN m y ON x.{a} = y.{b} ORDER BY 1, 2"),
        ("join_lt", f"SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id AND x.{a} < y.{b} ORDER BY 1, 2"),
        ("comma_join_eq", f"SELECT x.id, y.id FROM m x, m y WHERE x.{a} = y.{b} ORDER BY 1, 2"),
        ("derived_select", f"SELECT id, {dcmp} FROM (SELECT id, {a} AS ca, {b} AS cb FROM m) s ORDER BY id"),
        ("derived_where_eq", f"SELECT s.id FROM (SELECT id, {a} AS ca, {b} AS cb FROM m) s WHERE s.ca = s.cb ORDER BY 1"),
        ("derived_where_lt", f"SELECT s.id FROM (SELECT id, {a} AS ca, {b} AS cb FROM m) s WHERE s.ca < s.cb ORDER BY 1"),
        ("derived_case",
         "SELECT id, CASE WHEN ca = cb THEN 'eq' WHEN ca < cb THEN 'lt' ELSE 'other' END "
         f"FROM (SELECT id, {a} AS ca, {b} AS cb FROM m) s ORDER BY id"),
        ("derived_column_list", f"SELECT s.id FROM (SELECT id, {a}, {b} FROM m) s(id, p, q) WHERE s.p = s.q ORDER BY 1"),
        ("cte_select", f"WITH s AS (SELECT id, {a} AS ca, {b} AS cb FROM m) SELECT id, {dcmp} FROM s ORDER BY id"),
        ("cte_where", f"WITH s AS (SELECT id, {a} AS ca, {b} AS cb FROM m) SELECT id FROM s WHERE ca <= cb ORDER BY id"),
        ("view_select", f"SELECT id, {cmp} FROM vm ORDER BY id"),
        ("view_where", f"SELECT id FROM vm WHERE {a} = {b} ORDER BY id"),
        ("group_by", f"SELECT {a} = {b}, count(*) FROM m GROUP BY 1 ORDER BY 1"),
        ("having", f"SELECT id FROM m GROUP BY id, {a}, {b} HAVING {a} < {b} ORDER BY id"),
        ("order_by", f"SELECT id FROM m ORDER BY {a} < {b}, {a} = {b}, id"),
        ("union_distinct", f"SELECT count(*) FROM (SELECT {a} AS x FROM m UNION SELECT {b} FROM m) u"),
        ("in_subquery", f"SELECT id FROM m WHERE {a} IN (SELECT {b} FROM m) ORDER BY id"),
        ("exists_correlated",
         f"SELECT id FROM m o WHERE EXISTS (SELECT 1 FROM m i WHERE i.{b} = o.{a} AND i.id = o.id) ORDER BY id"),
        ("plpgsql_params", f"SELECT id, cmp_{a}_{b}({a}, {b}) FROM m ORDER BY id"),
        ("plpgsql_record_loop", f"SELECT rows_{a}_{b}()"),
    ]


def render(value):
    if value is None:
        return "<null>"
    if isinstance(value, bool):
        return "t" if value else "f"
    return str(value)


def main():
    conn = psycopg2.connect(sys.argv[1])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS tc CASCADE")
    cur.execute("CREATE SCHEMA tc")
    cur.execute("SET search_path = tc")
    cur.execute("SET TimeZone = 'UTC'")
    setup = setup_sql()
    for statement in setup:
        cur.execute(statement)
    cases = []
    for zone in TIME_ZONES:
        cur.execute(f"SET TimeZone = '{zone}'")
        for a, b in PAIRS:
            if zone != "UTC" and (a, b) not in TZ_PAIRS:
                continue
            for context, sql in contexts(a, b):
                cur.execute(sql)
                rows = [[render(v) for v in row] for row in cur.fetchall()]
                cases.append({
                    "time_zone": zone,
                    "pair": f"{TYPES[a]}/{TYPES[b]}",
                    "context": context,
                    "sql": sql,
                    "rows": rows,
                })
    for zone in TIME_ZONES:
        cur.execute(f"SET TimeZone = '{zone}'")
        for name, kind in OTHER_COLUMNS[1:]:
            if zone != "UTC" and "tz" not in (name,) and TYPES[name] != "timestamptz":
                continue
            for context, sql in [
                ("other_table_select",
                 f"SELECT m.id, m.{name} = o.{name}, m.{name} < o.{name} FROM m JOIN o ON m.id = o.id ORDER BY 1"),
                ("other_table_where",
                 f"SELECT m.id FROM m JOIN o ON m.id = o.id WHERE m.{name} = o.{name} ORDER BY 1"),
                ("other_table_join_on",
                 f"SELECT m.id, o.id FROM m JOIN o ON m.{name} = o.{name} ORDER BY 1, 2"),
            ]:
                cur.execute(sql)
                rows = [[render(v) for v in row] for row in cur.fetchall()]
                cases.append({
                    "time_zone": zone,
                    "pair": f"m.{TYPES[name]}/o.{kind}",
                    "context": context,
                    "sql": sql,
                    "rows": rows,
                })
    json.dump({"setup": setup, "cases": cases}, sys.stdout, indent=0)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
