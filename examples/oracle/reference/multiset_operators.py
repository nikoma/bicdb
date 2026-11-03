"""Source comparisons for dense named nested-table multiset operations.

Collection results are compared as bags, preserving multiplicity without imposing
an ordering contract. Only owned synthetic types/functions are created.
"""
from decimal import Decimal
import json
import re
from routine_namespaces import identifier

PLANS = [{'id': 'intersect_default',
  'sql': "SELECT {V}('a','a','b') MULTISET INTERSECT {V}('a','a','c') FROM DUAL",
  'expected': {'value': ['a', 'a']}},
 {'id': 'intersect_all',
  'sql': "SELECT {V}('a','a','b') MULTISET INTERSECT ALL {V}('a','a','c') FROM DUAL",
  'expected': {'value': ['a', 'a']}},
 {'id': 'intersect_distinct',
  'sql': "SELECT {V}('a','a','b') MULTISET INTERSECT DISTINCT {V}('a','a','c') FROM DUAL",
  'expected': {'value': ['a']}},
 {'id': 'intersect_null_elements',
  'sql': "SELECT {V}(NULL,NULL,'a') MULTISET INTERSECT {V}(NULL,'b') FROM DUAL",
  'expected': {'value': [None]}},
 {'id': 'intersect_empty',
  'sql': "SELECT {V}() MULTISET INTERSECT {V}('a') FROM DUAL",
  'expected': {'value': []}},
 {'id': 'intersect_null_collection',
  'sql': "SELECT CAST(NULL AS {V}) MULTISET INTERSECT {V}('a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'union_default',
  'sql': "SELECT {V}('a',NULL) MULTISET UNION {V}('a',NULL) FROM DUAL",
  'expected': {'value': ['a', None, 'a', None]}},
 {'id': 'union_distinct',
  'sql': "SELECT {V}('a',NULL) MULTISET UNION DISTINCT {V}('a',NULL) FROM DUAL",
  'expected': {'value': ['a', None]}},
 {'id': 'except_default',
  'sql': "SELECT {V}('a','a','b') MULTISET EXCEPT {V}('a') FROM DUAL",
  'expected': {'value': ['a', 'b']}},
 {'id': 'except_distinct',
  'sql': "SELECT {V}('a','a','b') MULTISET EXCEPT DISTINCT {V}('a') FROM DUAL",
  'expected': {'value': ['b']}},
 {'id': 'except_null_elements',
  'sql': "SELECT {V}(NULL,NULL,'a') MULTISET EXCEPT {V}(NULL) FROM DUAL",
  'expected': {'value': [None, 'a']}},
 {'id': 'numeric_scale',
  'sql': 'SELECT {N}(1,1.0,0.5) MULTISET INTERSECT {N}(1.00,.50) FROM DUAL',
  'expected': {'value': [1, 0.5]}},
 {'id': 'fixed_padding',
  'sql': "SELECT {C}('a','a ') MULTISET INTERSECT DISTINCT {C}('a  ' ) FROM DUAL",
  'expected': {'value': ['a  ']}},
 {'id': 'nominal_types',
  'sql': "SELECT {V}('a') MULTISET INTERSECT {W}('a') FROM DUAL",
  'expected': {'code': 'ORA-00932'}},
 {'id': 'different_element_types',
  'sql': "SELECT {V}('1') MULTISET INTERSECT {N}(1) FROM DUAL",
  'expected': {'code': 'ORA-00932'}},
 {'id': 'intersect_precedes_union',
  'sql': "SELECT {V}('a','b') MULTISET UNION {V}('b') MULTISET INTERSECT {V}('b') FROM DUAL",
  'expected': {'value': ['a', 'b', 'b']}},
 {'id': 'is_empty_empty',
  'sql': "SELECT CASE WHEN {V}() IS EMPTY THEN 'E' WHEN {V}() IS NOT EMPTY THEN 'N' ELSE 'U' END "
         'FROM DUAL',
  'expected': {'value': 'E'}},
 {'id': 'is_empty_nonempty',
  'sql': "SELECT CASE WHEN {V}(NULL) IS EMPTY THEN 'E' WHEN {V}(NULL) IS NOT EMPTY THEN 'N' ELSE "
         "'U' END FROM DUAL",
  'expected': {'value': 'N'}},
 {'id': 'is_empty_null',
  'sql': "SELECT CASE WHEN CAST(NULL AS {V}) IS EMPTY THEN 'E' WHEN CAST(NULL AS {V}) IS NOT EMPTY "
         "THEN 'N' ELSE 'U' END FROM DUAL",
  'expected': {'value': 'U'}},
 {'id': 'union_left_null',
  'sql': "SELECT CAST(NULL AS {V}) MULTISET UNION {V}('a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'union_right_null',
  'sql': "SELECT {V}('a') MULTISET UNION CAST(NULL AS {V}) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'union_both_null',
  'sql': 'SELECT CAST(NULL AS {V}) MULTISET UNION CAST(NULL AS {V}) FROM DUAL',
  'expected': {'value': None}},
 {'id': 'intersect_left_null',
  'sql': "SELECT CAST(NULL AS {V}) MULTISET INTERSECT {V}('a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'intersect_right_null',
  'sql': "SELECT {V}('a') MULTISET INTERSECT CAST(NULL AS {V}) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'intersect_both_null',
  'sql': 'SELECT CAST(NULL AS {V}) MULTISET INTERSECT CAST(NULL AS {V}) FROM DUAL',
  'expected': {'value': None}},
 {'id': 'except_left_null',
  'sql': "SELECT CAST(NULL AS {V}) MULTISET EXCEPT {V}('a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'except_right_null',
  'sql': "SELECT {V}('a') MULTISET EXCEPT CAST(NULL AS {V}) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'except_both_null',
  'sql': 'SELECT CAST(NULL AS {V}) MULTISET EXCEPT CAST(NULL AS {V}) FROM DUAL',
  'expected': {'value': None}},
 {'id': 'intersect_precedes_except',
  'sql': "SELECT {V}('a','b') MULTISET EXCEPT {V}('b') MULTISET INTERSECT {V}('b') FROM DUAL",
  'expected': {'value': ['a']}},
 {'id': 'union_except_left_associative',
  'sql': "SELECT {V}('a','a') MULTISET UNION {V}('a') MULTISET EXCEPT {V}('a','a') FROM DUAL",
  'expected': {'value': ['a']}},
 {'id': 'varchar_numeric_lexemes',
  'sql': "SELECT {V}('01') MULTISET INTERSECT {V}('1') FROM DUAL",
  'expected': {'value': []}},
 {'id': 'varchar_trailing_spaces',
  'sql': "SELECT {V}('a') MULTISET INTERSECT {V}('a ') FROM DUAL",
  'expected': {'value': []}},
 {'id': 'precise_numeric_distinct',
  'sql': 'SELECT CASE WHEN {N}(0.12345678901234567890123456789012345678) MULTISET INTERSECT '
         "{N}(0.12345678901234567890123456789012345679) IS EMPTY THEN 'E' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'E'}},
 {'id': 'precise_numeric_scale',
  'sql': 'SELECT CASE WHEN {N}(123456789012345678901234567890.1250) MULTISET INTERSECT '
         "{N}(123456789012345678901234567890.125) IS NOT EMPTY THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'empty_intersection_predicate',
  'sql': "SELECT CASE WHEN {V}('a') MULTISET INTERSECT {V}('b') IS EMPTY THEN 'E' ELSE 'N' END "
         'FROM DUAL',
  'expected': {'value': 'E'}},
 {'id': 'unicode_elements',
  'sql': "SELECT {V}('é','😀','é') MULTISET INTERSECT DISTINCT {V}('é','😀') FROM DUAL",
  'expected': {'value': ['é', '😀']}},
 {'id': 'empty_string_element_is_null',
  'sql': "SELECT {V}('','a') MULTISET INTERSECT {V}(NULL) FROM DUAL",
  'expected': {'value': [None]}},
 {'id': 'negated_boolean_predicates',
  'sql': "SELECT CASE WHEN NOT ({V}() IS NOT EMPTY) AND {V}(NULL) IS NOT EMPTY THEN 'T' ELSE 'F' "
         'END FROM DUAL',
  'expected': {'value': 'T'}}]


def canonical(value):
    if hasattr(value, "aslist"):
        value = value.aslist()
    if isinstance(value, list):
        return {"multiset": sorted((canonical(item) for item in value), key=repr)}
    if isinstance(value, (Decimal, int, float)) and not isinstance(value, bool):
        return {"number": format(Decimal(str(value)).normalize(), "f")}
    return value


def run(reference, target, schema, prefix):
    names = {key: identifier(schema) + "." + identifier(prefix + "_" + key)
             for key in ("V", "W", "N", "C", "LOCAL", "PARAM", "SHADOW")}
    report = dict(format=1, oracle_version=reference.version, cases=[], cleanup_errors=[],
                  application_qualified=False, target_protocol="prepared", success=False,
                  scope="Dense named nested-table multiset values, NULLs, nominal types, predicates and scoped routine bindings")
    created = []

    def execute(connection, sql):
        with connection.cursor() as cur:
            if connection is target:
                cur.execute(sql, prepare=True)
            else:
                cur.execute(sql)
            return canonical(cur.fetchone()[0]) if cur.description else None

    def failure(exc):
        if hasattr(exc, "sqlstate"):
            match = re.search(r"ORA-[0-9]{5}", getattr(exc.diag, "message_primary", "") or "")
            code = match[0] if match else exc.sqlstate
        else:
            code = getattr(exc.args[0], "full_code", None) if exc.args else None
        return dict(code=code, **({} if code else {"exception": type(exc).__name__}))

    try:
        from psycopg.types.array import ArrayLoader
        for backend, connection in enumerate((reference, target)):
            for key, declaration, element_oid in [("V", "VARCHAR2(8 BYTE)", 1043),
                                                 ("W", "VARCHAR2(8 BYTE)", 1043),
                                                 ("N", "NUMBER", 1700), ("C", "CHAR(3 BYTE)", 1042)]:
                execute(connection, "CREATE TYPE " + names[key] + " AS TABLE OF " + declaration)
                created.append((backend, "DROP TYPE " + names[key]))
                if connection is target:
                    with target.cursor() as cur:
                        cur.execute("SELECT CAST(NULL AS " + names[key] + ") FROM DUAL", prepare=True)
                        oid = cur.description[0].type_code
                        assert oid > 0 and cur.fetchone()[0] is None
                    target.adapters.register_loader(oid, type("MultisetArrayLoader" + key, (ArrayLoader,),
                                                             dict(base_oid=element_oid, delimiter=b",")))
            definitions = [
                (names["LOCAL"], "RETURN {V} IS a {V}:={V}('a','b'); b {V}:={V}('b','c'); c {V};BEGIN c:=a MULTISET INTERSECT b;RETURN c;END;"),
                (names["PARAM"] + "(a {V}, b {V})", "RETURN NUMBER IS BEGIN IF a MULTISET INTERSECT b IS NOT EMPTY THEN RETURN 1;ELSE RETURN 0;END IF;END;"),
                (names["SHADOW"], "RETURN {V} IS a {V}:={V}('a','b'); n NUMBER;BEGIN DECLARE a NUMBER:=7;BEGIN n:=a;END;IF n=7 THEN RETURN a MULTISET INTERSECT {V}('b');ELSE RETURN {V}();END IF;END;"),
            ]
            for index, (name, body) in enumerate(definitions):
                execute(connection, "CREATE FUNCTION " + name.format(**names) + " " + body.format(**names))
                created.append((backend, "DROP FUNCTION " + names[("LOCAL", "PARAM", "SHADOW")[index]]))
        plans = list(PLANS) + [
            dict(id="local_assignment", sql="SELECT {LOCAL}() FROM DUAL", expected={"value": ["b"]}),
            dict(id="typed_parameters", sql="SELECT {PARAM}({V}('a','b'),{V}('b')) FROM DUAL", expected={"value": 1}),
            dict(id="nested_shadow_restores_binding", sql="SELECT {SHADOW}() FROM DUAL", expected={"value": ["b"]}),
            dict(id="registered_function_result", sql="SELECT {LOCAL}() MULTISET UNION {V}('c') FROM DUAL", expected={"value": ["b", "c"]}),
        ]
        for plan in plans:
            sql = plan["sql"].format(**names)
            row = dict(id=plan["id"], sql=sql)
            for label, connection in (("oracle", reference), ("bicdb", target)):
                try:
                    row[label] = dict(value=execute(connection, sql))
                except Exception as exc:
                    row[label] = failure(exc)
            wanted = dict(value=canonical(plan["expected"]["value"])) if "value" in plan["expected"] else plan["expected"]
            row["source_qualified"] = row["oracle"] == wanted
            row["matched"] = row["source_qualified"] and row["bicdb"] == wanted
            report["cases"].append(row)
    except Exception as exc:
        report["setup_error"] = failure(exc)
    finally:
        for backend, sql in reversed(created):
            try:
                execute((reference, target)[backend], sql)
            except Exception as exc:
                report["cleanup_errors"].append(dict(backend=backend, sql=sql, error=failure(exc)))
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = report["total"] == len(PLANS) + 4 and all(row["source_qualified"] for row in report["cases"])
    report["success"] = report["source_qualified"] and report["matched"] == report["total"] and not report["cleanup_errors"]
    return report

def main():
    import argparse
    import os
    import uuid
    from pathlib import Path
    import oracledb
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        raise ValueError("a dedicated non-administrative schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(user=config["user"], password=password.read_text().strip(), host=config["host"], port=config["port"], service_name=config["service_name"]) as source:
        source.autocommit = True
        source.call_timeout = 10000
        with source.cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
            schema = cur.fetchone()[0]
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10, options="-c bicdb.sql_profile=oracle-preview-v1 -c statement_timeout=10000") as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("Oracle-preview target required")
            report = run(source, target, schema, "BICDB_MS_" + uuid.uuid4().hex[:8].upper())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
