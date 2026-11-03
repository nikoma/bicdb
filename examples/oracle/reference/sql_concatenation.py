"""Read-only Oracle concatenation chains, scalar subqueries and NULL contracts."""
import json
import re

PLANS = [{'id': 'fourteen_terms',
  'sql': "SELECT 'part0' || 'part1' || 'part2' || 'part3' || 'part4' || 'part5' || 'part6' || 'part7' || "
         "'part8' || 'part9' || 'part10' || 'part11' || 'part12' || 'part13' FROM DUAL",
  'expected': {'value': 'part0part1part2part3part4part5part6part7part8part9part10part11part12part13'}},
 {'id': 'statistics_subqueries',
  'sql': "SELECT 'A' || (SELECT TO_CHAR(COUNT(*),'FM999') FROM DUAL) || 'B' || (SELECT "
         "TO_CHAR(COUNT(*),'FM999') FROM DUAL) || 'C' || (SELECT TO_CHAR(COUNT(*),'FM999') FROM DUAL) || 'D' "
         "|| (SELECT TO_CHAR(COUNT(*),'FM999') FROM DUAL) || 'E' || (SELECT TO_CHAR(COUNT(*),'FM999') FROM "
         "DUAL) || 'F' || (SELECT TO_CHAR(COUNT(*),'FM999') FROM DUAL) || 'G' || (SELECT "
         "TO_CHAR(COUNT(*),'FM999') FROM DUAL) FROM DUAL",
  'expected': {'value': 'A1B1C1D1E1F1G1'}},
 {'id': 'nested_parentheses',
  'sql': "SELECT ((('a'||'b')))||((('c'||'d')))||'e' FROM DUAL",
  'expected': {'value': 'abcde'}},
 {'id': 'all_null',
  'sql': 'SELECT NULL||CAST(NULL AS VARCHAR2(8))||NULL FROM DUAL',
  'expected': {'value': None}},
 {'id': 'all_empty', 'sql': "SELECT ''||''||'' FROM DUAL", 'expected': {'value': None}},
 {'id': 'null_between', 'sql': "SELECT 'a'||NULL||'b'||NULL||'c' FROM DUAL", 'expected': {'value': 'abc'}},
 {'id': 'empty_between', 'sql': "SELECT 'a'||''||'b'||''||'c' FROM DUAL", 'expected': {'value': 'abc'}},
 {'id': 'nullif_empty',
  'sql': "SELECT NULLIF(CONCAT('a','b'),'')||'c' FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'nullif_matching_nonempty',
  'sql': "SELECT NULLIF(CONCAT('a','b'),'ab')||'c' FROM DUAL",
  'expected': {'value': 'c'}},
 {'id': 'nullif_nonmatching_nonempty',
  'sql': "SELECT NULLIF(CONCAT('a','b'),'other')||'c' FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'unicode', 'sql': "SELECT 'é'||NULL||'🙂'||'中' FROM DUAL", 'expected': {'value': 'é🙂中'}},
 {'id': 'zero_conversion', 'sql': "SELECT 'n='||0||';' FROM DUAL", 'expected': {'value': 'n=0;'}},
 {'id': 'integer_conversion', 'sql': "SELECT 'n='||42||';' FROM DUAL", 'expected': {'value': 'n=42;'}},
 {'id': 'formatted_date',
  'sql': "SELECT 'd='||TO_CHAR(DATE '2020-01-01','YYYY-MM-DD')||';' FROM DUAL",
  'expected': {'value': 'd=2020-01-01;'}},
 {'id': 'separate_chains',
  'sql': "SELECT ('a'||'b')||UPPER('c'||'d')||('e'||'f') FROM DUAL",
  'expected': {'value': 'abCDef'}},
 {'id': 'scalar_null',
  'sql': "SELECT 'a'||(SELECT CAST(NULL AS VARCHAR2(8)) FROM DUAL)||'b' FROM DUAL",
  'expected': {'value': 'ab'}}]

def run(reference, target):
    report = dict(format=1, oracle_version=reference.version, application_qualified=False,
                  target_protocol="prepared", cases=[], cleanup_errors=[], success=False)
    for plan in PLANS:
        from decimal import Decimal
        row = dict(id=plan["id"], sql=plan["sql"])
        parameters = [Decimal(value["numeric"]) if isinstance(value, dict) else value
                      for value in plan.get("params", [])]
        if "params" in plan:
            row["parameters"] = plan["params"]
            row["oracle_sql"] = plan["oracle_sql"]
        for label, connection in (("oracle", reference), ("bicdb", target)):
            try:
                with connection.cursor() as cur:
                    if connection is target:
                        cur.execute(plan["sql"], parameters or None, prepare=True)
                    else:
                        cur.execute(plan.get("oracle_sql", plan["sql"]), parameters or None)
                    row[label] = dict(value=cur.fetchone()[0])
            except Exception as exc:
                if connection is reference:
                    code = getattr(exc.args[0], "full_code", None) if exc.args else None
                else:
                    message = getattr(getattr(exc, "diag", None), "message_primary", "") or ""
                    match = re.search(r"ORA-[0-9]{5}", message)
                    code = match[0] if match else getattr(exc, "sqlstate", None)
                row[label] = dict(code=code, **({} if code else {"exception": type(exc).__name__}))
        row["source_qualified"] = row["oracle"] == plan["expected"]
        row["matched"] = row["source_qualified"] and row["bicdb"] == plan["expected"]
        report["cases"].append(row)
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"])
    report["success"] = report["source_qualified"] and report["matched"] == report["total"]
    return report


def main():
    import argparse
    import os
    from pathlib import Path
    import oracledb
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.oracle_config.read_text())
    if config["user"].upper() in ("SYS", "SYSTEM"):
        raise ValueError("a dedicated non-administrative reference schema is required")
    password = Path(config["password_file"])
    if not password.is_absolute():
        password = args.oracle_config.parent / password
    oracledb.defaults.fetch_decimals = True
    with oracledb.connect(user=config["user"], password=password.read_text().strip(),
                         host=config["host"], port=config["port"],
                         service_name=config["service_name"]) as source:
        source.call_timeout = 10000
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10,
                             options="-c bicdb.sql_profile=oracle-preview-v1 -c statement_timeout=10000") as target:
            if target.info.parameter_status("bicdb_sql_profile") != "oracle-preview-v1":
                raise RuntimeError("Oracle-preview target required")
            report = run(source, target)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
