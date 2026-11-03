"""Read-only Oracle text-minimum number format contracts and prepared controls."""
import json
import re

PLANS = [{'id': 'literal_0', 'sql': "SELECT TO_CHAR(0,'TM9') FROM DUAL", 'expected': {'value': '0'}},
 {'id': 'literal_1', 'sql': "SELECT TO_CHAR(1,'TM9') FROM DUAL", 'expected': {'value': '1'}},
 {'id': 'literal_2', 'sql': "SELECT TO_CHAR(-1,'TM9') FROM DUAL", 'expected': {'value': '-1'}},
 {'id': 'literal_3', 'sql': "SELECT TO_CHAR(12.500,'TM9') FROM DUAL", 'expected': {'value': '12.5'}},
 {'id': 'literal_4', 'sql': "SELECT TO_CHAR(.5,'TM9') FROM DUAL", 'expected': {'value': '.5'}},
 {'id': 'literal_5', 'sql': "SELECT TO_CHAR(-.5,'TM9') FROM DUAL", 'expected': {'value': '-.5'}},
 {'id': 'literal_6',
  'sql': "SELECT TO_CHAR(1e63,'TM9') FROM DUAL",
  'expected': {'value': '1000000000000000000000000000000000000000000000000000000000000000'}},
 {'id': 'literal_7', 'sql': "SELECT TO_CHAR(1e64,'TM9') FROM DUAL", 'expected': {'value': '1E+64'}},
 {'id': 'literal_8', 'sql': "SELECT TO_CHAR(-1e63,'TM9') FROM DUAL", 'expected': {'value': '-1E+63'}},
 {'id': 'literal_9',
  'sql': "SELECT TO_CHAR(1e-63,'TM9') FROM DUAL",
  'expected': {'value': '.000000000000000000000000000000000000000000000000000000000000001'}},
 {'id': 'literal_10', 'sql': "SELECT TO_CHAR(1e-64,'TM9') FROM DUAL", 'expected': {'value': '1E-64'}},
 {'id': 'literal_11',
  'sql': "SELECT TO_CHAR(-1e-62,'TM9') FROM DUAL",
  'expected': {'value': '-.00000000000000000000000000000000000000000000000000000000000001'}},
 {'id': 'literal_12',
  'sql': "SELECT TO_CHAR(12345678901234567890123456789012345678,'TM9') FROM DUAL",
  'expected': {'value': '12345678901234567890123456789012345678'}},
 {'id': 'literal_13',
  'sql': "SELECT TO_CHAR(1.234567890123456789e80,'TM9') FROM DUAL",
  'expected': {'value': '1.234567890123456789E+80'}},
 {'id': 'literal_14', 'sql': "SELECT TO_CHAR(1e125,'TM9') FROM DUAL", 'expected': {'value': '1E+125'}},
 {'id': 'literal_15', 'sql': "SELECT TO_CHAR(12.500,'TM') FROM DUAL", 'expected': {'value': '12.5'}},
 {'id': 'literal_16', 'sql': "SELECT TO_CHAR(12.500,'tm9') FROM DUAL", 'expected': {'value': '12.5'}},
 {'id': 'literal_17', 'sql': "SELECT TO_CHAR(12.500,'TME') FROM DUAL", 'expected': {'value': '1.25E+01'}},
 {'id': 'literal_18', 'sql': "SELECT TO_CHAR(12.500,'tme') FROM DUAL", 'expected': {'value': '1.25E+01'}},
 {'id': 'literal_19', 'sql': "SELECT TO_CHAR(12.500,'TM9E') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'literal_20', 'sql': "SELECT TO_CHAR(12.500,'FMTM9') FROM DUAL", 'expected': {'value': '12.5'}},
 {'id': 'literal_21', 'sql': "SELECT TO_CHAR(12.500,' TM9') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'literal_22', 'sql': "SELECT TO_CHAR(12.500,'TM9 ') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'literal_23', 'sql': "SELECT TO_CHAR(12.500,'TM99') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'literal_24', 'sql': "SELECT TO_CHAR(NULL,'TM9') FROM DUAL", 'expected': {'value': None}},
 {'id': 'literal_25', 'sql': "SELECT TO_CHAR('bad','TM9') FROM DUAL", 'expected': {'code': 'ORA-01722'}},
 {'id': 'literal_26', 'sql': 'SELECT TO_CHAR(12.5,NULL) FROM DUAL', 'expected': {'value': None}},
 {'id': 'literal_27',
  'sql': "SELECT TO_CHAR(12.5,'TM9','NLS_NUMERIC_CHARACTERS='',.''') FROM DUAL",
  'expected': {'value': '12,5'}},
 {'id': 'literal_28',
  'sql': "SELECT TO_CHAR(.5,'TM9','NLS_NUMERIC_CHARACTERS='',.''') FROM DUAL",
  'expected': {'value': ',5'}},
 {'id': 'literal_29',
  'sql': "SELECT TO_CHAR(DATE '2020-01-01','TM9') FROM DUAL",
  'expected': {'code': 'ORA-01821'}},
 {'id': 'literal_30', 'sql': "SELECT TO_CHAR(0,'TME') FROM DUAL", 'expected': {'value': '0E+00'}},
 {'id': 'literal_31', 'sql': "SELECT TO_CHAR(.5,'TME') FROM DUAL", 'expected': {'value': '5E-01'}},
 {'id': 'literal_32', 'sql': "SELECT TO_CHAR(-1,'TME') FROM DUAL", 'expected': {'value': '-1E+00'}},
 {'id': 'literal_33', 'sql': "SELECT TO_CHAR(12.5,'TM9',NULL) FROM DUAL", 'expected': {'value': None}},
 {'id': 'literal_34', 'sql': "SELECT TO_CHAR(NULL,'TM9E') FROM DUAL", 'expected': {'value': None}},
 {'id': 'literal_35', 'sql': "SELECT TO_CHAR('bad',NULL) FROM DUAL", 'expected': {'code': 'ORA-01722'}},
 {'id': 'literal_36', 'sql': "SELECT TO_CHAR('bad','TM9E') FROM DUAL", 'expected': {'code': 'ORA-01722'}},
 {'id': 'literal_37', 'sql': "SELECT TO_CHAR(NULL,'TM9','invalid') FROM DUAL", 'expected': {'value': None}},
 {'id': 'literal_38',
  'sql': "SELECT TO_CHAR(12.5,'TM9','invalid') FROM DUAL",
  'expected': {'code': 'ORA-12702'}},
 {'id': 'literal_39', 'sql': "SELECT TO_CHAR(1e-131,'TM9') FROM DUAL", 'expected': {'value': '0'}},
 {'id': 'literal_40', 'sql': "SELECT TO_CHAR(1e126,'TM9') FROM DUAL", 'expected': {'code': 'ORA-01426'}},
 {'id': 'literal_41', 'sql': "SELECT TO_CHAR(1,'FMFMTM9') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'literal_42', 'sql': "SELECT TO_CHAR(1,'TM9FM') FROM DUAL", 'expected': {'code': 'ORA-01481'}},
 {'id': 'bound_nls_0',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'FMTME'],
  'expected': {'value': '1.25E+01'}},
 {'id': 'bound_nls_1',
  'sql': 'SELECT TO_CHAR(%s,%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2,:3) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9E', 'invalid'],
  'expected': {'code': 'ORA-12702'}},
 {'id': 'bound_nls_2',
  'sql': 'SELECT TO_CHAR(%s,%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2,:3) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9', "NLS_NUMERIC_CHARACTERS='!?'"],
  'expected': {'value': '12!5'}},
 {'id': 'bound_nls_3',
  'sql': 'SELECT TO_CHAR(%s,%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2,:3) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9', "NLS_NUMERIC_CHARACTERS='.'''"],
  'expected': {'value': '12.5'}},
 {'id': 'bound_nls_4',
  'sql': 'SELECT TO_CHAR(%s,%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2,:3) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9', "NLS_NUMERIC_CHARACTERS=' .'"],
  'expected': {'value': '12 5'}},
 {'id': 'bound_nls_5',
  'sql': 'SELECT TO_CHAR(%s,%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2,:3) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9', "NLS_NUMERIC_CHARACTERS='..'"],
  'expected': {'code': 'ORA-12702'}},
 {'id': 'bound_decimal',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TM9'],
  'expected': {'value': '12.5'}},
 {'id': 'bound_text',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': ['12.500', 'TM9'],
  'expected': {'value': '12.5'}},
 {'id': 'bound_scientific',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'TME'],
  'expected': {'value': '1.25E+01'}},
 {'id': 'bound_native_fallback',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': [{'numeric': '12.500'}, 'FM99.9'],
  'expected': {'value': '12.5'}},
 {'id': 'bound_null',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': [None, 'TM9'],
  'expected': {'value': None}},
 {'id': 'bound_text_error',
  'sql': 'SELECT TO_CHAR(%s,%s) FROM DUAL',
  'oracle_sql': 'SELECT TO_CHAR(:1,:2) FROM DUAL',
  'params': ['bad', None],
  'expected': {'code': 'ORA-01722'}}]

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
