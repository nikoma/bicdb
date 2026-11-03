"""Compare bounded USERENV semantics against a live Oracle source.

Only read-only scalar SELECT statements are executed. Physical host and database names are environment dependent. Assertions compare
identity relationships, null/error rules and byte-length behavior. HOST is
unqualified when the transport does not provide a client hostname.
"""
import json
import re

PLANS = [{'id': 'nonempty_current_user',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER') IS NOT NULL THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'nonempty_session_user',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','SESSION_USER') IS NOT NULL THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'nonempty_current_schema',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_SCHEMA') IS NOT NULL THEN 'Y' ELSE 'N' END FROM "
         'DUAL',
  'expected': {'value': 'Y'}},
 {'id': 'nonempty_db_name',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','DB_NAME') IS NOT NULL THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'nonempty_ip_address',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','IP_ADDRESS') IS NOT NULL THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'case_insensitive',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('userenv','current_user')=SYS_CONTEXT('USERENV','CURRENT_USER') THEN "
         "'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'login_identity',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER')=SYS_CONTEXT('USERENV','SESSION_USER') THEN "
         "'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'unknown_namespace',
  'sql': "SELECT SYS_CONTEXT('MISSING','X') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'empty_namespace',
  'sql': "SELECT SYS_CONTEXT('','CURRENT_USER') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'empty_attribute', 'sql': "SELECT SYS_CONTEXT('USERENV','') FROM DUAL", 'expected': {'value': None}},
 {'id': 'namespace_padding',
  'sql': "SELECT SYS_CONTEXT(' USERENV','CURRENT_USER') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'null_namespace',
  'sql': "SELECT SYS_CONTEXT(NULL,'CURRENT_USER') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'null_attribute', 'sql': "SELECT SYS_CONTEXT('USERENV',NULL) FROM DUAL", 'expected': {'value': None}},
 {'id': 'null_length',
  'sql': "SELECT SYS_CONTEXT('USERENV','CURRENT_USER',NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'null_priority',
  'sql': "SELECT SYS_CONTEXT(NULL,'CURRENT_USER','invalid') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'null_unknown_parameter',
  'sql': "SELECT SYS_CONTEXT('USERENV','MISSING',NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'length_1',
  'sql': "SELECT CASE WHEN LENGTH(SYS_CONTEXT('USERENV','CURRENT_USER',1))=1 THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'length_2',
  'sql': "SELECT CASE WHEN LENGTH(SYS_CONTEXT('USERENV','CURRENT_USER',2))=2 THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'length_1.9',
  'sql': "SELECT CASE WHEN LENGTH(SYS_CONTEXT('USERENV','CURRENT_USER',1.9))=1 THEN 'Y' ELSE 'N' END FROM "
         'DUAL',
  'expected': {'value': 'Y'}},
 {'id': 'default_length_zero',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER',0)=SYS_CONTEXT('USERENV','CURRENT_USER') "
         "THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'default_length_negative',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER',-1)=SYS_CONTEXT('USERENV','CURRENT_USER') "
         "THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'default_length_over_limit',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER',4001)=SYS_CONTEXT('USERENV','CURRENT_USER') "
         "THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'default_length_small_fraction',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER',.9)=SYS_CONTEXT('USERENV','CURRENT_USER') "
         "THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'default_length_huge',
  'sql': "SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_USER',1e100)=SYS_CONTEXT('USERENV','CURRENT_USER') "
         "THEN 'Y' ELSE 'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'default_length_tiny',
  'sql': 'SELECT CASE WHEN '
         "SYS_CONTEXT('USERENV','CURRENT_USER',1e-100)=SYS_CONTEXT('USERENV','CURRENT_USER') THEN 'Y' ELSE "
         "'N' END FROM DUAL",
  'expected': {'value': 'Y'}},
 {'id': 'invalid_length',
  'sql': "SELECT SYS_CONTEXT('USERENV','CURRENT_USER','invalid') FROM DUAL",
  'expected': {'code': 'ORA-01722'}},
 {'id': 'invalid_parameter',
  'sql': "SELECT SYS_CONTEXT('USERENV','MISSING') FROM DUAL",
  'expected': {'code': 'ORA-02003'}},
 {'id': 'padded_parameter',
  'sql': "SELECT SYS_CONTEXT('USERENV','CURRENT_USER ') FROM DUAL",
  'expected': {'code': 'ORA-02003'}},
 {'id': 'long_parameter',
  'sql': "SELECT SYS_CONTEXT('USERENV','xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') FROM DUAL",
  'expected': {'code': 'ORA-02003'}},
 {'id': 'unknown_namespace_conversion',
  'sql': "SELECT SYS_CONTEXT('MISSING','X','invalid') FROM DUAL",
  'expected': {'code': 'ORA-01722'}},
 {'id': 'no_arguments', 'sql': 'SELECT SYS_CONTEXT() FROM DUAL', 'expected': {'code': 'ORA-00938'}},
 {'id': 'one_argument', 'sql': "SELECT SYS_CONTEXT('USERENV') FROM DUAL", 'expected': {'code': 'ORA-00938'}},
 {'id': 'too_many_arguments',
  'sql': "SELECT SYS_CONTEXT('USERENV','CURRENT_USER',1,2) FROM DUAL",
  'expected': {'code': 'ORA-00939'}}]

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
