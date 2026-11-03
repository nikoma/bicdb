"""Compare preview TRIM and bounded regex rules against a live Oracle source.

Only read-only scalar SELECT statements are executed. Native PostgreSQL text
semantics have a separate control fixture.
"""
import json
import re

PLANS = [{'id': 'trim_both',
  'sql': 'SELECT TRIM(\'"\' FROM \'"a"\') FROM DUAL',
  'expected': {'value': 'a'}},
 {'id': 'trim_leading',
  'sql': 'SELECT TRIM(LEADING \'"\' FROM \'""a"\') FROM DUAL',
  'expected': {'value': 'a"'}},
 {'id': 'trim_trailing',
  'sql': 'SELECT TRIM(TRAILING \'"\' FROM \'"a""\') FROM DUAL',
  'expected': {'value': '"a'}},
 {'id': 'trim_spaces', 'sql': "SELECT TRIM(' a ') FROM DUAL", 'expected': {'value': 'a'}},
 {'id': 'trim_other_space',
  'sql': 'SELECT TRIM(\'"\' FROM \'" a "\') FROM DUAL',
  'expected': {'value': ' a '}},
 {'id': 'trim_multiple',
  'sql': "SELECT TRIM('ab' FROM 'ababa') FROM DUAL",
  'expected': {'code': 'ORA-30001'}},
 {'id': 'trim_null_multiple',
  'sql': "SELECT TRIM('ab' FROM CAST(NULL AS VARCHAR2(8))) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'trim_null_trim',
  'sql': "SELECT TRIM(CAST(NULL AS VARCHAR2(8)) FROM 'a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'trim_empty_trim',
  'sql': "SELECT TRIM('' FROM 'a') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'trim_emoji', 'sql': "SELECT TRIM('😀' FROM '😀a😀') FROM DUAL", 'expected': {'value': 'a'}},
 {'id': 'trim_combining',
  'sql': "SELECT TRIM('á' FROM 'áx') FROM DUAL",
  'expected': {'code': 'ORA-30001'}},
 {'id': 'regex_replace_two',
  'sql': "SELECT REGEXP_REPLACE('a,b,c','[^,]*') FROM DUAL",
  'expected': {'value': ',,'}},
 {'id': 'regex_replace_default_all',
  'sql': "SELECT REGEXP_REPLACE('a1b2c3','[0-9]','x') FROM DUAL",
  'expected': {'value': 'axbxcx'}},
 {'id': 'regex_replace_occurrence',
  'sql': "SELECT REGEXP_REPLACE('a1b2c3','[0-9]','x',1,2) FROM DUAL",
  'expected': {'value': 'a1bxc3'}},
 {'id': 'regex_replace_null_pattern',
  'sql': "SELECT REGEXP_REPLACE('abc',NULL,'x') FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'regex_replace_empty',
  'sql': "SELECT REGEXP_REPLACE('abc','.*') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'regex_substr_first',
  'sql': 'SELECT REGEXP_SUBSTR(\'a,b\',\'(".*?"|.*?)(,|$)\',1,1,NULL,1) FROM DUAL',
  'expected': {'value': 'a'}},
 {'id': 'regex_substr_second',
  'sql': 'SELECT REGEXP_SUBSTR(\'a,b\',\'(".*?"|.*?)(,|$)\',1,2,NULL,1) FROM DUAL',
  'expected': {'value': 'b'}},
 {'id': 'regex_substr_end',
  'sql': 'SELECT REGEXP_SUBSTR(\'a,b\',\'(".*?"|.*?)(,|$)\',1,3,NULL,1) FROM DUAL',
  'expected': {'value': None}},
 {'id': 'regex_substr_quoted',
  'sql': 'SELECT REGEXP_SUBSTR(\'"a,b",c\',\'(".*?"|.*?)(,|$)\',1,1,NULL,1) FROM DUAL',
  'expected': {'value': '"a,b"'}},
 {'id': 'regex_substr_empty_field',
  'sql': 'SELECT REGEXP_SUBSTR(\'a,,c\',\'(".*?"|.*?)(,|$)\',1,2,NULL,1) FROM DUAL',
  'expected': {'value': None}},
 {'id': 'regex_substr_unicode_pos',
  'sql': "SELECT REGEXP_SUBSTR('é😀x','.',2,1) FROM DUAL",
  'expected': {'value': '😀'}},
 {'id': 'regex_substr_missing_group',
  'sql': "SELECT REGEXP_SUBSTR('abc','(a)',1,1,NULL,2) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'regex_substr_invalid_pos',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',0) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'regex_substr_invalid_occurrence',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,0) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'regex_substr_null_pos',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'regex_substr_flags_i',
  'sql': "SELECT REGEXP_SUBSTR('AbC','b',1,1,'i') FROM DUAL",
  'expected': {'value': 'b'}},
 {'id': 'regex_substr_invalid_flags',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,1,'z') FROM DUAL",
  'expected': {'code': 'ORA-01760'}},
 {'id': 'regex_substr_null_pattern',
  'sql': "SELECT REGEXP_SUBSTR('abc',NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'regex_replace_backref',
  'sql': "SELECT REGEXP_REPLACE('abc123','([a-z]+)([0-9]+)','\\2-\\1') FROM DUAL",
  'expected': {'value': '123-abc'}},
 {'id': 'edge_substr_fraction_pos',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1.9) FROM DUAL",
  'expected': {'value': 'a'}},
 {'id': 'edge_substr_fraction_occ',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,1.9) FROM DUAL",
  'expected': {'value': 'a'}},
 {'id': 'edge_substr_neg_fraction_pos',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',-0.5) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'edge_substr_neg_group',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,1,NULL,-1) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'edge_substr_big_group',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,1,NULL,10) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_substr_null_occ',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_substr_bad_parenthesis',
  'sql': "SELECT REGEXP_SUBSTR('abc','(') FROM DUAL",
  'expected': {'code': 'ORA-12725'}},
 {'id': 'edge_substr_bad_bracket',
  'sql': "SELECT REGEXP_SUBSTR('abc','[') FROM DUAL",
  'expected': {'code': 'ORA-12726'}},
 {'id': 'edge_substr_long_pattern',
  'sql': "SELECT REGEXP_SUBSTR('abc',RPAD('a',513,'a')) FROM DUAL",
  'expected': {'code': 'ORA-12733'}},
 {'id': 'edge_replace_pos',
  'sql': "SELECT REGEXP_REPLACE('abcabc','a','x',3) FROM DUAL",
  'expected': {'value': 'abcxbc'}},
 {'id': 'edge_replace_null_occ',
  'sql': "SELECT REGEXP_REPLACE('abc','a','x',1,NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_replace_null_pos',
  'sql': "SELECT REGEXP_REPLACE('abc','a','x',NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_replace_negative_occ',
  'sql': "SELECT REGEXP_REPLACE('abc','a','x',1,-1) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'edge_replace_empty_replacement',
  'sql': "SELECT REGEXP_REPLACE('ababa','a','') FROM DUAL",
  'expected': {'value': 'bb'}},
 {'id': 'edge_flags_ic',
  'sql': "SELECT REGEXP_SUBSTR('ABC','a',1,1,'ic') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_flags_ci',
  'sql': "SELECT REGEXP_SUBSTR('ABC','a',1,1,'ci') FROM DUAL",
  'expected': {'value': 'A'}},
 {'id': 'edge_substr_dot_newline',
  'sql': "SELECT REGEXP_SUBSTR(CHR(10),'.') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'edge_substr_dot_newline_n',
  'sql': "SELECT REGEXP_SUBSTR(CHR(10),'.',1,1,'n') FROM DUAL",
  'expected': {'value': '\n'}},
 {'id': 'priority_null_source_bad_pos',
  'sql': "SELECT REGEXP_SUBSTR(CAST(NULL AS VARCHAR2(8)),'.',0) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'priority_null_source_bad_regex',
  'sql': "SELECT REGEXP_SUBSTR(CAST(NULL AS VARCHAR2(8)),'(') FROM DUAL",
  'expected': {'code': 'ORA-12725'}},
 {'id': 'priority_null_pattern_bad_pos',
  'sql': "SELECT REGEXP_SUBSTR('abc',NULL,0) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'priority_replace_null_pattern_bad_pos',
  'sql': "SELECT REGEXP_REPLACE('abc',NULL,'x',0) FROM DUAL",
  'expected': {'code': 'ORA-01428'}},
 {'id': 'priority_null_source_bad_trim',
  'sql': "SELECT TRIM('ab' FROM CAST(NULL AS VARCHAR2(8))) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'priority_replace_unrecognized_escape',
  'sql': "SELECT REGEXP_REPLACE('a','a','\\q') FROM DUAL",
  'expected': {'value': '\\q'}},
 {'id': 'priority_replace_ampersand',
  'sql': "SELECT REGEXP_REPLACE('a','a','\\&') FROM DUAL",
  'expected': {'value': '\\&'}},
 {'id': 'priority_replace_nonexistent_group',
  'sql': "SELECT REGEXP_REPLACE('a','a','\\1x') FROM DUAL",
  'expected': {'value': '\\1x'}},
 {'id': 'priority_substr_null_group',
  'sql': "SELECT REGEXP_SUBSTR('abc','.',1,1,NULL,NULL) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'scope_null_position_bad_regex',
  'sql': "SELECT REGEXP_SUBSTR('abc','(',NULL) FROM DUAL",
  'expected': {'code': 'ORA-12725'}},
 {'id': 'scope_null_occurrence_bad_regex',
  'sql': "SELECT REGEXP_SUBSTR('abc','(',1,NULL) FROM DUAL",
  'expected': {'code': 'ORA-12725'}},
 {'id': 'scope_replace_missing_group_one_slash',
  'sql': "SELECT REGEXP_REPLACE('a','a',CHR(92)||'1x') FROM DUAL",
  'expected': {'value': '\\1x'}},
 {'id': 'scope_replace_unknown_escape_one_slash',
  'sql': "SELECT REGEXP_REPLACE('a','a',CHR(92)||'q') FROM DUAL",
  'expected': {'value': '\\q'}},
 {'id': 'scope_replace_ampersand_one_slash',
  'sql': "SELECT REGEXP_REPLACE('a','a',CHR(92)||'&') FROM DUAL",
  'expected': {'value': '\\&'}},
 {'id': 'anchor_substr_anchor_position',
  'sql': "SELECT REGEXP_SUBSTR('abc','^.',2) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'anchor_replace_anchor_position',
  'sql': "SELECT REGEXP_REPLACE('abc','^','x',2) FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'anchor_substr_dollar_position',
  'sql': "SELECT REGEXP_SUBSTR('abc','.$',2) FROM DUAL",
  'expected': {'value': 'c'}},
 {'id': 'anchor_substr_occurrence_empty',
  'sql': "SELECT REGEXP_SUBSTR('ab','.*?',1,2) FROM DUAL",
  'expected': {'value': None}},
 {'id': 'anchor_replace_optional_capture',
  'sql': "SELECT REGEXP_REPLACE('b','(a)?b',CHR(92)||'1x') FROM DUAL",
  'expected': {'value': 'x'}},
 {'id': 'empty_replace_empty_after_nonempty',
  'sql': "SELECT REGEXP_REPLACE('a','a*','x') FROM DUAL",
  'expected': {'value': 'xx'}},
 {'id': 'empty_replace_empty_after_nonempty_suffix',
  'sql': "SELECT REGEXP_REPLACE('ab','a*','x') FROM DUAL",
  'expected': {'value': 'xxbx'}},
 {'id': 'empty_replace_all_empty',
  'sql': "SELECT REGEXP_REPLACE('ab','.*?','x') FROM DUAL",
  'expected': {'value': 'xaxbx'}},
 {'id': 'empty_replace_empty_unicode',
  'sql': "SELECT REGEXP_REPLACE('😀é','x*','-') FROM DUAL",
  'expected': {'value': '-😀-é-'}},
 {'id': 'empty_replace_nonoverlap_position',
  'sql': "SELECT REGEXP_REPLACE('abc','..','x',2) FROM DUAL",
  'expected': {'value': 'ax'}},
 {'id': 'empty_substr_nonoverlap_position',
  'sql': "SELECT REGEXP_SUBSTR('abc','..',2) FROM DUAL",
  'expected': {'value': 'bc'}},
 {'id': 'alternative_alternation_short_first',
  'sql': "SELECT REGEXP_SUBSTR('ab','a|ab') FROM DUAL",
  'expected': {'value': 'a'}},
 {'id': 'alternative_alternation_empty_first',
  'sql': "SELECT REGEXP_SUBSTR('b','a*|b') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'alternative_greedy_then_lazy',
  'sql': "SELECT REGEXP_SUBSTR('abc','.*|.*?') FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'alternative_lazy_then_greedy',
  'sql': "SELECT REGEXP_SUBSTR('abc','.*?|.*') FROM DUAL",
  'expected': {'value': None}},
 {'id': 'conversion_replace_end_position',
  'sql': "SELECT REGEXP_REPLACE('abc','$','x',4) FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'conversion_replace_beyond_position',
  'sql': "SELECT REGEXP_REPLACE('abc','$','x',5) FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'conversion_replace_empty_end_position',
  'sql': "SELECT REGEXP_REPLACE('abc','a*','x',4) FROM DUAL",
  'expected': {'value': 'abc'}},
 {'id': 'conversion_replace_unicode_end_position',
  'sql': "SELECT REGEXP_REPLACE('😀','$','x',2) FROM DUAL",
  'expected': {'value': '😀'}},
 {'id': 'conversion_trim_numeric',
  'sql': 'SELECT TRIM(0.5) FROM DUAL',
  'expected': {'value': '.5'}},
 {'id': 'conversion_substr_numeric',
  'sql': "SELECT REGEXP_SUBSTR(0.5,'.') FROM DUAL",
  'expected': {'value': '.'}},
 {'id': 'conversion_replace_numeric_source',
  'sql': "SELECT REGEXP_REPLACE(0.5,'[.]','x') FROM DUAL",
  'expected': {'value': 'x5'}},
 {'id': 'conversion_replace_numeric_replacement',
  'sql': "SELECT REGEXP_REPLACE('a','a',0.5) FROM DUAL",
  'expected': {'value': '.5'}}]


PLANS.extend([
    dict(id="bind_numeric_source", sql="SELECT REGEXP_SUBSTR(%s,'.') FROM DUAL",
         oracle_sql="SELECT REGEXP_SUBSTR(:1,'.') FROM DUAL",
         params=[{"numeric": "0.5"}], expected={"value": "."}),
    dict(id="bind_numeric_replacement", sql="SELECT REGEXP_REPLACE('a','a',%s) FROM DUAL",
         oracle_sql="SELECT REGEXP_REPLACE('a','a',:1) FROM DUAL",
         params=[{"numeric": "0.5"}], expected={"value": ".5"}),
    dict(id="bind_unicode_source", sql="SELECT REGEXP_SUBSTR(%s,'.',2) FROM DUAL",
         oracle_sql="SELECT REGEXP_SUBSTR(:1,'.',2) FROM DUAL",
         params=["é😀x"], expected={"value": "😀"}),
    dict(id="bind_null_flags", sql="SELECT REGEXP_SUBSTR('abc','.',1,1,%s) FROM DUAL",
         oracle_sql="SELECT REGEXP_SUBSTR('abc','.',1,1,:1) FROM DUAL",
         params=[None], expected={"value": "a"}),
])


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
