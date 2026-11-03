"""Native PostgreSQL numeric-format controls independent of Oracle text minimum."""

PLANS = [{'id': 'native_tm9',
  'sql': "SELECT to_char(12.5,'TM9')",
  'postgres_expected': {'rows': [['TM #']]},
  'bicdb_expected': {'sqlstate': '0A000'},
  'known_gap': True},
 {'id': 'native_tme',
  'sql': "SELECT to_char(12.5,'TME')",
  'postgres_expected': {'rows': [['TME']]},
  'bicdb_expected': {'sqlstate': '0A000'},
  'known_gap': True},
 {'id': 'native_fmtm9',
  'sql': "SELECT to_char(12.5,'FMTM9')",
  'postgres_expected': {'rows': [['TM#']]},
  'bicdb_expected': {'sqlstate': '0A000'},
  'known_gap': True},
 {'id': 'native_fm',
  'sql': "SELECT to_char(12.5,'FM99.9')",
  'postgres_expected': {'rows': [['12.5']]},
  'bicdb_expected': {'rows': [['12.5']]},
  'known_gap': False},
 {'id': 'native_precision',
  'sql': "SELECT to_char(12345678901234567890123456789012345678,'FM99999999999999999999999999999999999999')",
  'postgres_expected': {'rows': [['12345678901234567890123456789012345678']]},
  'bicdb_expected': {'rows': [['12345678901234567890123456789012345678']]},
  'known_gap': False},
 {'id': 'native_timestamp',
  'sql': "SELECT to_char(TIMESTAMP '2020-01-02 03:04:05','YYYY-MM-DD')",
  'postgres_expected': {'rows': [['2020-01-02']]},
  'bicdb_expected': {'rows': [['2020-01-02']]},
  'known_gap': False},
 {'id': 'native_null',
  'sql': "SELECT to_char(NULL::numeric,'TM9')",
  'postgres_expected': {'rows': [[None]]},
  'bicdb_expected': {'rows': [[None]]},
  'known_gap': False},
 {'id': 'native_zero',
  'sql': "SELECT to_char(0,'FM9')",
  'postgres_expected': {'rows': [['0']]},
  'bicdb_expected': {'rows': [['0']]},
  'known_gap': False}]

def run(reference, target, schemas=None):
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  application_qualified=False, target_protocol="prepared", cases=[],
                  cleanup_errors=[], success=False,
                  scope="Native formatter behavior preservation, with existing PostgreSQL literal-model gaps explicitly retained")
    for plan in PLANS:
        row = dict(id=plan["id"], sql=plan["sql"], known_gap=plan["known_gap"])
        for label, connection in (("postgres", reference), ("bicdb", target)):
            try:
                with connection.cursor() as cur:
                    cur.execute(plan["sql"], prepare=True)
                    row[label] = dict(rows=[[None if (value := cur.pgresult.get_value(i,j)) is None else value.decode("utf8")
                                             for j in range(cur.pgresult.nfields)]
                                            for i in range(cur.pgresult.ntuples)])
            except Exception as exc:
                row[label] = dict(sqlstate=getattr(exc, "sqlstate", None))
        row["source_qualified"] = row["postgres"] == plan["postgres_expected"]
        row["matched"] = row["source_qualified"] and row["bicdb"] == row["postgres"]
        row["baseline_preserved"] = row["bicdb"] == plan["bicdb_expected"]
        report["cases"].append(row)
    report["source_qualified"] = all(row["source_qualified"] for row in report["cases"])
    report["matched"] = sum(row["matched"] for row in report["cases"])
    report["baseline_preserved"] = sum(row["baseline_preserved"] for row in report["cases"])
    report["total"] = len(report["cases"])
    report["known_gaps"] = sum(row["known_gap"] for row in report["cases"])
    report["full_postgres_format_parity"] = report["matched"] == report["total"]
    report["success"] = report["source_qualified"] and report["baseline_preserved"] == report["total"]
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schemas", nargs=2, required=True)
    args = parser.parse_args()
    with psycopg.connect(os.environ["ORACLE_CONTROL_PG_DSN"], autocommit=True, connect_timeout=10) as reference:
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True, connect_timeout=10) as target:
            if target.info.parameter_status("bicdb_sql_profile") != "postgres":
                raise RuntimeError("native PostgreSQL-profile target required")
            report = run(reference, target, args.schemas)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total", "baseline_preserved", "known_gaps")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
