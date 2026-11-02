#!/usr/bin/env python3
"""Compare native PostgreSQL binary decode and CASE contracts.

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN to dedicated native databases.
Creates no persistent objects. DSNs stay outside reports.
"""
import argparse
import json
import os
from pathlib import Path

from compare import canonical


def run(reference, target):
    queries = [
        ('hex', "SELECT decode('6162','hex')"),
        ('base64', "SELECT decode('YWI=','base64')"),
        ('escape', "SELECT decode('abc','escape')"),
        ('empty', "SELECT decode('','hex')"),
        ('null_text', "SELECT decode(NULL,'hex')"),
        ('null_format', "SELECT decode('6162',NULL)"),
        ('invalid_hex', "SELECT decode('!','hex')"),
        ('case_text_padding', "SELECT CASE CAST('x  ' AS VARCHAR) WHEN CAST('x' AS VARCHAR) THEN 1 ELSE 2 END"),
        ('case_null', "SELECT CASE NULL WHEN NULL THEN 1 ELSE 2 END"),
        ('case_unselected', "SELECT CASE WHEN true THEN 7 ELSE 1/0 END"),
        ('case_padding', "SELECT CASE CAST('x' AS CHAR(3)) WHEN CAST('x' AS CHAR(1)) THEN 1 ELSE 2 END"),
        ('qualified_decode', "SELECT pg_catalog.decode('6162','hex')"),
    ]
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  native_bicdb=True, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)
    for name, sql in queries:
        row = dict(id=name, sql=sql)
        for label, connection in (('postgres', reference), ('bicdb', target)):
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, prepare=False)
                    row[label] = dict(rows=[[canonical(v) for v in values]
                                            for values in cursor.fetchall()],
                                      type_oids=[v.type_code for v in cursor.description])
            except Exception as error:
                if getattr(error, 'sqlstate', None) is None:
                    raise
                row[label] = dict(sqlstate=error.sqlstate)
        row['matched'] = row['postgres'] == row['bicdb']
        report['cases'].append(row)
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(queries)
    report['success'] = (report['matched'] == report['total']
                         and not report['cleanup_errors'])
    return report


def main():
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'], autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'], autocommit=True) as target:
            report = run(reference, target)
    Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('success', 'matched', 'total')}))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
