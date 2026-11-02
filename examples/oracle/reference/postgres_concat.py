#!/usr/bin/env python3
"""Compare native PostgreSQL value, metadata and formatting contracts.

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN to dedicated native databases.
Creates and removes one uniquely named sequence. DSNs stay outside reports.
"""
import argparse
import json
import os
from pathlib import Path
import uuid

from compare import canonical


def run(reference, target):
    name = "concat_" + uuid.uuid4().hex[:16]
    sequence = f'"public"."{name}"'
    queries = [
        ("null_operator", "SELECT 'x'||NULL"),
        ("concat_function", "SELECT concat('x',NULL)"),
        ("fraction", "SELECT 'x'||CAST(0.5 AS NUMERIC)"),
        ("retained_scale", "SELECT 'x'||CAST(12.500 AS NUMERIC(10,3))"),
        ("format", "SELECT to_char(12.5,'FM99.9')"),
        ("text_type", "SELECT substr(CAST(NULL AS VARCHAR),1,2)"),
        ("bytea_type", "SELECT substr(CAST(NULL AS BYTEA),1,2)"),
        ("bit_type", "SELECT substr(CAST(NULL AS BIT(4)),1,2)"),
        ("valid_bit_substring", "SELECT substring(B'1010' FROM 1 FOR 2)"),
        ("overlay_text_type", "SELECT overlay(NULL::varchar PLACING 'x' FROM 1)"),
        ("nextval", f"SELECT nextval('{sequence}')"),
        ("currval", f"SELECT currval('{sequence}')"),
        ("setval", f"SELECT setval('{sequence}',5)"),
    ]
    for mask in ('99.9', 'FM99.9', 'FM999', 'FM99.99'):
        for number in ('12.5', '12', '0.5', '0', '-0.5', '-12.5',
                       '123.5', '99.99', '0.04', '-123.5', '-99.99', '-0.04'):
            queries.append((f"numeric_{mask}_{number}",
                            f"SELECT to_char(CAST({number} AS NUMERIC),'{mask}')"))
    queries.append(("exact_large_number", "SELECT to_char(9007199254740993::numeric,'FM9999999999999999')"))
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  native_bicdb=True, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)
    created = []
    try:
        for connection in (reference, target):
            connection.execute(f'CREATE SEQUENCE {sequence} START WITH 1')
            created.append(connection)
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
    finally:
        for connection in reversed(created):
            try:
                connection.execute(f'DROP SEQUENCE {sequence}')
            except Exception as error:
                report['cleanup_errors'].append(dict(error=type(error).__name__,
                                                      sqlstate=getattr(error, 'sqlstate', None)))
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
