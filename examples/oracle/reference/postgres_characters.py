#!/usr/bin/env python3
"""Independent PostgreSQL Unicode-scalar chr values, strict NULL and errors."""
import argparse
import json
import os
from pathlib import Path

from compare import canonical


def queries():
    cases = [('unicode_' + str(index), f'SELECT chr({code}) AS value')
             for index, code in enumerate([10,32,35,65,127,128,171,187,233,8364,20013,49835,49851,128512,1114111])]
    cases += [
        ('null', 'SELECT chr(CAST(NULL AS INTEGER)) AS value'),
        ('qualified', 'SELECT pg_catalog.chr(49835) AS value'),
        ('short_circuit', 'SELECT coalesce(chr(65),chr(-1)) AS value'),
        ('nul_error', 'SELECT chr(0) AS value'),
        ('negative_error', 'SELECT chr(-1) AS value'),
        ('surrogate_error', 'SELECT chr(55296) AS value'),
        ('too_large_error', 'SELECT chr(1114112) AS value'),
    ]
    return cases


def outcome(connection, sql):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, prepare=False)
            return dict(rows=[[canonical(value) for value in row] for row in cursor.fetchall()],
                        type_oids=[column.type_code for column in cursor.description])
    except Exception as error:
        if getattr(error, 'sqlstate', None) is None:
            raise
        return dict(sqlstate=error.sqlstate)


def run(reference, target):
    report = dict(format=1,postgres_version_num=reference.info.server_version,
                  native_bicdb=True,application_qualified=False,cases=[],success=False)
    for name, sql in queries():
        row = dict(id=name,sql=sql,postgres=outcome(reference,sql),bicdb=outcome(target,sql))
        row['matched'] = row['postgres'] == row['bicdb']
        report['cases'].append(row)
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(report['cases'])
    report['success'] = report['matched'] == report['total']
    return report


def main():
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'], autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'], autocommit=True) as target:
            report = run(reference,target)
    args.output.write_text(json.dumps(report,indent=2) + '\n')
    print(json.dumps({k:report[k] for k in ('success','matched','total')}))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
