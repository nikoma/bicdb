#!/usr/bin/env python3
"""Compare bound-cursor values, types and errors on native PostgreSQL/BicDB.

Use dedicated databases through ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN.
Only uniquely named synthetic objects are created and removed by this runner.
"""
import argparse
import json
import os
from pathlib import Path
import uuid

from compare import canonical
from routine_cursors import fixture


def run(reference, target):
    plan = fixture('public', 'BICDB_CP_' + uuid.uuid4().hex[:8].upper(), native=True)
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  native_bicdb=True, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)
    connections = (reference, target)
    created = []
    try:
        for obj in plan['setup']:
            connection = connections[obj['backend']]
            connection.execute(obj['sql'])
            created.append(obj)
            for sql in obj['inserts']:
                connection.execute(sql)
        for case in plan['cases']:
            row = dict(id=case['id'])
            for label, connection in zip(('postgres', 'bicdb'), connections):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(case['sql'], prepare=False)
                        row[label] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()],
                                          type_oids=[v.type_code for v in cursor.description])
                except Exception as error:
                    if getattr(error, 'sqlstate', None) is None:
                        raise
                    row[label] = dict(sqlstate=error.sqlstate)
            row['matched'] = row['postgres'] == row['bicdb']
            if 'expected_error' in case:
                row['matched'] &= all(row[label].get('sqlstate') == case['expected_error'][label]
                                      for label in ('postgres', 'bicdb'))
            else:
                row['matched'] &= row['postgres'].get('rows') == case['expected']
            report['cases'].append(row)
    finally:
        for obj in reversed(created):
            try:
                connections[obj['backend']].execute('DROP ' + obj['kind'] + ' ' + obj['name'])
            except Exception as error:
                report['cleanup_errors'].append(dict(backend=obj['backend'], error=type(error).__name__,
                                                      sqlstate=getattr(error, 'sqlstate', None)))
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(plan['cases'])
    report['success'] = report['matched'] == report['total'] and not report['cleanup_errors']
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
