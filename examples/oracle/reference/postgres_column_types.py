#!/usr/bin/env python3
"""Compare native PostgreSQL temporal assignment, COPY, and quoted ALTER TYPE.

Use dedicated native databases. The runner owns and drops one unique table on
each server. DATE remains a PostgreSQL calendar date, with wire OID 1082.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import uuid

from compare import canonical


def run(reference, target):
    relation = '"public"."bicdb_native_columns_' + uuid.uuid4().hex[:12] + '"'
    report = dict(format=1, postgres_version_num=reference.info.server_version,
                  native_bicdb=True, application_qualified=False,
                  cases=[], cleanup_errors=[], success=False)
    created = []
    try:
        for connection in (reference, target):
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {relation}(k INTEGER PRIMARY KEY,d DATE,t TIMESTAMP,t0 TIMESTAMP(0),"Mixed.Stamp" TIMESTAMP(3))', prepare=False)
                created.append(connection)
                cursor.execute(f"INSERT INTO {relation} VALUES(0,TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999')", prepare=False)
                stamp = datetime.datetime(1999, 12, 31, 23, 59, 59, 999999)
                with cursor.copy(f'COPY {relation} FROM STDIN') as copy:
                    copy.write_row([1, stamp.date(), stamp, stamp, stamp])
                cursor.execute(f'ALTER TABLE {relation} ALTER COLUMN "Mixed.Stamp" TYPE TIMESTAMP(0)', prepare=False)
        queries = [
            ('values_after_copy_and_alter', f'SELECT * FROM {relation} ORDER BY k'),
            ('date_subtraction', f'SELECT d-d FROM {relation} ORDER BY k'),
            ('timestamp_subtraction', f'SELECT t-t0 FROM {relation} ORDER BY k'),
            ('date_has_no_time', f"SELECT d=DATE '2024-02-29' FROM {relation} WHERE k=0"),
            ('quoted_name_survives', f'SELECT "Mixed.Stamp" FROM {relation} ORDER BY k'),
            ('metadata', f'SELECT d,t,t0,"Mixed.Stamp" FROM {relation} WHERE false'),
        ]
        for name, sql in queries:
            row = dict(id=name, sql=sql)
            for label, connection in [('postgres', reference), ('bicdb', target)]:
                with connection.cursor() as cursor:
                    cursor.execute(sql, prepare=False)
                    row[label] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()],
                                      names=[v.name for v in cursor.description],
                                      type_oids=[v.type_code for v in cursor.description])
            row['matched'] = row['postgres'] == row['bicdb']
            report['cases'].append(row)
    finally:
        for connection in reversed(created):
            try:
                with connection.cursor() as cursor:
                    cursor.execute('DROP TABLE ' + relation, prepare=False)
            except Exception as error:
                report['cleanup_errors'].append(dict(error=type(error).__name__, code=getattr(error, 'sqlstate', None)))
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(report['cases'])
    report['success'] = report['matched'] == report['total'] and not report['cleanup_errors']
    return report


def main():
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'], autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'], autocommit=True) as target:
            report = run(reference, target)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('success', 'matched', 'total')}))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
