"""Reconcile declared Oracle temporal types against an exported schema.

Only explicit source declarations can distinguish a legacy DATE column from a
physical timestamp(0). Each ALTER uses normal ownership, row, constraint and
index validation. The journal records no connection information or row values.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from import_bicdb import quote, save_report


def declaration(column):
    kind = column['type']
    if kind == 'DATE':
        return 'DATE', 1114, {0}
    match = re.fullmatch(r'TIMESTAMP(?:\(([0-6])\))?( WITH TIME ZONE)?', kind)
    if not match:
        raise ValueError('unqualified Oracle temporal declaration: ' + kind)
    precision = int(match[1] or 6)
    suffix = match[2] or ''
    return f'TIMESTAMP({precision}){suffix}', 1184 if suffix else 1114, {precision, -1} if precision == 6 else {precision}


def temporal_columns(manifest):
    tables = {table['name']: table for table in manifest['tables']}
    result = []
    seen = set()
    for column in manifest['columns']:
        if column['table'] not in tables:
            continue  # ALL_TAB_COLUMNS also includes view projections.
        if column['type'] != 'DATE' and not column['type'].startswith('TIMESTAMP'):
            continue
        key = (column['table'], column['name'])
        if key in seen:
            raise ValueError('duplicate source temporal column')
        seen.add(key)
        if column['name'] not in tables[column['table']]['columns']:
            raise ValueError('temporal declaration is absent from its exported table')
        declaration(column)
        result.append(column)
    return result


def current_type(cursor, schema, table, column):
    relation = quote(schema) + '.' + quote(table)
    cursor.execute('SELECT bicdb_oracle_column_type(%s,%s)', (relation, column))
    row = cursor.fetchone()
    if row is None:
        raise ValueError('missing target temporal metadata result')
    return row[0]


def check_physical_type(cursor, schema, table, column, expected_oid, expected_modifiers):
    cursor.execute('SELECT a."atttypid",a."atttypmod" FROM "pg_catalog"."pg_attribute" a '
                   'JOIN "pg_catalog"."pg_class" c ON c."oid"=a."attrelid" '
                   'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=c."relnamespace" '
                   'WHERE n."nspname"=%s AND c."relname"=%s AND a."attname"=%s '
                   'AND NOT a."attisdropped"', (schema, table, column))
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != expected_oid or rows[0][1] not in expected_modifiers:
        raise ValueError('target physical temporal type differs from the source declaration')


def probe_support(cursor, schema):
    # Verify the accessor before any DDL, so an old server cannot certify its
    # legacy timestamp mapping as logical DATE support.
    cursor.execute('SELECT bicdb_oracle_column_type(NULL,NULL)')
    if cursor.fetchone() != (None,):
        raise ValueError('target does not expose the temporal metadata contract')
    name = 'BICDB_TEMPORAL_' + uuid.uuid4().hex[:12].upper()
    relation = quote(schema) + '.' + quote(name)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + relation + '("D" DATE,"T" TIMESTAMP(0),"Z" TIMESTAMP(6) WITH TIME ZONE)', prepare=False)
        created = True
        for column, expected in [('D','DATE'), ('T','TIMESTAMP(0)'), ('Z','TIMESTAMP(6) WITH TIME ZONE')]:
            if current_type(cursor, schema, name, column) != expected:
                raise ValueError('target lacks durable temporal provenance')
        cursor.execute('INSERT INTO ' + relation + " VALUES(TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999',NULL)", prepare=False)
        cursor.execute('SELECT "D","T","Z" FROM ' + relation, prepare=False)
        values = cursor.fetchone()
        if values[0].isoformat() != '2024-02-29T23:59:59' or values[1].isoformat() != '2024-03-01T00:00:00' or values[2] is not None:
            raise ValueError('target temporal assignment behavior differs from Oracle')
        if [column.type_code for column in cursor.description] != [1114,1114,1184]:
            raise ValueError('target temporal PostgreSQL transport types differ')
    finally:
        if created:
            cursor.execute('DROP TABLE ' + relation, prepare=False)


def migrate(args):
    import psycopg
    data = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(data)
    fingerprint = hashlib.sha256(data).hexdigest()
    schema = manifest['source_schema']
    columns = temporal_columns(manifest)
    expected = {(column['table'], column['name']): declaration(column)[0] for column in columns}
    report = dict(format=1, phase='types', source_schema=schema, manifest_sha256=fingerprint,
                  completed=[], success=False, application_qualified=False,
                  source_temporal_columns=len(columns), validated_tables=0)
    if args.resume and args.report.exists():
        previous = json.loads(args.report.read_text())
        if any(previous.get(key) != report[key] for key in ['format','phase','source_schema','manifest_sha256']):
            raise ValueError('temporal journal does not match the source snapshot')
        for row in previous['completed']:
            if expected.get((row['table'],row['column'])) != row['declaration']:
                raise ValueError('temporal journal contains an unexpected declaration')
        report = previous
        report['success'] = False
    completed = {(row['table'],row['column']) for row in report['completed']}
    by_table = {}
    for column in columns:
        by_table.setdefault(column['table'], []).append(column)
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                raise ValueError('target did not confirm the durable preview profile')
            with connection.cursor() as cursor:
                probe_support(cursor, schema)
                report['validated_tables'] = 0
                for table in manifest['tables']:
                    if table['name'] not in by_table:
                        continue
                    relation = quote(schema) + '.' + quote(table['name'])
                    cursor.execute('SELECT COUNT(*) FROM ' + relation, prepare=False)
                    if int(cursor.fetchone()[0]) != table['rows']:
                        raise ValueError('target row count differs from the source snapshot')
                    for column in by_table[table['name']]:
                        key = (table['name'], column['name'])
                        source, oid, modifiers = declaration(column)
                        actual = current_type(cursor, schema, *key)
                        if key in completed and actual != source:
                            raise ValueError('completed temporal declaration is missing or changed')
                        if actual is not None and actual != source:
                            raise ValueError('target has a different logical temporal declaration')
                        check_physical_type(cursor, schema, *key, oid, modifiers)
                        if actual is None:
                            cursor.execute('ALTER TABLE ' + relation + ' MODIFY (' + quote(column['name']) + ' ' + source + ')', prepare=False)
                            if current_type(cursor, schema, *key) != source:
                                raise ValueError('target did not retain the source temporal declaration')
                        if key not in completed:
                            report['completed'].append(dict(table=key[0],column=key[1],declaration=source))
                            completed.add(key)
                            save_report(args.report, report)
                    cursor.execute('SELECT COUNT(*) FROM ' + relation, prepare=False)
                    if int(cursor.fetchone()[0]) != table['rows']:
                        raise ValueError('row count changed during temporal reconciliation')
                    report['validated_tables'] += 1
                    print(f'types: {table["name"]} ({len(completed)}/{len(columns)})', flush=True)
                    save_report(args.report, report)
        report['success'] = len(completed) == len(columns)
    finally:
        save_report(args.report, report)
    if not report['success']:
        raise ValueError('temporal reconciliation is incomplete')
