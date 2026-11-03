"""Attach exported CHAR/VARCHAR2 units without changing existing target values.

The source environment must be explicitly qualified as AL32UTF8/STANDARD.
Run with target writers and schema changes stopped. Read-only preflight checks
all affected tables before any capability DDL; each table's apply transaction
checks its full row multiset before committing. This is not Oracle DDL implicit-
commit qualification or an atomic upgrade of the whole database.
"""
import hashlib
import json
import os
import re

from import_bicdb import quote, save_report, probe_character_column_support
from temporal_types import column_types, physical_columns, table_fingerprint


def integer(value):
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]+', str(value)):
        raise ValueError('invalid source character length')
    return int(value)


def declaration(column):
    kind = column['type']
    if kind not in {'CHAR', 'VARCHAR2'}:
        raise ValueError('unqualified source character type')
    unit = {'B': 'BYTE', 'C': 'CHAR'}.get(column.get('character_unit'))
    if unit is None:
        raise ValueError('source character length-unit provenance is required')
    length = integer(column['characters'])
    maximum = 2000 if kind == 'CHAR' else 4000
    if not 1 <= length <= maximum:
        raise ValueError('source character length exceeds the STANDARD profile bound')
    expected_bytes = length if unit == 'BYTE' else min(length * 4, maximum)
    if integer(column['bytes']) != expected_bytes:
        raise ValueError('source character byte width differs from AL32UTF8/STANDARD')
    if column.get('virtual') != 'NO':
        raise ValueError('virtual character columns require expression migration')
    return f'{kind}({length} {unit})', 1042 if kind == 'CHAR' else 1043, length + 4


def character_columns(manifest):
    tables = {table['name']: table for table in manifest['tables']}
    if len(tables) != len(manifest['tables']):
        raise ValueError('duplicate source table')
    seen, result = set(), []
    for column in manifest['columns']:
        if column['table'] not in tables:
            continue  # Exported column catalogs also contain view projections.
        if column['type'] in {'NCHAR', 'NVARCHAR2'}:
            raise ValueError('national table columns require separate qualification')
        if column['type'] not in {'CHAR', 'VARCHAR2'}:
            continue
        key = column['table'], column['name']
        if key in seen:
            raise ValueError('duplicate source character column')
        seen.add(key)
        if tables[key[0]]['columns'].count(key[1]) != 1:
            raise ValueError('source character declaration is absent or ambiguous in its table')
        declaration(column)
        result.append(column)
    return result


def validate_saved_value(column, value):
    """Require that ordinary assignment to the source type preserves this value."""
    if value is None:
        return
    label = column['table'] + '.' + column['name']
    if not isinstance(value, str) or not value:
        raise ValueError('target character value is not canonical: ' + label)
    length = integer(column['characters'])
    actual = len(value.encode('utf-8')) if column['character_unit'] == 'B' else len(value)
    byte_length = len(value.encode('utf-8'))
    maximum = 2000 if column['type'] == 'CHAR' else 4000
    if actual > length or byte_length > maximum:
        raise ValueError('target character value exceeds source capacity: ' + label)
    if column['type'] == 'CHAR' and min(length - actual, maximum - byte_length) != 0:
        raise ValueError('target fixed character padding differs from source: ' + label)


def inspect_table(cursor, schema, table, columns):
    relation = quote(schema) + '.' + quote(table['name'])
    cursor.execute('SELECT * FROM ' + relation + ' WHERE FALSE', prepare=False)
    if [column.name for column in cursor.description] != table['columns']:
        raise ValueError('target table columns differ from the source snapshot')
    positions = [(table['columns'].index(column['name']), column) for column in columns]
    def validate(row):
        for index, column in positions:
            validate_saved_value(column, row[index])
    return table_fingerprint(cursor, schema, table, validate_row=validate)


def validate_logical_types(actual, columns, completed):
    for column in columns:
        source = declaration(column)[0]
        value = actual[column['name']]
        if value is not None and value != source:
            raise ValueError('target has a different logical character declaration')
        if completed and value != source:
            raise ValueError('completed character declaration is missing or changed')


def migrate(args):
    import psycopg
    if args.source_character_set != 'AL32UTF8' or args.source_string_size != 'STANDARD':
        raise ValueError('explicit AL32UTF8 and STANDARD source qualification is required')
    data = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(data)
    schema = manifest['source_schema']
    columns = character_columns(manifest)
    by_table = {}
    for column in columns:
        by_table.setdefault(column['table'], []).append(column)
    tables = [table for table in manifest['tables'] if table['name'] in by_table]
    expected = {(column['table'], column['name']): declaration(column) for column in columns}
    report = dict(format=1, phase='character-types', profile='oracle-preview-v1',
                  source_schema=schema, manifest_sha256=hashlib.sha256(data).hexdigest(),
                  source_character_set=args.source_character_set, source_string_size=args.source_string_size,
                  check_only=args.check_only, completed=[], success=False, application_qualified=False,
                  source_character_columns=len(columns), source_character_tables=len(tables),
                  table_fingerprints={}, source_row_count_differences=[],
                  read_only_preflight_passed=False, metadata_upgrade_started=False)
    if args.resume:
        previous = json.loads(args.report.read_text())
        identity = ['format','phase','profile','source_schema','manifest_sha256',
                    'source_character_set','source_string_size','check_only']
        if any(previous.get(key) != report[key] for key in identity):
            raise ValueError('character journal does not match the source snapshot and mode')
        done = previous['completed']
        if done != [table['name'] for table in tables[:len(done)]]:
            raise ValueError('character journal is not a completed table prefix')
        report = previous
        report['success'] = False
        report['read_only_preflight_passed'] = False
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                raise ValueError('target did not confirm the durable preview profile')
            endpoint = dict(host=connection.info.host, port=connection.info.port,
                            database=connection.info.dbname, user=connection.info.user)
            endpoint_hash = hashlib.sha256(json.dumps(endpoint,sort_keys=True).encode()).hexdigest()
            if args.resume and report.get('endpoint_sha256') != endpoint_hash:
                raise ValueError('character journal targets a different endpoint')
            report['endpoint_sha256'] = endpoint_hash
            with connection.cursor() as cursor:
                cursor.execute('SELECT bicdb_oracle_column_type(NULL,NULL)')
                if cursor.fetchone() != (None,):
                    raise ValueError('target lacks logical column metadata access')
                physical = physical_columns(cursor, schema, expected)
                report['source_row_count_differences'] = []
                for table in tables:
                    name = table['name']
                    actual = column_types(cursor, schema, name, by_table[name])
                    validate_logical_types(actual, by_table[name], name in report['completed'])
                    for column in by_table[name]:
                        _, oid, modifier = expected[name, column['name']]
                        if physical.get((name,column['name'])) != (oid,modifier):
                            raise ValueError('target physical character type differs from source')
                    before = inspect_table(cursor, schema, table, by_table[name])
                    prior = report['table_fingerprints'].get(name)
                    if prior is not None and prior['before'] != before:
                        raise ValueError('target values changed since character reconciliation began')
                    report['table_fingerprints'].setdefault(name, dict(before=before))
                    if before['rows'] != table['rows']:
                        report['source_row_count_differences'].append(dict(
                            table=name,source_rows=table['rows'],target_rows=before['rows']))
                report['read_only_preflight_passed'] = True
                save_report(args.report, report)
                if args.check_only or not tables:
                    report['success'] = True
                    return
                # All application tables have passed read-only validation before
                # a probe or ALTER can advance the durable profile format.
                report['metadata_upgrade_started'] = True
                save_report(args.report, report)
                probe_character_column_support(cursor, schema)
                for table in tables:
                    name = table['name']
                    if name in report['completed']:
                        continue  # This run rechecked its provenance and values above.
                    with connection.transaction():
                        before = inspect_table(cursor, schema, table, by_table[name])
                        if before != report['table_fingerprints'][name]['before']:
                            raise ValueError('target values changed after character preflight')
                        actual = column_types(cursor, schema, name, by_table[name])
                        validate_logical_types(actual, by_table[name], False)
                        relation = quote(schema) + '.' + quote(name)
                        for column in by_table[name]:
                            if actual[column['name']] is None:
                                source = expected[name,column['name']][0]
                                cursor.execute('ALTER TABLE ' + relation + ' MODIFY (' + quote(column['name']) + ' ' + source + ')',prepare=False)
                        actual = column_types(cursor, schema, name, by_table[name])
                        validate_logical_types(actual, by_table[name], True)
                        after = inspect_table(cursor, schema, table, by_table[name])
                        if before != after:
                            raise ValueError('target values changed during character reconciliation')
                    report['table_fingerprints'][name]['after'] = after
                    report['completed'].append(name)
                    save_report(args.report, report)
                    print(f'character-types: {name} ({len(report["completed"])}/{len(tables)})',flush=True)
                report['success'] = len(report['completed']) == len(tables)
    finally:
        save_report(args.report, report)
    if not report['success']:
        raise ValueError('character reconciliation is incomplete')
