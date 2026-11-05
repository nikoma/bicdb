"""Reconcile explicit source BLOB declarations on a quiesced migration target.

Physical BYTEA and the logical BLOB label alone do not prove locator-capable
storage. Every unjournaled column is explicitly rewritten, even if already
labelled BLOB. Journals contain fingerprints, not row values or connection URLs.
"""
import hashlib
import json
import os
import uuid

from import_bicdb import quote, save_report
from temporal_types import column_types, physical_columns, table_fingerprint


def blob_columns(manifest):
    tables = {table['name']: table for table in manifest['tables']}
    if len(tables) != len(manifest['tables']):
        raise ValueError('duplicate source table')
    if any(len(t['columns']) != len(set(t['columns'])) for t in tables.values()):
        raise ValueError('duplicate source table projection column')
    result, seen = [], set()
    for column in manifest['columns']:
        if column['table'] not in tables or column['type'] != 'BLOB':
            continue  # Source catalogs also describe view projections.
        key = (column['table'], column['name'])
        if key in seen or column['name'] not in tables[key[0]]['columns']:
            raise ValueError('duplicate or missing source BLOB column')
        if column.get('virtual', 'NO') != 'NO':
            raise ValueError('virtual BLOB columns require expression migration')
        seen.add(key)
        result.append(column)
    return result


def validate_journal(previous, identity, columns):
    if any(previous.get(key) != value for key, value in identity.items()):
        raise ValueError('BLOB journal does not match source, target and phase')
    expected = [(c['table'], c['name']) for c in columns]
    completed = [(c['table'], c['column']) for c in previous['completed']]
    if completed != expected[:len(completed)] or len(completed) > len(expected):
        raise ValueError('BLOB journal is not an ordered completed source prefix')
    if any(c.get('declaration') != 'BLOB' for c in previous['completed']):
        raise ValueError('BLOB journal has a different completed declaration')
    return set(completed)


def probe_support(cursor, schema):
    """Check a real stored append, not just accepted declaration syntax."""
    relation = quote(schema) + '.' + quote('__bicdb_blob_probe_' + uuid.uuid4().hex)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + relation + '("ID" INTEGER PRIMARY KEY,"B" BLOB)', prepare=False)
        created = True
        cursor.execute('INSERT INTO ' + relation + ' VALUES(1,HEXTORAW(\'AB\'))', prepare=False)
        cursor.execute('DECLARE d BLOB; BEGIN SELECT "B" INTO d FROM ' + relation +
                       ' WHERE "ID"=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW(\'00FF\')); END;', prepare=False)
        cursor.execute('SELECT "B" FROM ' + relation, prepare=False)
        if cursor.fetchall() != [(bytes.fromhex('AB00FF'),)]:
            raise ValueError('target lacks persistent BLOB append semantics')
    finally:
        if created:
            try:
                cursor.execute('DELETE FROM ' + relation, prepare=False)
            finally:
                cursor.execute('DROP TABLE ' + relation, prepare=False)


def migrate(args):
    if not args.check_only and not args.writers_quiesced:
        raise ValueError('BLOB apply requires --writers-quiesced and a retained pre-upgrade backup')
    import psycopg
    data = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(data)
    schema, columns = manifest['source_schema'], blob_columns(manifest)
    by_table = {}
    # Keep source order for a verifiable resumable prefix, including interleaved tables.
    for column in columns:
        by_table.setdefault(column['table'], []).append(column)
    report = dict(format=1, phase='blob-types', source_schema=schema,
                  manifest_sha256=hashlib.sha256(data).hexdigest(),
                  completed=[], table_fingerprints={}, success=False,
                  application_qualified=False, check_only=args.check_only,
                  conversion_capability_verified=False, source_blob_columns=len(columns),
                  data_compared_to_source=False, source_row_count_differences=[])
    can_write_report = not args.resume
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                raise ValueError('target did not confirm the preview profile')
            if not connection.info.parameter_status('TimeZone'):
                raise ValueError('target must report TimeZone before binary migration preflight')
            # Scope the journal to the target endpoint without publishing credentials.
            endpoint = [connection.info.host, connection.info.port, connection.info.dbname, connection.info.user]
            report['target_sha256'] = hashlib.sha256(json.dumps(endpoint).encode()).hexdigest()
            identity = {key:report[key] for key in ['format','phase','source_schema','manifest_sha256','target_sha256']}
            completed = set()
            if args.resume:
                previous = json.loads(args.report.read_text())
                completed = validate_journal(previous, identity, columns)
                can_write_report = True
                report['completed'] = previous['completed']
                report['table_fingerprints'] = previous['table_fingerprints']
            with connection.cursor() as cursor:
                expected = {(c['table'], c['name']) for c in columns}
                physical = physical_columns(cursor, schema, expected)
                tables = {t['name']:t for t in manifest['tables']}
                # Inspect all source declarations and saved target rows before any probe/ALTER.
                for table, declared in by_table.items():
                    actual = column_types(cursor, schema, table, declared)
                    for c in declared:
                        key = (table, c['name'])
                        if physical.get(key, (None,))[0] != 17:
                            raise ValueError('target BLOB column is missing or not physical BYTEA')
                        if actual[c['name']] not in (None, 'BLOB'):
                            raise ValueError('target has a different logical BLOB declaration')
                        if key in completed and actual[c['name']] != 'BLOB':
                            raise ValueError('completed BLOB declaration is missing or changed')
                    before = table_fingerprint(cursor, schema, tables[table])
                    prior = report['table_fingerprints'].get(table)
                    if prior is not None and prior['before'] != before:
                        raise ValueError('target rows changed since BLOB reconciliation began')
                    report['table_fingerprints'][table] = dict(before=before)
                    if before['rows'] != tables[table]['rows']:
                        report['source_row_count_differences'].append(dict(
                            table=table, source_rows=tables[table]['rows'], target_rows=before['rows']))
                save_report(args.report, report)
                if not args.check_only and columns:
                    probe_support(cursor, schema)
                    report['conversion_capability_verified'] = True
                for c in columns:
                    key = (c['table'], c['name'])
                    if args.check_only:
                        continue
                    # Recheck/rewrite completed columns too: endpoint identity plus
                    # the public BLOB label cannot prove the storage generation.
                    cursor.execute('ALTER TABLE ' + quote(schema) + '.' + quote(c['table']) +
                                   ' MODIFY (' + quote(c['name']) + ' BLOB)', prepare=False)
                    if column_types(cursor, schema, c['table'], [c])[c['name']] != 'BLOB':
                        raise ValueError('target did not retain the BLOB declaration')
                    # Verify all table columns and duplicate multiplicity after each native commit.
                    after = table_fingerprint(cursor, schema, tables[c['table']])
                    if report['table_fingerprints'][c['table']]['before'] != after:
                        raise ValueError('target values changed during BLOB reconciliation')
                    if key not in completed:
                        report['completed'].append(dict(table=key[0], column=key[1], declaration='BLOB'))
                        completed.add(key)
                    save_report(args.report, report)
                for table in by_table:
                    after = table_fingerprint(cursor, schema, tables[table])
                    if report['table_fingerprints'][table]['before'] != after:
                        raise ValueError('target rows changed during BLOB reconciliation')
                    report['table_fingerprints'][table]['after'] = after
            report['success'] = args.check_only or len(completed) == len(columns)
    finally:
        if can_write_report:
            save_report(args.report, report)
    if not report['success']:
        raise ValueError('BLOB reconciliation is incomplete')
