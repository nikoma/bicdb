#!/usr/bin/env python3
"""Replay exported rows in isolated Oracle temporary tables after digest checks.

This reference checks full row values and counts FK orphans from the saved
export. It never repairs or changes the source application schema. Oracle is
used as a reference only; BicDB application transport remains PostgreSQL.
"""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import uuid

from foreign_keys import declarations
from foreign_key_orphans import count_orphans
from import_bicdb import decode, oracle_type, quote, save_report, source_row_hashes, verify_values
from temporal_types import declaration as temporal_declaration


def replay_columns(manifest):
    result = collections.defaultdict(list)
    tables = {table['name']: table for table in manifest['tables']}
    if len(tables) != len(manifest['tables']):
        raise ValueError('duplicate source table')
    for column in manifest['columns']:
        if column['table'] in tables:
            if column['virtual'] != 'NO':
                raise ValueError('virtual columns need a separately qualified replay policy')
            reference_type(column)
            result[column['table']].append(column)
    for name, table in tables.items():
        if not table['columns'] or [column['name'] for column in result[name]] != table['columns'] or len(set(table['columns'])) != len(table['columns']):
            raise ValueError('source column metadata differs from exported table projection')
    return result


def reference_type(column):
    if column['type'] == 'RAW':
        size = column['bytes']
        # Oracle NUMBER catalog fields can be exported as Decimal strings.
        if isinstance(size, str) and re.fullmatch('[1-9][0-9]*', size):size = int(size)
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise ValueError('invalid RAW byte width')
        return 'RAW(' + str(size) + ')'
    if column['type'].startswith('TIMESTAMP'):
        return temporal_declaration(column)[0]
    return oracle_type(column)


def table_mapping(manifest, nonce):
    if re.fullmatch('[0-9A-F]{12}', nonce) is None:
        raise ValueError('invalid replay namespace nonce')
    return {table['name']: 'BIC_FKR_' + nonce + '_' + format(index, '04X')
            for index, table in enumerate(manifest['tables'])}


def create_table_sql(name, columns):
    # Explicit inserts and readback qualify the data, not application DDL.
    # Defaults, constraints, triggers and indexes are deliberately not copied.
    return ('CREATE GLOBAL TEMPORARY TABLE ' + quote(name) + '(' +
            ','.join(quote(column['name']) + ' ' + reference_type(column) for column in columns) +
            ') ON COMMIT PRESERVE ROWS')


def cleanup_tables(cursor, mapping):
    errors = []
    for name in reversed(list(mapping.values())):
        try:cursor.execute('DROP TABLE ' + quote(name))
        except Exception as error:
            code = getattr(error.args[0], 'code', None) if error.args else None
            if code != 942:  # Never created, or removed by a prior cleanup.
                errors.append(dict(table=name, error_type=type(error).__name__, oracle_code=code))
    return errors


def replay(args):
    if not args.isolated_reference_schema:
        raise ValueError('replay requires --isolated-reference-schema in a separate test schema')
    source = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(source)
    plan, columns = declarations(manifest), replay_columns(manifest)
    cfg = json.loads(args.oracle_config.read_text())
    password_path = Path(cfg['password_file'])
    if not password_path.is_absolute():password_path = args.oracle_config.parent / password_path
    if args.report.resolve().is_relative_to(args.snapshot.resolve()) or args.report.resolve() in {args.oracle_config.resolve(), password_path.resolve()}:
        raise ValueError('reference report must not overwrite source inputs or connection configuration')
    if cfg['user'].upper() in {'SYS', 'SYSTEM', 'PDBADMIN', manifest['source_schema'].upper()}:
        raise ValueError('use a non-administrative reference owner distinct from the source schema')
    target = hashlib.sha256(json.dumps([cfg[key] for key in ['host', 'port', 'service_name', 'user']]).encode()).hexdigest()
    manifest_digest = hashlib.sha256(source).hexdigest()
    if args.cleanup_only:
        report = json.loads(args.report.read_text())
        if (report.get('phase') != 'oracle-export-reference' or report.get('format') != 1 or
                report.get('manifest_sha256') != manifest_digest or report.get('target_sha256') != target or
                report.get('table_map') != table_mapping(manifest, report.get('nonce', ''))):
            raise ValueError('cleanup journal does not identify this source and reference endpoint')
    else:
        nonce = uuid.uuid4().hex[:12].upper()
        report = dict(format=1, phase='oracle-export-reference', manifest_sha256=manifest_digest,
                      target_sha256=target, source_schema=manifest['source_schema'], nonce=nonce,
                      table_map=table_mapping(manifest, nonce), verified_tables=[], completed=[],
                      source_foreign_keys=len(plan), inventory_completed=False,
                      reference_rows_equal_export=False, cleanup_completed=False, cleanup_errors=[],
                      success=False, data_replay_only=True, live_source_compared=False,
                      target_inventory_compared=False, application_qualified=False)
        # Persist every possible owned table name before any Oracle DDL, so an
        # interrupted attempt can be cleaned even after an uncertain DDL result.
        save_report(args.report, report)
    import oracledb
    oracledb.defaults.fetch_decimals = True
    oracledb.defaults.fetch_lobs = False

    def connect():
        connection = oracledb.connect(user=cfg['user'], password=password_path.read_text().strip(),
                                      host=cfg['host'], port=cfg['port'], service_name=cfg['service_name'], disable_oob=True)
        connection.call_timeout = 30000
        return connection

    def check_owner(cursor):
        cursor.execute("SELECT SYS_CONTEXT('USERENV','SESSION_USER'),SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
        owner, schema = cursor.fetchone()
        if owner.upper() in {'SYS', 'SYSTEM', 'PDBADMIN', manifest['source_schema'].upper()} or owner != schema or owner != cfg['user'].upper():
            raise ValueError('Oracle session is not the configured isolated reference owner')
        return owner

    def cleanup():
        # The loading connection is closed first, releasing session-specific
        # rows and temporary-table bindings before DROP TABLE in a new session.
        with connect() as connection:
            with connection.cursor() as cursor:
                check_owner(cursor)
                report['cleanup_errors'] = cleanup_tables(cursor, report['table_map'])
        report['cleanup_completed'] = not report['cleanup_errors']

    if args.cleanup_only:
        report['cleanup_completed'] = False
        try:cleanup()
        finally:save_report(args.report, report)
        if not report['cleanup_completed']:raise ValueError('reference cleanup is incomplete')
        return report  # Cleanup never promotes an incomplete inventory to success.

    try:
        with connect() as connection:
            report['oracle_version'] = connection.version
            with connection.cursor() as cursor:
                owner = check_owner(cursor)
                cursor.arraysize = 500
                for index, table in enumerate(manifest['tables']):
                    source_row_hashes(args.snapshot, table)
                    name = report['table_map'][table['name']]
                    cursor.execute(create_table_sql(name, columns[table['name']]))
                    types = []
                    for column in columns[table['name']]:
                        kind = column['type']
                        if kind == 'NUMBER':types.append(oracledb.DB_TYPE_NUMBER)
                        elif kind in {'BLOB', 'CLOB', 'RAW', 'DATE'}:types.append(getattr(oracledb, 'DB_TYPE_' + kind))
                        elif kind.startswith('TIMESTAMP'):types.append(oracledb.DB_TYPE_TIMESTAMP_TZ if kind.endswith('WITH TIME ZONE') else oracledb.DB_TYPE_TIMESTAMP)
                        else:types.append(oracledb.DB_TYPE_VARCHAR)
                    sql = 'INSERT INTO ' + quote(name) + ' VALUES(' + ','.join(':' + str(i+1) for i in range(len(types))) + ')'
                    # Array-bind state must not be reused by the bind-free
                    # verification and counting queries on the read cursor.
                    with connection.cursor() as writer:
                        writer.setinputsizes(*types)
                        with (args.snapshot / table['file']).open() as rows:
                            batch = []
                            for line in rows:
                                batch.append([decode(value) for value in json.loads(line)])
                                if len(batch) == 500:writer.executemany(sql, batch);batch.clear()
                            if batch:writer.executemany(sql, batch)
                    digest = verify_values(cursor, args.snapshot, owner, dict(table, name=name))
                    report['verified_tables'].append(dict(table=table['name'], rows=table['rows'], sha256=digest))
                    if (index+1) % 32 == 0:
                        save_report(args.report, report)
                        print('oracle-reference-values: ' + str(index+1) + '/' + str(len(manifest['tables'])), flush=True)
                report['reference_rows_equal_export'] = True
                save_report(args.report, report)
                for index, item in enumerate(plan):
                    mapped = dict(item, table=report['table_map'][item['table']], parent_table=report['table_map'][item['parent_table']])
                    report['completed'].append(dict(declaration=item, orphan_rows=count_orphans(cursor, owner, mapped)))
                    if (index+1) % 32 == 0:
                        save_report(args.report, report)
                        print('oracle-reference-orphans: ' + str(index+1) + '/' + str(len(plan)), flush=True)
                report['inventory_completed'] = True
                report['validated_relationships_clean'] = all(item['orphan_rows'] == 0 or not item['declaration']['validated'] for item in report['completed'])
    finally:
        try:cleanup()
        finally:save_report(args.report, report)
    report['success'] = report['inventory_completed'] and report['cleanup_completed'] and report['validated_relationships_clean']
    save_report(args.report, report)
    if not report['success']:raise ValueError('reference replay or validated relationship counts are incomplete')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--isolated-reference-schema', action='store_true')
    parser.add_argument('--cleanup-only', action='store_true')
    replay(parser.parse_args())


if __name__ == '__main__':main()
