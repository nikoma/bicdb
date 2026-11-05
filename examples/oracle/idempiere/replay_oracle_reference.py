#!/usr/bin/env python3
"""Replay exported rows in isolated Oracle temporary tables after digest checks.

This reference checks full row values and counts FK orphans from the saved
export. It never repairs or changes the source application schema. Oracle is
used as a reference only; BicDB application transport remains PostgreSQL.
"""
import argparse
import collections
import datetime
import hashlib
import json
from pathlib import Path
import re
import uuid

from foreign_keys import declarations
from export_reference import value_projection, decode_reference_row
from foreign_key_orphans import count_orphans
from import_bicdb import decode, oracle_type, quote, row_hash, save_report, source_row_hashes
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


def reference_bindings(columns, driver, empty_columns=()):
    expressions, types = [], []
    for index, column in enumerate(columns):
        kind = column['type']
        if index in empty_columns:
            expressions.append('EMPTY_' + kind + '()')
            continue
        position = len(types) + 1
        if kind.endswith('WITH TIME ZONE'):
            expressions.append("TO_TIMESTAMP_TZ(:" + str(position) + ", 'YYYY-MM-DD HH24:MI:SS.FF6TZH:TZM')")
            types.append(driver.DB_TYPE_VARCHAR)
        else:
            expressions.append(':' + str(position))
            key = kind if kind in {'NUMBER', 'RAW', 'DATE', 'BLOB', 'CLOB'} else ('TIMESTAMP' if kind.startswith('TIMESTAMP') else 'VARCHAR')
            types.append(getattr(driver, 'DB_TYPE_' + key))
    return expressions, types


def encode_reference_row(row, columns, lob_value=None, empty_columns=()):
    result = []
    for index, (value, column) in enumerate(zip(row, columns, strict=True)):
        if index in empty_columns:continue
        kind = column['type']
        if kind in {'BLOB', 'CLOB'}:
            if value == (b'' if kind == 'BLOB' else ''):raise ValueError('empty Oracle LOB requires its SQL constructor')
            if value is not None and lob_value is None:raise ValueError('Oracle LOB requires a locator')
            result.append(lob_value(kind, value) if value is not None else None)
        elif kind.endswith('WITH TIME ZONE') and value is not None:
            if not isinstance(value, datetime.datetime) or value.utcoffset() is None:
                raise ValueError('source zoned timestamp must retain an explicit offset')
            result.append(value.isoformat(sep=' ', timespec='microseconds'))
        else:result.append(value)
    return result


def insert_reference_batch(connection, name, columns, rows, driver, lob_value):
    groups = collections.defaultdict(list)
    for row in rows:
        empty = tuple(i for i, (value, column) in enumerate(zip(row, columns, strict=True))
                      if column['type'] in {'BLOB', 'CLOB'} and value == (b'' if column['type'] == 'BLOB' else ''))
        groups[empty].append(row)
    for empty, group in groups.items():
        expressions, types = reference_bindings(columns, driver, empty)
        sql = 'INSERT INTO ' + quote(name) + ' VALUES(' + ','.join(expressions) + ')'
        with connection.cursor() as writer:
            if types:
                writer.setinputsizes(*types)
                writer.executemany(sql, [encode_reference_row(row, columns, lob_value, empty) for row in group])
            else:
                for _ in group:writer.execute(sql)


def verify_reference_values(cursor, snapshot, owner, table, columns):
    expected = source_row_hashes(snapshot, table)
    cursor.execute('SELECT ' + value_projection(columns) + ' FROM ' + quote(owner) + '.' + quote(table['name']), fetch_lobs=True)
    actual = []
    while rows := cursor.fetchmany(500):
        actual.extend(row_hash(decode_reference_row(row, columns)) for row in rows)
    actual.sort()
    if actual != expected:raise ValueError('Oracle reference row values differ from exported table ' + table['name'])
    digest = hashlib.sha256()
    for value in actual:digest.update(value)
    return digest.hexdigest()


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
            def lob_value(kind, value):
                lob = connection.createlob(getattr(oracledb, 'DB_TYPE_' + kind))
                lob.write(value)
                return lob
            with connection.cursor() as cursor:
                owner = check_owner(cursor)
                cursor.arraysize = 500
                for index, table in enumerate(manifest['tables']):
                    source_row_hashes(args.snapshot, table)
                    name = report['table_map'][table['name']]
                    cursor.execute(create_table_sql(name, columns[table['name']]))
                    # Array-bind state must not be reused by the bind-free
                    # verification and counting queries on the read cursor.
                    with (args.snapshot / table['file']).open() as rows:
                        batch = []
                        for line in rows:
                            batch.append([decode(value) for value in json.loads(line)])
                            if len(batch) == 500:
                                insert_reference_batch(connection, name, columns[table['name']], batch, oracledb, lob_value)
                                batch.clear()
                        if batch:insert_reference_batch(connection, name, columns[table['name']], batch, oracledb, lob_value)
                    digest = verify_reference_values(cursor, args.snapshot, owner, dict(table, name=name), columns[table['name']])
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
