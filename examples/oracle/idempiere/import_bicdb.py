#!/usr/bin/env python3
"""Import the exported Oracle application into an isolated BicDB preview database.

Each phase records its own result. Tables/data alone are not application
acceptance. The foreign-key phase verifies source validation and deferment state;
orphan inventory and full application qualification remain separate gates.
"""
import argparse
import base64
import collections
import datetime
import decimal
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def oracle_type(column):
    kind = column['type']
    if kind == 'NUMBER':
        precision, scale = column['precision'], column['scale']
        if precision is None and scale is None: return 'NUMBER'
        return f"NUMBER({int(precision) if precision is not None else 38},{int(scale or 0)})"
    if kind in {'VARCHAR2', 'CHAR'}:
        unit = {'B': 'BYTE', 'C': 'CHAR'}.get(column.get('character_unit'))
        if unit is None:
            raise ValueError('character length-unit provenance is required')
        return f"{kind}({int(column['characters'])} {unit})"
    if kind in {'DATE', 'CLOB', 'BLOB'} or kind.startswith('TIMESTAMP('): return kind
    if kind == 'RAW': return 'BYTEA'
    raise ValueError(f"unsupported source column type {kind}")


def table_ddl(schema, table, columns, constraints):
    parts = []
    for column in columns:
        if column['virtual'] != 'NO':
            raise ValueError('virtual columns require explicit expression migration')
        definition = quote(column['name']) + ' ' + oracle_type(column)
        if column['default'] is not None:
            definition += ' DEFAULT ' + column['default'].strip()
        if column['nullable'] == 'N': definition += ' NOT NULL'
        parts.append(definition)
        if column['type'] == 'RAW':
            value = quote(column['name'])
            if column['type'] == 'RAW':
                # CHECK evaluation needs the binary overload explicitly; the
                # storage representation is hexadecimal PostgreSQL bytea text.
                value = f'CAST({value} AS BYTEA)'
            parts.append(f"CHECK(octet_length({value}) <= {int(column['bytes'])})")
    for constraint in constraints:
        if constraint['status'] != 'ENABLED':
            raise ValueError('disabled source constraints require a separate policy')
        kind = constraint['kind']
        if kind == 'R': continue
        if constraint['deferrable'] != 'NOT DEFERRABLE':
            raise ValueError('deferrable unique/check constraints require equivalent target support')
        prefix = 'CONSTRAINT ' + quote(constraint['name']) + ' '
        if kind in {'P', 'U'}:
            keys = ','.join(quote(column) for column in constraint['columns'])
            parts.append(prefix + ('PRIMARY KEY' if kind == 'P' else 'UNIQUE') + f'({keys})')
        elif kind == 'C':
            condition = constraint['condition']
            if condition is None: raise ValueError('missing source check condition')
            # Oracle represents NOT NULL as both a column property and a check.
            # Keep it as the native column property, avoiding duplicate guards.
            anchored = re.fullmatch(r'"((?:[^"]|"")+)"\s+IS\s+NOT\s+NULL', condition.strip(), re.IGNORECASE)
            if anchored and any(c['name'] == anchored.group(1).replace('""', '"') and c['nullable'] == 'N' for c in columns):
                continue
            parts.append(prefix + 'CHECK(' + condition + ')')
        else:
            raise ValueError(f'unsupported source constraint kind {kind}')
    return 'CREATE TABLE ' + quote(schema) + '.' + quote(table) + '(' + ','.join(parts) + ')'


def nullable_composite_constraints(columns, constraints):
    nullable = {column['name']: column['nullable'] != 'N' for column in columns}
    return [(constraint, all(nullable[name] for name in constraint['columns']))
            for constraint in constraints
            if constraint['kind'] == 'U' and len(constraint['columns']) > 1
            and any(nullable[name] for name in constraint['columns'])]


def verify_constraint_null_policies(cursor, schema, table, columns, constraints):
    """Do not let an old import journal certify weaker NULL uniqueness rules."""
    verified = []
    for constraint, all_nullable in nullable_composite_constraints(columns, constraints):
        cursor.execute('SELECT "nulls_distinct" FROM "information_schema"."table_constraints" '
                       'WHERE "constraint_schema"=%s AND "table_name"=%s '
                       'AND "constraint_name"=%s AND "constraint_type"=%s',
                       (schema, table, constraint['name'], 'UNIQUE'))
        expected = None if all_nullable else 'NO'
        if cursor.fetchall() != [(expected,)]:
            raise ValueError('target constraint lacks Oracle nullable-key semantics: ' + constraint['name'])
        cursor.execute('SELECT i."indisvalid",i."indisready",i."indnullsnotdistinct",'
                       'i."indpred" IS NOT NULL FROM "pg_catalog"."pg_constraint" c '
                       'JOIN "pg_catalog"."pg_index" i ON i."indexrelid"=c."conindid" '
                       'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=c."connamespace" '
                       'WHERE n."nspname"=%s AND c."conname"=%s', (schema, constraint['name']))
        if cursor.fetchall() != [(True, True, True, all_nullable)]:
            raise ValueError('target constraint catalog does not confirm an executable native NULL-aware index: ' + constraint['name'])
        verified.append(constraint['name'])
    return verified


def probe_constraint_null_support(cursor, schema):
    """Qualify ordinary Oracle UNIQUE syntax, including the wholly NULL exception."""
    import uuid
    name = 'BICDB_CONSTRAINT_PROBE_' + uuid.uuid4().hex[:12].upper()
    table = quote(schema) + '.' + quote(name)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + table + '(a NUMBER,b VARCHAR2(20),'
                       'CONSTRAINT ' + quote(name + '_UQ') + ' UNIQUE(a,b))', prepare=False)
        created = True
        cursor.execute('INSERT INTO ' + table + " VALUES(NULL,NULL),(NULL,NULL),(1,NULL)", prepare=False)
        try:
            cursor.execute('INSERT INTO ' + table + ' VALUES(1.00,NULL)', prepare=False)
        except Exception as error:
            if getattr(error, 'sqlstate', None) != '23505': raise
        else:
            raise ValueError('target accepts duplicate partially NULL Oracle unique keys')
    finally:
        if created:
            cursor.execute('DROP TABLE ' + table, prepare=False)


def verify_character_column_units(cursor, schema, table, columns):
    columns = [column for column in columns if column['type'] in {'VARCHAR2', 'CHAR'}]
    if not columns: return []
    relation = quote(schema) + '.' + quote(table)
    expressions = ','.join('bicdb_oracle_column_type(%s,%s)' for _ in columns)
    arguments = [value for column in columns for value in (relation, column['name'])]
    cursor.execute('SELECT ' + expressions, arguments)
    row = cursor.fetchone()
    expected = tuple(oracle_type(column) for column in columns)
    if row is None or tuple(row) != expected:
        raise ValueError('target character provenance differs; reconcile legacy columns before resuming: ' + table)
    return [table + '.' + column['name'] for column in columns]


def probe_character_column_support(cursor, schema):
    """Require byte capacity and byte-aware fixed padding before importing data."""
    import uuid
    name = 'BICDB_CHARACTER_PROBE_' + uuid.uuid4().hex[:12].upper()
    table = quote(schema) + '.' + quote(name)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + table + '(b VARCHAR2(1 BYTE),c VARCHAR2(1 CHAR),f CHAR(3 BYTE))', prepare=False)
        created = True
        try:
            cursor.execute('INSERT INTO ' + table + '(b) VALUES(%s)', ('😀',), prepare=False)
        except Exception as error:
            if getattr(error, 'sqlstate', None) != '22001': raise
        else:
            raise ValueError('target does not enforce Oracle byte capacity')
        cursor.execute('INSERT INTO ' + table + '(c,f) VALUES(%s,%s)', ('😀','é'), prepare=False)
        cursor.execute('SELECT c,f FROM ' + table, prepare=False)
        if cursor.fetchall() != [('😀','é ')]:
            raise ValueError('target does not preserve character units and byte-aware padding')
    finally:
        if created: cursor.execute('DROP TABLE ' + table, prepare=False)


def decode(value):
    if not isinstance(value, dict): return value
    if set(value) == {'number'}: return decimal.Decimal(value['number'])
    if set(value) == {'timestamp'}: return datetime.datetime.fromisoformat(value['timestamp'])
    if set(value) == {'bytes'}: return base64.b64decode(value['bytes'], validate=True)
    raise ValueError('unknown exported value encoding')


def canonical(value):
    if value is None or isinstance(value, (str, bool)): return value
    if isinstance(value, (decimal.Decimal, int)):
        number = decimal.Decimal(value)
        text = format(number, 'f')
        if '.' in text: text = text.rstrip('0').rstrip('.')
        return {'number': '0' if number == 0 else text}
    if isinstance(value, datetime.datetime): return {'timestamp': value.isoformat(sep=' ')}
    if isinstance(value, bytes): return {'bytes': base64.b64encode(value).decode('ascii')}
    raise TypeError(f'unsupported target value type {type(value).__name__}')


def row_hash(row):
    return hashlib.sha256(json.dumps([canonical(value) for value in row], ensure_ascii=False,
                                   separators=(',', ':')).encode()).digest()


def source_row_hashes(snapshot, table, *, copy_to=None):
    """Validate the exact bytes, projection and row count of one exported file."""
    columns = table['columns']
    count = table['rows']
    if (not isinstance(columns, list) or not columns or
            any(not isinstance(column, str) or not column for column in columns) or
            len(columns) != len(set(columns))):
        raise ValueError('invalid snapshot table projection')
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError('invalid snapshot table row count')
    path = snapshot / table['file']
    if path.resolve().parent != snapshot.resolve(): raise ValueError('invalid snapshot row path')
    expected, digest = [], hashlib.sha256()
    with path.open('rb') as source:
        for line in source:
            digest.update(line)
            values = json.loads(line)
            if not isinstance(values, list) or len(values) != len(columns):
                raise ValueError('snapshot row width differs from table projection')
            expected.append(row_hash([decode(value) for value in values]))
            if copy_to is not None:
                copy_to.write(line)
    if digest.hexdigest() != table['sha256']:
        raise ValueError('snapshot row file digest mismatch')
    if len(expected) != count:
        raise ValueError('snapshot row count differs from table manifest')
    expected.sort()
    return expected


def copy_table(connection, cursor, snapshot, schema, table):
    # Keep one private on-disk copy of the bytes checked by the shared validator.
    # Reopening the exported path after hashing would permit a different file to
    # be consumed by COPY. Do not open target COPY until all source checks pass.
    with tempfile.TemporaryFile(mode='w+b') as frozen:
        source_row_hashes(snapshot, table, copy_to=frozen)
        frozen.seek(0)
        relation = quote(schema) + '.' + quote(table['name'])
        columns = ','.join(quote(name) for name in table['columns'])
        with connection.transaction():
            cursor.execute('SELECT COUNT(*) FROM ' + relation)
            if cursor.fetchone() != (0,):
                raise ValueError('data import requires an empty uncompleted target table')
            with cursor.copy('COPY ' + relation + '(' + columns + ') FROM STDIN') as copy:
                for line in frozen:
                    copy.write_row([decode(value) for value in json.loads(line)])
            cursor.execute('SELECT COUNT(*) FROM ' + relation)
            if cursor.fetchone() != (table['rows'],):
                raise ValueError('target row count differs')
        # The caller advances its journal only after the COPY transaction commits.


def verify_values(cursor, snapshot, schema, table):
    # Sorting row digests preserves multiplicity without assuming that the two
    # engines enumerate heap rows in the same order. No float/Decimal-context
    # rounding or whitespace trimming is allowed in this comparison.
    expected = source_row_hashes(snapshot, table)
    columns = ','.join(quote(name) for name in table['columns'])
    cursor.execute('SELECT ' + columns + ' FROM ' + quote(schema) + '.' + quote(table['name']))
    actual = []
    while batch := cursor.fetchmany(500): actual.extend(row_hash(row) for row in batch)
    actual.sort()
    if actual != expected: raise ValueError(f"target values differ in {table['name']}")
    digest = hashlib.sha256()
    for value in actual: digest.update(value)
    return digest.hexdigest()


def save_report(path, report):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as target:
        json.dump(report, target, indent=2)
        target.write('\n')
        target.flush()
        os.fsync(target.fileno())
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True, type=Path)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--phase', choices=['tables', 'data', 'verify', 'indexes', 'types', 'character-types', 'blob-types', 'foreign-keys', 'routines', 'views'], required=True)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--resume', action='store_true', help='validate the prior journal and completed objects before resuming')
    parser.add_argument('--source-character-set', choices=['AL32UTF8'], help='qualified source environment for character-types')
    parser.add_argument('--source-string-size', choices=['STANDARD'], help='qualified source environment for character-types')
    parser.add_argument('--check-only', action='store_true', help='read-only character-types/blob-types/foreign-keys preflight without probe DDL or ALTER')
    parser.add_argument('--writers-quiesced', action='store_true', help='confirm target writers and schema changes are stopped for BLOB, foreign-key, or stored-code installation')
    args = parser.parse_args()
    if args.phase in {'routines', 'views'}:
        if args.check_only or args.source_character_set or args.source_string_size:
            parser.error('stored-code phases do not accept character environment or check-only options')
        if not args.writers_quiesced:
            parser.error('stored-code installation requires --writers-quiesced')
        from install_schema_code import migrate
        return migrate(args)
    if args.phase == 'foreign-keys':
        if args.source_character_set or args.source_string_size:
            parser.error('character environment options require --phase character-types')
        from foreign_keys import migrate
        return migrate(args)
    if args.phase == 'blob-types':
        if args.source_character_set or args.source_string_size:
            parser.error('character environment options require --phase character-types')
        from blob_types import migrate
        return migrate(args)
    if args.writers_quiesced:
        parser.error('--writers-quiesced requires --phase blob-types or foreign-keys')
    if args.phase == 'character-types':
        from character_types import migrate
        return migrate(args)
    if args.check_only or args.source_character_set or args.source_string_size:
        parser.error('character environment and check-only options require --phase character-types')
    if args.phase == 'types':
        from temporal_types import migrate
        return migrate(args)
    if args.phase == 'indexes':
        from indexes import migrate
        return migrate(args)
    import psycopg
    manifest_bytes = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    fingerprint = hashlib.sha256(manifest_bytes).hexdigest()
    schema = manifest['source_schema']
    by_table, checks = collections.defaultdict(list), collections.defaultdict(list)
    for column in manifest['columns']: by_table[column['table']].append(column)
    for constraint in manifest['constraints']: checks[constraint['table']].append(constraint)
    report = dict(format=1, phase=args.phase, completed=[], success=False,
                   schema_manifest_sha256=fingerprint,
                   application_qualified=False, profile='oracle-preview-v1',
                   adaptations=['physical table/index storage replaced by native storage',
                                'LOB values mapped to native text/bytea',
                                'source NOT NULL constraints represented by native column properties'],
                   outstanding=['source foreign keys with validation/deferment state',
                                'secondary indexes', 'types and routines', 'views',
                                'application driver integration and workflows'])
    if args.resume:
        previous = json.loads(args.report.read_text())
        if previous.get('schema_manifest_sha256') != fingerprint or previous['phase'] != args.phase or previous['profile'] != report['profile']:
            raise ValueError('resume journal does not match this snapshot, profile and phase')
        completed = previous['completed']
        if completed != [table['name'] for table in manifest['tables'][:len(completed)]]:
            raise ValueError('resume journal is not a completed prefix of this snapshot')
        report = previous
        report['success'] = False
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                raise RuntimeError('server did not confirm preview SQL profile')
            target = dict(host=connection.info.host, port=connection.info.port, database=connection.info.dbname, user=connection.info.user)
            if args.resume and report.get('target') != target:
                raise ValueError('resume journal targets a different server connection')
            report['target'] = target
            with connection.cursor() as cursor:
                if args.phase == 'tables' and not args.resume:
                    cursor.execute('CREATE SCHEMA ' + quote(schema))
                if args.phase == 'tables' and args.resume:
                    # Reject legacy provenance before a capability probe can
                    # advance the target's durable metadata format.
                    for table in manifest['tables']:
                        if table['name'] in report['completed']:
                            verify_character_column_units(cursor,schema,table['name'],by_table[table['name']])
                if args.phase == 'tables':
                    probe_character_column_support(cursor, schema)
                    probe_constraint_null_support(cursor, schema)
                report['verified_nullable_unique_constraints'] = []
                report['verified_character_columns'] = []
                for table in manifest['tables']:
                    name = table['name']
                    if args.phase != 'tables' or name in report['completed']:
                        report['verified_character_columns'].extend(verify_character_column_units(cursor,schema,name,by_table[name]))
                        report['verified_nullable_unique_constraints'].extend(verify_constraint_null_policies(
                            cursor, schema, name, by_table[name], checks[name]))
                    if name in report['completed']:
                        cursor.execute('SELECT * FROM ' + quote(schema) + '.' + quote(name) + ' WHERE FALSE')
                        if [description.name for description in cursor.description] != table['columns']:
                            raise ValueError('completed target table columns differ from snapshot')
                        if args.phase == 'verify':
                            digest = verify_values(cursor, args.snapshot, schema, table)
                            if report.get('value_digests', {}).get(name) != digest:
                                raise ValueError('completed value verification has no matching saved digest')
                        else:
                            cursor.execute('SELECT COUNT(*) FROM ' + quote(schema) + '.' + quote(name))
                            expected = 0 if args.phase == 'tables' else table['rows']
                            if int(cursor.fetchone()[0]) != expected:
                                raise ValueError('completed target table row count differs from journal phase')
                        continue
                    print(f"{args.phase}: {name}", flush=True)
                    if args.phase == 'tables':
                        cursor.execute(table_ddl(schema, name, by_table[name], checks[name]), prepare=False)
                        report['verified_character_columns'].extend(verify_character_column_units(cursor,schema,name,by_table[name]))
                        report['verified_nullable_unique_constraints'].extend(verify_constraint_null_policies(
                            cursor, schema, name, by_table[name], checks[name]))
                    elif args.phase == 'verify':
                        digest = verify_values(cursor, args.snapshot, schema, table)
                        report.setdefault('value_digests', {})[name] = digest
                    else:
                        copy_table(connection, cursor, args.snapshot, schema, table)
                    report['completed'].append(name)
                    save_report(args.report, report)
        report['success'] = True
    finally:
        save_report(args.report, report)


if __name__ == '__main__': main()
