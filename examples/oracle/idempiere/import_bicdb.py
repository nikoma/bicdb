#!/usr/bin/env python3
"""Import the exported Oracle application into an isolated BicDB preview database.

Each phase records its own result. Tables/data alone are not application
acceptance. Source foreign-key validation and deferment must be retained when
the foreign-key phase is implemented; they are never silently discarded.
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
        return f"{kind}({int(column['characters'])} CHAR)"
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
        if column['character_unit'] == 'B' or column['type'] == 'RAW':
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


def verify_values(cursor, snapshot, schema, table):
    # Sorting row digests preserves multiplicity without assuming that the two
    # engines enumerate heap rows in the same order. No float/Decimal-context
    # rounding or whitespace trimming is allowed in this comparison.
    path = snapshot / table['file']
    if path.resolve().parent != snapshot.resolve(): raise ValueError('invalid snapshot row path')
    with path.open('rb') as source:
        if hashlib.file_digest(source, 'sha256').hexdigest() != table['sha256']:
            raise ValueError('snapshot row file digest mismatch')
    with path.open() as source:
        expected = sorted(row_hash([decode(value) for value in json.loads(line)]) for line in source)
    columns = ','.join(quote(name) for name in table['columns'])
    cursor.execute('SELECT ' + columns + ' FROM ' + quote(schema) + '.' + quote(table['name']))
    actual = []
    while batch := cursor.fetchmany(500): actual.extend(row_hash(row) for row in batch)
    actual.sort()
    if actual != expected: raise ValueError(f"target values differ in {table['name']}")
    return hashlib.sha256(b''.join(actual)).hexdigest()


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
    parser.add_argument('--phase', choices=['tables', 'data', 'verify'], required=True)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--resume', action='store_true', help='validate the prior journal and completed tables before resuming')
    args = parser.parse_args()
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
                for table in manifest['tables']:
                    name = table['name']
                    if name in report['completed']:
                        cursor.execute('SELECT * FROM ' + quote(schema) + '.' + quote(name) + ' WHERE FALSE')
                        if [description.name for description in cursor.description] != table['columns']:
                            raise ValueError('completed target table columns differ from snapshot')
                        cursor.execute('SELECT COUNT(*) FROM ' + quote(schema) + '.' + quote(name))
                        expected = 0 if args.phase == 'tables' else table['rows']
                        if int(cursor.fetchone()[0]) != expected:
                            raise ValueError('completed target table row count differs from journal phase')
                        continue
                    print(f"{args.phase}: {name}", flush=True)
                    if args.phase == 'tables':
                        cursor.execute(table_ddl(schema, name, by_table[name], checks[name]), prepare=False)
                    elif args.phase == 'verify':
                        digest = verify_values(cursor, args.snapshot, schema, table)
                        report.setdefault('value_digests', {})[name] = digest
                    else:
                        path = args.snapshot / table['file']
                        if path.resolve().parent != args.snapshot.resolve():
                            raise ValueError('snapshot row file must be directly inside its snapshot')
                        with path.open('rb') as source:
                            if hashlib.file_digest(source, 'sha256').hexdigest() != table['sha256']:
                                raise ValueError('snapshot row file digest mismatch')
                        columns = ','.join(quote(name) for name in table['columns'])
                        sql = 'COPY ' + quote(schema) + '.' + quote(name) + '(' + columns + ') FROM STDIN'
                        count = 0
                        with cursor.copy(sql) as copy, path.open() as source:
                            for line in source:
                                row = json.loads(line)
                                if len(row) != len(table['columns']): raise ValueError('snapshot row width differs')
                                copy.write_row([decode(value) for value in row])
                                count += 1
                        if count != table['rows']: raise ValueError('snapshot row count differs')
                        cursor.execute('SELECT COUNT(*) FROM ' + quote(schema) + '.' + quote(name))
                        if int(cursor.fetchone()[0]) != count: raise ValueError('target row count differs')
                    report['completed'].append(name)
                    save_report(args.report, report)
        report['success'] = True
    finally:
        save_report(args.report, report)


if __name__ == '__main__': main()
