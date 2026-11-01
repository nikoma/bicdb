"""Apply logical Oracle application indexes with explicit semantic checks.

The source snapshot is executable migration input and must be trusted. Oracle
LOB storage indexes are represented by native value storage, not SQL indexes.
Unsupported unique-key semantics are reported instead of silently weakened.
"""
import hashlib
import json
import os
from pathlib import Path

from import_bicdb import quote, save_report


def index_plan(manifest):
    backed = {item['index'] for item in manifest['constraints']
              if item['kind'] in {'P', 'U'}}
    logical, storage = [], []
    for index in manifest['indexes']:
        if index['status'] != 'VALID':
            raise ValueError('non-valid source indexes require an explicit migration policy')
        if index['kind'] == 'LOB':
            storage.append(index['name'])
            continue
        if index['name'] in backed:
            continue
        if index['kind'] not in {'NORMAL', 'FUNCTION-BASED NORMAL'}:
            raise ValueError('unsupported source index kind: ' + index['kind'])
        if index['uniqueness'] not in {'UNIQUE', 'NONUNIQUE'}:
            raise ValueError('unknown source uniqueness policy')
        keys = index['columns']
        if not keys or [k['position'] for k in keys] != list(range(1, len(keys) + 1)):
            raise ValueError('missing or unordered source index columns')
        if any(k['direction'] not in {'ASC', 'DESC'} for k in keys):
            raise ValueError('unsupported index direction')
        logical.append(index)
    return logical, storage


def completed_in_source_order(completed, names):
    if (not isinstance(completed, list) or any(not isinstance(n, str) for n in completed)
            or len(set(completed)) != len(completed) or any(n not in names for n in completed)):
        raise ValueError('index journal is not a unique subset of this snapshot')
    # A resumed pass can finish an earlier unsupported index after later ones.
    # Normalize those valid older journals and keep every new write ordered.
    return [name for name in names if name in completed]


def known_nonnull_key(table, key, columns):
    if key['expression'] is None:
        return columns[(table, key['column'])]['nullable'] == 'N'
    # These are proofs for the pinned expression subset, not a general SQL
    # nullability inference engine. Unknown expressions remain nullable.
    import re
    expression = key['expression'].strip()
    upper = re.fullmatch(r'UPPER\("((?:[^"]|"")+)"\)', expression, re.IGNORECASE)
    if upper:
        return columns[(table, upper.group(1).replace('""', '"'))]['nullable'] == 'N'
    return bool(re.fullmatch(r'COALESCE\("(?:[^"]|"")+",\s*\(?-?\d+\)?\)',
                             expression, re.IGNORECASE))


def needs_composite_null_policy(index, columns):
    return index['uniqueness'] == 'UNIQUE' and len(index['columns']) > 1 and any(
        not known_nonnull_key(index['table'], key, columns) for key in index['columns'])


def index_ddl(schema, index, columns, composite_nulls=False):
    keys = [key['expression'] if key['expression'] is not None else quote(key['column'])
            for key in index['columns']]
    needs_nulls = needs_composite_null_policy(index, columns)
    if needs_nulls and not composite_nulls:
        raise ValueError('Oracle nullable composite uniqueness needs verified native support')
    ordered = ','.join((('(' + expr + ')') if key['expression'] is not None else expr)
                       + ' ' + key['direction']
                       for expr, key in zip(keys, index['columns']))
    sql = ('CREATE ' + ('UNIQUE ' if index['uniqueness'] == 'UNIQUE' else '')
           + 'INDEX ' + quote(schema) + '.' + quote(index['name']) + ' ON '
           + quote(schema) + '.' + quote(index['table']) + '(' + ordered + ')')
    if needs_nulls:
        sql += ' NULLS NOT DISTINCT'
        # Oracle permits duplicate keys only when every key component is NULL.
        # A known non-NULL key makes that exception impossible and preserves
        # ordinary prefix-index access without adding a partial predicate.
        if not any(known_nonnull_key(index['table'], key, columns)
                   for key in index['columns']):
            sql += ' WHERE ' + ' OR '.join('(' + expr + ') IS NOT NULL' for expr in keys)
    return sql


def target_definition(cursor, schema, name):
    cursor.execute('SELECT "tablename","indexdef" FROM "pg_catalog"."pg_indexes" '
                   'WHERE "schemaname"=%s AND "indexname"=%s', (schema, name))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError('expected one target index catalog entry: ' + name)
    result = dict(table=rows[0][0], definition=rows[0][1])
    cursor.execute('SELECT i."indisvalid",i."indisready" '
                   'FROM "pg_catalog"."pg_index" i '
                   'JOIN "pg_catalog"."pg_class" c ON c."oid"=i."indexrelid" '
                   'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=c."relnamespace" '
                   'WHERE c."relname"=%s AND n."nspname"=%s', (name, schema))
    flags = cursor.fetchall()
    if len(flags) != 1:
        raise ValueError('expected one target index state: ' + name)
    result['executable'] = flags[0] == (True, True)
    return result


def executable_status_support(cursor, schema):
    """Reject older servers that label non-executable index placeholders valid."""
    import uuid
    name = 'BICDB_INDEX_PROBE_' + uuid.uuid4().hex[:12].upper()
    table = quote(schema) + '.' + quote(name)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + table + '(a NUMBER)', prepare=False)
        created = True
        cursor.execute('CREATE INDEX ' + quote(schema) + '.' + quote(name + '_REAL')
                       + ' ON ' + table + '(a)', prepare=False)
        cursor.execute('CREATE INDEX ' + quote(schema) + '.' + quote(name + '_PLACEHOLDER')
                       + ' ON ' + table + '(bicdb_unimplemented_index_probe(a))', prepare=False)
        real = target_definition(cursor, schema, name + '_REAL')
        placeholder = target_definition(cursor, schema, name + '_PLACEHOLDER')
        return real['executable'] and not placeholder['executable']
    finally:
        if created:
            cursor.execute('DROP TABLE ' + table, prepare=False)


def composite_null_support(cursor, schema):
    """Check enforcement, not parser acceptance of NULLS NOT DISTINCT."""
    import uuid
    name = 'BICDB_NULL_PROBE_' + uuid.uuid4().hex[:12].upper()
    table = quote(schema) + '.' + quote(name)
    created = False
    try:
        cursor.execute('CREATE TABLE ' + table + '(a NUMBER,b NUMBER)', prepare=False)
        created = True
        cursor.execute('CREATE UNIQUE INDEX ' + quote(schema) + '.' + quote(name + '_UQ')
                       + ' ON ' + table + '(a,b) NULLS NOT DISTINCT WHERE a IS NOT NULL OR b IS NOT NULL', prepare=False)
        cursor.execute('INSERT INTO ' + table + ' VALUES(NULL,NULL),(NULL,NULL),(1,NULL)', prepare=False)
        try:
            cursor.execute('INSERT INTO ' + table + ' VALUES(1,NULL)', prepare=False)
        except Exception as error:
            if getattr(error, 'sqlstate', None) == '23505':
                return True
            raise
        return False
    finally:
        if created:
            cursor.execute('DROP TABLE ' + table, prepare=False)


def migrate(args):
    import psycopg
    content = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(content)
    fingerprint = hashlib.sha256(content).hexdigest()
    schema = manifest['source_schema']
    items, lob = index_plan(manifest)
    columns = {(c['table'], c['name']): c for c in manifest['columns']}
    names = [item['name'] for item in items]
    if len(set(names)) != len(names):
        raise ValueError('duplicate source index names')
    report = dict(format=2, phase='indexes', profile='oracle-preview-v1',
                  schema_manifest_sha256=fingerprint, completed=[], definitions={},
                  failures=[], success=False, application_qualified=False,
                  source_logical_indexes=len(items), native_lob_storage_adaptations=lob,
                  outstanding=['foreign keys and deferment', 'nullable composite UNIQUE constraints',
                               'types and stored routines', 'views', 'release migrations',
                               'application workflows and operational qualification'])
    if args.resume:
        previous = json.loads(args.report.read_text())
        if any(previous.get(key) != report[key] for key in
               ['format', 'phase', 'profile', 'schema_manifest_sha256', 'source_logical_indexes']):
            raise ValueError('index journal does not match this snapshot and phase')
        previous['completed'] = completed_in_source_order(previous['completed'], names)
        report = previous
        report.update(success=False, failures=[])
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10,
                             prepare_threshold=None) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != report['profile']:
                raise ValueError('target did not confirm the preview profile')
            target = dict(host=connection.info.host, port=connection.info.port,
                          database=connection.info.dbname, user=connection.info.user)
            if args.resume and report.get('target') != target:
                raise ValueError('index journal targets a different server')
            report['target'] = target
            nulls = None
            with connection.cursor() as cursor:
                report['native_index_status_verified'] = executable_status_support(cursor, schema)
                if not report['native_index_status_verified']:
                    raise ValueError('target does not distinguish executable indexes from catalog placeholders')
                for index in items:
                    name = index['name']
                    if name in report['completed']:
                        definition = target_definition(cursor, schema, name)
                        if definition != report['definitions'][name] or not definition['executable']:
                            raise ValueError('completed index definition/state differs from journal: ' + name)
                        continue
                    if needs_composite_null_policy(index, columns):
                        if nulls is None:
                            nulls = composite_null_support(cursor, schema)
                            report['native_composite_null_support_verified'] = nulls
                        if not nulls:
                            report['failures'].append(dict(name=name, reason='nullable_composite_uniqueness_unavailable'))
                            save_report(args.report, report)
                            continue
                    print('indexes: ' + name, flush=True)
                    try:
                        cursor.execute(index_ddl(schema, index, columns, composite_nulls=bool(nulls)), prepare=False)
                        definition = target_definition(cursor, schema, name)
                        if definition['table'] != index['table']:
                            raise ValueError('created index targets an unexpected table')
                        if not definition['executable']:
                            # This index was created by this attempt and has no
                            # executable backing. Remove that placeholder so a
                            # later implementation can be retried safely.
                            cursor.execute('DROP INDEX ' + quote(schema) + '.' + quote(name), prepare=False)
                            report['failures'].append(dict(name=name, reason='expression_index_unavailable'))
                        else:
                            report['definitions'][name] = definition
                            report['completed'].append(name)
                            report['completed'] = completed_in_source_order(report['completed'], names)
                    except psycopg.Error as error:
                        report['failures'].append(dict(name=name, reason='target_sql_error', sqlstate=error.sqlstate))
                    save_report(args.report, report)
        report['success'] = len(report['completed']) == len(items) and not report['failures']
    finally:
        save_report(args.report, report)
    if not report['success']:
        raise SystemExit('Index phase is incomplete; inspect the private report')
