"""Install explicit source foreign keys without weakening their state.

Requires quiesced target writers/schema changes for apply. Journals retain
metadata and target row fingerprints, not connection strings or row values.
Target-value preservation is separate from source data equality and application
qualification. Disabled source constraints require a separate migration policy.
"""
import hashlib
import json
import os
import uuid

from import_bicdb import quote, save_report
from temporal_types import table_fingerprint


def declarations(manifest):
    schema = manifest['source_schema']
    tables = {t['name']: t for t in manifest['tables']}
    if len(tables) != len(manifest['tables']):
        raise ValueError('duplicate source table')
    for table in tables.values():
        if len(table['columns']) != len(set(table['columns'])):
            raise ValueError('duplicate source projection column')
    keys = {}
    seen = set()
    for constraint in manifest['constraints']:
        name = constraint['name']
        if name in seen:
            raise ValueError('duplicate source constraint name')
        seen.add(name)
        if constraint['kind'] in ('P', 'U'):
            keys[name] = constraint
    result = []
    for c in manifest['constraints']:
        if c['kind'] != 'R':
            continue
        if c['status'] != 'ENABLED':
            raise ValueError('disabled source foreign key requires a separate policy')
        if c['referenced_owner'] != schema or c['referenced_constraint'] not in keys:
            raise ValueError('foreign-key parent is outside the exported schema or unresolved')
        parent = keys[c['referenced_constraint']]
        if parent['status'] != 'ENABLED' or parent['deferrable'] != 'NOT DEFERRABLE' or parent['validated'] != 'VALIDATED':
            raise ValueError('unsupported parent-key enforcement or timing')
        for item in (c, parent):
            columns = item['columns']
            if item['table'] not in tables or not columns or len(columns) != len(set(columns)):
                raise ValueError('missing source table or invalid key columns')
            if not set(columns).issubset(tables[item['table']]['columns']):
                raise ValueError('key column missing from source table projection')
        if len(c['columns']) != len(parent['columns']):
            raise ValueError('foreign-key parent/child arity differs')
        if c['deferrable'] not in ('DEFERRABLE', 'NOT DEFERRABLE') or c['deferred'] not in ('DEFERRED', 'IMMEDIATE'):
            raise ValueError('unsupported source timing state')
        if c['deferrable'] == 'NOT DEFERRABLE' and c['deferred'] != 'IMMEDIATE':
            raise ValueError('nondeferrable source key cannot initially defer')
        if c['validated'] not in ('VALIDATED', 'NOT VALIDATED') or c['delete_rule'] not in ('NO ACTION', 'CASCADE', 'SET NULL'):
            raise ValueError('unsupported validation state or delete rule')
        result.append(dict(table=c['table'], name=c['name'], columns=c['columns'],
                           parent_table=parent['table'], parent_columns=parent['columns'],
                           parent_constraint=parent['name'], parent_kind=parent['kind'],
                           deferrable=c['deferrable'] == 'DEFERRABLE',
                           initially_deferred=c['deferred'] == 'DEFERRED',
                           validated=c['validated'] == 'VALIDATED', delete_rule=c['delete_rule']))
    return result


def ddl(schema, item):
    relation = lambda table: quote(schema) + '.' + quote(table)
    columns = lambda names: '(' + ','.join(quote(name) for name in names) + ')'
    sql = ('ALTER TABLE ' + relation(item['table']) + ' ADD CONSTRAINT ' + quote(item['name']) +
           ' FOREIGN KEY' + columns(item['columns']) + ' REFERENCES ' + relation(item['parent_table']) +
           columns(item['parent_columns']))
    if item['delete_rule'] != 'NO ACTION':
        sql += ' ON DELETE ' + item['delete_rule']
    sql += (' DEFERRABLE INITIALLY ' + ('DEFERRED' if item['initially_deferred'] else 'IMMEDIATE')
            if item['deferrable'] else ' NOT DEFERRABLE')
    if not item['validated']:
        sql += ' NOT VALID'
    return sql


CONSTRAINT_FIELDS = ('conname', 'conrelid', 'contype', 'confrelid', 'conkey', 'confkey',
                     'confdeltype', 'confupdtype', 'confmatchtype', 'condeferrable',
                     'condeferred', 'convalidated', 'conenforced')
CONSTRAINT_SELECT = 'SELECT ' + ','.join(quote(c) for c in CONSTRAINT_FIELDS) + ' FROM "pg_catalog"."pg_constraint"'


def catalog(cursor, schema):
    cursor.execute('SELECT "oid" FROM "pg_catalog"."pg_namespace" WHERE "nspname"=%s', (schema,))
    namespaces = cursor.fetchall()
    if len(namespaces) != 1:
        raise ValueError('target namespace is missing or ambiguous')
    namespace = namespaces[0][0]
    cursor.execute('SELECT "oid","relname" FROM "pg_catalog"."pg_class" WHERE "relnamespace"=%s AND "relkind"=\'r\'', (namespace,))
    relations = dict(cursor.fetchall())
    if len(relations) != len(set(relations.values())):
        raise ValueError('target table names are ambiguous')
    attributes = {}
    # Inventory attributes once and retain only this namespace's tables.
    # Repeated large IN predicates made catalog preflight expensive on broad schemas.
    cursor.execute('SELECT "attrelid","attnum","attname" FROM "pg_catalog"."pg_attribute" WHERE NOT "attisdropped" AND "attnum">0')
    for relation, number, name in cursor:
        if relation not in relations:
            continue
        key = (relation, number)
        if key in attributes:
            raise ValueError('target attribute numbers are ambiguous')
        attributes[key] = name
    cursor.execute(CONSTRAINT_SELECT + ' WHERE "connamespace"=%s', (namespace,))
    constraints = {}
    for values in cursor.fetchall():
        row = dict(zip(CONSTRAINT_FIELDS, values, strict=True))
        if row['contype'] not in ('p', 'u', 'f'):
            continue
        row = decode_constraint(row, relations, attributes)
        key = (row['table'], row['name'])
        if key in constraints:
            raise ValueError('target constraint names are ambiguous')
        constraints[key] = row
    columns = {table: set() for table in relations.values()}
    for (relation, _), name in attributes.items():
        columns[relations[relation]].add(name)
    return dict(namespace=namespace, relations=relations, attributes=attributes,
                by_name={name: oid for oid, name in relations.items()}, columns=columns, constraints=constraints)


def decode_constraint(row, relations, attributes):
    def columns(relation, numbers):
        return [attributes[(relation, int(number))] for number in (numbers or [])]
    try:
        return dict(table=relations[row['conrelid']], name=row['conname'], kind=row['contype'],
                    columns=columns(row['conrelid'], row['conkey']),
                    parent_table=relations.get(row['confrelid']),
                    parent_columns=columns(row['confrelid'], row['confkey']),
                    delete_rule={'a': 'NO ACTION', 'c': 'CASCADE', 'n': 'SET NULL'}.get(row['confdeltype']),
                    update_rule=row['confupdtype'], match=row['confmatchtype'],
                    deferrable=row['condeferrable'], initially_deferred=row['condeferred'],
                    validated=row['convalidated'], enforced=row['conenforced'])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError('unresolved target constraint attributes or relations') from error


def matches(actual, expected):
    fields = ('table', 'name', 'columns', 'parent_table', 'parent_columns', 'delete_rule',
              'deferrable', 'initially_deferred', 'validated')
    return (actual['kind'] == 'f' and actual['enforced'] is True and
            actual['update_rule'] == 'a' and actual['match'] == 's' and
            all(actual[key] == expected[key] for key in fields))


def current_constraint(cursor, state, item):
    cursor.execute(CONSTRAINT_SELECT + ' WHERE "conrelid"=%s AND "conname"=%s', (state['by_name'][item['table']], item['name']))
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise ValueError('target foreign key is ambiguous')
    return None if not rows else decode_constraint(dict(zip(CONSTRAINT_FIELDS, rows[0], strict=True)), state['relations'], state['attributes'])


def enforcement_enabled(cursor, schema, tables):
    tables = sorted(set(tables))
    for at in range(0, len(tables), 64):
        chunk = tables[at:at + 64]
        cursor.execute('SELECT ' + ','.join('bicdb_referential_checks_enabled(%s)' for _ in chunk),
                       [quote(schema) + '.' + quote(table) for table in chunk], prepare=False)
        values = cursor.fetchone()
        if values is None or len(values) != len(chunk) or any(value is not True for value in values):
            raise ValueError('target table referential checks are disabled, missing or unverifiable')


def validate_journal(previous, identity, plan):
    if any(previous.get(key) != value for key, value in identity.items()):
        raise ValueError('foreign-key journal does not match source, target and phase')
    completed = previous['completed']
    if not isinstance(completed, list):
        raise ValueError('foreign-key journal completed declarations must be a list')
    if completed != plan[:len(completed)] or len(completed) > len(plan):
        raise ValueError('foreign-key journal is not an exact ordered declaration prefix')
    return completed


def probe_support(cursor, schema):
    stem = 'BIC_FK_' + uuid.uuid4().hex[:12].upper()
    parent, child = [quote(schema) + '.' + quote(stem + suffix) for suffix in ('_P', '_C')]
    created = []
    try:
        cursor.execute('CREATE TABLE ' + parent + '("ID" INTEGER PRIMARY KEY)', prepare=False)
        created.append(parent)
        cursor.execute('CREATE TABLE ' + child + '("ID" INTEGER PRIMARY KEY,"PID" INTEGER)', prepare=False)
        created.append(child)
        cursor.execute('INSERT INTO ' + child + ' VALUES(1,999)', prepare=False)
        cursor.execute('ALTER TABLE ' + child + ' ADD CONSTRAINT ' + quote(stem + '_FK') + ' FOREIGN KEY("PID") REFERENCES ' + parent + '("ID") DEFERRABLE INITIALLY DEFERRED NOT VALID', prepare=False)
        cursor.execute('BEGIN')
        cursor.execute('UPDATE ' + child + ' SET "PID"="PID" WHERE "ID"=1', prepare=False)
        try:
            cursor.execute('COMMIT')
        except Exception as error:
            if getattr(error, 'sqlstate', None) != '23503':
                raise
        else:
            raise ValueError('target lacks Oracle deferred unchanged-key enforcement')
        cursor.execute('ROLLBACK')
        cursor.execute('SELECT "PID" FROM ' + child, prepare=False)
        if cursor.fetchone() != (999,):
            raise ValueError('failed probe commit did not preserve legacy row')
    finally:
        cleanup_error = None
        try:
            cursor.execute('ROLLBACK')
        except Exception as error:
            cleanup_error = error
        for table in reversed(created):
            try:
                cursor.execute('DROP TABLE ' + table, prepare=False)
            except Exception as error:
                cleanup_error = error
        if cleanup_error is not None:
            raise cleanup_error


def migrate(args):
    if not args.check_only and not args.writers_quiesced:
        raise ValueError('foreign-key apply requires --writers-quiesced and a retained pre-upgrade backup')
    import psycopg
    source = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(source)
    schema, plan = manifest['source_schema'], declarations(manifest)
    tables = {table['name']: table for table in manifest['tables']}
    affected = sorted({item[key] for item in plan for key in ('table', 'parent_table')})
    report = dict(format=1, phase='foreign-keys', profile='oracle-preview-v1', source_schema=schema,
                  manifest_sha256=hashlib.sha256(source).hexdigest(), completed=[],
                  table_fingerprints={}, source_foreign_keys=len(plan), check_only=args.check_only,
                  capability_verified=False, migration_completed=False, success=False,
                  application_qualified=False, data_compared_to_source=False, orphan_inventory_qualified=False)
    can_write = not args.resume
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True, connect_timeout=10,
                             options='-c bicdb.sql_profile=oracle-preview-v1') as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != report['profile']:
                raise ValueError('target did not confirm the Oracle preview profile')
            if not connection.info.parameter_status('TimeZone'):
                raise ValueError('target must report TimeZone before row fingerprinting')
            endpoint = [connection.info.host, connection.info.port, connection.info.dbname, connection.info.user]
            report['target_sha256'] = hashlib.sha256(json.dumps(endpoint).encode()).hexdigest()
            identity = {key: report[key] for key in ('format', 'phase', 'profile', 'source_schema', 'manifest_sha256', 'target_sha256')}
            if args.resume:
                previous = json.loads(args.report.read_text())
                report['completed'] = validate_journal(previous, identity, plan)
                report['table_fingerprints'] = previous['table_fingerprints']
                if not isinstance(report['table_fingerprints'], dict) or not set(report['table_fingerprints']).issubset(affected):
                    raise ValueError('foreign-key journal table fingerprint set differs from source')
                if report['completed'] and set(report['table_fingerprints']) != set(affected):
                    raise ValueError('foreign-key journal is missing completed table fingerprints')
                can_write = True
            with connection.cursor() as cursor:
                print('foreign-keys: catalog and declaration preflight', flush=True)
                state = catalog(cursor, schema)
                expected = {(item['table'], item['name']): item for item in plan}
                for key, actual in state['constraints'].items():
                    if actual['kind'] == 'f' and actual['table'] in tables:
                        if key not in expected or not matches(actual, expected[key]):
                            raise ValueError('existing target foreign key differs from source')
                for item in plan:
                    parent = state['constraints'].get((item['parent_table'], item['parent_constraint']))
                    if parent is None or parent['kind'] != item['parent_kind'].lower() or parent['columns'] != item['parent_columns'] or parent['enforced'] is not True:
                        raise ValueError('target referenced key differs from source')
                    for table, columns in [(item['table'], item['columns']), (item['parent_table'], item['parent_columns'])]:
                        if not set(columns).issubset(state['columns'].get(table, set())):
                            raise ValueError('missing target foreign-key table or columns')
                for item in report['completed']:
                    actual = state['constraints'].get((item['table'], item['name']))
                    if actual is None or not matches(actual, item):
                        raise ValueError('completed target foreign key is missing or changed')
                enforcement_enabled(cursor, schema, [item['table'] for item in plan])
                for table in affected:
                    if state['columns'].get(table) != set(tables[table]['columns']):
                        raise ValueError('target table projection differs from the exported source')
                print('foreign-keys: recording baseline table fingerprints', flush=True)
                for ordinal, table in enumerate(affected, 1):
                    fingerprint = table_fingerprint(cursor, schema, tables[table])
                    prior = report['table_fingerprints'].get(table)
                    if prior is not None and prior['before'] != fingerprint:
                        raise ValueError('target rows changed since foreign-key migration began')
                    report['table_fingerprints'][table] = dict(before=fingerprint)
                    if ordinal % 32 == 0 or ordinal == len(affected):
                        print('foreign-keys: baseline fingerprints ' + str(ordinal) + '/' + str(len(affected)), flush=True)
                save_report(args.report, report)
                if not args.check_only and plan:
                    print('foreign-keys: checking target enforcement capabilities', flush=True)
                    probe_support(cursor, schema)
                    report['capability_verified'] = True
                # The exact completed prefix was already checked against the
                # complete catalog above, before any probe or source DDL. With
                # writers/schema changes quiesced, rereading each of those keys
                # adds no validation. Pending and unjournaled entries still get
                # individual readback, including after every new installation.
                completed_prefix = len(report['completed'])
                for index, item in enumerate(plan):
                    if args.check_only or index < completed_prefix:
                        continue
                    actual = current_constraint(cursor, state, item)
                    if actual is None:
                        cursor.execute(ddl(schema, item), prepare=False)
                        actual = current_constraint(cursor, state, item)
                    if actual is None or not matches(actual, item):
                        raise ValueError('installed foreign key differs from source declaration')
                    if index >= len(report['completed']):
                        report['completed'].append(item)
                    if (index + 1) % 32 == 0:
                        save_report(args.report, report)
                        print('foreign-keys: ' + str(index + 1) + '/' + str(len(plan)), flush=True)
                enforcement_enabled(cursor, schema, [item['table'] for item in plan])
                print('foreign-keys: verifying final table fingerprints', flush=True)
                for ordinal, table in enumerate(affected, 1):
                    after = table_fingerprint(cursor, schema, tables[table])
                    if report['table_fingerprints'][table]['before'] != after:
                        raise ValueError('target rows changed during foreign-key migration')
                    report['table_fingerprints'][table]['after'] = after
                    if ordinal % 32 == 0 or ordinal == len(affected):
                        print('foreign-keys: final fingerprints ' + str(ordinal) + '/' + str(len(affected)), flush=True)
                report['migration_completed'] = not args.check_only and len(report['completed']) == len(plan)
                report['success'] = args.check_only or report['migration_completed']
    finally:
        if can_write:
            save_report(args.report, report)
    if not report['success']:
        raise ValueError('foreign-key migration is incomplete')
    return report
