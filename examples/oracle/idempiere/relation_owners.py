#!/usr/bin/env python3
"""Map an exported schema and its tables/views to an existing target owner.

Only relation ownership is changed. Roles, login credentials, grants, routine
rights, types and sequence ownership require separate migration policies.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

from import_bicdb import quote, save_report
from schema_code import identifier


def declarations(manifest):
    schema = identifier(manifest['source_schema'])
    items = []
    seen = set()
    for key, kind in [('tables', 'TABLE'), ('views', 'VIEW')]:
        for item in manifest[key]:
            name = identifier(item['name'])
            if name in seen:
                raise ValueError('duplicate source relation: ' + name)
            seen.add(name)
            items.append(dict(name=name, kind=kind))
    return schema, items


def catalog(cursor, schema):
    cursor.execute('SELECT r."rolname" FROM "pg_catalog"."pg_namespace" n '
                   'JOIN "pg_catalog"."pg_roles" r ON r."oid"=n."nspowner" '
                   'WHERE n."nspname"=%s', (schema,))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError('target must have exactly one exported namespace')
    schema_owner = rows[0][0]
    cursor.execute('SELECT c."relname",c."relkind",r."rolname" '
                   'FROM "pg_catalog"."pg_class" c '
                   'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=c."relnamespace" '
                   'JOIN "pg_catalog"."pg_roles" r ON r."oid"=c."relowner" '
                   'WHERE n."nspname"=%s AND c."relkind" IN (%s,%s)',
                   (schema, 'r', 'v'))
    relations = {}
    for name, kind, owner in cursor.fetchall():
        if name in relations:
            raise ValueError('ambiguous target relation: ' + name)
        relations[name] = dict(kind={'r': 'TABLE', 'v': 'VIEW'}[kind], owner=owner)
    return dict(schema_owner=schema_owner, relations=relations)


def validate_catalog(actual, plan, allowed_owners):
    expected = {item['name']: item['kind'] for item in plan}
    if ({name: item['kind'] for name, item in actual['relations'].items()} != expected):
        raise ValueError('target tables/views differ from the exported relation inventory')
    owners = [actual['schema_owner'], *(item['owner'] for item in actual['relations'].values())]
    if any(owner not in allowed_owners for owner in owners):
        raise ValueError('target has an owner outside the explicit migration-role mapping')


def migrate(args):
    if not args.writers_quiesced:
        raise ValueError('relation ownership requires --writers-quiesced, including schema writers')
    manifest_path = args.snapshot / 'manifest.json'
    if args.report.resolve() == manifest_path.resolve():
        raise ValueError('ownership report must not overwrite its source manifest')
    content = manifest_path.read_bytes()
    schema, plan = declarations(json.loads(content))
    owner = identifier(args.target_owner)
    identity = dict(format=1, phase='relation-owners', profile=args.profile,
                    manifest_sha256=hashlib.sha256(content).hexdigest(),
                    source_schema=schema, target_owner=owner, check_only=args.check_only)
    previous = json.loads(args.report.read_bytes()) if args.resume else None
    if not args.resume and args.report.exists():
        raise ValueError('existing ownership report requires --resume')
    report = dict(identity, success=False, ownership_transaction_committed=False,
                  relation_owners_verified=False, writers_quiesced_asserted=True,
                  writer_lease_acquired=False, source_relations=len(plan),
                  routine_ownership_qualified=False, sequence_ownership_qualified=False,
                  type_ownership_qualified=False, data_compared_to_source=False,
                  role_privilege_closure_qualified=False,
                  application_qualified=False)
    can_write = False
    import psycopg
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=' + args.profile) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != identity['profile']:
                raise ValueError('target did not confirm the Oracle preview profile')
            endpoint = [connection.info.host, connection.info.port, connection.info.dbname, connection.info.user]
            report['target_sha256'] = hashlib.sha256(json.dumps(endpoint).encode()).hexdigest()
            if args.resume and (not isinstance(previous, dict) or
                    any(previous.get(key) != value for key, value in
                        {**identity, 'target_sha256': report['target_sha256']}.items())):
                raise ValueError('ownership journal differs from snapshot, endpoint, mode or owner mapping')
            with connection.cursor() as cursor:
                cursor.execute('SELECT "rolsuper" FROM "pg_catalog"."pg_roles" WHERE "rolname"=%s', (owner,))
                if cursor.fetchall() != [(False,)]:
                    raise ValueError('target owner must be an existing dedicated non-superuser role')
                # This is PostgreSQL driver/catalog administration, so discover
                # the registered effective role through the native catalog
                # boundary rather than treating it as an Oracle expression.
                cursor.execute('SELECT CURRENT_USER FROM "pg_catalog"."pg_roles" '
                               'WHERE "rolname"=CURRENT_USER')
                identities = cursor.fetchall()
                if len(identities) != 1:
                    raise ValueError('migration role must have one registered catalog identity')
                migration_role, = identities[0]
                report['migration_role'] = migration_role
                before = catalog(cursor, schema)
                validate_catalog(before, plan, {migration_role, owner})
                report['owners_before'] = before
                can_write = True
                save_report(args.report, report)
                if args.check_only:
                    report['success'] = True
                    return report
                # Publish no successful ownership result until every DDL and
                # its readback have passed and the transaction has committed.
                with connection.transaction():
                    cursor.execute('ALTER SCHEMA ' + quote(schema) + ' OWNER TO ' + quote(owner), prepare=False)
                    for item in plan:
                        cursor.execute('ALTER ' + item['kind'] + ' ' + quote(schema) + '.' +
                                       quote(item['name']) + ' OWNER TO ' + quote(owner), prepare=False)
                    after = catalog(cursor, schema)
                    validate_catalog(after, plan, {owner})
                report.update(success=True, ownership_transaction_committed=True,
                              relation_owners_verified=True, owners_after=after)
    finally:
        if can_write:
            save_report(args.report, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--target-owner', required=True,
                        help='exact registered target role; roles and credentials are not created')
    parser.add_argument('--profile', choices=['oracle-preview-v1', 'postgres'], default='oracle-preview-v1')
    parser.add_argument('--writers-quiesced', action='store_true')
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--resume', action='store_true')
    migrate(parser.parse_args())


if __name__ == '__main__':
    main()
