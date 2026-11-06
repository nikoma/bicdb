#!/usr/bin/env python3
"""Map installed exported functions and procedures to an existing target role.

This changes ownership, including the execution identity of definer routines.
Use only after reviewing that role's privileges and the trusted routine bodies.
Roles, grants and source privilege equivalence are separate migration steps.
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
    plan = {}
    for item in manifest['routines']:
        name = identifier(item['name'])
        if item['kind'] not in {'FUNCTION', 'PROCEDURE'}:
            raise ValueError('only exported functions and procedures are supported')
        # The preview currently stores one case-folded bare name per kind.
        # Refuse ambiguous source identities instead of choosing an overload.
        key = name.lower()
        if key in plan:
            raise ValueError('ambiguous folded source routine name: ' + name)
        plan[key] = dict(source_name=name, kind=item['kind'])
    return schema, plan


def catalog(cursor, schema, plan):
    cursor.execute('SELECT p."proname",p."prokind",n."nspname",r."rolname",'
                   'p."oid",p."prosrc",p."pronargs",p."proargtypes",'
                   'p."prorettype",p."prosecdef" '
                   'FROM "pg_catalog"."pg_proc" p '
                   'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=p."pronamespace" '
                   'JOIN "pg_catalog"."pg_roles" r ON r."oid"=p."proowner"')
    actual = {}
    for name, kind, namespace, owner, *definition in cursor.fetchall():
        if namespace != schema and name.lower() not in plan:
            continue
        if namespace != schema or name not in plan or name in actual:
            raise ValueError('extra, colliding or ambiguous target routine: ' + name)
        expected = {'FUNCTION': 'f', 'PROCEDURE': 'p'}[plan[name]['kind']]
        if kind != expected:
            raise ValueError('target routine kind differs from the export: ' + name)
        actual[name] = dict(owner=owner, definition=definition)
    if set(actual) != set(plan):
        raise ValueError('target routine inventory differs from the export')
    return actual


def validate_owners(actual, allowed):
    if any(item['owner'] not in allowed for item in actual.values()):
        raise ValueError('routine owner is outside the explicit migration-role mapping')


def migrate(args):
    if not args.writers_quiesced:
        raise ValueError('routine ownership requires --writers-quiesced, including schema writers')
    manifest_path = args.snapshot / 'manifest.json'
    if args.report.resolve() == manifest_path.resolve():
        raise ValueError('ownership report must not overwrite its source manifest')
    content = manifest_path.read_bytes()
    schema, plan = declarations(json.loads(content))
    owner = identifier(args.target_owner)
    identity = dict(format=1, phase='routine-owners', profile=args.profile,
                    manifest_sha256=hashlib.sha256(content).hexdigest(), source_schema=schema,
                    target_owner=owner, check_only=args.check_only)
    previous = json.loads(args.report.read_bytes()) if args.resume else None
    if not args.resume and args.report.exists():
        raise ValueError('existing ownership report requires --resume')
    report = dict(identity, success=False, ownership_transaction_committed=False,
                  routine_owners_verified=False, source_routines=len(plan),
                  writers_quiesced_asserted=True, writer_lease_acquired=False,
                  source_privilege_equivalence_qualified=False,
                  role_privilege_closure_qualified=False, routine_execution_qualified=False,
                  application_qualified=False)
    can_write = False
    import psycopg
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile='+args.profile) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != args.profile:
                raise ValueError('target did not confirm the requested SQL profile')
            endpoint = [connection.info.host, connection.info.port, connection.info.dbname, connection.info.user]
            report['target_sha256'] = hashlib.sha256(json.dumps(endpoint).encode()).hexdigest()
            if args.resume and (not isinstance(previous, dict) or
                    any(previous.get(key) != value for key,value in
                        {**identity, 'target_sha256': report['target_sha256']}.items())):
                raise ValueError('ownership journal differs from snapshot, endpoint, mode or owner mapping')
            with connection.cursor() as cursor:
                cursor.execute('SELECT "rolsuper" FROM "pg_catalog"."pg_roles" WHERE "rolname"=%s', (owner,))
                if cursor.fetchall() != [(False,)]:
                    raise ValueError('target owner must be an existing dedicated non-superuser role')
                cursor.execute('SELECT CURRENT_USER FROM "pg_catalog"."pg_roles" WHERE "rolname"=CURRENT_USER')
                roles = cursor.fetchall()
                if len(roles) != 1:
                    raise ValueError('migration role must have one registered catalog identity')
                migration_role, = roles[0]
                before = catalog(cursor, schema, plan)
                validate_owners(before, {migration_role, owner})
                report.update(migration_role=migration_role, owners_before=before)
                can_write = True
                save_report(args.report, report)
                if args.check_only:
                    report['success'] = True
                    return report
                with connection.transaction():
                    for name,item in plan.items():
                        # The complete catalog above proves this name is unique.
                        # No untrusted type/signature SQL is interpolated.
                        cursor.execute('ALTER '+item['kind']+' '+quote(schema)+'.'+quote(name)+
                                       ' OWNER TO '+quote(owner), prepare=False)
                    after = catalog(cursor, schema, plan)
                    validate_owners(after, {owner})
                    if {n:x['definition'] for n,x in before.items()} != {n:x['definition'] for n,x in after.items()}:
                        raise ValueError('routine definition changed during owner mapping')
                report.update(success=True, ownership_transaction_committed=True,
                              routine_owners_verified=True, definitions_preserved=True, owners_after=after)
    finally:
        if can_write:
            save_report(args.report, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--target-owner', required=True)
    parser.add_argument('--profile', choices=['oracle-preview-v1','postgres'], default='oracle-preview-v1')
    parser.add_argument('--writers-quiesced', action='store_true')
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--resume', action='store_true')
    migrate(parser.parse_args())


if __name__ == '__main__':
    main()
