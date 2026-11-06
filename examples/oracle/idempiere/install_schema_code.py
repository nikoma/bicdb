"""Install journaled standalone routines or views from a trusted saved export.

DDL acceptance and catalog stability are checked here. Execution, privileges,
source view values, and complete application behavior require separate tests.
"""
import hashlib
import json
import os

from import_bicdb import save_report
from schema_code import code_plan


def target_definition(cursor, schema, item, phase):
    if phase == 'routines':
        cursor.execute('SELECT p."proname",p."prokind",p."prosrc",p."pronargs",'
                       'p."prorettype",p."proowner",p."prosecdef",n."nspname" '
                       'FROM "pg_catalog"."pg_proc" p JOIN "pg_catalog"."pg_namespace" n '
                       'ON n."oid"=p."pronamespace"')
        # The preview routine catalog currently folds names to lowercase. Reject
        # ambiguous matches instead of assuming overloaded/quoted identity parity.
        rows = [list(row) for row in cursor.fetchall() if row[0].lower() == item['name'].lower()]
        # The preview currently keys stored routines by their bare name. A
        # declaration in another namespace could otherwise be replaced.
        if any(row[-1] != schema for row in rows):
            raise ValueError('routine name collides with another namespace: ' + item['name'])
        if len(rows) > 1:
            raise ValueError('ambiguous target routine identity: ' + item['name'])
        if rows and rows[0][1] != ('f' if item['kind'] == 'FUNCTION' else 'p'):
            raise ValueError('target routine has a different kind: ' + item['name'])
        return dict(catalog=rows[0]) if rows else None
    cursor.execute('SELECT "definition","viewowner" FROM "pg_catalog"."pg_views" '
                   'WHERE "schemaname"=%s AND "viewname"=%s', (schema, item['name']))
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise ValueError('ambiguous target view identity: ' + item['name'])
    if not rows:
        return None
    cursor.execute('SELECT "column_name" FROM "information_schema"."columns" '
                   'WHERE "table_schema"=%s AND "table_name"=%s ORDER BY "ordinal_position"',
                   (schema, item['name']))
    return dict(definition=rows[0][0], owner=rows[0][1],
                columns=[row[0] for row in cursor.fetchall()])


def install(cursor, schema, items, phase, report, checkpoint, database_error):
    names = [item['name'] for item in items]
    completed = report['completed']
    if (len(set(completed)) != len(completed) or any(name not in names for name in completed)
            or set(report['definitions']) != set(completed)):
        raise ValueError('stored-code journal contains unknown, duplicate, or missing objects')
    # Validate the whole prior journal before executing any new definitions.
    for item in items:
        actual = target_definition(cursor, schema, item, phase)
        if item['name'] in completed:
            if actual != report['definitions'][item['name']]:
                raise ValueError('completed definition changed outside the journal: ' + item['name'])
        elif actual is not None:
            raise ValueError('unjournaled target object requires explicit reconciliation: ' + item['name'])
    pending = [item for item in items if item['name'] not in completed]
    while pending:
        failures = []
        progressed = False
        for item in pending:
            try:
                cursor.execute(item['ddl'], prepare=False)
            except database_error as error:
                failures.append(dict(name=item['name'], sqlstate=error.sqlstate, detail=str(error)))
                continue
            definition = target_definition(cursor, schema, item, phase)
            if definition is None:
                raise ValueError('DDL accepted without a target catalog definition: ' + item['name'])
            if phase == 'views' and definition['columns'] != item['columns']:
                raise ValueError('target view labels differ from exported labels: ' + item['name'])
            report['definitions'][item['name']] = definition
            completed.append(item['name'])
            progressed = True
            checkpoint()
        report['failures'] = failures
        checkpoint()
        if not progressed:
            return False
        pending = [item for item in pending if item['name'] not in completed]
    return True


def migrate(args):
    import psycopg
    if not args.writers_quiesced:
        raise ValueError('stored-code installation requires --writers-quiesced')
    content = (args.snapshot / 'manifest.json').read_bytes()
    plan = code_plan(json.loads(content))
    items = plan[args.phase]
    dependency_digest = None
    if getattr(args, 'dependency_report', None):
        if args.phase != 'views':
            raise ValueError('dependency reports are only supported for views')
        from schema_dependencies import ordered_views
        dependency_content = args.dependency_report.read_bytes()
        order = ordered_views(json.loads(content), json.loads(dependency_content),
                              hashlib.sha256(content).hexdigest())
        by_name = {item['name']: item for item in items}
        items = [by_name[name] for name in order]
        dependency_digest = hashlib.sha256(dependency_content).hexdigest()
    report = dict(format=1, phase=args.phase, profile='oracle-preview-v1',
                  schema_manifest_sha256=hashlib.sha256(content).hexdigest(),
                  input_definitions={item['name']: item['ddl_sha256'] for item in items},
                  completed=[], definitions={}, failures=[], success=False,
                  dependency_resolution_qualified=False, view_types_qualified=False,
                  routine_execution_qualified=False, view_values_qualified=False,
                  ownership_mapping_qualified=False, application_qualified=False,
                  writers_quiesced_confirmed=True)
    report['source_dependency_report_sha256'] = dependency_digest
    if args.resume:
        previous = json.loads(args.report.read_text())
        for key in ['format', 'phase', 'profile', 'schema_manifest_sha256', 'input_definitions', 'source_dependency_report_sha256']:
            if previous.get(key) != report[key]:
                raise ValueError('stored-code journal differs from snapshot, adapter, or phase')
        report = previous
        report.update(success=False, failures=[])
    elif args.report.exists():
        raise ValueError('report exists; use --resume or choose a new path')
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options='-c bicdb.sql_profile=oracle-preview-v1',
                             prepare_threshold=None, connect_timeout=10) as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != report['profile']:
                raise ValueError('target did not confirm the Oracle preview profile')
            target = dict(host=connection.info.host, port=connection.info.port,
                          database=connection.info.dbname, user=connection.info.user)
            if args.resume and report.get('target') != target:
                raise ValueError('stored-code journal targets a different connection identity')
            report['target'] = target
            with connection.cursor() as cursor:
                # Unqualified references in the unchanged source resolve within
                # the original namespace before public objects.
                from import_bicdb import quote
                cursor.execute('SET search_path TO ' + quote(plan['source_schema']) + ',public', prepare=False)
                report['success'] = install(cursor, plan['source_schema'], items, args.phase,
                                            report, lambda: save_report(args.report, report), psycopg.Error)
    finally:
        save_report(args.report, report)
    return 0 if report['success'] else 1
