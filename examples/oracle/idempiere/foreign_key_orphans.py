#!/usr/bin/env python3
"""Read-only orphan counts after a quiesced source-declared FK migration.

Counts retain duplicate child rows and exclude partially NULL keys (MATCH SIMPLE).
No row values, connection strings, repair SQL or validation DDL enter the report.
Operator-asserted quiescence is required; this tool does not acquire a writer lease.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

from foreign_keys import catalog, declarations, enforcement_enabled, matches, validate_journal
from import_bicdb import quote, save_report
from temporal_types import table_fingerprint


def orphan_count_sql(schema, item):
    child = quote(schema) + '.' + quote(item['table'])
    parent = quote(schema) + '.' + quote(item['parent_table'])
    columns, referenced = item['columns'], item['parent_columns']
    if not columns or len(columns) != len(referenced):
        raise ValueError('orphan inventory requires equally sized nonempty keys')
    nonnull = ' AND '.join('c.' + quote(name) + ' IS NOT NULL' for name in columns)
    equality = ' AND '.join('p.' + quote(p) + '=c.' + quote(c)
                            for c, p in zip(columns, referenced, strict=True))
    return ('SELECT COUNT(*) FROM ' + child + ' c WHERE ' + nonnull +
            ' AND NOT EXISTS(SELECT 1 FROM ' + parent + ' p WHERE ' + equality + ')')


def count_orphans(cursor, schema, item):
    cursor.execute(orphan_count_sql(schema, item))
    rows = cursor.fetchall()
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] is None:
        raise ValueError('orphan count did not return one nonnegative integer')
    value = rows[0][0]
    if isinstance(value, bool) or int(value) != value or value < 0:
        raise ValueError('orphan count did not return one nonnegative integer')
    return int(value)


def require_completed_migration(previous, identity, plan, affected):
    completed = validate_journal(previous, identity, plan)
    if (len(completed) != len(plan) or previous.get('source_foreign_keys') != len(plan) or previous.get('success') is not True or
            previous.get('migration_completed') is not True or
            previous.get('check_only') is not False or
            (plan and previous.get('capability_verified') is not True)):
        raise ValueError('orphan inventory requires a successfully completed FK migration')
    fingerprints = previous.get('table_fingerprints')
    if not isinstance(fingerprints, dict) or set(fingerprints) != set(affected):
        raise ValueError('migration journal has an incomplete fingerprint inventory')
    for value in fingerprints.values():
        if not isinstance(value, dict) or value.get('before') is None or value.get('before') != value.get('after'):
            raise ValueError('migration journal does not confirm unchanged target rows')
    return fingerprints


def validate_inventory_journal(previous, identity, plan, baseline):
    """Reuse counts only for the exact source/target and unchanged migration baseline."""
    if (not isinstance(previous, dict) or type(previous.get('format')) is not int or
            any(previous.get(key) != value for key, value in identity.items()) or
            previous.get('writers_quiesced_asserted') is not True or
            type(previous.get('source_foreign_keys')) is not int or
            previous['source_foreign_keys'] != len(plan)):
        raise ValueError('orphan journal does not match source, target and completed migration')
    completed = previous.get('completed')
    if not isinstance(completed, list) or len(completed) > len(plan):
        raise ValueError('orphan journal must contain an ordered count prefix')
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    for entry, declaration in zip(completed, plan):
        if (not isinstance(entry, dict) or
                canonical(entry.get('declaration')) != canonical(declaration) or
                type(entry.get('orphan_rows')) is not int or entry['orphan_rows'] < 0 or
                (declaration['validated'] and entry['orphan_rows'] != 0)):
            raise ValueError('orphan journal has a changed declaration or invalid count prefix')
    fingerprints = previous.get('table_fingerprints')
    if (not isinstance(fingerprints, dict) or not set(fingerprints).issubset(baseline) or
            (completed and set(fingerprints) != set(baseline))):
        raise ValueError('orphan journal has an incomplete row baseline')
    for table, value in fingerprints.items():
        expected = canonical(baseline[table]['after'])
        if (not isinstance(value, dict) or canonical(value.get('before')) != expected or
                ('after' in value and canonical(value['after']) != expected)):
            raise ValueError('orphan journal row baseline differs from completed migration')
    return completed


def inventory(args):
    if not args.writers_quiesced:
        raise ValueError('orphan inventory requires --writers-quiesced, including schema writers')
    if args.report.resolve() in {args.migration_report.resolve(), (args.snapshot / 'manifest.json').resolve()}:
        raise ValueError('inventory report must not overwrite its source or migration journal')
    resume = getattr(args, 'resume', False)
    prior_inventory = json.loads(args.report.read_bytes()) if resume else None
    import psycopg
    source = (args.snapshot / 'manifest.json').read_bytes()
    manifest = json.loads(source)
    schema, plan = manifest['source_schema'], declarations(manifest)
    tables = {table['name']: table for table in manifest['tables']}
    affected = sorted({item[key] for item in plan for key in ('table', 'parent_table')})
    migration_source = args.migration_report.read_bytes()
    previous = json.loads(migration_source)
    report = dict(format=1, phase='foreign-key-orphans', profile='oracle-preview-v1',
                  source_schema=schema, manifest_sha256=hashlib.sha256(source).hexdigest(),
                  migration_report_sha256=hashlib.sha256(migration_source).hexdigest(),
                  source_foreign_keys=len(plan), completed=[], table_fingerprints={},
                  writers_quiesced_asserted=True, writer_lease_acquired=False,
                  resumed_from_relationships=0,
                  inventory_completed=False, success=False, all_relationships_clean=False,
                  validated_relationships_clean=False, data_compared_to_source=False,
                  orphan_inventory_qualified=False, application_qualified=False)
    # A new attempt must not leave an older successful report visible throughout
    # a long baseline scan, including if the process is later interrupted.
    can_write = not resume
    if can_write:
        save_report(args.report, report)
    try:
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True, connect_timeout=10,
                             options='-c bicdb.sql_profile=oracle-preview-v1') as connection:
            if connection.info.parameter_status('bicdb_sql_profile') != report['profile']:
                raise ValueError('target did not confirm the Oracle preview profile')
            if not connection.info.parameter_status('TimeZone'):
                raise ValueError('target must report TimeZone before row fingerprinting')
            endpoint = [connection.info.host, connection.info.port, connection.info.dbname, connection.info.user]
            report['target_sha256'] = hashlib.sha256(json.dumps(endpoint).encode()).hexdigest()
            identity = {key: report[key] for key in ('format', 'profile', 'source_schema', 'manifest_sha256', 'target_sha256')}
            identity['phase'] = 'foreign-keys'
            baseline = require_completed_migration(previous, identity, plan, affected)
            if resume:
                journal_identity = {key: report[key] for key in
                    ('format', 'phase', 'profile', 'source_schema', 'manifest_sha256',
                     'migration_report_sha256', 'target_sha256')}
                report['completed'] = list(validate_inventory_journal(prior_inventory, journal_identity, plan, baseline))
                report['table_fingerprints'] = {
                    table: dict(before=value['before'])
                    for table, value in prior_inventory['table_fingerprints'].items()}
                report['resumed_from_relationships'] = len(report['completed'])
                can_write = True
                # Invalidate prior completion while retaining resumable progress.
                save_report(args.report, report)
            with connection.cursor() as cursor:
                def verify_catalog():
                    state = catalog(cursor, schema)
                    expected = {(item['table'], item['name']): item for item in plan}
                    actual = {key: item for key, item in state['constraints'].items()
                              if item['kind'] == 'f' and item['table'] in tables}
                    if set(actual) != set(expected) or any(not matches(actual[key], item) for key, item in expected.items()):
                        raise ValueError('target FK catalog differs from completed source migration')
                    for item in plan:
                        parent = state['constraints'].get((item['parent_table'], item['parent_constraint']))
                        if (parent is None or parent['kind'] != item['parent_kind'].lower() or
                                parent['columns'] != item['parent_columns'] or parent['enforced'] is not True or
                                parent['validated'] is not True or parent['deferrable'] is not False):
                            raise ValueError('target parent key differs from completed source migration')
                    for table in affected:
                        if state['columns'].get(table) != set(tables[table]['columns']):
                            raise ValueError('target projection differs from exported source')
                    enforcement_enabled(cursor, schema, [item['table'] for item in plan])
                print('foreign-key-orphans: verifying catalog and row baseline', flush=True)
                verify_catalog()
                for ordinal, table in enumerate(affected, 1):
                    fingerprint = table_fingerprint(cursor, schema, tables[table])
                    if fingerprint != baseline[table]['after']:
                        raise ValueError('target rows changed since completed FK migration')
                    report['table_fingerprints'][table] = dict(before=fingerprint)
                    if ordinal % 32 == 0 or ordinal == len(affected):
                        print('foreign-key-orphans: baseline fingerprints ' + str(ordinal) + '/' + str(len(affected)), flush=True)
                save_report(args.report, report)
                completed_prefix = len(report['completed'])
                print('foreign-key-orphans: counting from prefix ' + str(completed_prefix) + '/' + str(len(plan)), flush=True)
                for index, item in enumerate(plan):
                    if index < completed_prefix:
                        continue
                    report['completed'].append(dict(declaration=item, orphan_rows=count_orphans(cursor, schema, item)))
                    if (index + 1) % 32 == 0:
                        save_report(args.report, report)
                        print('foreign-key-orphans: ' + str(index + 1) + '/' + str(len(plan)), flush=True)
                print('foreign-key-orphans: verifying final catalog and row fingerprints', flush=True)
                verify_catalog()
                for ordinal, table in enumerate(affected, 1):
                    fingerprint = table_fingerprint(cursor, schema, tables[table])
                    if fingerprint != baseline[table]['after']:
                        raise ValueError('target rows changed during orphan inventory')
                    report['table_fingerprints'][table]['after'] = fingerprint
                    if ordinal % 32 == 0 or ordinal == len(affected):
                        print('foreign-key-orphans: final fingerprints ' + str(ordinal) + '/' + str(len(affected)), flush=True)
                report['inventory_completed'] = True
                report['all_relationships_clean'] = all(item['orphan_rows'] == 0 for item in report['completed'])
                report['validated_relationships_clean'] = all(item['orphan_rows'] == 0 or not item['declaration']['validated'] for item in report['completed'])
                report['success'] = report['validated_relationships_clean']
    finally:
        if can_write:
            save_report(args.report, report)
    if not report['success']:
        raise ValueError('validated source relationship has unmatched target rows; no repair was attempted')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--migration-report', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--writers-quiesced', action='store_true')
    parser.add_argument('--resume', action='store_true', help='revalidate the retained journal/baseline and count its remaining relationships')
    args = parser.parse_args()
    inventory(args)


if __name__ == '__main__':
    main()
