#!/usr/bin/env python3
"""Establish independent Oracle and PostgreSQL nested-block state contracts.

Use fresh connections to dedicated test databases. Only synthetic tables with
unique names are created; writes are rolled back and the tables are removed.
Optional BicDB connections execute the same contracts with each SQL profile.
"""
import argparse
import json
import os
from pathlib import Path
import uuid

from compare import canonical
from routine_namespaces import error_summary, identifier


def fixture(table, results, backend):
    oracle = backend == 'oracle'
    zero = 'ZERO_DIVIDE' if oracle else 'division_by_zero'
    duplicate = 'DUP_VAL_ON_INDEX' if oracle else 'unique_violation'
    missing = 'NO_DATA_FOUND' if oracle else 'no_data_found'
    cases = [
        ('handled_assignment', f'''BEGIN
            UPDATE {table} SET n=20 WHERE id=1;
            local_n:=7; RAISE {zero};
            EXCEPTION WHEN {zero} THEN
                SELECT n INTO result_n FROM {table} WHERE id=1;
            END;'''),
        ('failed_statement_atomicity', f'''BEGIN
            UPDATE {table} SET n=20 WHERE id=1; local_n:=7;
            INSERT INTO {table} SELECT 2,30{' FROM DUAL' if oracle else ''}
                UNION ALL SELECT 1,40{' FROM DUAL' if oracle else ''};
            EXCEPTION WHEN {duplicate} THEN
                SELECT n INTO result_n FROM {table} WHERE id=1;
            END;'''),
        ('handler_propagation', f'''BEGIN BEGIN
            UPDATE {table} SET n=20 WHERE id=1; RAISE {zero};
            EXCEPTION WHEN {zero} THEN local_n:=7; RAISE {missing};
            WHEN {missing} THEN local_n:=-999; END;
            EXCEPTION WHEN {missing} THEN
                SELECT n INTO result_n FROM {table} WHERE id=1;
            END;'''),
    ]
    data_type = 'NUMBER' if oracle else 'INTEGER'
    plan = []
    for name, body in cases:
        sql = f'''DECLARE local_n {data_type}:=0; result_n {data_type};
            BEGIN INSERT INTO {table} VALUES(1,10); {body}
            INSERT INTO {results}
                SELECT result_n,local_n,COUNT(*) FROM {table}; END;'''
        if not oracle:
            sql = 'DO $nested_contract$' + sql + '$nested_contract$'
        plan.append(dict(id=name, sql=sql, expected=dict(
            value=canonical(20 if oracle else 10), local_value=canonical(7),
            row_count=canonical(1))))
    return plan


def run_reference(connection, backend, schema=None, *, bicdb=False):
    if backend not in ('oracle', 'postgres'):
        raise ValueError('expected an Oracle or PostgreSQL reference')
    if backend == 'oracle' and not bicdb and (not schema or schema.upper() in ('SYS', 'SYSTEM')):
        raise ValueError('a dedicated non-administrative Oracle schema is required')
    prefix = 'BICDB_NEST_' + uuid.uuid4().hex[:10].upper()
    names = [identifier(prefix + suffix) for suffix in ('_DATA', '_RESULTS')]
    if backend == 'oracle' and schema:
        names = [identifier(schema) + '.' + name for name in names]
    table, results = names
    report = dict(format=1, backend=('bicdb_' if bicdb else '') + backend, reference_only=not bicdb,
                  application_qualified=False, cases=[], cleanup_errors=[], success=False,
                  scope='Synthetic nested-block write rollback, local values and handler propagation')
    report['database_version'] = (connection.version if backend == 'oracle' and not bicdb
                                  else connection.info.server_version)
    previous_autocommit = connection.autocommit
    created = []
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            temporary = '' if backend == 'oracle' else 'TEMP '
            data_type = 'NUMBER' if backend == 'oracle' else 'INTEGER'
            for name, columns in [(table, f'id {data_type} PRIMARY KEY,n {data_type}'),
                                  (results, f'value {data_type},local_value {data_type},row_count {data_type}')]:
                cursor.execute(f'CREATE {temporary}TABLE {name}({columns})')
                created.append(name)
        connection.autocommit = False
        for case in fixture(table, results, backend):
            row = dict(id=case['id'], expected=case['expected'], matched=False)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(case['sql'], **({'prepare': False} if bicdb else {}))
                    cursor.execute(f'SELECT value,local_value,row_count FROM {results}')
                    values = cursor.fetchall()
                    if len(values) != 1:
                        raise ValueError('expected one synthetic result row')
                    row['actual'] = dict(zip(case['expected'], map(canonical, values[0])))
                    row['matched'] = row['actual'] == case['expected']
            except Exception as error:
                row['error'] = error_summary(error)
            finally:
                connection.rollback()
            with connection.cursor() as cursor:
                row['rollback_verified'] = True
                for name in names:
                    cursor.execute(f'SELECT COUNT(*) FROM {name}')
                    row['rollback_verified'] &= cursor.fetchone()[0] == 0
            report['cases'].append(row)
    except Exception as error:
        report['setup_error'] = error_summary(error)
    finally:
        connection.rollback()
        connection.autocommit = True
        with connection.cursor() as cursor:
            for name in reversed(created):
                try:
                    cursor.execute('DROP TABLE ' + name + (' PURGE' if backend == 'oracle' and not bicdb else ''))
                except Exception as error:
                    report['cleanup_errors'].append(error_summary(error))
        connection.autocommit = previous_autocommit
    report['success'] = (len(report['cases']) == 3 and not report['cleanup_errors']
                         and 'setup_error' not in report
                         and all(row['matched'] and row['rollback_verified'] for row in report['cases']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--postgres-dsn-env', default='ORACLE_CONTROL_PG_DSN')
    parser.add_argument('--bicdb-oracle-dsn-env', help='Environment variable containing a dedicated preview-profile DSN')
    parser.add_argument('--bicdb-postgres-dsn-env', help='Environment variable containing a dedicated native-profile DSN')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    import oracledb
    import psycopg
    oracledb.defaults.fetch_decimals = True
    report = dict(format=1, reference_only=True, bicdb_executed=False, success=False)
    try:
        config = json.loads(args.oracle_config.read_text())
        password = Path(config['password_file'])
        if not password.is_absolute():
            password = args.oracle_config.parent / password
        with oracledb.connect(user=config['user'], password=password.read_text().strip(),
                              host=config['host'], port=config['port'],
                              service_name=config['service_name']) as oracle:
            oracle.call_timeout = 15000
            report['oracle'] = run_reference(oracle, 'oracle', config['user'])
        with psycopg.connect(os.environ[args.postgres_dsn_env], autocommit=True,
                             options='-c statement_timeout=15000', connect_timeout=10) as postgres:
            report['postgres'] = run_reference(postgres, 'postgres')
        report['success'] = report['oracle']['success'] and report['postgres']['success']
        for backend in ('oracle', 'postgres'):
            variable = getattr(args, 'bicdb_' + backend + '_dsn_env')
            if variable:
                profile = 'oracle-preview-v1' if backend == 'oracle' else 'postgres'
                with psycopg.connect(os.environ[variable], autocommit=True, connect_timeout=10,
                                     options='-c bicdb.sql_profile=' + profile) as bicdb:
                    report['bicdb_' + backend] = run_reference(bicdb, backend, bicdb=True)
                report['reference_only'] = False
                report['bicdb_executed'] = True
                report['success'] &= report['bicdb_' + backend]['success']
    except Exception as error:
        report['error'] = error_summary(error)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print('Nested-block reference contracts passed' if report['success'] else 'Nested-block reference contracts failed')
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
