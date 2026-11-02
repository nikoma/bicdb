#!/usr/bin/env python3
"""Compare generated routine namespace cases with Oracle and BicDB preview.

Use dedicated test databases. This runner creates and drops only its own
uniquely named tables/functions; connection information stays outside reports.
"""
import argparse
import datetime
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from compare import canonical, compare_cases


def identifier(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('expected a nonempty identifier without NUL')
    return '"' + value.replace('"', '""') + '"'


def error_summary(error):
    detail = error.args[0] if error.args else None
    return dict(error=type(error).__name__,
                code=getattr(error, 'sqlstate', None) or getattr(detail, 'full_code', None))


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    owner = identifier(schema)
    table, auxiliary, cte = (prefix + suffix for suffix in ('_T', '_A', '_C'))
    normal = [(7, '33'), (8, '44'), (9, '0'), (7, '33')]
    bodies = [
        ('cursor', f'total NUMBER:=0; CURSOR c IS SELECT v FROM {table} WHERE bucket=p_bucket; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;', normal),
        ('cte', f'total NUMBER:=0; CURSOR c IS WITH {cte} AS (SELECT {table}.v FROM {table} WHERE bucket=p_bucket) SELECT v FROM {cte}; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;', normal),
        ('select', f'result NUMBER; BEGIN SELECT SUM(v) INTO result FROM {table} WHERE bucket=p_bucket; RETURN result; END;', [(7, '33'), (8, '44'), (9, None), (7, '33')]),
        ('query', f'total NUMBER:=0; BEGIN FOR r IN (SELECT v FROM {table} WHERE bucket=p_bucket) LOOP total:=total+r.v; END LOOP; RETURN total; END;', normal),
        ('nested', f'total NUMBER:=0; chosen_bucket NUMBER; CURSOR parents IS SELECT DISTINCT bucket FROM {table} WHERE bucket<=p_bucket; CURSOR children IS SELECT v FROM {table} WHERE bucket=chosen_bucket; BEGIN FOR r IN parents LOOP chosen_bucket:=r.bucket; FOR child IN children LOOP total:=total+r.bucket*child.v; END LOOP; END LOOP; RETURN total; END;', [(7, '231'), (8, '583'), (6, '0'), (8, '583')]),
        ('qualified', f'total NUMBER:=0; CURSOR c IS SELECT v FROM {owner}.{identifier(auxiliary)} WHERE bucket=p_bucket; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;', [(7, '100'), (8, '200'), (9, '0'), (7, '100')]),
    ]
    setup = []
    for name, rows in [(table, ['7,11', '7,22', '8,44']),
                       (auxiliary, ['7,100', '8,200'])]:
        qualified = f'{owner}.{identifier(name)}'
        for backend in (0, 1):
            setup.append(dict(backend=backend, kind='TABLE', name=qualified,
                              sql=f'CREATE TABLE {qualified}(bucket NUMBER,v NUMBER)',
                              inserts=[f'INSERT INTO {qualified} VALUES({row})' for row in rows]))
    for name in (table, auxiliary, cte):
        qualified = f'"public".{identifier(name)}'
        setup.append(dict(backend=1, kind='TABLE', name=qualified,
                          sql=f'CREATE TABLE {qualified}(bucket NUMBER,v NUMBER)',
                          inserts=[f'INSERT INTO {qualified} VALUES(7,-999)']))
    cases = []
    for suffix, body, arguments in bodies:
        name = f'{owner}.{identifier(prefix + "_" + suffix.upper())}'
        for backend in (0, 1):
            setup.append(dict(backend=backend, kind='FUNCTION', name=name,
                              sql=f'CREATE FUNCTION {name}(p_bucket NUMBER) RETURN NUMBER IS {body}',
                              inserts=[]))
        for index, (bucket, expected) in enumerate(arguments):
            cases.append(dict(id=f'{suffix}_{index}',
                              sql=f'SELECT {name}({bucket}) FROM DUAL',
                              expected=[[canonical(None if expected is None else decimal.Decimal(expected))]]))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_CURSOR').lower())


def run(oracle, bicdb, schema, prefix, suite='namespaces'):
    if suite == 'procedures':
        from routine_procedures import fixture as make_fixture
    elif suite == 'nvl':
        from routine_nvl import fixture as make_fixture
    elif suite == 'characters':
        from routine_characters import fixture as make_fixture
    elif suite == 'columns':
        from routine_column_types import fixture as make_fixture
    elif suite == 'decode':
        from routine_decode import fixture as make_fixture
    elif suite == 'numberops':
        from routine_number_arithmetic import fixture as make_fixture
    elif suite == 'cursors':
        from routine_cursors import fixture as make_fixture
    elif suite == 'concat':
        from routine_concat import fixture as make_fixture
    elif suite == 'case':
        from routine_case import fixture as make_fixture
    elif suite == 'anchors':
        from routine_anchors import fixture as make_fixture
    elif suite == 'temporal':
        from routine_temporal import fixture as make_fixture
    elif suite == 'week':
        from routine_week import fixture as make_fixture
    elif suite == 'calendar':
        from routine_calendar import fixture as make_fixture
    else:
        make_fixture = fixture
    plan = make_fixture(schema, prefix)
    report = dict(format=1, oracle_database_version=oracle.version,
                  bicdb_profile='oracle-preview-v1',
                  fixture_prefix=prefix,
                  fixture_sha256=hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
                  total=len(plan['cases']), matched=0, cases=[], cleanup_errors=[], success=False,
                  scope=plan.get('scope', 'Generated static routine namespace values; not full PL/SQL or AUTHID semantics'))
    created = []
    with oracle.cursor() as source, bicdb.cursor() as target:
        cursors = (source, target)
        try:
            if suite in ('anchors', 'concat', 'decode', 'characters', 'nvl', 'procedures'):
                source.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER='NLS_NUMERIC_CHARACTERS'")
                report['oracle_nls_numeric_characters'] = source.fetchone()[0]
                if report['oracle_nls_numeric_characters'] != '.,':
                    raise ValueError('routine fixture requires dot decimal reference formatting')
            if suite in ('concat', 'decode', 'characters', 'nvl', 'procedures'):
                source.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_DATE_FORMAT','NLS_DATE_LANGUAGE','NLS_LENGTH_SEMANTICS','NLS_CALENDAR')")
                report['oracle_nls_concat'] = dict(source.fetchall())
                if report['oracle_nls_concat'] != dict(NLS_DATE_FORMAT='DD-MON-RR', NLS_DATE_LANGUAGE='AMERICAN', NLS_LENGTH_SEMANTICS='BYTE', NLS_CALENDAR='GREGORIAN'):
                    raise ValueError('concatenation fixture requires the recorded preview NLS defaults')
                source.execute("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'")
                report['oracle_database_character_set'] = source.fetchone()[0]
                if report['oracle_database_character_set'] != 'AL32UTF8':
                    raise ValueError('concatenation byte fixtures require AL32UTF8')
            if suite in ('calendar', 'week'):
                source.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER='NLS_CALENDAR'")
                report['oracle_nls_calendar'] = source.fetchone()[0]
                if report['oracle_nls_calendar'] != 'GREGORIAN':
                    raise ValueError('calendar fixture requires the Gregorian reference calendar')
            if suite == 'week':
                source.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER='NLS_TERRITORY'")
                report['oracle_nls_territory'] = source.fetchone()[0]
                if report['oracle_nls_territory'] != 'AMERICA':
                    raise ValueError('week fixture requires the America reference territory')
            for obj in plan['setup']:
                cursor = cursors[obj['backend']]
                cursor.execute(obj['sql'])
                created.append(obj)
                for sql in obj['inserts']:
                    cursor.execute(sql)
                if 'copy_rows' in obj:
                    if obj['backend'] != 1:
                        raise ValueError('COPY fixture requires PostgreSQL transport')
                    def copy_value(value):
                        if isinstance(value, dict) and set(value) == {'timestamp'}:
                            return datetime.datetime.fromisoformat(value['timestamp'])
                        if isinstance(value, dict) and set(value) == {'number'}:
                            return decimal.Decimal(value['number'])
                        return value
                    with cursor.copy('COPY ' + obj['name'] + ' FROM STDIN') as copy:
                        for row in obj['copy_rows']:
                            copy.write_row([copy_value(value) for value in row])
            target.execute('''SELECT l."lanname" FROM "pg_catalog"."pg_proc" p
                JOIN "pg_catalog"."pg_language" l ON p."prolang"=l."oid"
                WHERE p."proname"=%s''', (plan['compiler_probe'],))
            report['routine_compiler'] = target.fetchone()[0]
            if 'temporal_metadata' in plan:
                metadata = plan['temporal_metadata']
                source.execute('SELECT COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER=:owner AND TABLE_NAME=:table_name',
                               dict(owner=metadata['owner'], table_name=metadata['table']))
                reference_types = dict(source.fetchall())
                report['temporal_column_metadata'] = []
                for column, expected in metadata['expected'].items():
                    target.execute('SELECT bicdb_oracle_column_type(%s,%s)', (metadata['relation'], column))
                    actual = target.fetchone()[0]
                    matched = reference_types.get(column) == actual == expected
                    report['temporal_column_metadata'].append(dict(column=column, oracle=reference_types.get(column), bicdb=actual, expected=expected, matched=matched))
                if not all(row['matched'] for row in report['temporal_column_metadata']):
                    raise ValueError('logical temporal column metadata differs from Oracle')
            if 'projection_types' in plan:
                projection = plan['projection_types']
                source.execute(projection['sql'])
                target.execute(projection['sql'])
                report['oracle_projection_types'] = [column.type_code.name for column in source.description]
                report['bicdb_projection_types'] = [column.type_code for column in target.description]
                if report['oracle_projection_types'] != projection['oracle'] or report['bicdb_projection_types'] != projection['bicdb']:
                    raise ValueError('temporal projection metadata differs from its declared transport contract')
            report['cases'] = compare_cases(oracle, bicdb, plan['cases'])
            for case, result in zip(plan['cases'], report['cases']):
                if 'expected_error' in case:
                    result['expected_error'] = case['expected_error']
                else:
                    result['expected'] = case['expected']
                    result['matched'] &= result['oracle'].get('rows') == case['expected']
            if suite == 'nvl':
                from routine_nvl import compare_bindings
                bindings = compare_bindings(oracle, bicdb)
                report['cases'].extend(bindings)
                report['total'] += len(bindings)
            if suite == 'procedures':
                from routine_procedures import compare_bindings
                bindings = compare_bindings(oracle, bicdb, schema, prefix)
                report['cases'].extend(bindings)
                report['total'] += len(bindings)
            report['matched'] = sum(result['matched'] for result in report['cases'])
        except Exception as error:
            report['setup_error'] = error_summary(error)
        finally:
            for obj in reversed(created):
                try:
                    suffix = ' PURGE' if obj['backend'] == 0 and obj['kind'] == 'TABLE' else ''
                    cursors[obj['backend']].execute(f'DROP {obj["kind"]} {obj["name"]}{suffix}')
                except Exception as error:
                    report['cleanup_errors'].append(dict(error_summary(error),
                        backend='oracle' if obj['backend'] == 0 else 'bicdb',
                        kind=obj['kind'], name=obj['name']))
    report['success'] = (report['matched'] == report['total']
                         and not report['cleanup_errors'] and 'setup_error' not in report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', required=True, type=Path)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--suite', choices=('namespaces', 'temporal', 'calendar', 'week', 'anchors', 'case', 'concat', 'cursors', 'numberops', 'decode', 'columns', 'characters', 'nvl', 'procedures'), default='namespaces')
    args = parser.parse_args()
    report = dict(format=1, success=False)
    try:
        import oracledb
        import psycopg
        config = json.loads(args.oracle_config.read_text())
        if config['user'].upper() in ('SYS', 'SYSTEM'):
            raise ValueError('a dedicated non-administrative reference schema is required')
        password = Path(config['password_file'])
        if not password.is_absolute():
            password = args.oracle_config.parent / password
        oracledb.defaults.fetch_decimals = True
        with oracledb.connect(user=config['user'], password=password.read_text().strip(),
                              host=config['host'], port=config['port'],
                              service_name=config['service_name']) as oracle:
            oracle.autocommit = True
            oracle.call_timeout = 10000
            with oracle.cursor() as cursor:
                cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
                schema = cursor.fetchone()[0]
            with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                                 options='-c bicdb.sql_profile=oracle-preview-v1', connect_timeout=10) as bicdb:
                if bicdb.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                    raise RuntimeError('server did not confirm the requested SQL profile')
                report = run(oracle, bicdb, schema, 'BICDB_RF_' + uuid.uuid4().hex[:10].upper(), args.suite)
                report.update(oracle_driver_version=oracledb.__version__, pg_driver_version=psycopg.__version__)
    except Exception as error:
        report['error'] = error_summary(error)
        report['success'] = False
    report['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'{report.get("matched", 0)}/{report.get("total", "?")} routine results match; success={report["success"]}')
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
