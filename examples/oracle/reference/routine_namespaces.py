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
    if suite == 'temporal':
        from routine_temporal import fixture as make_fixture
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
            for obj in plan['setup']:
                cursor = cursors[obj['backend']]
                cursor.execute(obj['sql'])
                created.append(obj)
                for sql in obj['inserts']:
                    cursor.execute(sql)
            target.execute('''SELECT l."lanname" FROM "pg_catalog"."pg_proc" p
                JOIN "pg_catalog"."pg_language" l ON p."prolang"=l."oid"
                WHERE p."proname"=%s''', (plan['compiler_probe'],))
            report['routine_compiler'] = target.fetchone()[0]
            report['cases'] = compare_cases(oracle, bicdb, plan['cases'])
            for case, result in zip(plan['cases'], report['cases']):
                result['expected'] = case['expected']
                result['matched'] &= result['oracle'].get('rows') == case['expected']
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
    parser.add_argument('--suite', choices=('namespaces', 'temporal'), default='namespaces')
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
