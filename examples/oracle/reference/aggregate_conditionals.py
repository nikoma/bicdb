"""Compare aggregate typing and conditional evaluation on owned schema objects.

Run separately against Oracle and PostgreSQL with the matching BicDB profile.
Query results use prepared PostgreSQL transport; setup DDL uses simple queries.
This fixture qualifies values and selected errors, not complete SQL conformance.
"""
import json
import uuid

from compare import canonical


def execute(connection, sql, params=None, rows=False):
    with connection.cursor() as cursor:
        if type(connection).__module__.startswith('psycopg'):
            cursor.execute(sql, params, prepare=rows)
        else:
            cursor.execute(sql, params or [])
        if rows:
            return [[canonical(value) for value in row] for row in cursor.fetchall()]


def run(reference, target, native=False):
    token = uuid.uuid4().hex[:12].upper()
    table, routine = ['BICDB_AG_' + name + '_' + token for name in ['ROWS', 'TOTAL']]
    number = lambda value: [[{'number': str(value)}]]
    plans = [
        dict(id='sum', sql=f'SELECT COALESCE(SUM(n),0) FROM {table}', rows=number(15)),
        dict(id='empty_sum', sql=f'SELECT COALESCE(SUM(n),0) FROM {table} WHERE bucket=9', rows=number(0)),
        dict(id='bound_predicate', sql=f'SELECT COALESCE(SUM(n),0) FROM {table} WHERE bucket={{P}}', params=[1], rows=number(15)),
        dict(id='routine_parameter', sql=f'SELECT {routine}({{P}})', params=[1], rows=number(15)),
        dict(id='routine_empty', sql=f'SELECT {routine}({{P}})', params=[9], rows=number(0)),
        dict(id='coalesce_skips_scalar', sql=f'SELECT COALESCE(SUM(n),1/(COUNT(*)-2)) FROM {table}', rows=number(15)),
        dict(id='case_skips_scalar', sql=f'SELECT CASE WHEN SUM(n) IS NOT NULL THEN SUM(n) ELSE 1/(COUNT(*)-2) END FROM {table}', rows=number(15)),
        dict(id='coalesce_skips_subquery', sql=f'SELECT COALESCE(SUM(n),(SELECT SUM(1/(n-n)) FROM {table})) FROM {table} GROUP BY bucket', rows=number(15)),
        dict(id='coalesce_prepares_aggregate', sql=f'SELECT COALESCE(SUM(n),SUM(1/(n-n))) FROM {table}', oracle_code=1476, sqlstate='22012'),
        dict(id='case_prepares_aggregate', sql=f'SELECT CASE WHEN SUM(n) IS NOT NULL THEN SUM(n) ELSE SUM(1/(n-n)) END FROM {table}', oracle_code=1476, sqlstate='22012'),
        dict(id='grouped_conditional', sql=f'SELECT bucket,COALESCE(SUM(n),0) FROM {table} GROUP BY bucket ORDER BY bucket', rows=[[{'number':'1'},{'number':'15'}]]),
        dict(id='empty_group_after_error', sql=f'SELECT COALESCE(SUM(n),0) FROM {table} WHERE bucket=9', rows=number(0)),
    ]
    if not native:
        plans += [
            dict(id='nvl_sum', sql=f'SELECT NVL(SUM(n),0) FROM {table}', rows=number(15)),
            dict(id='nvl_empty', sql=f'SELECT NVL(SUM(n),0) FROM {table} WHERE bucket=9', rows=number(0)),
            dict(id='nvl_max_number', sql=f'SELECT NVL(MAX(n),0) FROM {table}', rows=number(10)),
            dict(id='nvl_max_text', sql=f"SELECT NVL(MAX(label),'empty') FROM {table}", rows=[['Z']]),
            dict(id='nvl_max_date', sql=f"SELECT NVL(MAX(day),DATE '1999-01-01') FROM {table}", rows=[[{'timestamp':'2024-01-02T00:00:00.000000'}]]),
            dict(id='nvl_eager_scalar', sql=f'SELECT NVL(SUM(n),1/(COUNT(*)-2)) FROM {table}', oracle_code=1476, sqlstate='22012'),
            dict(id='nvl_eager_conversion', sql=f"SELECT NVL(SUM(n),'bad') FROM {table}", oracle_code=1722, sqlstate='22P02'),
        ]
    ddl = [("TABLE", table, f"CREATE TABLE {table}(bucket INTEGER,n NUMERIC,label VARCHAR(20),day DATE)")]
    if native:
        body = f"CREATE FUNCTION {routine}(p_id INTEGER) RETURNS NUMERIC LANGUAGE plpgsql AS $$ DECLARE result NUMERIC; BEGIN SELECT COALESCE(SUM(n),0) INTO result FROM {table} WHERE bucket=p_id; RETURN result; END; $$"
    else:
        body = f"CREATE FUNCTION {routine}(p_id NUMBER) RETURN NUMBER IS result NUMBER; BEGIN SELECT NVL(SUM(n),0) INTO result FROM {table} WHERE bucket=p_id; RETURN result; END;"
    ddl.append(('FUNCTION', routine, body))
    report = dict(format=1, reference='postgres' if native else 'oracle', application_qualified=False, target_protocol='prepared', cases=[], cleanup_errors=[], success=False)
    created = []
    try:
        for label, conn in [('source', reference), ('bicdb', target)]:
            for kind, name, sql in ddl:
                execute(conn, sql)
                created.append((label, conn, kind, name))
            for value, text, day in [(10,'A','2024-01-01'),(5,'Z','2024-01-02')]:
                execute(conn, f"INSERT INTO {table} VALUES(1,{value},'{text}',DATE '{day}')")
        for plan in plans:
            case = {key: value for key, value in plan.items() if key != 'rows'}
            for label, conn in [('source', reference), ('bicdb', target)]:
                marker = ':1' if label == 'source' and not native else '%s'
                sql = plan['sql'].replace('{P}', marker)
                if label == 'source' and not native and plan['id'].startswith('routine_'):
                    sql += ' FROM DUAL'
                try:
                    case[label] = dict(rows=execute(conn, sql, plan.get('params'), rows=True))
                except Exception as exc:
                    code = getattr(exc.args[0], 'code', None) if exc.args else None
                    case[label] = dict(oracle_code=code, sqlstate=getattr(exc, 'sqlstate', None), exception=type(exc).__name__)
            if 'rows' in plan:
                case['expected'] = dict(rows=plan['rows'])
                case['source_qualified'] = case['source'] == case['expected']
                case['matched'] = case['source_qualified'] and case['bicdb'] == case['expected']
            else:
                key = 'sqlstate' if native else 'oracle_code'
                case['source_qualified'] = case['source'].get(key) == plan[key]
                case['matched'] = case['source_qualified'] and case['bicdb'].get('sqlstate') == plan['sqlstate']
            report['cases'].append(case)
    finally:
        for label, conn, kind, name in reversed(created):
            try:
                suffix = '(INTEGER)' if native and kind == 'FUNCTION' else ''
                execute(conn, f'DROP {kind} {name}{suffix}')
            except Exception as exc:
                report['cleanup_errors'].append(dict(backend=label, kind=kind, exception=type(exc).__name__))
    report['total'] = len(report['cases'])
    report['matched'] = sum(case['matched'] for case in report['cases'])
    report['source_qualified'] = len(report['cases']) == len(plans) and all(case['source_qualified'] for case in report['cases'])
    report['success'] = report['source_qualified'] and report['matched'] == report['total'] and not report['cleanup_errors']
    return report


def main(runner=run, description=None):
    import argparse
    import os
    from pathlib import Path
    import psycopg
    parser = argparse.ArgumentParser(description=description or __doc__)
    parser.add_argument('--reference', choices=['oracle', 'postgres'], required=True)
    parser.add_argument('--oracle-config', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.reference == 'oracle':
        import oracledb
        oracledb.defaults.fetch_decimals = True
        if args.oracle_config is None:
            parser.error('--oracle-config required for Oracle')
        config = json.loads(args.oracle_config.read_text())
        if config['user'].upper() in ['SYS', 'SYSTEM']:
            raise ValueError('dedicated non-administrative reference schema required')
        password = Path(config['password_file'])
        if not password.is_absolute():
            password = args.oracle_config.parent / password
        source = oracledb.connect(user=config['user'], password=password.read_text().strip(), host=config['host'], port=config['port'], service_name=config['service_name'])
        source.call_timeout = 15000
    else:
        source = psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'], autocommit=True, connect_timeout=10)
    with source, psycopg.connect(os.environ['BICDB_PG_DSN'], autocommit=True, connect_timeout=10) as target:
        source.autocommit = True
        expected = 'postgres' if args.reference == 'postgres' else 'oracle-preview-v1'
        if target.info.parameter_status('bicdb_sql_profile') != expected:
            raise RuntimeError('target profile does not match selected reference')
        report = runner(source, target, args.reference == 'postgres')
        report['source_version'] = source.info.server_version if args.reference == 'postgres' else source.version
        report['target_sql_profile'] = expected
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: report[key] for key in ['success', 'matched', 'total']}))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
