"""Logical numeric primary keys in MIN/MAX, range locators and transactions.

Runs only on disposable nonce tables. Oracle and PostgreSQL are independently
checked against exact expected values; no application schema is required.
This does not qualify every primary-key type, collation or concurrent schedule.
"""
import decimal
import uuid
from row_lock_waits import main as comparison_main


def canonical(value):
    if isinstance(value, (int, float, decimal.Decimal)):
        # Decimal.normalize uses the ambient precision, which can silently
        # round the 30-digit integer controls this fixture must preserve.
        with decimal.localcontext() as context:
            context.prec = 100
            return {'number': str(decimal.Decimal(str(value)).normalize())}
    return value


def observe(factory):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        driver = type(conn).__module__.startswith('oracledb')
        conn.autocommit = True
        report['server_version'] = conn.version if driver else conn.info.server_version
        nonce = 'BICDB_EX_' + uuid.uuid4().hex[:10].upper()
        single, composite = nonce + 'S', nonce + 'C'
        made = []

        def execute(sql, parameters=None):
            with conn.cursor() as cursor:
                if driver:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql, parameters, prepare=True)
                return ([[canonical(v) for v in row] for row in cursor.fetchall()]
                        if cursor.description else cursor.rowcount)

        def check(label, sql, expected, parameters=None):
            case = dict(id=label, expected=[[canonical(v) for v in row] for row in expected])
            try:
                if not conn.autocommit:
                    execute('SAVEPOINT extrema_check')
                case['actual'] = execute(sql, parameters)
                case['qualified'] = case['actual'] == case['expected']
            except Exception as error:
                if not conn.autocommit:
                    execute('ROLLBACK TO SAVEPOINT extrema_check')
                case.update(qualified=False, error=type(error).__name__, sqlstate=getattr(error, 'sqlstate', None), detail=str(error)[:500])
            report['cases'].append(case)

        try:
            numeric = 'NUMBER' if driver else 'NUMERIC'
            text = 'VARCHAR2(10)' if driver else 'VARCHAR(10)'
            execute(f'CREATE TABLE {single}(id {numeric}(30,0) PRIMARY KEY)')
            made.append(single)
            for value in [-100000000000000000000000000001, 2, 100000000000000000000000000003]:
                execute(f'INSERT INTO {single} VALUES ({value})')
            execute(f'CREATE TABLE {composite}(tenant {numeric}(10,2), k {numeric}(10,2), suffix {text}, PRIMARY KEY(tenant,k,suffix))')
            made.append(composite)
            for tenant, key, suffix in [(1, '-2.5', '01'), (1, '3.25', '1'), (1, '10.5', '01'), (2, '20', '01')]:
                execute(f"INSERT INTO {composite} VALUES ({tenant},{key},'{suffix}')")
            check('numeric_min', f'SELECT MIN(id) FROM {single}', [[-100000000000000000000000000001]])
            check('numeric_max', f'SELECT MAX(id) FROM {single}', [[100000000000000000000000000003]])
            check('numeric_both', f'SELECT MIN(id),MAX(id) FROM {single}', [[-100000000000000000000000000001, 100000000000000000000000000003]])
            check('empty_min', f'SELECT MIN(id) FROM {single} WHERE id< -100000000000000000000000000001', [[None]])
            check('empty_max', f'SELECT MAX(id) FROM {single} WHERE id>100000000000000000000000000003', [[None]])
            check('prefix_min', f'SELECT MIN(k) FROM {composite} WHERE tenant=1.00', [[decimal.Decimal('-2.5')]])
            check('prefix_max', f'SELECT MAX(k) FROM {composite} WHERE tenant=1', [[decimal.Decimal('10.5')]])
            check('suffix_filter', f"SELECT MIN(k) FROM {composite} WHERE tenant=1 AND suffix='1'", [[decimal.Decimal('3.25')]])
            check('suffix_filter_max', f"SELECT MAX(k) FROM {composite} WHERE tenant=1 AND suffix='01'", [[decimal.Decimal('10.5')]])
            check('range_inclusive', f'SELECT k FROM {composite} WHERE tenant=1 AND k>=3.25 AND k<=10.5 ORDER BY k', [[decimal.Decimal('3.25')], [decimal.Decimal('10.5')]])
            check('range_exclusive', f'SELECT k FROM {composite} WHERE tenant=1 AND k>3.25 AND k<10.5 ORDER BY k', [])
            check('range_negative', f'SELECT k FROM {composite} WHERE tenant=1 AND k<0 ORDER BY k', [[decimal.Decimal('-2.5')]])
            check('range_empty', f'SELECT k FROM {composite} WHERE tenant=1 AND k>30', [])
            check('text_suffix_min', f'SELECT MIN(suffix) FROM {composite} WHERE tenant=1 AND k=-2.5', [['01']])
            bind = ':tenant' if driver else '%s'
            check('prepared_prefix', f'SELECT MAX(k) FROM {composite} WHERE tenant={bind}', [[decimal.Decimal('20')]], {'tenant': 2} if driver else (2,))
            conn.autocommit = False
            execute(f"INSERT INTO {composite} VALUES(1,30,'01')")
            check('pending_insert', f'SELECT MAX(k) FROM {composite} WHERE tenant=1', [[30]])
            check('pending_range', f'SELECT k FROM {composite} WHERE tenant=1 AND k>10.5 ORDER BY k', [[30]])
            execute(f"UPDATE {composite} SET k=40 WHERE tenant=1 AND k=30")
            check('pending_key_update', f'SELECT MAX(k) FROM {composite} WHERE tenant=1', [[40]])
            execute(f'DELETE FROM {composite} WHERE tenant=1 AND k=40')
            check('pending_delete', f'SELECT MAX(k) FROM {composite} WHERE tenant=1', [[decimal.Decimal('10.5')]])
            conn.rollback()
            conn.autocommit = True
            check('rollback', f'SELECT MAX(k) FROM {composite} WHERE tenant=1', [[decimal.Decimal('10.5')]])
        finally:
            conn.rollback()
            conn.autocommit = True
            for table in reversed(made):
                try:
                    execute('DROP TABLE ' + table + (' PURGE' if driver else ''))
                except Exception as error:
                    report['cleanup_errors'].append(dict(table=table, detail=str(error)[:300]))
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'], matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False, reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(c['matched'] for c in cases), source_qualified=all(c['source_qualified'] for c in cases),
                cleanup_errors=cleanup, success=len(cases) == 20 and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
