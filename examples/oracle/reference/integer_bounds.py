"""Integer primary/secondary index bounds with independent numeric controls."""
import decimal
import uuid
from row_update import canonical
from row_lock_waits import main as comparison_main


def observe(factory):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        driver = type(conn).__module__.startswith('oracledb')
        conn.autocommit = True
        report['server_version'] = conn.version if driver else conn.info.server_version
        table = 'BICDB_IB_' + uuid.uuid4().hex[:10].upper()
        made = False

        def execute(sql, parameters=None):
            with conn.cursor() as q:
                if driver:
                    q.execute(sql, parameters)
                else:
                    q.execute(sql, parameters, prepare=True)
                return ([[canonical(v) for v in row] for row in q.fetchall()]
                        if q.description else q.rowcount)

        try:
            execute(f'CREATE TABLE {table}(id INTEGER PRIMARY KEY,n INTEGER)')
            made = True
            for value in [-2, 0, 2, 3]:
                execute(f'INSERT INTO {table} VALUES({value},{value})')
            execute(f'CREATE INDEX {table}I ON {table}(n)')
            for column in ['id', 'n']:
                controls = [
                    ('positive', column+'>0', [2, 3]),
                    ('inclusive', column+'>=1', [2, 3]),
                    ('fractional_lower', column+'>1.5', [2, 3]),
                    ('fractional_upper', column+'<=2.5', [-2, 0, 2]),
                    ('outside_lower', column+'>9223372036854775808', []),
                    ('outside_upper', column+'<9223372036854775808', [-2, 0, 2, 3]),
                    ('reversed_lower', '1.5<'+column, [2, 3]),
                    ('reversed_upper', '2.5>='+column, [-2, 0, 2]),
                    ('prepared_fraction', column+'>'+(':bound' if driver else '%s'), [2, 3]),
                ]
                for label, predicate, expected in controls:
                    case = dict(id=column+'_'+label,
                                expected=[[canonical(v)] for v in expected])
                    parameters = None
                    if label == 'prepared_fraction':
                        value = decimal.Decimal('1.5')
                        parameters = {'bound': value} if driver else (value,)
                    try:
                        case['actual'] = execute(f'SELECT id FROM {table} WHERE {predicate} ORDER BY id', parameters)
                        case['qualified'] = case['actual'] == case['expected']
                    except Exception as error:
                        case.update(qualified=False, error=str(error)[:400])
                    report['cases'].append(case)
        finally:
            if made:
                try:
                    execute('DROP TABLE '+table+(' PURGE' if driver else ''))
                except Exception as error:
                    report['cleanup_errors'].append(dict(detail=str(error)[:300]))
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors']+target['cleanup_errors']
    return dict(format=1, application_qualified=False,
                reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(c['matched'] for c in cases),
                source_qualified=all(c['source_qualified'] for c in cases),
                cleanup_errors=cleanup,
                success=len(cases) == 18 and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
