"""Matched-update MERGE, filters, repeated matches and statement rollback.

Oracle's UPDATE WHERE and repeated no-op matches have separate PostgreSQL
controls. This fixture does not qualify insert/delete actions, concurrency,
all trigger behavior, top-level WITH or complete MERGE conformance.
"""
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main

CASES = ['matched', 'action_filter', 'derived_join', 'empty_source', 'false_on',
         'null_on', 'check_rollback', 'on_column', 'noop_then_change',
         'change_then_noop', 'all_noop', 'repeat_change', 'two_noops_then_change', 'quoted_case', 'bound_parameters']


def error_kind(error):
    code = getattr(error.args[0], 'code', None) if error.args else None
    state = getattr(error, 'sqlstate', None)
    if code == 30926 or state == '21000':
        return 'cardinality'
    if code == 2290 or state == '23514':
        return 'check'
    if code == 38104 or 'ORA-38104' in str(error):
        return 'on_column'
    return 'unexpected'


def observe(factory, oracle):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        driver = type(conn).__module__.startswith('oracledb')
        report['server_version'] = conn.version if driver else conn.info.server_version

        def execute(sql, parameters=None):
            with conn.cursor() as q:
                q.execute(sql, parameters) if driver else q.execute(sql, parameters, prepare=True)
                return ([[canonical(v) for v in row] for row in q.fetchall()]
                        if q.description else q.rowcount)

        for label in CASES:
            nonce = 'BICDB_MG_' + uuid.uuid4().hex[:10].upper()
            target, source = nonce + 'T', nonce + 'S'
            made = []
            initial = [[canonical(1), canonical(0)], [canonical(2), canonical(20)], [canonical(4), None]]
            after = [[canonical(1), canonical(10)], [canonical(2), canonical(30)], [canonical(4), None]]
            expected = dict(affected=3, error=None, rows=after)
            values = [(1, 1, 10), (2, 2, 30), (3, 3, 40), (4, 4, None)]
            using = source + ' s'
            on = 't.id=s.id'
            assignment = ('t.n' if oracle else 'n') + '=s.n'
            predicate = ''
            if label == 'action_filter':
                predicate = "t.active='Y'"
                expected.update(affected=2, rows=[[canonical(1), canonical(10)], initial[1], initial[2]])
            if label == 'derived_join':
                using = f'(SELECT a.id,COALESCE(b.n,a.n) AS n FROM {source} a LEFT JOIN {source} b ON a.seq=b.seq WHERE a.seq<=2) s'
                expected.update(affected=2)
            if label == 'empty_source':
                using = f'(SELECT * FROM {source} WHERE seq<0) s'
                expected.update(affected=0, rows=initial)
            if label in ['false_on', 'null_on']:
                on = '1=0' if label == 'false_on' else 't.n=s.n'
                expected.update(affected=0, rows=initial)
                if label == 'null_on':
                    assignment = ('t.active' if oracle else 'active') + "='N'"
            if label == 'check_rollback':
                values = [(1, 1, 10), (2, 2, -1)]
                expected.update(affected=None, error='check', rows=initial)
            if label == 'on_column':
                on = 't.n=s.id'
                expected.update(affected=None if oracle else 0, error='on_column' if oracle else None, rows=initial)
            duplicate_values = {'noop_then_change': [0, 1], 'change_then_noop': [1, 0],
                                'all_noop': [0, 0], 'repeat_change': [1, 1], 'two_noops_then_change': [0, 0, 1]}
            if label in duplicate_values:
                ns = duplicate_values[label]
                values = [(i + 1, 1, n) for i, n in enumerate(ns)]
                using = f'(SELECT * FROM {source} ORDER BY seq) s'
                success = oracle and label in ['noop_then_change', 'all_noop', 'two_noops_then_change']
                expected.update(affected=len(ns) if success else None, error=None if success else 'cardinality',
                                rows=[[canonical(1), canonical(ns[-1])], initial[1], initial[2]] if success else initial)
            parameters = None
            if label == 'bound_parameters':
                bind = lambda name: ':' + name if driver else '%s'
                using = f'(SELECT * FROM {source} WHERE seq>={bind("cutoff")}) s'
                assignment = ('t.n' if oracle else 'n') + '=s.n+' + bind('delta')
                predicate = 't.active=' + bind('flag')
                parameters = {'cutoff': 1, 'delta': 5, 'flag': 'Y'} if driver else ((1, 5, 'Y') if oracle else (1, 'Y', 5))
                expected.update(affected=2, rows=[[canonical(1), canonical(15)], initial[1], initial[2]])
            extra = ',"Key" INTEGER,"key" INTEGER' if label == 'quoted_case' else ''
            projection = 'id,n,"Key","key"' if label == 'quoted_case' else 'id,n'
            if label == 'quoted_case':
                on = 't."Key"=s.id'
                assignment = ('t."key"' if oracle else '"key"') + '=s.n'
                expected['rows'] = [r + [canonical(1), canonical(10)] for r in initial]
            row = dict(id=label, expected=expected)
            try:
                execute(f'CREATE TABLE {target}(id INTEGER PRIMARY KEY,n INTEGER CHECK(n>=0),active CHAR(1){extra})')
                made.append(target)
                execute(f'CREATE TABLE {source}(seq INTEGER PRIMARY KEY,id INTEGER,n INTEGER)')
                made.append(source)
                for ident, number, active in [(1, 0, 'Y'), (2, 20, 'N'), (4, None, 'Y')]:
                    suffix = ',1,2' if label == 'quoted_case' else ''
                    execute(f"INSERT INTO {target} VALUES({ident},{'NULL' if number is None else number},'{active}'{suffix})")
                for seq, ident, number in values:
                    execute(f"INSERT INTO {source} VALUES({seq},{ident},{'NULL' if number is None else number})")
                clause = ('WHEN MATCHED THEN UPDATE SET ' + assignment + (' WHERE ' + predicate if predicate else '') if oracle else
                          'WHEN MATCHED' + (' AND ' + predicate if predicate else '') + ' THEN UPDATE SET ' + assignment)
                sql = f'MERGE INTO {target} t USING {using} ON({on}) {clause}'
                actual = dict(affected=None, error=None)
                try:
                    actual['affected'] = execute(sql, parameters)
                except Exception as error:
                    actual['error'] = error_kind(error)
                    row['error_detail'] = dict(sqlstate=getattr(error, 'sqlstate', None), detail=str(error)[:600])
                actual['rows'] = execute(f'SELECT {projection} FROM {target} ORDER BY id')
                row.update(actual=actual, qualified=actual == expected)
            except Exception as error:
                row.update(exception=type(error).__name__, detail=str(error)[:600], qualified=False)
            finally:
                for table in reversed(made):
                    try:
                        execute('DROP TABLE ' + table + (' PURGE' if driver else ''))
                    except Exception as error:
                        report['cleanup_errors'].append(dict(table=table, detail=str(error)[:300]))
            report['cases'].append(row)
    return report


def run(source_factory, target_factory):
    with source_factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'], matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False, reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(c['matched'] for c in cases), source_qualified=all(c['source_qualified'] for c in cases),
                cleanup_errors=cleanup, success=len(cases) == len(CASES) and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
