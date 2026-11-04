"""Independent row-valued UPDATE controls on disposable nonce objects.

This covers selected constructors/subqueries, cardinality, original-row values,
primary keys, errors and statement rollback. DEFAULT, generated columns,
concurrent schedules and full UPDATE conformance remain outside this fixture.
"""
import decimal
import uuid
from row_lock_waits import main as comparison_main


def canonical(value):
    if isinstance(value, (int, float, decimal.Decimal)):
        with decimal.localcontext() as context:
            context.prec = 100
            return {'number': str(decimal.Decimal(str(value)).normalize())}
    return value


def error_kind(error, driver):
    code = getattr(error.args[0], 'code', None) if driver and error.args else None
    state = getattr(error, 'sqlstate', None)
    if code in (1427, 30926) or state == '21000':
        return 'cardinality'
    if code in (947, 913, 957) or state == '42601':
        return 'shape'
    if code == 2290 or state == '23514':
        return 'check'
    if code == 1 or state == '23505':
        return 'unique'
    return 'unqualified'


def observe(factory, oracle):
    report = dict(cases=[], cleanup_errors=[])
    labels = ['constant', 'constructor_swap', 'correlated_aggregate', 'empty_query',
              'multiple_rows', 'too_few_columns', 'too_many_columns', 'mixed_old_values',
              'key_move', 'key_collision', 'check_rollback', 'quoted_columns',
              'prepared', 'selected_null', 'sequence_once', 'unmatched_sequence',
              'caller_rollback', 'empty_aggregate']
    with factory() as conn:
        driver = type(conn).__module__.startswith('oracledb')
        conn.autocommit = True
        report['server_version'] = conn.version if driver else conn.info.server_version

        def execute(sql, parameters=None):
            with conn.cursor() as q:
                if driver:
                    q.execute(sql, parameters)
                else:
                    q.execute(sql, parameters, prepare=True)
                return ([[canonical(v) for v in row] for row in q.fetchall()]
                        if q.description else q.rowcount)

        for label in labels:
            nonce = 'BICDB_RU_' + uuid.uuid4().hex[:10].upper()
            target, source, sequence, function = nonce+'T', nonce+'S', nonce+'Q', nonce+'F'
            made = []
            initial = [[1, 10, 20, 0], [2, 30, 40, 0], [3, 50, 60, 0]]
            expected = dict(affected=1, error=None, rows=[row[:] for row in initial])
            dual = ' FROM DUAL' if oracle else ''
            parameters = None
            projection = 'id,a,b,c'
            assignment = f'(a,b)=(SELECT 7,8{dual})'
            selection = 'id=1'
            after = []
            if label == 'constant':
                expected['rows'][0][1:3] = [7, 8]
            elif label == 'constructor_swap':
                assignment = '(a,b)=(b,a)'
                expected['rows'][0][1:3] = [20, 10]
            elif label == 'correlated_aggregate':
                assignment = f'(a,b)=(SELECT SUM(x),SUM(y) FROM {source} s WHERE s.parent=t.id)'
                selection = 'id>0'
                expected.update(affected=3, rows=[[1, 2, 3, 0], [2, 10, 12, 0], [3, None, None, 0]])
            elif label == 'empty_query':
                assignment = f'(a,b)=(SELECT x,y FROM {source} WHERE parent=99)'
                expected['rows'][0][1:3] = [None, None]
            elif label == 'multiple_rows':
                assignment = f'(a,b)=(SELECT x,y FROM {source} s WHERE s.parent=t.id)'
                selection = 'id>0'
                expected.update(affected=None, error='cardinality')
            elif label in ('too_few_columns', 'too_many_columns'):
                values = '7' if label == 'too_few_columns' else '7,8,9'
                assignment = f'(a,b)=(SELECT {values}{dual})'
                expected.update(affected=None, error='shape')
            elif label == 'mixed_old_values':
                assignment = f'(a,b)=(SELECT t.b,t.a{dual}),c=a+b'
                expected['rows'][0][1:] = [20, 10, 30]
            elif label in ('key_move', 'key_collision'):
                key = 4 if label == 'key_move' else 2
                assignment = f'(id,a)=(SELECT {key},7{dual})'
                if label == 'key_move':
                    expected['rows'] = [initial[1], initial[2], [4, 7, 20, 0]]
                else:
                    expected.update(affected=None, error='unique')
            elif label == 'check_rollback':
                assignment = f'(a,b)=(SELECT CASE WHEN t.id=2 THEN -1 ELSE 7 END,8{dual})'
                selection = 'id>0'
                expected.update(affected=None, error='check')
            elif label == 'quoted_columns':
                assignment = f'("Key","key")=(SELECT t."key",t."Key"{dual})'
                projection += ',"Key","key"'
                expected['rows'] = [row + [10, 20] for row in initial]
                expected['rows'][0][-2:] = [20, 10]
            elif label == 'prepared':
                assignment = f'(a,b)=(SELECT ' + (':a,:b' if driver else '%s,%s') + dual + ')'
                parameters = {'a': 17, 'b': 18} if driver else (17, 18)
                expected['rows'][0][1:3] = [17, 18]
            elif label == 'selected_null':
                assignment = f'(a,b)=(SELECT CAST(NULL AS INTEGER),7{dual})'
                expected['rows'][0][1:3] = [None, 7]
            elif label in ('sequence_once', 'unmatched_sequence'):
                nextval = sequence+'.NEXTVAL' if oracle else f"nextval('{sequence}')"
                assignment = f'(a,b)=(SELECT {function}(),8{dual})'
                if label == 'sequence_once':
                    expected['rows'][0][1:3] = [1, 8]
                    current = sequence+'.CURRVAL' if oracle else f"currval('{sequence}')"
                    after = [f'SELECT {current}{dual}']
                else:
                    selection = 'id=99'
                    expected['affected'] = 0
                    after = [f'SELECT {nextval}{dual}']
                expected['after'] = [[[canonical(1)]]]
            elif label == 'caller_rollback':
                expected['rows'] = initial
            elif label == 'empty_aggregate':
                assignment = f'(a,b)=(SELECT COUNT(*),SUM(x) FROM {source} WHERE parent=99)'
                expected['rows'][0][1:3] = [0, None]
            expected['rows'] = [[canonical(v) for v in row] for row in expected['rows']]
            case = dict(id=label, expected=expected)
            try:
                execute(f'CREATE TABLE {target}(id INTEGER PRIMARY KEY,a INTEGER CHECK(a>=0),b INTEGER,c INTEGER,"Key" INTEGER,"key" INTEGER)')
                made.append(('TABLE', target))
                execute(f'CREATE TABLE {source}(parent INTEGER,x INTEGER,y INTEGER)')
                made.append(('TABLE', source))
                for row in initial:
                    execute(f'INSERT INTO {target} VALUES('+','.join(map(str, row))+',10,20)')
                for row in [(1, 2, 3), (2, 4, 5), (2, 6, 7)]:
                    execute(f'INSERT INTO {source} VALUES('+','.join(map(str, row))+')')
                if after:
                    execute(f'CREATE SEQUENCE {sequence} START WITH 1')
                    made.append(('SEQUENCE', sequence))
                    if oracle:
                        execute(f'CREATE FUNCTION {function} RETURN NUMBER AS BEGIN RETURN {sequence}.NEXTVAL; END;')
                    else:
                        execute(f"CREATE FUNCTION {function}() RETURNS BIGINT LANGUAGE sql VOLATILE AS $$SELECT nextval('{sequence}')$$")
                    made.append(('FUNCTION', function))
                if label == 'caller_rollback':
                    conn.autocommit = False
                actual = dict(affected=None, error=None)
                try:
                    actual['affected'] = execute(f'UPDATE {target} t SET {assignment} WHERE {selection}', parameters)
                except Exception as error:
                    actual['error'] = error_kind(error, driver)
                    case['error_detail'] = str(error)[:400]
                if label == 'caller_rollback':
                    conn.rollback()
                    conn.autocommit = True
                actual['rows'] = execute(f'SELECT {projection} FROM {target} ORDER BY id')
                if after:
                    actual['after'] = [execute(sql) for sql in after]
                case.update(actual=actual, qualified=actual == expected)
            except Exception as error:
                case.update(qualified=False, setup_error=str(error)[:500])
            finally:
                conn.rollback()
                conn.autocommit = True
                for kind, name in reversed(made):
                    try:
                        signature = '()' if kind == 'FUNCTION' and not oracle else ''
                        execute(f'DROP {kind} {name}{signature}' + (' PURGE' if driver and kind == 'TABLE' else ''))
                    except Exception as error:
                        report['cleanup_errors'].append(dict(kind=kind, detail=str(error)[:300]))
            report['cases'].append(case)
    return report


def run(source_factory, target_factory):
    with source_factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'] and a['expected'] == b['expected'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    return dict(format=1, application_qualified=False, reference_version=source['server_version'], cases=cases,
                source_qualified=all(c['source_qualified'] for c in cases),
                matched=sum(c['matched'] for c in cases), total=len(cases),
                cleanup_errors=source['cleanup_errors']+target['cleanup_errors'],
                success=all(c['matched'] for c in cases) and not source['cleanup_errors'] and not target['cleanup_errors'])


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
