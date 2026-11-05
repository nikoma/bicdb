"""Composite-key mutations retain residual scalar-subquery and NULL predicates."""
import uuid
from row_lock_waits import main as comparison_main


def observe(factory):
    table = 'BICDB_BM_' + uuid.uuid4().hex[:8].upper()
    original = [[1, 1, 'done'], [1, 2, 'done'], [2, 1, 'done']]
    expected = {
        'update_conflicting_key': dict(affected=0, rows=original),
        'delete_conflicting_key': dict(affected=0, rows=original),
        'update_repeated_key': dict(affected=1, rows=[[1, 1, 'changed'], [1, 2, 'done'], [2, 1, 'done']]),
        'delete_repeated_key': dict(affected=1, rows=[[1, 2, 'done'], [2, 1, 'done']]),
        'update_empty_scalar': dict(affected=0, rows=original),
        'delete_empty_scalar': dict(affected=0, rows=original),
        'update_empty_aggregate': dict(affected=0, rows=original),
        'delete_empty_aggregate': dict(affected=0, rows=original),
        'update_null_literal': dict(affected=0, rows=original),
        'delete_null_literal': dict(affected=0, rows=original),
        'update_nonnull_scalar': dict(affected=1, rows=[[1, 1, 'done'], [1, 2, 'changed'], [2, 1, 'done']]),
        'delete_nonnull_scalar': dict(affected=1, rows=[[1, 1, 'done'], [2, 1, 'done']]),
    }
    cases = {key: dict(id=key, expected=value, qualified=False)
             for key, value in expected.items()}
    report = dict(cases=list(cases.values()), cleanup_errors=[])
    made = False
    with factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
        conn.autocommit = not oracle
        report['server_version'] = conn.version if oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                if oracle:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, prepare=True)
                return cursor.fetchall() if cursor.description else cursor.rowcount

        try:
            execute(f'CREATE TABLE {table}(a NUMERIC(10,0),b NUMERIC(10,0),status VARCHAR(12),PRIMARY KEY(a,b))')
            made = True
            for a, b, status in original:
                execute(f"INSERT INTO {table} VALUES({a},{b},'{status}')")
            conn.commit()
            empty = f"(SELECT b FROM {table} WHERE status='queued')"
            aggregate = f"(SELECT MAX(b) FROM {table} WHERE status='queued')"
            nonnull = f'(SELECT MAX(b) FROM {table} WHERE a=1)'
            for name, predicate in [
                ('update_conflicting_key', 'a=1 AND b=1 AND b=2'),
                ('delete_conflicting_key', 'a=1 AND b=1 AND b=2'),
                ('update_repeated_key', 'a=1 AND b=1 AND b=1'),
                ('delete_repeated_key', 'a=1 AND b=1 AND b=1'),
                ('update_empty_scalar', f'a=1 AND b={empty}'),
                ('delete_empty_scalar', f'a=1 AND b={empty}'),
                ('update_empty_aggregate', f'a=1 AND b={aggregate}'),
                ('delete_empty_aggregate', f'a=1 AND b={aggregate}'),
                ('update_null_literal', 'a=1 AND b=CAST(NULL AS NUMERIC)'),
                ('delete_null_literal', 'a=1 AND b=CAST(NULL AS NUMERIC)'),
                ('update_nonnull_scalar', f'a=1 AND b={nonnull}'),
                ('delete_nonnull_scalar', f'a=1 AND b={nonnull}'),
            ]:
                if not oracle:
                    execute('BEGIN')
                operation = (f"UPDATE {table} SET status='changed'" if name.startswith('update')
                             else f'DELETE FROM {table}')
                affected = execute(operation + ' WHERE ' + predicate)
                rows = [[int(a), int(b), status] for a, b, status in
                        execute(f'SELECT a,b,status FROM {table} ORDER BY a,b')]
                actual = dict(affected=affected, rows=rows)
                cases[name].update(actual=actual, qualified=actual == expected[name])
                conn.rollback()
        except Exception as error:
            report['execution_error'] = type(error).__name__ + ':' + str(error)[:1500]
        finally:
            try:
                conn.rollback()
                conn.autocommit = True
                if made:
                    execute('DROP TABLE ' + table + (' PURGE' if oracle else ''))
            except Exception as error:
                report['cleanup_errors'].append(str(error)[:300])
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False,
                reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(case['matched'] for case in cases),
                source_qualified=all(case['source_qualified'] for case in cases),
                cleanup_errors=cleanup,
                source_execution_error=source.get('execution_error'),
                target_execution_error=target.get('execution_error'),
                success=len(cases) == 12 and all(case['matched'] for case in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
