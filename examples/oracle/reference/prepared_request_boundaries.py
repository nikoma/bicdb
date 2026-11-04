"""PostgreSQL prepared-request boundaries, errors and transaction recovery.

The reference is PostgreSQL, including for BicDB's Oracle SQL profile: this
qualifies the selected PostgreSQL transport, not Oracle Net/TTC. Nonce objects
are removed, and the fixture never uses application data.
"""
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main


def observe(factory, oracle_profile=False):
    suffix = uuid.uuid4().hex[:12].upper()
    table, routine = 'BICDB_PB_' + suffix + 'T', 'BICDB_PB_' + suffix + 'F'
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        report['server_version'] = conn.info.server_version

        def execute(sql, prepared=True):
            with conn.cursor() as cursor:
                cursor.execute(sql, prepare=prepared)
                return [[canonical(value) for value in row] for row in cursor.fetchall()] if cursor.description else None

        def check(name, sql, expected, prepared=True):
            row = dict(id=name, expected=expected)
            try:
                actual = execute(sql, prepared)
                row.update(actual=actual, qualified=actual == expected)
            except Exception as error:
                state = getattr(error, 'sqlstate', None)
                row.update(sqlstate=state, exception=type(error).__name__,
                           qualified=isinstance(expected, str) and state == expected)
            report['cases'].append(row)

        try:
            execute(f'CREATE TABLE {table}(id INTEGER)')
            created.append(('TABLE', table))
            check('select_batch_rejected', 'SELECT 1; SELECT 2', '42601')
            check('write_batch_rejected', f'INSERT INTO {table} VALUES(1); SELECT 2', '42601')
            check('rejected_batch_did_not_write', f'SELECT COUNT(*) FROM {table}', [[canonical(0)]])
            check('quoted_semicolon', "SELECT CAST(';' AS VARCHAR(4))", [[';']])
            check('trailing_delimiters_comments', "/* leading ; */ SELECT CAST(';' AS VARCHAR(4)); ; -- trailing ;", [[';']])
            definition = (f'CREATE FUNCTION {routine} RETURN INTEGER IS n INTEGER:=1;BEGIN n:=n+2;RETURN n;END;'
                          if oracle_profile else
                          f'CREATE FUNCTION {routine}() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE n INTEGER:=1;BEGIN n:=n+2;RETURN n;END$$')
            check('routine_body', definition, None)
            created.append(('FUNCTION', routine + '()'))
            check('routine_execution', f'SELECT {routine}()', [[canonical(3)]])
            execute('BEGIN', False)
            check('transaction_parse_error', 'SELECT 1; SELECT 2', '42601')
            check('transaction_aborted', 'SELECT 3', '25P02')
            execute('ROLLBACK', False)
            check('transaction_recovered', 'SELECT 3', [[canonical(3)]])
            # Simple Query still accepts batches. Clear baseline writes first.
            execute(f'DELETE FROM {table}', False)
            execute(f'INSERT INTO {table} VALUES(1); UPDATE {table} SET id=id+1', False)
            check('simple_query_batch_retained', f'SELECT id FROM {table}', [[canonical(2)]])
        finally:
            try:
                execute('ROLLBACK', False)
            except Exception:
                pass
            for kind, name in reversed(created):
                try:
                    execute(f'DROP {kind} {name}', False)
                except Exception as error:
                    report['cleanup_errors'].append(dict(kind=kind, exception=type(error).__name__))
    return report


def qualified(row):
    return bool(row.get('qualified', False))


def run(source_factory, target_factory, target_profile="postgres"):
    source, target = observe(source_factory), observe(target_factory, oracle_profile=target_profile == "oracle-preview-v1")
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False, reference_version=source['server_version'],
                cases=cases, total=len(cases), matched=sum(c['matched'] for c in cases),
                source_qualified=all(c['source_qualified'] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 11 and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__, references=('postgres',)))
