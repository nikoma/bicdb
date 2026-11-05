"""Session isolation defaults and visibility through independent connections.

Oracle references use ALTER SESSION outside transactions. PostgreSQL clients
use SET SESSION CHARACTERISTICS, including clients of the Oracle SQL profile.
The fixture compares visibility and writes, not equivalence of those grammars.
"""
import uuid
from row_lock_waits import main as comparison_main


def observe(factory, oracle_contract):
    expected = {
        'read_committed_refresh': [1, 2],
        'read_own_writes': [7, 0],
        'rollback_removes_writes': 0,
        'default_visibility': [2, 2 if oracle_contract else 3],
        'default_reused': [3, 3 if oracle_contract else 4],
        'explicit_read_committed_override': [4, 5],
        'connection_defaults_independent': [5, 6],
    }
    if oracle_contract:
        expected.update(stale_writer='serialization_failure',
                        fixed_snapshot_own_write=[2, 1],
                        fixed_snapshot_rollback=1)
    expected['restored_read_committed'] = [7, 8] if oracle_contract else [6, 7]
    cases = {name: dict(id=name, expected=value, qualified=False)
             for name, value in expected.items()}
    report = dict(cases=list(cases.values()), cleanup_errors=[])
    table = 'BICDB_SI_' + uuid.uuid4().hex[:10].upper()
    made = False

    with factory() as a, factory() as b:
        oracle = type(a).__module__.startswith('oracledb')
        report['server_version'] = a.version if oracle else a.info.server_version
        a.autocommit = not oracle
        b.autocommit = True

        def execute(conn, sql):
            with conn.cursor() as q:
                if oracle:
                    q.execute(sql)
                else:
                    q.execute(sql, prepare=True)
                return q.fetchall() if q.description else q.rowcount

        def scalar(conn, sql):
            return int(execute(conn, sql)[0][0])

        def count(conn):
            return scalar(conn, 'SELECT COUNT(*) FROM ' + table)

        def value(conn):
            return scalar(conn, 'SELECT v FROM ' + table + ' WHERE id=1')

        def configure(mode):
            sql = ('ALTER SESSION SET ISOLATION_LEVEL=' if oracle else
                   'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ')
            execute(a, sql + mode)

        def begin(conn, mode=None):
            if oracle:
                if mode:
                    execute(conn, 'SET TRANSACTION ISOLATION LEVEL ' + mode)
            else:
                execute(conn, 'BEGIN' + (' ISOLATION LEVEL ' + mode if mode else ''))

        def check(name, actual):
            cases[name].update(actual=actual, qualified=actual == expected[name])

        try:
            execute(b, f'CREATE TABLE {table}(id INTEGER PRIMARY KEY,v INTEGER)')
            made = True
            execute(b, f'INSERT INTO {table} VALUES(1,0)')
            configure('READ COMMITTED')
            begin(a)
            first = count(a)
            execute(b, f'INSERT INTO {table} VALUES(2,0)')
            check('read_committed_refresh', [first, count(a)])
            execute(a, f'UPDATE {table} SET v=7 WHERE id=1')
            check('read_own_writes', [value(a), value(b)])
            a.rollback()
            check('rollback_removes_writes', value(b))

            configure('SERIALIZABLE' if oracle_contract else 'READ COMMITTED')
            begin(a)
            first = count(a)
            execute(b, f'INSERT INTO {table} VALUES(3,0)')
            check('default_visibility', [first, count(a)])
            a.commit()
            begin(a)
            first = count(a)
            execute(b, f'INSERT INTO {table} VALUES(4,0)')
            check('default_reused', [first, count(a)])
            a.rollback()

            begin(a, 'READ COMMITTED')
            first = count(a)
            execute(b, f'INSERT INTO {table} VALUES(5,0)')
            check('explicit_read_committed_override', [first, count(a)])
            a.rollback()

            b.autocommit = not oracle
            begin(b)
            first = count(b)
            begin(a)
            execute(a, f'INSERT INTO {table} VALUES(6,0)')
            a.commit()
            check('connection_defaults_independent', [first, count(b)])
            b.rollback()
            b.autocommit = True

            if oracle_contract:
                begin(a)
                assert value(a) == 0
                execute(b, f'UPDATE {table} SET v=1 WHERE id=1')
                try:
                    execute(a, f'UPDATE {table} SET v=2 WHERE id=1')
                except Exception as error:
                    code = getattr(error.args[0], 'code', None) if error.args else None
                    outcome = ('serialization_failure' if code == 8177 or
                               getattr(error, 'sqlstate', None) == '40001' else
                               type(error).__name__ + ':' + str(error)[:200])
                else:
                    outcome = 'accepted_stale_update'
                check('stale_writer', outcome)
                a.rollback()
                begin(a)
                assert value(a) == 1
                # Exercise own writes without an intervening writer; stale
                # writers and concurrent insert visibility are separate cases.
                execute(a, f'UPDATE {table} SET v=2 WHERE id=1')
                check('fixed_snapshot_own_write', [value(a), value(b)])
                a.rollback()
                check('fixed_snapshot_rollback', value(b))
                execute(b, f'INSERT INTO {table} VALUES(7,0)')

            configure('READ COMMITTED')
            begin(a)
            first = count(a)
            execute(b, f'INSERT INTO {table} VALUES(8,0)')
            check('restored_read_committed', [first, count(a)])
        except Exception as error:
            report['execution_error'] = type(error).__name__ + ':' + str(error)[:500]
        finally:
            for conn in [a, b]:
                try:
                    conn.rollback()
                except Exception as error:
                    report['cleanup_errors'].append(str(error)[:300])
            if made:
                try:
                    b.autocommit = True
                    execute(b, 'DROP TABLE ' + table + (' PURGE' if oracle else ''))
                except Exception as error:
                    report['cleanup_errors'].append(str(error)[:300])
    return report


def run(source_factory, target_factory):
    with source_factory() as probe:
        oracle_contract = type(probe).__module__.startswith('oracledb')
    source = observe(source_factory, oracle_contract)
    target = observe(target_factory, oracle_contract)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    count = 11 if oracle_contract else 8
    return dict(format=1, application_qualified=False,
                reference_version=source['server_version'],
                oracle_contract=oracle_contract, cases=cases,
                total=len(cases), matched=sum(c['matched'] for c in cases),
                source_qualified=all(c['source_qualified'] for c in cases),
                cleanup_errors=cleanup,
                source_execution_error=source.get('execution_error'),
                target_execution_error=target.get('execution_error'),
                success=len(cases) == count and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
