"""Compare typed secondary-index visibility within transactions.

Each workload runs with and without a secondary index. PostgreSQL mutations and
reads use prepared transport; Oracle is an independent source for its profile.
"""
import decimal
import uuid

from aggregate_conditionals import execute, main as comparison_main


def mutate(connection, sql, value):
    native = type(connection).__module__.startswith('psycopg')
    with connection.cursor() as cursor:
        if native:
            cursor.execute(sql.replace('{P}', '%s'), [value], prepare=True)
        else:
            cursor.execute(sql.replace('{P}', ':1'), [value])


def run(reference, target, native=False):
    prefix = 'public.' if native else ''
    expected = [
        ('own_update', '185.25'), ('range_read', '185.25'),
        ('correlated_read', '185.25'), ('parent_update', '185.25'),
        ('pending_insert', '190.5'), ('move_from', '5.25'),
        ('move_to', '205.5'), ('pending_delete', '0'),
        ('savepoint_restore', '185.25'), ('savepoint_other', '20.25'),
        ('rollback_restore', '123.5'), ('commit_visible', '200.5'),
    ]
    report = dict(format=1, reference='postgres' if native else 'oracle', application_qualified=False, target_protocol='prepared mutations and reads, simple DDL', cases=[], backend_errors=[], cleanup_errors=[], success=False)
    for indexed in [False, True]:
        token = uuid.uuid4().hex[:12].upper()
        parent, child = [prefix+'COMPAT_IV_'+kind+'_'+token for kind in ['H','L']]
        index = 'COMPAT_IV_I_'+token
        observed = {}
        for label, conn in [('source', reference), ('bicdb', target)]:
            created = []
            values = {}
            def observe(name, sql):
                values[name] = dict(rows=execute(conn, sql, rows=True))
            try:
                conn.autocommit = True
                for name, sql in [(parent,f'CREATE TABLE {parent}(k NUMERIC(20,0) PRIMARY KEY,total NUMERIC(20,2))'),(child,f'CREATE TABLE {child}(line_k NUMERIC(20,0) PRIMARY KEY,k NUMERIC(20,0),amount NUMERIC(20,2))')]:
                    execute(conn, sql)
                    created.append(name)
                if indexed:
                    execute(conn, f'CREATE INDEX {index} ON {child}(k)')
                for key, amount in [(1,'123.50'), (2,'20.25')]:
                    execute(conn, f'INSERT INTO {parent} VALUES({key},0)')
                    execute(conn, f'INSERT INTO {child} VALUES({key},{key},{amount})')
                conn.autocommit = False
                mutate(conn, f'UPDATE {child} SET amount={{P}} WHERE line_k=1', decimal.Decimal('185.25'))
                observe('own_update', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                observe('range_read', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k>=1 AND k<=1')
                observe('correlated_read', f'SELECT (SELECT COALESCE(SUM(amount),0) FROM {child} il WHERE i.k=il.k) FROM {parent} i WHERE k=1')
                mutate(conn, f'UPDATE {parent} i SET total=(SELECT COALESCE(SUM(amount),0) FROM {child} il WHERE i.k=il.k) WHERE k={{P}}', 1)
                observe('parent_update', f'SELECT total FROM {parent} WHERE k=1')
                execute(conn, 'SAVEPOINT before_delta')
                mutate(conn, f'INSERT INTO {child} VALUES(3,1,{{P}})', decimal.Decimal('5.25'))
                observe('pending_insert', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                mutate(conn, f'UPDATE {child} SET k={{P}} WHERE line_k=1', 2)
                observe('move_from', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                observe('move_to', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=2')
                execute(conn, f'DELETE FROM {child} WHERE line_k=3')
                observe('pending_delete', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                execute(conn, 'ROLLBACK TO SAVEPOINT before_delta')
                observe('savepoint_restore', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                observe('savepoint_other', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=2')
                conn.rollback()
                observe('rollback_restore', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
                mutate(conn, f'UPDATE {child} SET amount={{P}} WHERE line_k=1', decimal.Decimal('200.50'))
                conn.commit()
                observe('commit_visible', f'SELECT COALESCE(SUM(amount),0) FROM {child} WHERE k=1')
            except Exception as exc:
                report['backend_errors'].append(dict(backend=label,indexed=indexed,exception=type(exc).__name__,sqlstate=getattr(exc,'sqlstate',None),message=str(exc).splitlines()[0]))
            finally:
                conn.rollback()
                conn.autocommit = True
                for name in reversed(created):
                    try:
                        execute(conn, f'DROP TABLE {name}')
                    except Exception as exc:
                        report['cleanup_errors'].append(dict(backend=label,exception=type(exc).__name__))
            observed[label] = values
        for name, number in expected:
            wanted = dict(rows=[[{'number':number}]])
            case = dict(id=('indexed_' if indexed else 'scan_')+name,indexed=indexed,expected=wanted)
            for label in ['source','bicdb']:
                case[label] = observed[label].get(name, dict(not_executed=True))
            case['source_qualified'] = case['source'] == wanted
            case['matched'] = case['source_qualified'] and case['bicdb'] == wanted
            report['cases'].append(case)
    report['total'] = len(report['cases'])
    report['matched'] = sum(case['matched'] for case in report['cases'])
    report['source_qualified'] = report['total'] == 24 and all(case['source_qualified'] for case in report['cases'])
    report['success'] = report['source_qualified'] and report['matched'] == report['total'] and not report['backend_errors'] and not report['cleanup_errors']
    return report


def main():
    return comparison_main(runner=run, description=__doc__)


if __name__ == '__main__':
    raise SystemExit(main())
