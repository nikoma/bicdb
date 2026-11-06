"""Compare bounded sequence state and allocation over PostgreSQL transport.

Min/max, cycle/cache policy, instance failure, distributed ownership and complete
sequence conformance remain separate qualification work.
"""
from concurrent.futures import ThreadPoolExecutor
import uuid

from row_lock_waits import main as comparison_main


def observe(factory):
    base = factory()
    report = dict(driver_kind='oracle' if type(base).__module__.startswith('oracledb') else 'postgresql-transport',cases=[],cleanup_errors=[])
    base.autocommit = True
    try:
        for label in ['rollback_and_savepoint','overlapping_allocators']:
            name = 'BICDB_SQ_' + uuid.uuid4().hex[:12].upper()
            created = False
            connections = []
            item = dict(id=label)
            with base.cursor() as cursor:
                try:
                    cursor.execute(f'CREATE SEQUENCE {name} START WITH 1 INCREMENT BY 1')
                    created = True
                    if label == 'rollback_and_savepoint':
                        one = factory();connections.append(one);one.autocommit = False
                        two = factory();connections.append(two);two.autocommit = False
                        with one.cursor() as first,two.cursor() as second:
                            def value(handle,kind):
                                handle.execute(f'SELECT {name}.{kind} FROM DUAL')
                                return int(handle.fetchone()[0])
                            actual = [value(first,'NEXTVAL')]
                            one.rollback()
                            actual.append(value(first,'CURRVAL'))
                            actual.append(value(second,'NEXTVAL'))
                            two.rollback()
                            actual.append(value(first,'NEXTVAL'))
                            first.execute('SAVEPOINT sequence_point')
                            actual.append(value(first,'NEXTVAL'))
                            first.execute('ROLLBACK TO sequence_point')
                            actual.append(value(first,'CURRVAL'))
                            actual.append(value(first,'NEXTVAL'))
                            expected = [1,1,2,3,4,4,5]
                    else:
                        def allocate(_):
                            connection = factory();connection.autocommit = False
                            try:
                                values = []
                                with connection.cursor() as query:
                                    for _ in range(8):
                                        query.execute(f'SELECT {name}.NEXTVAL FROM DUAL')
                                        values.append(int(query.fetchone()[0]))
                                        connection.rollback()
                                return values
                            finally:
                                connection.close()
                        with ThreadPoolExecutor(max_workers=4) as executor:
                            actual = sorted(value for values in executor.map(allocate,range(4)) for value in values)
                        expected = list(range(1,33))
                    item.update(actual=actual,expected=expected,qualified=actual==expected)
                except Exception as error:
                    item.update(qualified=False,error_class=type(error).__name__,sqlstate=getattr(error,'sqlstate',None),oracle_code=getattr(error.args[0],'code',None) if error.args else None)
                finally:
                    for connection in reversed(connections):
                        try:connection.rollback();connection.close()
                        except Exception as error:report['cleanup_errors'].append(type(error).__name__)
                    if created:
                        try:cursor.execute('DROP SEQUENCE '+name)
                        except Exception as error:report['cleanup_errors'].append(type(error).__name__)
                report['cases'].append(item)
    finally:
        base.close()
    return report


def run(source_factory,target_factory):
    source,target = observe(source_factory),observe(target_factory)
    cases = [dict(id=a['id'],source=a,bicdb=b,matched=a['id']==b['id'] and a['qualified'] and b['qualified'])
             for a,b in zip(source['cases'],target['cases'],strict=True)]
    cleanup = source['cleanup_errors']+target['cleanup_errors']
    source_ok = source['driver_kind']=='oracle' and len(cases)==2 and all(x['qualified'] for x in source['cases'])
    return dict(format=1,cases=cases,total=len(cases),matched=sum(x['matched'] for x in cases),source_qualified=source_ok,cleanup_errors=cleanup,success=source_ok and all(x['matched'] for x in cases) and not cleanup,application_qualified=False)


if __name__ == '__main__':raise SystemExit(comparison_main(run,__doc__,references=('oracle',)))
