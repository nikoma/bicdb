"""Compare read-only SQL BLOB metrics using only nonce-owned fixture tables.

CLOBs, BFILEs, temporary-LOB mutation and whole-application qualification remain
separate. The same SQL read expressions run on Oracle and the preview target.
"""
import uuid

from row_lock_waits import main as comparison_main


def integers(rows):
    result = []
    for row in rows:
        values = []
        for value in row:
            if value is None: values.append(None)
            elif isinstance(value, bool) or value != int(value):
                raise ValueError('BLOB metric result is not an integer')
            else: values.append(int(value))
        result.append(values)
    return result


def observe(factory):
    table = 'BICDB_BLR_'+uuid.uuid4().hex[:16].upper()
    report = dict(cases=[], cleanup_errors=[], application_qualified=False)
    with factory() as connection, factory() as observer:
        native = type(connection).__module__.startswith('oracledb')
        report['driver_kind'] = 'oracle' if native else 'postgresql-transport'
        connection.autocommit = observer.autocommit = True
        created = False
        with connection.cursor() as cursor, observer.cursor() as other:
            try:
                cursor.execute(f'CREATE TABLE {table}(id NUMBER PRIMARY KEY,payload BLOB)')
                created = True
                cursor.execute(f"INSERT INTO {table} VALUES(1,HEXTORAW('00FF'))")
                cursor.execute(f'INSERT INTO {table} VALUES(2,EMPTY_BLOB())')
                cursor.execute(f'INSERT INTO {table} VALUES(3,NULL)')
                cases = [
                    ('metrics',f'SELECT id,DBMS_LOB.GETLENGTH(payload),DBMS_LOB.ISTEMPORARY(payload),CASE WHEN payload IS NULL THEN 1 ELSE 0 END FROM {table} ORDER BY id',[[1,2,0,0],[2,0,0,0],[3,None,None,1]]),
                    ('qualified_named',f'SELECT id,SYS.DBMS_LOB.GETLENGTH(lob_loc=>p.payload) FROM {table} p ORDER BY id',[[1,2],[2,0],[3,None]]),
                    ('positive_filter',f'SELECT id FROM {table} WHERE DBMS_LOB.GETLENGTH(payload)>0 ORDER BY id',[[1]]),
                    ('nvl',f'SELECT id,NVL(DBMS_LOB.GETLENGTH(payload),9) FROM {table} ORDER BY id',[[1,2],[2,0],[3,9]]),
                    ('empty_selection',f'SELECT DBMS_LOB.GETLENGTH(payload) FROM {table} WHERE 1=0',[]),
                ]
                for name,sql,expected in cases:
                    if native: cursor.execute(sql)
                    else: cursor.execute(sql,prepare=True)
                    actual = integers(cursor.fetchall())
                    report['cases'].append(dict(id=name,actual=actual,expected=expected,qualified=actual==expected))
                connection.autocommit = False
                def lengths():
                    sql=f'SELECT DBMS_LOB.GETLENGTH(payload) FROM {table} WHERE id=1'
                    cursor.execute(sql);other.execute(sql)
                    return [integers(cursor.fetchall())[0][0],integers(other.fetchall())[0][0]]
                cursor.execute(f"UPDATE {table} SET payload=HEXTORAW('AB') WHERE id=1")
                actual = [lengths()]
                cursor.execute('SAVEPOINT blob_read')
                cursor.execute(f"UPDATE {table} SET payload=HEXTORAW('010203') WHERE id=1")
                actual.append(lengths())
                cursor.execute('ROLLBACK TO blob_read')
                actual.append(lengths())
                connection.rollback()
                actual.append(lengths())
                expected = [[1,2],[3,2],[1,2],[2,2]]
                report['cases'].append(dict(id='writer_observer_savepoint',actual=actual,expected=expected,qualified=actual==expected))
            finally:
                connection.rollback();observer.rollback()
                connection.autocommit = True
                if created:
                    try: cursor.execute('DROP TABLE '+table+(' PURGE' if native else ''))
                    except Exception as error: report['cleanup_errors'].append(type(error).__name__)
    return report


def run(source_factory, target_factory):
    source,target = observe(source_factory),observe(target_factory)
    cases = [dict(id=a['id'],source=a,bicdb=b,matched=a['id']==b['id'] and a['qualified'] and b['qualified'])
             for a,b in zip(source['cases'],target['cases'],strict=True)]
    cleanup = source['cleanup_errors']+target['cleanup_errors']
    source_ok = source['driver_kind']=='oracle' and len(source['cases'])==6 and all(item['qualified'] for item in source['cases'])
    return dict(format=1,cases=cases,total=len(cases),matched=sum(c['matched'] for c in cases),
        cleanup_errors=cleanup,source_qualified=source_ok,
        success=source_ok and len(cases)==6 and all(c['matched'] for c in cases) and not cleanup,
        application_qualified=False)


if __name__ == '__main__': raise SystemExit(comparison_main(run,__doc__,references=('oracle',)))
