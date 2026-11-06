"""Compare concurrent child references and non-key parent updates with Oracle.

Uses nonce-owned integer tables. Parent-mutation lock waiting, deadlocks,
distributed ownership and complete application acceptance remain separate.
"""
import uuid

from row_lock_waits import main as comparison_main


CASES = [('immediate_children',False,False),('deferred_children',True,False),
         ('unique_parent_nonkey_update',False,True),('savepoint_releases_reference',True,False)]


def observe(factory):
    report = dict(cases=[],cleanup_errors=[],application_qualified=False)
    base = factory()
    native = type(base).__module__.startswith('oracledb')
    report['driver_kind'] = 'oracle' if native else 'postgresql-transport'
    base.autocommit = True
    try:
        for label,deferred,unique_parent in CASES:
            prefix = 'BICDB_FR_'+uuid.uuid4().hex[:12].upper()
            parent,child = prefix+'_P',prefix+'_C'
            created=[]
            writers=[]
            item=dict(id=label)
            with base.cursor() as cursor:
                try:
                    cursor.execute(f'CREATE TABLE {parent}(id INTEGER PRIMARY KEY,marker INTEGER,code INTEGER UNIQUE)')
                    created.append(parent)
                    referred = 'code' if unique_parent else 'id'
                    timing = ' DEFERRABLE INITIALLY DEFERRED' if deferred else ''
                    cursor.execute(f'CREATE TABLE {child}(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES {parent}({referred}){timing})')
                    created.append(child)
                    cursor.execute(f'INSERT INTO {parent} VALUES(1,0,10)')
                    one,two = factory(),factory()
                    writers.extend([one,two])
                    one.autocommit = two.autocommit = False
                    with one.cursor() as first,two.cursor() as second:
                        if label == 'savepoint_releases_reference':
                            first.execute('SAVEPOINT before_child')
                            first.execute(f'INSERT INTO {child} VALUES(1,1)')
                            first.execute('ROLLBACK TO before_child')
                            cursor.execute(f'DELETE FROM {parent} WHERE id=1')
                            one.commit()
                            cursor.execute(f'SELECT COUNT(*) FROM {parent}')
                            actual=[int(cursor.fetchone()[0])]
                            cursor.execute(f'SELECT COUNT(*) FROM {child}')
                            actual.append(int(cursor.fetchone()[0]))
                            expected=[0,0]
                        else:
                            key = 10 if unique_parent else 1
                            first.execute(f'INSERT INTO {child} VALUES(1,{key})')
                            second.execute(f'INSERT INTO {child} VALUES(2,{key})')
                            if unique_parent:
                                cursor.execute(f'UPDATE {parent} SET marker=7 WHERE id=1')
                            one.commit();two.commit()
                            cursor.execute(f'SELECT COUNT(*) FROM {child}')
                            actual=[int(cursor.fetchone()[0])]
                            cursor.execute(f'SELECT marker FROM {parent} WHERE id=1')
                            actual.append(int(cursor.fetchone()[0]))
                            expected=[2,7 if unique_parent else 0]
                        item.update(actual=actual,expected=expected,qualified=actual==expected)
                except Exception as error:
                    item.update(qualified=False,error_class=type(error).__name__,
                                sqlstate=getattr(error,'sqlstate',None),
                                oracle_code=getattr(error.args[0],'code',None) if error.args else None)
                finally:
                    for connection in reversed(writers):
                        try:connection.rollback();connection.close()
                        except Exception as error:report['cleanup_errors'].append(type(error).__name__)
                    for table in reversed(created):
                        try:cursor.execute('DROP TABLE '+table+(' PURGE' if native else ''))
                        except Exception as error:report['cleanup_errors'].append(type(error).__name__)
                report['cases'].append(item)
    finally:
        base.close()
    return report


def run(source_factory,target_factory):
    source,target = observe(source_factory),observe(target_factory)
    cases=[dict(id=a['id'],source=a,bicdb=b,matched=a['id']==b['id'] and a['qualified'] and b['qualified'])
           for a,b in zip(source['cases'],target['cases'],strict=True)]
    cleanup=source['cleanup_errors']+target['cleanup_errors']
    source_ok=source['driver_kind']=='oracle' and len(source['cases'])==4 and all(x['qualified'] for x in source['cases'])
    return dict(format=1,cases=cases,total=len(cases),matched=sum(x['matched'] for x in cases),
                source_qualified=source_ok,cleanup_errors=cleanup,
                success=source_ok and len(cases)==4 and all(x['matched'] for x in cases) and not cleanup,
                application_qualified=False)


if __name__ == '__main__':raise SystemExit(comparison_main(run,__doc__,references=('oracle',)))
