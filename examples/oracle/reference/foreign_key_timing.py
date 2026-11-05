"""Compare selected deferred foreign-key contracts on disposable nonce tables.

Oracle ENABLE NOVALIDATE is adapted to PostgreSQL NOT VALID only for this
comparison. Explicit setup commits isolate FK behavior from Oracle DDL commits.
No application tables are read or changed. This is not complete FK conformance.
"""
import uuid

from row_lock_waits import main as comparison_main


CASES = [
    'initial_timing_catalog', 'child_before_parent', 'unresolved_commit',
    'set_immediate_repair', 'savepoint_discards_check', 'parent_delete_repaired',
    'novalidate_payload_update', 'novalidate_unchanged_key',
    'validate_existing_orphan', 'cascade_is_immediate', 'set_null_is_immediate',
    'set_immediate_failure_keeps_pending_check', 'nondeferrable_cannot_defer',
]


def expectations(case, oracle):
    """Require explicit behavior, so equal failures cannot count as compatibility."""
    legacy = case.startswith('novalidate_') or case == 'validate_existing_orphan'
    fixed = case == 'nondeferrable_cannot_defer'
    action = {'cascade_is_immediate': 'CASCADE', 'set_null_is_immediate': 'SET NULL'}.get(case, 'NO ACTION')
    catalog = [not fixed, not fixed, not legacy, action, ['NO', 'NO'] if fixed else ['YES', 'YES']]
    details = {
        'initial_timing_catalog': dict(rows=1),
        'child_before_parent': dict(commit='ok', rows=2),
        'unresolved_commit': dict(commit='foreign_key', children=1, marker=0),
        'set_immediate_repair': dict(immediate='foreign_key', pending=1, commit='ok'),
        'savepoint_discards_check': dict(commit='ok', rows=1),
        'parent_delete_repaired': dict(commit='ok', rows=0),
        'novalidate_payload_update': dict(update='ok', commit='ok', rows=1, note=1),
        'novalidate_unchanged_key': dict(update='ok', commit='foreign_key' if oracle else 'ok', rows=1, note=0),
        'validate_existing_orphan': dict(validate='foreign_key', after_failure=catalog, after_repair=[True, True, True, 'NO ACTION', ['YES', 'YES']]),
        'cascade_is_immediate': dict(rows_before_commit=0, commit='ok'),
        'set_null_is_immediate': dict(rows_before_commit=1, parent_before_commit=None, commit='ok'),
        'set_immediate_failure_keeps_pending_check': dict(immediate='foreign_key', commit='foreign_key', children=1),
        'nondeferrable_cannot_defer': dict(set_deferred='constraint_mode', set_immediate='constraint_mode' if oracle else 'ok', rows=1),
    }
    return dict(catalog=catalog, **details[case])


def error_category(error):
    value = error.args[0] if error.args else None
    code = getattr(value, 'code', None)
    state = getattr(error, 'sqlstate', None)
    if code in (2091, 2291, 2292, 2298) or state == '23503':
        return 'foreign_key'
    if code == 2447 or state in ('42809','55000'):
        return 'constraint_mode'
    return 'other:' + str(code or state or type(error).__name__)


def observe(factory, progress=None):
    report = dict(cases=[], cleanup_errors=[])
    for case in CASES:
        created = []
        outcome = {}
        diagnostics = []
        current_statement = None
        with factory() as connection:
            native = type(connection).__module__.startswith('oracledb')
            oracle_semantics = native or connection.info.parameter_status('bicdb_sql_profile') == 'oracle-preview-v1'
            connection.autocommit = False
            report['server_version'] = connection.version if native else connection.info.server_version
            stem = 'BIC_FK_' + uuid.uuid4().hex[:12].upper()
            parent, child, name = (stem + suffix for suffix in ('_P','_C','_FK'))
            if progress is not None:progress(dict(case=case,parent=parent,child=child,constraint=name))
            p, c, fk = ('"' + value + '"' for value in (parent,child,name))
            cursor = connection.cursor()
            report['reference_driver'] = 'oracledb' if native else 'psycopg'
            def execute(sql, parameters=None):
                nonlocal current_statement
                current_statement = sql
                cursor.execute(sql, parameters)
            def scalar(sql):
                execute(sql)
                value = cursor.fetchone()[0]
                return None if value is None else int(value)
            def attempt(call):
                try:
                    call()
                    return 'ok'
                except Exception as error:
                    value=error.args[0] if error.args else None
                    diagnostics.append(dict(oracle_code=getattr(value,'code',None),sqlstate=getattr(error,'sqlstate',None)))
                    return error_category(error)
            def metadata():
                if native:
                    execute('SELECT DEFERRABLE,DEFERRED,VALIDATED,DELETE_RULE FROM USER_CONSTRAINTS WHERE CONSTRAINT_NAME=:1',[name])
                    row = cursor.fetchone()
                    return [row[0]=='DEFERRABLE',row[1]=='DEFERRED',row[2]=='VALIDATED',row[3],[('YES' if row[0]=='DEFERRABLE' else 'NO'),('YES' if row[1]=='DEFERRED' else 'NO')]]
                execute('SELECT "condeferrable","condeferred","convalidated","confdeltype" FROM "pg_catalog"."pg_constraint" WHERE "conname"=%s',[name])
                row = cursor.fetchone()
                result=[bool(row[0]),bool(row[1]),bool(row[2]),{'a':'NO ACTION','c':'CASCADE','n':'SET NULL'}.get(row[3],row[3])]
                execute('SELECT \"is_deferrable\",\"initially_deferred\" FROM \"information_schema\".\"table_constraints\" WHERE \"constraint_name\"=%s',[name])
                result.append(list(cursor.fetchone()))
                return result
            legacy = case.startswith('novalidate_') or case == 'validate_existing_orphan'
            action = 'CASCADE' if case=='cascade_is_immediate' else 'SET NULL' if case=='set_null_is_immediate' else 'NO ACTION'
            try:
                execute('CREATE TABLE '+p+'("ID" INTEGER PRIMARY KEY)');created.append(p);connection.commit()
                execute('CREATE TABLE '+c+'("ID" INTEGER PRIMARY KEY,"PID" INTEGER,"NOTE" INTEGER)');created.append(c);connection.commit()
                execute('INSERT INTO '+p+' VALUES(1)')
                execute('INSERT INTO '+c+' VALUES(1,'+('999' if legacy else '1')+',0)');connection.commit()
                ddl='ALTER TABLE '+c+' ADD CONSTRAINT '+fk+' FOREIGN KEY("PID") REFERENCES '+p+'("ID")'
                if action!='NO ACTION':ddl+=' ON DELETE '+action
                ddl+=(' NOT DEFERRABLE' if case=='nondeferrable_cannot_defer' else ' DEFERRABLE INITIALLY DEFERRED')
                if legacy:ddl+=' ENABLE NOVALIDATE' if native else ' NOT VALID'
                execute(ddl);connection.commit()
                outcome['catalog']=metadata();connection.commit()
                if case=='initial_timing_catalog':
                    outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                elif case=='child_before_parent':
                    execute('INSERT INTO '+c+' VALUES(2,2,0)');execute('INSERT INTO '+p+' VALUES(2)')
                    outcome['commit']=attempt(connection.commit)
                    outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                elif case=='unresolved_commit':
                    execute('INSERT INTO '+p+' VALUES(2)');execute('INSERT INTO '+c+' VALUES(2,999,0)')
                    outcome['commit']=attempt(connection.commit)
                    # Observe the connection immediately, without a rollback masking state.
                    outcome['children']=scalar('SELECT COUNT(*) FROM '+c)
                    outcome['marker']=scalar('SELECT COUNT(*) FROM '+p+' WHERE "ID"=2')
                elif case=='set_immediate_repair':
                    execute('INSERT INTO '+c+' VALUES(2,2,0)');execute('SAVEPOINT before_mode')
                    outcome['immediate']=attempt(lambda:execute('SET CONSTRAINTS '+fk+' IMMEDIATE'))
                    if not oracle_semantics:execute('ROLLBACK TO SAVEPOINT before_mode')
                    outcome['pending']=scalar('SELECT COUNT(*) FROM '+c+' WHERE "ID"=2')
                    execute('INSERT INTO '+p+' VALUES(2)');execute('SET CONSTRAINTS '+fk+' IMMEDIATE')
                    outcome['commit']=attempt(connection.commit)
                elif case=='set_immediate_failure_keeps_pending_check':
                    execute('INSERT INTO '+c+' VALUES(2,2,0)');execute('SAVEPOINT before_mode')
                    outcome['immediate']=attempt(lambda:execute('SET CONSTRAINTS '+fk+' IMMEDIATE'))
                    if not oracle_semantics:execute('ROLLBACK TO SAVEPOINT before_mode')
                    outcome['commit']=attempt(connection.commit)
                    outcome['children']=scalar('SELECT COUNT(*) FROM '+c)
                elif case=='nondeferrable_cannot_defer':
                    outcome['set_deferred']=attempt(lambda:execute('SET CONSTRAINTS '+fk+' DEFERRED'))
                    connection.rollback()
                    outcome['set_immediate']=attempt(lambda:execute('SET CONSTRAINTS '+fk+' IMMEDIATE'))
                    connection.rollback();outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                elif case=='savepoint_discards_check':
                    execute('SAVEPOINT safe');execute('INSERT INTO '+c+' VALUES(2,999,0)');execute('ROLLBACK TO SAVEPOINT safe')
                    outcome['commit']=attempt(connection.commit);outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                elif case=='parent_delete_repaired':
                    execute('DELETE FROM '+p+' WHERE "ID"=1');execute('DELETE FROM '+c+' WHERE "ID"=1')
                    outcome['commit']=attempt(connection.commit);outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                elif case in ('novalidate_payload_update','novalidate_unchanged_key'):
                    assignment='"NOTE"=1' if case=='novalidate_payload_update' else '"PID"="PID"'
                    outcome['update']=attempt(lambda:execute('UPDATE '+c+' SET '+assignment+' WHERE "ID"=1'))
                    outcome['commit']=attempt(connection.commit)
                    outcome['rows']=scalar('SELECT COUNT(*) FROM '+c)
                    outcome['note']=scalar('SELECT "NOTE" FROM '+c+' WHERE "ID"=1')
                elif case=='validate_existing_orphan':
                    validate='ALTER TABLE '+c+(' ENABLE VALIDATE CONSTRAINT ' if native else ' VALIDATE CONSTRAINT ')+fk
                    outcome['validate']=attempt(lambda:execute(validate));connection.rollback()
                    outcome['after_failure']=metadata();connection.commit()
                    execute('INSERT INTO '+p+' VALUES(999)');connection.commit()
                    execute(validate);connection.commit();outcome['after_repair']=metadata()
                elif case in ('cascade_is_immediate','set_null_is_immediate'):
                    execute('DELETE FROM '+p+' WHERE "ID"=1')
                    outcome['rows_before_commit']=scalar('SELECT COUNT(*) FROM '+c)
                    if action=='SET NULL':outcome['parent_before_commit']=scalar('SELECT "PID" FROM '+c)
                    outcome['commit']=attempt(connection.commit)
            except Exception as error:
                outcome['unexpected_error']=error_category(error)
                outcome['error_statement']=current_statement
            finally:
                connection.rollback()
                for table in reversed(created):
                    try:
                        execute('DROP TABLE '+table+(' PURGE' if native else ''))
                        connection.commit()
                    except Exception as error:
                        report['cleanup_errors'].append(dict(case=case,error=error_category(error)))
                        connection.rollback()
                cursor.close()
        expected = expectations(case, oracle_semantics)
        report['cases'].append(dict(id=case, observations=outcome, expected=expected,
                                    diagnostics=diagnostics, qualified=outcome == expected))
    return report


def run(source_factory,target_factory,progress=None):
    def callback(side):
        return None if progress is None else lambda value:progress(dict(value,side=side))
    source=observe(source_factory,callback('source'))
    target=observe(target_factory,callback('bicdb'))
    diagnostics_compared=source.get('reference_driver')=='psycopg'
    cases=[]
    for a,b in zip(source['cases'],target['cases'],strict=True):
        diagnostics_matched=not diagnostics_compared or a['diagnostics']==b['diagnostics']
        cases.append(dict(id=a['id'],source=a,bicdb=b,diagnostics_compared=diagnostics_compared,diagnostics_matched=diagnostics_matched,matched=a['id']==b['id'] and a['qualified'] and b['qualified'] and a['observations']==b['observations'] and diagnostics_matched))
    return dict(format=1,application_qualified=False,oracle_error_codes_qualified=False,source_version=source.get('server_version'),target_version=target.get('server_version'),cases=cases,total=len(cases),matched=sum(x['matched'] for x in cases),source_qualified=all(x['qualified'] for x in source['cases']),cleanup_errors=source['cleanup_errors']+target['cleanup_errors'],success=all(x['matched'] for x in cases) and not source['cleanup_errors'] and not target['cleanup_errors'])


if __name__=='__main__':raise SystemExit(comparison_main(runner=run,description=__doc__))
