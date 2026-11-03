"""Independent PostgreSQL controls for qualified key updates and UPDATE policy."""
import uuid
from primary_key_updates import run as run_core, execute, outcome
from routine_namespaces import identifier


def run(reference,target,schema="public"):
    prefix='bicdb_key_'+uuid.uuid4().hex[:12];qs=identifier(schema);role=prefix+'_writer';qr=identifier(role)
    table=qs+'.'+identifier(prefix+'_ACL');rls=qs+'.'+identifier(prefix+'_RLS')
    report=dict(format=1,postgres_version_num=reference.info.server_version,native_bicdb=True,application_qualified=False,cases=[],cleanup_errors=[],success=False)
    created=[]
    def compare(case,sql,error=None):
        row=dict(id=case,postgres=outcome(reference,sql,'postgres'),bicdb=outcome(target,sql,'postgres'))
        row['matched']=(row['postgres']==row['bicdb'] and (row['postgres']==dict(error_code=error) if error else 'error_code' not in row['postgres']))
        report['cases'].append(row)
    def both(sql):
        for connection in (reference,target):execute(connection,sql)
    try:
        for backend,connection in enumerate((reference,target)):
            execute(connection,f'CREATE ROLE {qr} LOGIN');created.append((backend,'ROLE',qr))
        core=run_core(reference,target,schema,prefix,'postgres');report['core']=dict(matched=core['matched'],total=core['total']);report['cases'].extend(core['cases']);report['cleanup_errors'].extend(core['cleanup_errors'])
        both(f'CREATE TABLE {table}("K" INTEGER PRIMARY KEY,"V" INTEGER)')
        both(f'INSERT INTO {table} VALUES(1,10)')
        both(f'GRANT USAGE ON SCHEMA {qs} TO {qr}')
        both(f'GRANT SELECT,UPDATE("K") ON {table} TO {qr}')
        both(f'SET ROLE {qr}')
        compare('update_column_grant_without_insert_delete',f'UPDATE {table} SET "K"="K"+1 WHERE "K"=1')
        compare('insert_still_denied',f'INSERT INTO {table} VALUES(3,30)','42501')
        compare('delete_still_denied',f'DELETE FROM {table}','42501')
        compare('ungranted_assignment_denied',f'UPDATE {table} SET "K"=3,"V"=4','42501')
        both('RESET ROLE');compare('acl_state',f'SELECT "K","V" FROM {table}')
        both(f'REVOKE SELECT ON {table} FROM {qr}');both(f'SET ROLE {qr}')
        compare('reading_without_select_denied',f'UPDATE {table} SET "K"="V"','42501')
        compare('constant_without_select_allowed',f'UPDATE {table} SET "K"=4')
        both('RESET ROLE');compare('constant_state',f'SELECT "K","V" FROM {table}')
        both(f'CREATE TABLE {rls}("K" INTEGER PRIMARY KEY,"Tenant" VARCHAR(100))')
        both(f"INSERT INTO {rls} VALUES(1,'{role}')");both(f"INSERT INTO {rls} VALUES(2,'other')")
        both(f'GRANT SELECT,UPDATE ON {rls} TO {qr}')
        both(f'ALTER TABLE {rls} ENABLE ROW LEVEL SECURITY')
        both(f'CREATE POLICY read_rows ON {rls} FOR SELECT USING(true)')
        both(f'CREATE POLICY update_rows ON {rls} FOR UPDATE USING("Tenant"=current_user) WITH CHECK("Tenant"=current_user AND "K"<100)')
        both(f'SET ROLE {qr}')
        compare('rls_update_using',f'UPDATE {rls} SET "K"="K"+10')
        compare('rls_check_failure',f'UPDATE {rls} SET "K"=101 WHERE "K"=11','42501')
        compare('rls_check_preserves_state',f'SELECT "K","Tenant" FROM {rls} ORDER BY "K"')
        both('RESET ROLE');both(f'DROP POLICY read_rows ON {rls}');both(f'SET ROLE {qr}')
        compare('rls_no_select_policy_column_read',f'UPDATE {rls} SET "K"=12 WHERE "K"=11')
        compare('rls_no_select_policy_constant',f'UPDATE {rls} SET "K"=13')
        both('RESET ROLE');compare('rls_owner_state',f'SELECT "K","Tenant" FROM {rls} ORDER BY "K"')
    finally:
        for connection in (reference,target):
            connection.rollback();connection.autocommit=True
            execute(connection,'RESET ROLE')
        for backend,connection in enumerate((reference,target)):
            for obj in [rls,table]:
                try:execute(connection,f'DROP TABLE IF EXISTS {obj}')
                except Exception as error:report['cleanup_errors'].append(dict(backend=backend,object='table',error=type(error).__name__))
            try:execute(connection,f'REVOKE USAGE ON SCHEMA {qs} FROM {qr}')
            except Exception:pass
        for backend,kind,name in reversed(created):
            try:execute((reference,target)[backend],f'DROP {kind} {name}')
            except Exception as error:report['cleanup_errors'].append(dict(backend=backend,object=kind,error=type(error).__name__))
    report['matched']=sum(row['matched'] for row in report['cases']);report['total']=len(report['cases']);report['success']=report['matched']==report['total'] and not report['cleanup_errors'];return report


def main():
    import argparse,json,os
    from pathlib import Path
    import psycopg
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--schema',default='public');args=parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True) as target:report=run(reference,target,args.schema)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ['success','matched','total']}));return 0 if report['success'] else 1

if __name__=='__main__':raise SystemExit(main())
