"""Independent PostgreSQL rethrow diagnostics, scope and native routine calls."""
import uuid

from compare import canonical
from routine_namespaces import identifier


def run(reference,target):
    prefix='bicdb_error_'+uuid.uuid4().hex[:12]
    q=lambda name:'public.'+identifier(prefix+'_'+name)
    definitions=[
        ('TABLE',q('rows'),f'CREATE TABLE {q("rows")}(v NUMERIC)'),
        ('FUNCTION',q('detail'),f"CREATE FUNCTION {q('detail')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'native original' USING ERRCODE='22012',DETAIL='native detail';EXCEPTION WHEN OTHERS THEN RAISE;END$$"),
        ('FUNCTION',q('trailing'),f"CREATE FUNCTION {q('trailing')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'native trailing ';EXCEPTION WHEN OTHERS THEN RAISE;END$$"),
        ('FUNCTION',q('restore'),f"CREATE FUNCTION {q('restore')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN BEGIN RAISE EXCEPTION 'outer' USING ERRCODE='22012';EXCEPTION WHEN OTHERS THEN BEGIN RAISE EXCEPTION 'inner' USING ERRCODE='23505';EXCEPTION WHEN OTHERS THEN NULL;END;RAISE;END;RETURN 0;END$$"),
        ('FUNCTION',q('constraint'),f"CREATE FUNCTION {q('constraint')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'constraint original' USING ERRCODE='23505',CONSTRAINT='native_unique',TABLE='native_table',COLUMN='native_column';EXCEPTION WHEN OTHERS THEN RAISE;END$$"),
        ('FUNCTION',q('outside'),f"CREATE FUNCTION {q('outside')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE;END$$"),
        ('FUNCTION',q('shadow'),f"CREATE FUNCTION {q('shadow')}() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE sqlcode INTEGER:=17;sqlerrm TEXT:='user value';BEGIN IF sqlerrm='user value' THEN RETURN sqlcode;END IF;RETURN 0;END$$"),
        ('PROCEDURE',q('raise_application_error'),f"CREATE PROCEDURE {q('raise_application_error')}(num NUMERIC,msg TEXT) LANGUAGE plpgsql AS $$BEGIN INSERT INTO {q('rows')} VALUES(num);END$$"),
        ('FUNCTION',q('caller'),f"CREATE FUNCTION {q('caller')}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CALL {q('raise_application_error')}(-20100,'native');RETURN 42;END$$"),
    ]
    cases=[
        ('detail',f'SELECT {q("detail")}()',dict(sqlstate='22012',message='native original',detail='native detail',table=None,column=None,constraint=None)),
        ('trailing',f'SELECT {q("trailing")}()',dict(sqlstate='P0001',message='native trailing ',detail=None,table=None,column=None,constraint=None)),
        ('restore',f'SELECT {q("restore")}()',dict(sqlstate='22012',message='outer',detail=None,table=None,column=None,constraint=None)),
        ('constraint',f'SELECT {q("constraint")}()',dict(sqlstate='23505',message='constraint original',detail=None,table='native_table',column='native_column',constraint='native_unique')),
        ('outside',f'SELECT {q("outside")}()',dict(sqlstate='0Z002',message='RAISE without parameters cannot be used outside an exception handler',detail=None,table=None,column=None,constraint=None)),
        ('user_variables',f'SELECT {q("shadow")}()',dict(rows=[[canonical(17)]],type_oids=[23])),
        ('native_call',f'SELECT {q("caller")}()',dict(rows=[[canonical(42)]],type_oids=[23])),
        ('native_call_effect',f'SELECT v FROM {q("rows")}',dict(rows=[[canonical(-20100)]],type_oids=[1700])),
        ('recovery','SELECT 7',dict(rows=[[canonical(7)]],type_oids=[23])),
    ]
    report=dict(format=1,postgres_version_num=reference.info.server_version,native_bicdb=True,
                application_qualified=False,cases=[],cleanup_errors=[],success=False)
    created=[]
    try:
        for backend,conn in enumerate((reference,target)):
            for kind,name,sql in definitions:
                with conn.cursor() as cursor:cursor.execute(sql,prepare=False)
                created.append((backend,kind,name))
        for name,sql,expected in cases:
            row=dict(id=name,expected=expected)
            for label,conn in [('postgres',reference),('bicdb',target)]:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(sql,prepare=False)
                        row[label]=dict(rows=[[canonical(v) for v in r] for r in cursor.fetchall()],
                                        type_oids=[d.type_code for d in cursor.description])
                except Exception as error:
                    if not getattr(error,'sqlstate',None):raise
                    row[label]=dict(sqlstate=error.sqlstate,message=error.diag.message_primary,
                                    detail=error.diag.message_detail,table=error.diag.table_name,
                                    column=error.diag.column_name,constraint=error.diag.constraint_name)
            row['matched']=row['postgres']==row['bicdb']==expected
            report['cases'].append(row)
    finally:
        for backend,kind,name in reversed(created):
            try:
                with (reference,target)[backend].cursor() as cursor:cursor.execute(f'DROP {kind} {name}',prepare=False)
            except Exception as error:report['cleanup_errors'].append(dict(backend=backend,kind=kind,error=type(error).__name__,sqlstate=getattr(error,'sqlstate',None)))
    report['matched']=sum(row['matched'] for row in report['cases']);report['total']=len(report['cases'])
    report['success']=report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse,json,os
    from pathlib import Path
    import psycopg
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True) as target:report=run(reference,target)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ['success','matched','total']}))
    return 0 if report['success'] else 1


if __name__=='__main__':raise SystemExit(main())
