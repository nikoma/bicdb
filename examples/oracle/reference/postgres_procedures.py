"""Independent PostgreSQL CALL inputs, outputs, metadata and nested copyback."""
import json
import uuid

from compare import canonical
from routine_namespaces import identifier


def run(reference, target):
    prefix = 'bicdb_proc_' + uuid.uuid4().hex[:12]
    q = lambda suffix: identifier('public')+'.'+identifier(prefix+'_'+suffix)
    definitions = [
        ('TABLE',q('rows'),f'CREATE TABLE {q("rows")}(v NUMERIC)'),
        ('PROCEDURE',q('p'),f'CREATE OR REPLACE PROCEDURE {q("p")}(IN amount NUMERIC,OUT doubled NUMERIC,INOUT total NUMERIC) LANGUAGE plpgsql AS $$BEGIN doubled:=amount*2;total:=coalesce(total,0)+coalesce(doubled,0); END$$'),
        ('PROCEDURE',q('defaults'),f'CREATE PROCEDURE {q("defaults")}(INOUT value NUMERIC DEFAULT 5) LANGUAGE plpgsql AS $$BEGIN value:=value+1;END$$'),
        ('PROCEDURE',q('wide'),f'CREATE PROCEDURE {q("wide")}(IN amount BIGINT,OUT doubled BIGINT) LANGUAGE plpgsql AS $$BEGIN doubled:=amount*2;END$$'),
        ('PROCEDURE',q('types'),f"CREATE PROCEDURE {q('types')}(OUT day DATE,OUT label TEXT) LANGUAGE plpgsql AS $$BEGIN day:=DATE '2024-02-29';label:='';END$$"),
        ('FUNCTION',q('caller'),f'CREATE FUNCTION {q("caller")}() RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE doubled NUMERIC;total NUMERIC:=4;BEGIN CALL {q("p")}(total=>total,amount=>3,doubled=>doubled);RETURN doubled+100*total;END$$'),
        ('FUNCTION',q('record_caller'),f'CREATE FUNCTION {q("record_caller")}() RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE r RECORD;BEGIN SELECT CAST(99 AS NUMERIC) AS doubled,CAST(4 AS NUMERIC) AS total INTO r;CALL {q("p")}(3,r.doubled,r.total);RETURN r.doubled+100*r.total;END$$'),
    ]
    cases = [
        ('out_not_evaluated',f'CALL {q("p")}(3,1/0,4)',[[6,10]],[1700,1700],None),
        ('named_order',f'CALL {q("p")}(total=>4,doubled=>NULL,amount=>3)',[[6,10]],[1700,1700],None),
        ('null_outputs_typed',f'CALL {q("p")}(NULL,NULL,NULL)',[[None,0]],[1700,1700],None),
        ('nested_copyback',f'SELECT {q("caller")}()',[[1006]],[1700],None),
        ('record_copyback',f'SELECT {q("record_caller")}()',[[1006]],[1700],None),
        ('default_inout',f'CALL {q("defaults")}()',[[6]],[1700],None),
        ('explicit_inout',f'CALL {q("defaults")}(8)',[[9]],[1700],None),
        ('declared_bigint_output',f'CALL {q("wide")}(1,NULL)',[[2]],[20],None),
        ('date_and_empty_text',f'CALL {q("types")}(NULL,NULL)',None,[1082,25],None),
        ('wrong_schema',f'CALL pg_catalog.{identifier(prefix+"_p")}(3,NULL,4)',None,None,'42883'),
        ('missing_out_slot',f'CALL {q("p")}(3,4)',None,None,'42883'),
        ('unknown_named',f'CALL {q("p")}(missing=>3,doubled=>NULL,total=>4)',None,None,'42883'),
        ('recovery',f'CALL {q("p")}(2,NULL,5)',[[4,9]],[1700,1700],None),
    ]
    report = dict(format=1,postgres_version_num=reference.info.server_version,
                  native_bicdb=True,application_qualified=False,cases=[],cleanup_errors=[],success=False)
    created = []
    try:
        for backend, connection in enumerate((reference,target)):
            for kind, name, sql in definitions:
                with connection.cursor() as cursor:
                    cursor.execute(sql,prepare=False)
                    created.append((backend,kind,name))
        for name,sql,expected,oids,error in cases:
            row = dict(id=name)
            for label,connection in [('postgres',reference),('bicdb',target)]:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(sql,prepare=False)
                        row[label] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()],
                                          type_oids=[column.type_code for column in cursor.description])
                except Exception as exception:
                    if getattr(exception,'sqlstate',None) is None:
                        raise
                    row[label] = dict(sqlstate=exception.sqlstate)
            row['matched'] = row['postgres']==row['bicdb']
            if error:
                row['expected_error'] = error
                row['matched'] &= row['postgres'].get('sqlstate')==error
            else:
                if name=='date_and_empty_text':
                    expected_rows = [[{'date':'2024-02-29'},'']]
                else:
                    expected_rows = [[canonical(v) for v in values] for values in expected]
                row['expected'] = dict(rows=expected_rows,type_oids=oids)
                row['matched'] &= row['postgres']==row['expected']
            report['cases'].append(row)
    finally:
        for backend,kind,name in reversed(created):
            try:
                suffix='()' if kind=='FUNCTION' else ''
                # PostgreSQL accepts a procedure's unique name without a
                # signature. This fixture never creates overloads.
                with (reference,target)[backend].cursor() as cursor:
                    cursor.execute(f'DROP {kind} {name}{suffix}',prepare=False)
            except Exception as error:
                report['cleanup_errors'].append(dict(backend=backend,kind=kind,
                    error=type(error).__name__,sqlstate=getattr(error,'sqlstate',None)))
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(report['cases'])
    report['success'] = report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse
    import os
    from pathlib import Path
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True) as source:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True) as target:
            report = run(source,target)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ['success','matched','total']}))
    return 0 if report['success'] else 1


if __name__=='__main__':
    raise SystemExit(main())
