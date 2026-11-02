"""Independent native PostgreSQL loop, lock-list and grammar isolation checks."""
import uuid

from compare import canonical
from routine_namespaces import identifier


def run(reference, target):
    prefix = 'bicdb_grammar_' + uuid.uuid4().hex[:12]
    q = lambda suffix: 'public.' + identifier(prefix+'_'+suffix)
    table = q('rows')
    definitions = [
        ('TABLE', table, f'CREATE TABLE {table}(k INTEGER PRIMARY KEY,v NUMERIC)'),
        ('FUNCTION', q('count'), f'''CREATE FUNCTION {q('count')}() RETURNS NUMERIC
          LANGUAGE plpgsql AS $$DECLARE total NUMERIC:=0;BEGIN
          FoR i IN 1..3 LoOp total:=total+i;EnD
          LoOp;RETURN total;END$$'''),
        ('FUNCTION', q('query'), f'''CREATE FUNCTION {q('query')}() RETURNS NUMERIC
          LANGUAGE plpgsql AS $$DECLARE total NUMERIC:=0;r RECORD;BEGIN
          FoR r IN SELECT v FROM {table} LOOP total:=total+r.v;END
          LOOP;RETURN total;END$$'''),
    ]
    cases = [
        ('integer_loop',f'SELECT {q("count")}()',[[6]],[1700],None),
        ('query_loop',f'SELECT {q("query")}()',[[30]],[1700],None),
        ('native_concat_null',"SELECT 'x'||CAST(NULL AS TEXT)",[[None]],[25],None),
        ('quoted_marker_text',"SELECT '(+)'",[['(+)']],[25],None),
        ('reject_oracle_marker',f'SELECT a.k FROM {table} a,{table} b WHERE a.k=b.k(+)',None,None,'42601'),
        ('reject_delete_without_from',f'DELETE {table} WHERE k=1',None,None,'42601'),
    ]
    for suffix in ('', ' NOWAIT', ' SKIP LOCKED'):
        cases.append(('lock_list'+str(len(cases)),f'SELECT r.k,r.v FROM {table} r ORDER BY r.k FOR UPDATE OF r,r{suffix}',[[1,10],[2,20]],[23,1700],None))
    report = dict(format=1,postgres_version_num=reference.info.server_version,
                  native_bicdb=True,application_qualified=False,cases=[],cleanup_errors=[],success=False)
    created = []
    try:
        for backend, connection in enumerate((reference,target)):
            with connection.cursor() as cursor:
                for kind,name,sql in definitions:
                    cursor.execute(sql,prepare=False)
                    created.append((backend,kind,name))
                    if kind=='TABLE':
                        cursor.execute(f'INSERT INTO {table} VALUES(1,10),(2,20)',prepare=False)
        for name,sql,values,oids,error in cases:
            row = dict(id=name)
            for label,connection in [('postgres',reference),('bicdb',target)]:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(sql,prepare=False)
                        row[label] = dict(rows=[[canonical(v) for v in r] for r in cursor.fetchall()],
                                          type_oids=[d.type_code for d in cursor.description])
                except Exception as exception:
                    if getattr(exception,'sqlstate',None) is None:
                        raise
                    row[label] = dict(sqlstate=exception.sqlstate)
            if error:
                expected = dict(sqlstate=error)
            else:
                expected = dict(rows=[[canonical(v) for v in r] for r in values],type_oids=oids)
            row.update(expected=expected,matched=row['postgres']==row['bicdb']==expected)
            report['cases'].append(row)
    finally:
        for backend,kind,name in reversed(created):
            try:
                with (reference,target)[backend].cursor() as cursor:
                    cursor.execute(f'DROP {kind} {name}',prepare=False)
            except Exception as error:
                report['cleanup_errors'].append(dict(backend=backend,kind=kind,
                    error=type(error).__name__,sqlstate=getattr(error,'sqlstate',None)))
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(report['cases'])
    report['success'] = report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import psycopg
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True) as reference:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True) as target:
            report = run(reference,target)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('success','matched','total')}))
    return 0 if report['success'] else 1


if __name__=='__main__':
    raise SystemExit(main())
