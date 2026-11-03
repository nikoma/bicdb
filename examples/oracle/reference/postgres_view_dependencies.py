"""Compare schema-qualified view dependencies and retained types with PostgreSQL."""
import re
import uuid
from compare import canonical


def run(reference, target, schemas):
    if len(schemas) != 2 or schemas[0] == schemas[1] or not all(re.fullmatch(r'[a-z][a-z0-9_]{1,31}', s) for s in schemas):
        raise ValueError('two distinct simple schema names are required')
    report=dict(format=1,postgres_version_num=reference.info.server_version,cases=[],cleanup_errors=[],success=False,
                scope='Explicit-column view dependency namespace resolution and quoted identifiers; wildcard dependencies and general view invalidation are separate qualification')
    prefix='Quoted.'+uuid.uuid4().hex[:10]
    table='"'+prefix+'"';view='"'+prefix+'.View"'
    first,second=(schema+'.'+table for schema in schemas)
    projection=schemas[1]+'.'+view
    created=[]
    def query(conn,sql):
        with conn.cursor() as cur:
            cur.execute(sql)
            return dict(rows=[[canonical(v) for v in row] for row in cur.fetchall()]) if cur.description else dict(status='ok')
    def compare(name,sql):
        row=dict(id=name)
        for label,conn in [('postgres',reference),('bicdb',target)]:
            try:row[label]=query(conn,sql)
            except Exception as e:row[label]=dict(sqlstate=getattr(e,'sqlstate',None))
        row['matched']=row['postgres']==row['bicdb'] and row['postgres']!=dict(sqlstate=None)
        report['cases'].append(row)
    try:
        for backend,conn in enumerate((reference,target)):
            for relation in [first,second]:
                query(conn,'CREATE TABLE '+relation+'(v VARCHAR(3))');created.append((backend,'TABLE',relation))
            query(conn,'CREATE VIEW '+projection+' AS SELECT v AS label FROM '+first);created.append((backend,'VIEW',projection))
        compare('referenced_namespace_is_protected','ALTER TABLE '+first+' ALTER COLUMN v TYPE VARCHAR(4)')
        compare('same_table_name_in_other_namespace_can_change','ALTER TABLE '+second+' ALTER COLUMN v TYPE VARCHAR(4)')
        sql=("SELECT table_schema,character_maximum_length FROM information_schema.columns WHERE table_name='"+prefix+"' AND column_name='v' AND table_schema IN ('"+schemas[0]+"','"+schemas[1]+"') ORDER BY table_schema")
        compare('failed_alter_preserves_original_type',sql)
        for backend,conn in enumerate((reference,target)):
            query(conn,'DROP VIEW '+projection);created.remove((backend,'VIEW',projection))
        compare('type_change_succeeds_after_dependency_removed','ALTER TABLE '+first+' ALTER COLUMN v TYPE VARCHAR(4)')
        compare('final_types_match',sql)
    finally:
        for backend,kind,relation in reversed(created):
            try:query((reference,target)[backend],'DROP '+kind+' '+relation)
            except Exception as e:report['cleanup_errors'].append(dict(backend=backend,error=type(e).__name__))
    report['matched']=sum(c['matched'] for c in report['cases']);report['total']=len(report['cases'])
    report['success']=report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse,json,os
    from pathlib import Path
    import psycopg
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--schemas',nargs=2,required=True);args=p.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True,connect_timeout=10) as ref:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True,connect_timeout=10) as target:
            if target.info.parameter_status('bicdb_sql_profile')!='postgres':raise RuntimeError('native PostgreSQL-profile target required')
            report=run(ref,target,args.schemas)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ['success','matched','total']}));return 0 if report['success'] else 1


if __name__=='__main__':raise SystemExit(main())
