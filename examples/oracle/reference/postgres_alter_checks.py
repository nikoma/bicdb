"""Compare ALTER constraint validation, atomic failure and retained data with PostgreSQL."""
import re
import uuid
from compare import canonical


def run(reference,target,schemas):
    if len(schemas)!=2 or schemas[0]==schemas[1] or not all(re.fullmatch(r'[a-z][a-z0-9_]{1,31}',s) for s in schemas):
        raise ValueError('two distinct simple schema names are required')
    name='Alter.Check_'+uuid.uuid4().hex[:10];table=schemas[0]+'."'+name+'"';created=[]
    report=dict(format=1,postgres_version_num=reference.info.server_version,scope='Owner ALTER rewrite checks, NOT NULL and CHECK enforcement, failed-type/data rollback and subsequent valid rewrite; no Oracle DDL equivalence claim',cases=[],cleanup_errors=[],success=False)
    def query(c,sql):
        with c.cursor() as cur:
            cur.execute(sql)
            return dict(rows=[[canonical(v) for v in row] for row in cur.fetchall()]) if cur.description else dict(status='ok')
    def compare(name,sql):
        row=dict(id=name)
        for label,c in [('postgres',reference),('bicdb',target)]:
            try:row[label]=query(c,sql)
            except Exception as e:row[label]=dict(sqlstate=getattr(e,'sqlstate',None))
        row['matched']=row['postgres']==row['bicdb'] and row['postgres']!=dict(sqlstate=None);report['cases'].append(row)
    try:
        for index,c in enumerate((reference,target)):
            query(c,'CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,CONSTRAINT positive CHECK(v>0),CONSTRAINT upper_bound CHECK(v<=500))');created.append(index)
            query(c,'INSERT INTO '+table+' VALUES '+','.join(f'({n},{n})' for n in range(1,501)))
        descriptor=f"SELECT data_type FROM information_schema.columns WHERE table_schema='{schemas[0]}' AND table_name='{name}' AND column_name='v'"
        snapshot='SELECT COUNT(*),MIN(v),MAX(v) FROM '+table
        compare('failed_rewrite_check','ALTER TABLE '+table+' ALTER COLUMN v TYPE BIGINT USING v+1')
        compare('failed_rewrite_preserves_type',descriptor)
        compare('failed_rewrite_preserves_values',snapshot)
        compare('valid_rewrite','ALTER TABLE '+table+' ALTER COLUMN v TYPE BIGINT USING v+0')
        compare('valid_rewrite_type',descriptor)
        compare('valid_rewrite_values',snapshot)
        compare('check_still_enforced','UPDATE '+table+' SET v=0 WHERE id=1')
        compare('not_null_still_enforced','UPDATE '+table+' SET v=NULL WHERE id=1')
        compare('failed_updates_preserve_values',snapshot)
    finally:
        for index in reversed(created):
            try:query((reference,target)[index],'DROP TABLE '+table)
            except Exception as e:report['cleanup_errors'].append(dict(backend=index,error=type(e).__name__))
    report['matched']=sum(c['matched'] for c in report['cases']);report['total']=len(report['cases']);report['success']=report['matched']==report['total'] and not report['cleanup_errors'];return report


def main():
    import argparse,json,os
    from pathlib import Path
    import psycopg
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--schemas',nargs=2,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    with psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True,connect_timeout=10) as ref:
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True,connect_timeout=10) as target:
            if target.info.parameter_status('bicdb_sql_profile')!='postgres':raise RuntimeError('native PostgreSQL-profile target required')
            report=run(ref,target,a.schemas)
    a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ['success','matched','total']}));return 0 if report['success'] else 1


if __name__=='__main__':raise SystemExit(main())
