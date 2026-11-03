"""Compare qualified primary-key UPDATE values, errors and rollback with Oracle."""
import json
from decimal import Decimal
import re
import uuid
from pathlib import Path

from compare import canonical
from routine_namespaces import identifier


def execute(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        if cursor.description:
            return dict(rows=[[canonical(v) for v in row] for row in cursor.fetchall()])
        return dict(affected=cursor.rowcount)


def outcome(connection, sql, kind):
    try:
        return execute(connection, sql)
    except Exception as error:
        if kind == 'oracle':
            return dict(error_code=error.args[0].code)
        if not getattr(error, 'sqlstate', None):
            raise
        return dict(error_code=error.sqlstate)


def run(reference, target, schema, prefix, kind='oracle'):
    if kind not in ('oracle','postgres') or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{1,30}', prefix):
        raise ValueError('invalid reference kind or fixture prefix')
    q=lambda suffix: identifier(schema)+'.'+identifier(prefix+'_'+suffix)
    table=q('ROWS');child=q('CHILD');parent=q('PARENT');composite=q('COMPOSITE');shift=q('SHIFT')
    definitions=[
        (shift,f'CREATE TABLE {shift}("K" INTEGER PRIMARY KEY,"V" INTEGER)'),
        (table,f'CREATE TABLE {table}("K" INTEGER DEFAULT 9 PRIMARY KEY,"V" INTEGER DEFAULT 7 UNIQUE CHECK("V">0),"Text" VARCHAR(30),"N" NUMERIC(30,6),"Stamp" TIMESTAMP(6))'),
        (parent,f'CREATE TABLE {parent}("K" INTEGER PRIMARY KEY)'),
        (child,f'CREATE TABLE {child}("K" INTEGER PRIMARY KEY,"P" INTEGER REFERENCES {parent}("K"))'),
        (composite,f'CREATE TABLE {composite}("A" NUMERIC(20,3),"B" VARCHAR(20),"V" INTEGER,PRIMARY KEY("A","B"))'),
    ]
    seed=[f'INSERT INTO {table} VALUES(1,10,\'first\',123456789012345678901234.123456,TIMESTAMP \'2024-02-29 12:34:56.123456\')',f'INSERT INTO {table} VALUES(2,20,NULL,NULL,NULL)',f'INSERT INTO {parent} VALUES(1)',f'INSERT INTO {child} VALUES(10,1)',f'INSERT INTO {composite} VALUES(1.250,\'x\',3)']
    seed.extend([f'INSERT INTO {shift} VALUES(1,10)',f'INSERT INTO {shift} VALUES(2,20)'])
    report=dict(format=1,reference_kind=kind,application_qualified=False,cases=[],cleanup_errors=[],success=False,
        scope='Qualified key changes, aliases, simultaneous assignments, defaults, typed values, composite keys, immediate constraint errors, statement atomicity and explicit rollback. Referenced parent keys, triggers, FROM/RETURNING and concurrent key moves remain outside this fixture.')
    report['reference_version']=reference.version if kind=='oracle' else reference.info.server_version
    created=[]
    def compare(case, sql, oracle_code=None, pg_code=None):
        row=dict(id=case)
        for label,conn,backend in [('reference',reference,kind),('bicdb',target,'postgres')]:
            row[label]=outcome(conn,sql,backend)
        if oracle_code is not None:
            row['matched']=(row['reference']==dict(error_code=oracle_code if kind=='oracle' else pg_code)
                            and row['bicdb']==dict(error_code=pg_code))
        else:
            row['matched']=row['reference']==row['bicdb'] and 'error_code' not in row['reference']
        report['cases'].append(row)
    try:
        for backend,connection in enumerate((reference,target)):
            for name,ddl in definitions:
                execute(connection,ddl);created.append((backend,name))
            for sql in seed:execute(connection,sql)
        read=f'SELECT "K","V","Text","N","Stamp" FROM {table} ORDER BY "K"'
        compare('initial_typed_values',read)
        compare('ordered_shift_into_vacated_keys',f'UPDATE {shift} SET "K"="K"-1')
        compare('ordered_shift_state',f'SELECT "K","V" FROM {shift} ORDER BY "K"')
        steps=[
            ('qualified_alias',f'UPDATE {table} r SET "K"=r."K"+10,"V"=r."K" WHERE r."K"=1',None,None),
            ('existing_key',f'UPDATE {table} SET "K"=2 WHERE "K"=11',1,'23505'),
            ('batch_duplicate',f'UPDATE {table} SET "K"=7',1,'23505'),
            ('immediate_key_swap',f'UPDATE {table} SET "K"=13-"K"',1,'23505'),
            ('null_key',f'UPDATE {table} SET "K"=NULL WHERE "K"=11',1407,'23502'),
            ('check_error',f'UPDATE {table} SET "K"=12,"V"=0 WHERE "K"=11',2290,'23514'),
            ('secondary_unique_error',f'UPDATE {table} SET "K"=12,"V"=20 WHERE "K"=11',1,'23505'),
            ('secondary_key_swap',f'UPDATE {table} SET "K"="K","V"=21-"V"',1,'23505'),
            ('key_identity',f'UPDATE {table} SET "K"="K" WHERE "K"=11',None,None),
            ('no_match',f'UPDATE {table} SET "K"=44 WHERE "K"=999',None,None),
            ('defaults',f'UPDATE {table} SET "K"=DEFAULT,"V"=DEFAULT WHERE "K"=11',None,None),
            ('composite_move',f'UPDATE {composite} SET "A"="A"+2.5,"B"=\'y\',"V"="V"+1',None,None),
            ('foreign_key_error',f'UPDATE {child} SET "K"=99,"P"=999 WHERE "K"=10',2291,'23503'),
        ]
        for case,sql,oracle_code,pg_code in steps:
            statement_swap=case in ('immediate_key_swap','secondary_key_swap') and kind=='oracle'
            compare(case,sql,None if statement_swap else oracle_code,None if statement_swap else pg_code)
            compare(case+'_state',f'SELECT "A","B","V" FROM {composite}' if case=='composite_move' else f'SELECT "K","P" FROM {child}' if case=='foreign_key_error' else read)
            if statement_swap:
                compare(case+'_restore',sql)
                compare(case+'_restored_state',read)
        row=dict(id='bound_key_and_numeric_values')
        parameters=(29,Decimal('99999999999999999999.654321'),9)
        for label,connection,placeholders in [('reference',reference,(':1',':2',':3') if kind=='oracle' else ('%s','%s','%s')),('bicdb',target,('%s','%s','%s'))]:
            with connection.cursor() as cursor:
                cursor.execute(f'UPDATE {table} SET "K"={placeholders[0]},"N"={placeholders[1]} WHERE "K"={placeholders[2]}',parameters)
                row[label]=dict(affected=cursor.rowcount)
        row['matched']=row['reference']==row['bicdb']==dict(affected=1);report['cases'].append(row)
        compare('bound_values_state',read)
        for connection in (reference,target):connection.autocommit=False
        compare('transaction_key_move',f'UPDATE {table} SET "K"=19 WHERE "K"=29')
        compare('transaction_read',read)
        for connection in (reference,target):connection.rollback();connection.autocommit=True
        compare('rollback_restored',read)
        for connection in (reference,target):connection.autocommit=False
        for connection in (reference,target):
            execute(connection,f'INSERT INTO {child} VALUES(20,1)');execute(connection,'SAVEPOINT key_move_mark')
        compare('transaction_reference_error',f'UPDATE {child} SET "K"=99,"P"=999 WHERE "K"=10',2291,'23503')
        for connection in (reference,target):execute(connection,'ROLLBACK TO SAVEPOINT key_move_mark')
        compare('prior_transaction_write_preserved',f'SELECT "K","P" FROM {child} ORDER BY "K"')
        for connection in (reference,target):connection.rollback();connection.autocommit=True
        compare('prior_write_rolled_back',f'SELECT "K","P" FROM {child} ORDER BY "K"')
    finally:
        for connection in (reference,target):connection.rollback();connection.autocommit=True
        for backend,name in reversed(created):
            try:execute((reference,target)[backend],f'DROP TABLE {name}')
            except Exception as error:report['cleanup_errors'].append(dict(backend=backend,error=type(error).__name__,code=getattr(error,'sqlstate',None)))
    report['matched']=sum(row['matched'] for row in report['cases']);report['total']=len(report['cases'])
    report['success']=report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse,os,oracledb,psycopg
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--oracle-config',required=True,type=Path);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    config=json.loads(args.oracle_config.read_text());password=Path(config['password_file']);password=password if password.is_absolute() else args.oracle_config.parent/password
    if config['user'].upper() in ('SYS','SYSTEM'):raise ValueError('a dedicated non-administrative schema is required')
    oracledb.defaults.fetch_decimals=True
    with oracledb.connect(user=config['user'],password=password.read_text().strip(),host=config['host'],port=config['port'],service_name=config['service_name']) as reference:
        reference.autocommit=True;reference.call_timeout=10000
        with reference.cursor() as cursor:cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL");schema=cursor.fetchone()[0]
        with psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True,options='-c bicdb.sql_profile=oracle-preview-v1') as target:
            report=run(reference,target,schema,'BICDB_K_'+uuid.uuid4().hex[:10].upper())
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ['success','matched','total']}));return 0 if report['success'] else 1

if __name__=='__main__':raise SystemExit(main())
