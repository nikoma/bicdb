"""Oracle procedure execution, replacement, typed calls and exception state."""
import decimal
import datetime
import json
from pathlib import Path
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    table = q('ROWS')
    setup, cases = [], []
    definitions = [
        ('PROCEDURE','ADD','(p_delta NUMBER,p_day DATE)', '',
         f'BEGIN UPDATE {table} SET n=n+NVL(p_delta,0),d=p_day WHERE id=1; END;'),
        ('PROCEDURE','ROUTE','(p_delta NUMBER)', '',
         f"BEGIN IF p_delta<0 THEN {q('ADD')}(-p_delta,DATE '2024-03-01'); ELSE FOR i IN 1..p_delta LOOP {q('ADD')}(1,DATE '2024-03-02'); END LOOP; END IF; END;"),
        ('PROCEDURE','MIX','(p_value IN NUMBER,p_result OUT NUMBER,p_total IN OUT NUMBER)', '',
         'BEGIN p_result:=2*p_value;p_total:=NVL(p_total,0)+NVL(p_result,0); END;'),
        ('PROCEDURE','DATEVALUE','(p_value IN DATE,p_result OUT DATE)', '',
         'BEGIN p_result:=p_value; END;'),
        ('FUNCTION','WRAP','(p_value NUMBER)', ' RETURN NUMBER',
         f'v NUMBER:=999;total NUMBER:=4; BEGIN {q("MIX")}(p_value,v,total); RETURN NVL(v,0)+100*total; END;'),
        ('PROCEDURE','DEFAULT','(p_value NUMBER DEFAULT CAST(4 AS NUMBER))', '',
         f'BEGIN UPDATE {table} SET n=n+p_value WHERE id=1; END;'),
        ('PROCEDURE','EMPTY','', '', f'BEGIN UPDATE {table} SET n=n+7 WHERE id=1; END;'),
        ('PROCEDURE','FAIL','(p_value NUMBER,p_text VARCHAR2)', '',
         f"v NUMBER; BEGIN UPDATE {table} SET n=n+100 WHERE id=1;v:=NVL(p_value,p_text); UPDATE {table} SET n=v WHERE id=1; END;"),
        ('PROCEDURE','HANDLED','(p_value NUMBER,p_text VARCHAR2)', '',
         f"v NUMBER; BEGIN UPDATE {table} SET n=n+1 WHERE id=1; BEGIN UPDATE {table} SET n=n+2 WHERE id=1;v:=NVL(p_value,p_text); UPDATE {table} SET n=v WHERE id=1; EXCEPTION WHEN OTHERS THEN NULL; END; END;"),
    ]
    for backend in (0, 1):
        setup.append(dict(backend=backend,kind='TABLE',name=table,
                          sql=f'CREATE TABLE {table}(id NUMBER PRIMARY KEY,n NUMBER,d DATE)',
                          inserts=[f'INSERT INTO {table} VALUES(1,0,NULL)']))
        for kind, suffix, args, returns, body in definitions:
            setup.append(dict(backend=backend, kind=kind, name=q(suffix), inserts=[],
                              sql=f'CREATE OR REPLACE {kind} {q(suffix)}{args}{returns} IS {body}'))
    statements = [
        ('add', f"CALL {q('ADD')}(3,TIMESTAMP '2024-02-29 12:34:56.999999')"),
        ('after_add', f'SELECT n,d FROM {table} WHERE id=1'),
        ('route_loop', f"CALL {q('ROUTE')}(2)"),
        ('after_loop', f'SELECT n,d FROM {table} WHERE id=1'),
        ('route_then', f"CALL {q('ROUTE')}(-3)"),
        ('after_then', f'SELECT n,d FROM {table} WHERE id=1'),
        ('default_omitted', f"CALL {q('DEFAULT')}()"),
        ('default_explicit', f"CALL {q('DEFAULT')}(2)"),
        ('parameterless', f"CALL {q('EMPTY')}()"),
        ('after_defaults', f'SELECT n FROM {table} WHERE id=1'),
        ('unhandled_call', f"CALL {q('FAIL')}(1,'bad')"),
        ('unhandled_rolled_back', f'SELECT n FROM {table} WHERE id=1'),
        ('handled_call', f"CALL {q('HANDLED')}(1,'bad')"),
        ('handled_statements_preserved', f'SELECT n FROM {table} WHERE id=1'),
    ]
    for value in ['3','NULL','-2','3']:
        statements.append(('copyback_'+str(len(statements)), f"SELECT {q('WRAP')}({value}) FROM DUAL"))
    # Replacement runs after the earlier call has already been compiled/cached.
    replacement = f"CREATE OR REPLACE PROCEDURE {q('DEFAULT')}(p_value NUMBER DEFAULT 4) IS BEGIN UPDATE {table} SET n=n+2*p_value WHERE id=1; END;"
    statements += [('replace', replacement),('replaced_call',f"CALL {q('DEFAULT')}(3)"),
                   ('after_replace',f'SELECT n FROM {table} WHERE id=1')]
    source_path = Path(__file__).with_name('procedure-reference.json')
    source = json.loads(source_path.read_text()) if source_path.exists() else {}
    for name, sql in statements:
        case = dict(id=name,sql=sql)
        case.update(source.get('expectations',{}).get(name,{}))
        cases.append(case)
    return dict(setup=setup,cases=cases,compiler_probe=(prefix+'_ADD').lower(),
                scope='Fixed-preview Oracle procedures: actual CALL effects, DATE inputs, nested qualified calls, defaults, replacement, OUT/IN OUT copyback and statement exception behavior. Not packages, autonomous/transaction control, dynamic SQL or full diagnostics.')


def compare_bindings(oracle, bicdb, schema, prefix):
    """Oracle output binds and PostgreSQL output rows share logical values."""
    import oracledb
    q = lambda suffix: identifier(schema)+'.'+identifier(prefix+'_'+suffix)
    cases = []
    for value, initial in [(3,4),(None,4),(-2,None)]:
        row = dict(id='bound_outputs_'+str(len(cases)))
        with oracle.cursor() as source:
            # Text output binds avoid a binary float intermediary for NUMBER.
            output = source.var(oracledb.DB_TYPE_VARCHAR,size=4000)
            total = source.var(oracledb.DB_TYPE_VARCHAR,size=4000)
            total.setvalue(0,None if initial is None else str(initial))
            source.callproc(q('MIX'),[value,output,total])
            values = [None if v is None else decimal.Decimal(v)
                      for v in [output.getvalue(),total.getvalue()]]
            row['oracle'] = dict(rows=[[canonical(v) for v in values]])
        with bicdb.cursor() as target:
            target.execute(f'CALL {q("MIX")}(%s,NULL,%s)',[value,initial])
            row['bicdb'] = dict(rows=[[canonical(v) for v in values] for values in target.fetchall()])
            row['type_oids'] = [column.type_code for column in target.description]
        doubled = None if value is None else value*2
        total = (initial or 0)+(doubled or 0)
        row['expected'] = [[canonical(doubled),canonical(total)]]
        row['matched'] = (row['oracle'].get('rows') == row['bicdb'].get('rows') == row['expected']
                          and row['type_oids']==[1700,1700])
        cases.append(row)
    for value in [datetime.datetime(2024,2,29,12,34,56,999999),None]:
        row = dict(id='bound_date_'+str(len(cases)))
        with oracle.cursor() as source:
            actual = source.var(oracledb.DB_TYPE_TIMESTAMP)
            actual.setvalue(0,value)
            output = source.var(oracledb.DB_TYPE_DATE)
            source.callproc(q('DATEVALUE'),[actual,output])
            row['oracle'] = dict(rows=[[canonical(output.getvalue())]])
        with bicdb.cursor() as target:
            target.execute(f'CALL {q("DATEVALUE")}(%s,NULL)',[value])
            row['bicdb'] = dict(rows=[[canonical(v) for v in values] for values in target.fetchall()])
            row['type_oids'] = [column.type_code for column in target.description]
        row['expected'] = [[canonical(value.replace(microsecond=0) if value else None)]]
        row['matched'] = (row['oracle']['rows']==row['bicdb']['rows']==row['expected']
                          and row['type_oids']==[1114])
        cases.append(row)
    return cases
