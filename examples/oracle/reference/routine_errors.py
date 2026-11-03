"""Compare Oracle application error values, exception context and PG metadata."""
import json
from pathlib import Path
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema,prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}',prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema)+'.'+identifier(prefix+'_'+suffix)
    reference=json.loads(Path(__file__).with_name('application-error-reference.json').read_text())
    setup,cases=[],[]
    for index,row in enumerate(reference['cases']):
        name=q('F'+str(index))
        statement=row['statement']
        body=("BEGIN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM; END;" if statement is None else
              "BEGIN "+statement+" RETURN 'not raised'; EXCEPTION WHEN OTHERS THEN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM; END;")
        for backend in (0,1):
            setup.append(dict(backend=backend,kind='FUNCTION',name=name,inserts=[],
                              sql=f'CREATE FUNCTION {name} RETURN VARCHAR2 IS {body}'))
        cases.append(dict(id=row['id'],sql=f'SELECT {name}() FROM DUAL',expected=[[row['expected']]]))
    extra=[
        ('STATUS',"BEGIN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM;END;"),
        ('INHERIT',f"BEGIN RAISE_APPLICATION_ERROR(-20100,'handled');EXCEPTION WHEN OTHERS THEN RETURN {q('STATUS')}();END;"),
        ('RESTORE',"text_value VARCHAR2(100);BEGIN BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN text_value:=TO_CHAR(SQLCODE)||'|'||SQLERRM;END;RETURN text_value||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;END;"),
        ('SHADOW',"SQLCODE NUMBER:=42;BEGIN RETURN TO_CHAR(SQLCODE);END;"),
        ('QUOTED_LOW','"sqlcode" NUMBER:=99;BEGIN RETURN TO_CHAR(SQLCODE);END;'),
        ('COLLISION','"__bicdb_oracle_sqlcode_0" NUMBER:=99;BEGIN RETURN TO_CHAR(SQLCODE)||\'|\'||SQLERRM;END;'),
        ('INIT',"v NUMBER:=SQLCODE;BEGIN RETURN TO_CHAR(v)||'|'||SQLERRM;END;"),
        ('INHERIT_INIT',f"BEGIN RAISE_APPLICATION_ERROR(-20105,'init');EXCEPTION WHEN OTHERS THEN RETURN {q('INIT')}();END;"),
        ('CLEAR_CHILD',"BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RETURN 'done';END;"),
        ('DYNAMIC',f"BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN RETURN SQLERRM||'/'||{q('CLEAR_CHILD')}()||'/'||SQLERRM;END;"),
        ('DEFAULT_CODE','(p NUMBER DEFAULT SQLCODE) RETURN NUMBER IS BEGIN RETURN p;END;'),
        ('DEFAULT_CALL',f"BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN RETURN TO_CHAR({q('DEFAULT_CODE')}());END;"),
        ('QUERY',f"v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN SELECT {q('STATUS')}() INTO v FROM DUAL;RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;"),
        ('QUERY_CLEAR',f"v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN SELECT {q('CLEAR_CHILD')}() INTO v FROM DUAL;RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;"),
        ('RECEIVE',"(p VARCHAR2,r OUT VARCHAR2) IS BEGIN r:=p;END;"),
        ('CALL_ARG',f"v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN {q('RECEIVE')}(SQLERRM,v);RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;"),
    ]
    for backend in (0,1):
        for name,body in extra:
            kind='PROCEDURE' if name=='RECEIVE' else 'FUNCTION'
            header='' if name in ('DEFAULT_CODE','RECEIVE') else ' RETURN VARCHAR2 IS '
            setup.append(dict(backend=backend,kind=kind,name=q(name),inserts=[],sql=f'CREATE {kind} {q(name)}{header}{body}'))
        for name,body in [
            ('TRAILING',"BEGIN RAISE_APPLICATION_ERROR(-20112,'tail ');END;"),
            ('MULTILINE',"BEGIN RAISE_APPLICATION_ERROR(-20113,'a'||CHR(10)||'b');END;"),
            ('ERR',"BEGIN RAISE_APPLICATION_ERROR(-20109,'driver message');END;"),
            ('KEEP',"BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20102,'outer',TRUE);END;"),
            ('REPLACE',"BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20102,'outer',FALSE);END;"),
            ('NULL_KEEP',"BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20102,'outer',NULL);END;"),
            ('LONG',"BEGIN RAISE_APPLICATION_ERROR(-20110,RPAD('x',3000,'x'));END;"),
            ('UNICODE',"BEGIN RAISE_APPLICATION_ERROR(-20111,RPAD('é',1025,'é'));END;"),
        ]:
            setup.append(dict(backend=backend,kind='PROCEDURE',name=q(name),inserts=[],sql=f'CREATE PROCEDURE {q(name)} IS {body}'))
    for suffix,expected in [
        ('STATUS','0|ORA-0000: normal, successful completion'),
        ('INHERIT','-20100|ORA-20100: handled'),
        ('STATUS','0|ORA-0000: normal, successful completion'),
        ('RESTORE','-20101|ORA-20101: inner/-20100|ORA-20100: '),
        ('SHADOW','42'),('QUOTED_LOW','0'),('COLLISION','0|ORA-0000: normal, successful completion'),
        ('INHERIT_INIT','-20105|ORA-20105: init'),
        ('DYNAMIC','ORA-20100: outer/done/ORA-20100: '),
        ('DEFAULT_CALL','-20100'),
        ('QUERY','0|ORA-0000: normal, successful completion/-20100|ORA-20100: outer'),
        ('QUERY_CLEAR','done/-20100|ORA-20100: outer'),
        ('CALL_ARG','ORA-20100: outer/-20100|ORA-20100: outer'),
    ]:
        cases.append(dict(id='context_'+str(len(cases)),sql=f'SELECT {q(suffix)}() FROM DUAL',expected=[[expected]]))
    buffer=json.loads(Path(__file__).with_name('application-error-buffer-reference.json').read_text())
    for index,row in enumerate(buffer['cases']):
        kind='PROCEDURE' if row['unhandled'] else 'FUNCTION'
        name=q('BUFFER'+str(index))
        for backend in (0,1):
            returns='' if kind=='PROCEDURE' else ' RETURN VARCHAR2'
            setup.append(dict(backend=backend,kind=kind,name=name,inserts=[],sql=f"CREATE {kind} {name}{returns} IS v NUMBER;t VARCHAR2(4000);{row['body']}"))
        if not row['unhandled']:
            cases.append(dict(id='buffer_'+row['id'],sql=f'SELECT {name}() FROM DUAL',expected=[[row['expected']]]))
    return dict(setup=setup,cases=cases,compiler_probe=(prefix+'_F0').lower(),
                scope='Fixed-preview RAISE_APPLICATION_ERROR codes, rounding, ranges, selected argument conversions, message byte boundaries, current SQLCODE/SQLERRM and scoped nested call/handler context. PG error metadata records logical Oracle codes/messages; full diagnostic catalog and Oracle call-site backtraces are unqualified.')


def compare_bindings(oracle,bicdb,schema,prefix):
    rows=[]
    calls=[(suffix,'driver_'+suffix.lower(),'P0001',None) for suffix in ['ERR','KEEP','REPLACE','NULL_KEEP','LONG','UNICODE','TRAILING','MULTILINE']]
    buffer=json.loads(Path(__file__).with_name('application-error-buffer-reference.json').read_text())
    calls.extend(('BUFFER'+str(i),'driver_buffer_'+row['id'],row['pg_sqlstate'],row['expected_stack']) for i,row in enumerate(buffer['cases']) if row['unhandled'])
    for suffix,case_id,sqlstate,expected_stack in calls:
        name=identifier(schema)+'.'+identifier(prefix+'_'+suffix)
        row=dict(id=case_id)
        try:
            with oracle.cursor() as cursor:cursor.callproc(name,[])
            row['oracle']=dict(raised=False)
        except Exception as error:
            detail=error.args[0]
            stack=[]
            for code,message in re.findall(r'^ORA-(\d{5}): ([\s\S]*?)(?=\nORA-\d{5}:|\nHelp:|\Z)',detail.message,re.MULTILINE):
                if code=='06512':continue
                stack.append(dict(code=-int(code),message='ORA-'+code+': '+message))
            row['oracle']=dict(stack=stack)
        try:
            with bicdb.cursor() as cursor:cursor.execute(f'CALL {name}()',prepare=False)
            row['bicdb']=dict(raised=False)
        except Exception as error:
            detail=json.loads(error.diag.message_detail)
            row['bicdb']=dict(stack=detail['oracle_error_stack'])
            row['sqlstate']=error.sqlstate
            row['message_primary']=error.diag.message_primary
        stack=row['oracle'].get('stack',[])
        row['matched']=(row['oracle']==row['bicdb'] and row.get('sqlstate')==sqlstate
                        and bool(stack) and row.get('message_primary')==stack[0]['message'])
        if expected_stack is not None:
            row['expected_stack']=expected_stack
            row['matched'] &= stack==expected_stack
        rows.append(row)
    return rows
