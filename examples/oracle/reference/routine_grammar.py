"""Conjunctive Oracle outer joins, cursor loops and column lock targets."""
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    parent, child, detail = (q(suffix) for suffix in ('P', 'C', 'D'))
    setup = []
    tables = [
        (parent, 'k NUMBER PRIMARY KEY,value NUMBER', ['1,10', '2,20', '3,30']),
        (child, 'parent_k NUMBER,v NUMBER', ['1,100', '1,101', '2,200']),
        (detail, 'child_v NUMBER,label VARCHAR2(10)', ["100,'one'"]),
    ]
    bodies = [
        ('CURSOR', f'''total NUMBER:=0;
          CURSOR rows_to_read IS SELECT p.value pv,c.v cv FROM {parent} p,{child} c
            WHERE p.k=c.parent_k(+);
          BEGIN For row_value IN rows_to_read LOOP
            total:=total+row_value.pv+NVL(row_value.cv,0);
          EnD
          LoOp; RETURN total; END;'''),
        ('LOCKS', f'''a NUMBER;b NUMBER;
          BEGIN SELECT k,value INTO a,b FROM {parent} WHERE k=p_key
            FOR UPDATE OF k,value;
          RETURN a+b; END;'''),
        ('LOCKNOWAIT', f'''a NUMBER;b NUMBER;
          BEGIN SELECT p.k,p.value INTO a,b FROM {parent} p WHERE p.k=p_key
            FOR UPDATE OF p.k,p.value NOWAIT;
          RETURN a+b; END;'''),
    ]
    for backend in (0, 1):
        for table, columns, rows in tables:
            setup.append(dict(backend=backend,kind='TABLE',name=table,
                              sql=f'CREATE TABLE {table}({columns})',
                              inserts=[f'INSERT INTO {table} VALUES({row})' for row in rows]))
        for suffix, body in bodies:
            if suffix == 'CURSOR':
                kind, declaration = 'FUNCTION', '(p_key NUMBER) RETURN NUMBER'
            else:
                kind, declaration = 'PROCEDURE', '(p_key NUMBER,p_result OUT NUMBER)'
                body = body.replace('RETURN a+b;', 'p_result:=a+b;')
            setup.append(dict(backend=backend,kind=kind,name=q(suffix),inserts=[],
                              sql=f'CREATE {kind} {q(suffix)}{declaration} IS {body}'))
    specs = [
        ('null_extension', f'SELECT p.k,c.v FROM {parent} p,{child} c WHERE p.k=c.parent_k(+) ORDER BY p.k,c.v', [[1,100],[1,101],[2,200],[3,None]]),
        ('optional_filter', f'SELECT p.k,c.v FROM {parent} p,{child} c WHERE p.k=c.parent_k(+) AND c.v(+)=100 ORDER BY p.k', [[1,100],[2,None],[3,None]]),
        ('preserved_filter', f'SELECT p.k,c.v FROM {parent} p,{child} c WHERE p.k=c.parent_k(+) AND c.v=100 ORDER BY p.k', [[1,100]]),
        ('unqualified', f'SELECT k,v FROM {parent},{child} WHERE k=parent_k(+) ORDER BY k,v', [[1,100],[1,101],[2,200],[3,None]]),
        ('reverse_chain', f'SELECT p.k,d.label FROM {detail} d,{child} c,{parent} p WHERE p.k=c.parent_k(+) AND c.v=d.child_v(+) ORDER BY p.k,c.v', [[1,'one'],[1,None],[2,None],[3,None]]),
        ('star_order', f'SELECT * FROM {child} c,{parent} p WHERE p.k=c.parent_k(+) AND p.k=3', [[None,None,3,30]]),
        ('keyword_column', f'SELECT p.value,c.v FROM {parent} p,{child} c WHERE p.value=c.v(+) ORDER BY p.value', [[10,None],[20,None],[30,None]]),
        ('cursor_loop', f'SELECT {q("CURSOR")}(1) FROM DUAL', [[471]]),
    ]
    cases = [dict(id=name,sql=sql,expected=[[canonical(v) for v in row] for row in rows])
             for name,sql,rows in specs]
    return dict(setup=setup,cases=cases,compiler_probe=(prefix+'_CURSOR').lower(),
                scope='Conjunctive legacy outer joins including null-extension, filter placement, chained dependencies and projection order; mixed-case cursor loops and uncontended column lock lists. Not general Oracle join grammar or concurrent locking qualification.')


def compare_bindings(oracle, bicdb, schema, prefix):
    import decimal
    import oracledb
    cases = []
    for suffix in ('LOCKS', 'LOCKNOWAIT'):
        name = identifier(schema)+'.'+identifier(prefix+'_'+suffix)
        for key in (1, 2, 3):
            row = dict(id=suffix.lower()+str(key))
            with oracle.cursor() as source:
                output = source.var(oracledb.DB_TYPE_VARCHAR,size=4000)
                source.callproc(name,[key,output])
                row['oracle'] = dict(rows=[[canonical(decimal.Decimal(output.getvalue()))]])
            with bicdb.cursor() as target:
                target.execute(f'CALL {name}(%s,NULL)',[key])
                row['bicdb'] = dict(rows=[[canonical(v) for v in values] for values in target.fetchall()])
                row['type_oids'] = [column.type_code for column in target.description]
            row['expected'] = [[canonical(key*11)]]
            row['matched'] = (row['oracle']['rows']==row['bicdb']['rows']==row['expected']
                              and row['type_oids']==[1700])
            cases.append(row)
    return cases
