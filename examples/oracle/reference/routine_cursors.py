"""Independent bound-cursor expectations for Oracle and native PostgreSQL."""
import re
from routine_namespaces import identifier


def fixture(schema, prefix, native=False):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    num = 'NUMERIC' if native else 'NUMBER'
    text = 'VARCHAR' if native else 'VARCHAR2'
    dual = '' if native else ' FROM DUAL'
    def cursor(parameters, query, name='c'):
        if native:
            return f'{name} CURSOR ({parameters}) FOR {query};'
        return f'CURSOR {name}({parameters}) IS {query};'
    definitions = []
    cases = []
    def add(suffix, declarations, statements, checks):
        if native:
            sql = f'CREATE FUNCTION {q(suffix)}(p_value NUMERIC) RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE {declarations} BEGIN {statements} END$$'
        else:
            sql = f'CREATE FUNCTION {q(suffix)}(p_value NUMBER) RETURN NUMBER IS {declarations} BEGIN {statements} END;'
        definitions.append((suffix, sql))
        for index, (value, expected) in enumerate(checks):
            cases.append(dict(id=suffix.lower() + '_' + str(index),
                              sql=f'SELECT {q(suffix)}({value}){dual}',
                              expected=[[None if expected is None else dict(number=str(expected))]]))
    checks = [('1', 470), ('5', 870), ('NULL', None)]
    for suffix, arguments in [('POSITIONAL', 'p_value,3'),
                              ('NAMED', 'v_extra=>3,v_arg=>p_value'),
                              ('MIXED', 'p_value,v_extra=>3')]:
        add(suffix, f'v_arg {num}:=70; total {num}:=0;' + cursor(f'v_arg {num},v_extra {num}', 'SELECT v_arg+v_extra AS amount' + dual),
            f'FOR r IN c({arguments}) LOOP total:=total+r.amount; END LOOP; RETURN total*100+v_arg;', checks)
    add('ACTUAL_SCOPE', f'v_arg {num}:=70; total {num}:=0;' + cursor(f'v_arg {num},v_extra {num}', 'SELECT v_arg+v_extra AS amount' + dual),
        'FOR r IN c(v_arg+1,v_arg+2) LOOP total:=r.amount; END LOOP; RETURN total*100+v_arg;', [('1', 14370)])
    add('REOPEN', f'v {num}; total {num};' + cursor(f'x {num}', 'SELECT x AS amount' + dual),
        'OPEN c(p_value); FETCH c INTO v; CLOSE c; total:=v; OPEN c(p_value+1); FETCH c INTO v; CLOSE c; RETURN total*100+v;', [('1', 102), ('3', 304)])
    add('SNAPSHOT', f'v_arg {num}:=p_value; v {num};' + cursor(f'v_arg {num}', 'SELECT v_arg AS amount' + dual),
        'OPEN c(v_arg); v_arg:=v_arg+10; FETCH c INTO v; CLOSE c; RETURN v*100+v_arg;', [('1', 111), ('3', 313)])
    add('COALESCE_REOPEN', f'current_day DATE:=DATE \'2002-07-03\'; total {num}:=0; chosen {text}(20):=\'text\';' +
        cursor(f'x {num},pp_date DATE', f"SELECT amount FROM {q('K')} WHERE category=x AND day>=pp_date AND COALESCE(amount,0)>=0 AND tag<>'excluded' ORDER BY day"),
        f"FOR i IN 1..3 LOOP FOR r IN c(p_value,current_day) LOOP SELECT COALESCE(chosen,'fallback') INTO chosen{dual}; total:=total+COALESCE(r.amount,0); END LOOP; current_day:=current_day+1; END LOOP; RETURN total;",
        [('1', 21), ('2', 6)])
    add('BODY_SCOPE', f'v_arg {text}(20):=\'outside\'; total {num}:=0;' + cursor(f'v_arg {num}', 'SELECT v_arg AS amount' + dual),
        "FOR r IN c(p_value) LOOP IF v_arg<>'outside' THEN RETURN -999; END IF; total:=r.amount; END LOOP; RETURN total;", [('1', 1), ('3', 3)])
    add('QUALIFIED', cursor(f'x {num}', 'SELECT c.x AS amount' + dual),
        'FOR r IN c(p_value) LOOP RETURN r.amount; END LOOP; RETURN -999;', [('1', 1), ('3', 3)])
    add('COLUMN', f'total {num}:=0;' + cursor(f'v_arg {num}', f'SELECT SUM(amount) AS amount FROM {q("T")} WHERE v_arg=1'),
        'FOR r IN c(p_value) LOOP total:=r.amount; END LOOP; RETURN total;', [('1', 12), ('3', 12)])
    add('NESTED', f'total {num}:=0;' + cursor(f'x {num}', 'SELECT x AS amount' + dual),
        'FOR parent IN c(p_value) LOOP DECLARE ' + cursor(f'x {num}', 'SELECT x+1 AS amount' + dual) +
        ' BEGIN FOR child IN c(parent.amount) LOOP total:=child.amount; END LOOP; END; END LOOP; RETURN total;', [('1', 2), ('3', 4)])
    add('ERROR_SCOPE', f'x {text}(20):=\'safe\'; total {num};' + cursor(f'x {num}', 'SELECT 10/(x-x) AS amount' + dual),
        'BEGIN FOR r IN c(p_value) LOOP total:=r.amount; END LOOP; EXCEPTION WHEN OTHERS THEN NULL; END; RETURN LENGTH(x);', [('1', 4), ('3', 4)])
    add('PRIOR_OPEN', f'v {num};' + cursor(f'x {num}', 'SELECT x AS amount' + dual),
        'OPEN c(p_value); BEGIN FOR r IN c(99) LOOP NULL; END LOOP; EXCEPTION WHEN OTHERS THEN NULL; END; FETCH c INTO v; CLOSE c; RETURN v;', [('1', 1), ('3', 3)])
    sequence = f"nextval('{q('SEQ')}')" if native else q('SEQ') + '.NEXTVAL'
    add('ONCE', f'total {num};' + cursor(f'x {num}', 'SELECT x AS amount' + dual),
        'FOR r IN c(' + sequence + ') LOOP total:=r.amount; END LOOP; RETURN total;', [('1', 1), ('1', 2)])
    if not native:
        add('DEFAULT_SCOPE', 'chosen NUMBER:=70; total NUMBER;' + cursor('x NUMBER DEFAULT 2,y NUMBER DEFAULT chosen', 'SELECT x+y AS amount FROM DUAL'),
            'FOR r IN c LOOP total:=r.amount; END LOOP; RETURN total;', [('1', 72)])
        add('DEFAULT_OPEN', 'chosen NUMBER:=p_value; total NUMBER:=0;' + cursor('x NUMBER DEFAULT chosen', 'SELECT x AS amount FROM DUAL'),
            'chosen:=p_value+2; FOR r IN c LOOP total:=r.amount; END LOOP; chosen:=p_value+3; FOR r IN c() LOOP total:=total+r.amount; END LOOP; RETURN total;', [('1', 7), ('3', 11)])
        add('DEFAULT_SKIP', cursor('x NUMBER DEFAULT 1/0', 'SELECT x AS amount FROM DUAL'),
            'FOR r IN c(p_value) LOOP RETURN r.amount; END LOOP; RETURN -999;', [('1', 1), ('3', 3)])
        add('DATE', cursor('x DATE', 'SELECT x AS chosen FROM DUAL'),
            "FOR r IN c(TIMESTAMP '2024-02-29 12:34:56.999999') LOOP RETURN (r.chosen-TRUNC(r.chosen))*86400; END LOOP; RETURN -999;", [('1', 45296)])
    setup = []
    for backend in (0, 1):
        setup.append(dict(backend=backend, kind='TABLE', name=q('K'),
                          sql=f'CREATE TABLE {q("K")}(category {num},amount {num},day DATE,tag {text}(20))',
                          inserts=[f"INSERT INTO {q('K')} VALUES(1,NULL,DATE '2002-07-04','A')",
                                   f"INSERT INTO {q('K')} VALUES(1,7,DATE '2002-07-05','B')",
                                   f"INSERT INTO {q('K')} VALUES(2,3,DATE '2002-07-04','C')"]))
        setup.append(dict(backend=backend, kind='TABLE', name=q('T'),
                          sql=f'CREATE TABLE {q("T")}(v_arg {num},amount {num})',
                          inserts=[f'INSERT INTO {q("T")} VALUES(1,5)', f'INSERT INTO {q("T")} VALUES(1,7)', f'INSERT INTO {q("T")} VALUES(2,9)']))
        setup.append(dict(backend=backend, kind='SEQUENCE', name=q('SEQ'), sql=f'CREATE SEQUENCE {q("SEQ")} START WITH 1', inserts=[]))
        setup.extend(dict(backend=backend, kind='FUNCTION', name=q(suffix), sql=sql, inserts=[]) for suffix, sql in definitions)
    if native:
        for case in cases:
            if case['id'].startswith('column_'):
                case.pop('expected')
                case['expected_error'] = dict(postgres='42702', bicdb='42702')
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_POSITIONAL').lower(),
                scope='Bound parameterized cursor arguments, OPEN-time snapshots, query/outer scope, named notation, cleanup and Oracle defaults/logical DATE; not refcursors, scroll portals or full cursor attributes')
