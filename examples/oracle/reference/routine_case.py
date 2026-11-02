"""Independent procedural CASE expectations for generated Oracle/PG comparisons."""
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch('[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    owner = identifier(schema)
    q = lambda suffix: owner + '.' + identifier(prefix + '_' + suffix)
    definitions = [
        ('SEARCH', '(p_value NUMBER) RETURN NUMBER IS result NUMBER:=5; BEGIN CASE WHEN p_value<0 THEN result:=10; result:=result+1; WHEN p_value=0 THEN result:=20; ELSE result:=30; END CASE; RETURN result; END;'),
        ('LAZY', '(p_value NUMBER) RETURN NUMBER IS BEGIN CASE WHEN p_value=1 THEN RETURN 10; WHEN 1/p_value>0 THEN RETURN 20; ELSE RETURN 30; END CASE; END;'),
        ('NOELSE', '(p_value NUMBER) RETURN NUMBER IS BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; END;'),
        ('HANDLED', '(p_value NUMBER) RETURN NUMBER IS BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; EXCEPTION WHEN CASE_NOT_FOUND THEN RETURN 99; END;'),
        ('NESTED', '(p_value NUMBER) RETURN NUMBER IS result NUMBER:=0; BEGIN IF p_value IS NOT NULL THEN FOR n IN 1..3 LOOP CASE WHEN n=2 THEN CASE WHEN p_value>0 THEN result:=result+10; ELSE result:=result+20; END CASE; ELSE IF p_value=0 THEN result:=result+100; ELSE result:=result+1; END IF; END CASE; END LOOP; ELSE result:=30; END IF; RETURN result; END;'),
        ('EXPR', '(p_value NUMBER) RETURN NUMBER IS result NUMBER:=0; BEGIN CASE WHEN (CASE WHEN p_value<0 THEN 1 ELSE 2 END)=1 THEN result:=CASE WHEN p_value=-1 THEN 10 ELSE 20 END; WHEN p_value=0 THEN SELECT CASE WHEN p_value=0 THEN 30 ELSE 40 END INTO result FROM DUAL; ELSE result:=50; END CASE; RETURN result; END;'),
        ('SIMPLE', '(p_value NUMBER) RETURN NUMBER IS BEGIN CASE p_value WHEN 1 THEN RETURN 10; WHEN 2 THEN RETURN 20; WHEN NULL THEN RETURN 30; ELSE RETURN 40; END CASE; END;'),
        ('ONCE', ' RETURN NUMBER IS BEGIN CASE ' + q('SEQ') + '.NEXTVAL WHEN 0 THEN RETURN 0; WHEN 1 THEN RETURN 10; ELSE RETURN -1; END CASE; END;'),
    ]
    definitions.extend([
        ('H_BODY', '(p_value NUMBER) RETURN NUMBER IS BEGIN CASE WHEN 1=0 THEN RETURN 0; END CASE; RETURN 0; EXCEPTION WHEN CASE_NOT_FOUND THEN CASE WHEN p_value=1 THEN RETURN 41; ELSE RETURN 42; END CASE; WHEN OTHERS THEN RETURN -1; END;'),
        ('SQLNULL', '(p_value NUMBER) RETURN NUMBER IS BEGIN RETURN CASE WHEN p_value=1 THEN 11 END; END;'),
    ])
    cases = []

    def case(kind, value, expected=None, error=None):
        row = dict(id=kind.lower() + '_' + str(value).replace('-', 'neg'), sql=f'SELECT {q(kind)}({value}) FROM DUAL' if value != '' else f'SELECT {q(kind)}() FROM DUAL')
        if error:
            row['expected_error'] = error
        else:
            row['expected'] = [[None if expected is None else dict(number=str(expected))]]
        cases.append(row)
    for v, e in [('-1', 11), ('0', 20), ('1', 30), ('NULL', 30)]:
        case('SEARCH', v, e)
    for v, e in [('1', 10), ('2', 20), ('-1', 30), ('NULL', 30)]:
        case('LAZY', v, e)
    case('LAZY', '0', error='ORA-01476')
    case('NOELSE', '1', 10)
    for v in ['0', 'NULL']:
        case('NOELSE', v, error='ORA-06592')
    for v, e in [('1', 10), ('0', 99), ('NULL', 99)]:
        case('HANDLED', v, e)
    for v, e in [('1', 12), ('-1', 22), ('0', 220), ('NULL', 30)]:
        case('NESTED', v, e)
    for v, e in [('-1', 10), ('-2', 20), ('0', 30), ('1', 50), ('NULL', 50)]:
        case('EXPR', v, e)
    for v, e in [('1', 10), ('2', 20), ('3', 40), ('NULL', 40)]:
        case('SIMPLE', v, e)
    case('ONCE', '', 10)
    for value, expected in [('1', 41), ('2', 42)]:
        case('H_BODY', value, expected)
    for value, expected in [('1', 11), ('0', None), ('NULL', None)]:
        case('SQLNULL', value, expected)
    setup = [dict(backend=backend, kind='SEQUENCE', name=q('SEQ'), sql='CREATE SEQUENCE ' + q('SEQ') + ' START WITH 1', inserts=[]) for backend in (0, 1)]
    for suffix, body in definitions:
        setup.extend((dict(backend=backend, kind='FUNCTION', name=q(suffix), sql='CREATE FUNCTION ' + q(suffix) + body, inserts=[]) for backend in (0, 1)))
    for row in cases:
        if 'expected_error' in row:
            row['expected_error'] = dict(oracle=row['expected_error'], bicdb={'ORA-01476': '22012', 'ORA-06592': '20000'}[row['expected_error']])
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_SEARCH').lower(), scope='Simple and searched procedural CASE, lazy conditions, nested statements, errors and selector evaluated once; not labels, general coercion or full PL/SQL')
