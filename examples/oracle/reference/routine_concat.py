"""Independent Oracle routine concatenation expectations under fixed preview NLS."""
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch('[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    definitions = [
        ('TEXT', '(p_left VARCHAR2,p_right VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN p_left||p_right; END;'),
        ('LOCAL', '(p_left VARCHAR2,p_right VARCHAR2) RETURN VARCHAR2 IS result VARCHAR2(32767):=p_left||p_right; BEGIN IF result IS NULL THEN RETURN NULL; END IF; FOR n IN 1..2 LOOP result:=result||\':\'; END LOOP; RETURN result; END;'),
        ('SCOPED', '(p_left VARCHAR2,p_right VARCHAR2) RETURN VARCHAR2 IS result VARCHAR2(32767); BEGIN DECLARE p_left VARCHAR2(10):=\'inside\'; BEGIN result:=p_left||p_right; END; RETURN result||\':\'||p_left; END;'),
        ('QUERY', '(p_left VARCHAR2,p_right VARCHAR2) RETURN VARCHAR2 IS result VARCHAR2(32767); BEGIN SELECT p_left||p_right INTO result FROM DUAL; RETURN result; END;'),
        ('PAD', ' RETURN VARCHAR2 IS p_left CHAR(3):=\'x\'; p_right CHAR(3):=\'y\'; BEGIN RETURN p_left||p_right; END;'),
        ('NUMBER', '(p_value NUMBER) RETURN VARCHAR2 IS BEGIN RETURN \'[\'||p_value||\']\'; END;'),
        ('NUMSQL', '(p_value NUMBER) RETURN VARCHAR2 IS result VARCHAR2(32767); BEGIN SELECT \'[\'||p_value||\']\' INTO result FROM DUAL; RETURN result; END;'),
        ('DATE', '(p_value DATE) RETURN VARCHAR2 IS BEGIN RETURN \'[\'||p_value||\']\'; END;'),
        ('ONCE', ' RETURN VARCHAR2 IS BEGIN RETURN \'[\'||' + q('SEQ') + '.NEXTVAL||\']\'; END;'),
    ]
    definitions.extend([
        ('LEN', '(p_left VARCHAR2,p_right VARCHAR2) RETURN NUMBER IS BEGIN RETURN LENGTH(' + q('TEXT') + '(p_left,p_right)); END;'),
        ('NESTSQL', '(p_left VARCHAR2,p_right VARCHAR2) RETURN NUMBER IS result VARCHAR2(32767); BEGIN SELECT ' + q('TEXT') + '(p_left,p_right) INTO result FROM DUAL; RETURN LENGTH(result); END;'),
        ('OVER', '(p_left NUMBER,p_right NUMBER) RETURN NUMBER IS result VARCHAR2(32767); BEGIN result:=RPAD(\'x\',p_left,\'x\')||RPAD(\'y\',p_right,\'y\'); RETURN LENGTH(result); END;'),
    ])
    definitions.extend([
        ('TOCHAR', '(p_value NUMBER) RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(p_value); END;'),
        ('TOSQL', '(p_value NUMBER) RETURN VARCHAR2 IS result VARCHAR2(32767); BEGIN SELECT TO_CHAR(p_value) INTO result FROM DUAL; RETURN result; END;'),
        ('TODATE', '(p_value DATE) RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(p_value); END;'),
    ])
    cases = []
    def case(name, args, expected=None, error=None):
        row = dict(id=name.lower() + '_' + str(len(cases)), sql='SELECT ' + q(name) + '(' + args + ') FROM DUAL')
        if error:
            row['expected_error'] = dict(oracle=error, bicdb='22001')
        else:
            row['expected'] = [[expected]]
        cases.append(row)
    for name in ('TEXT', 'QUERY'):
        for args, expected in [("'a','b'", 'ab'), ("NULL,'x'", 'x'), ("'x',NULL", 'x'), ('NULL,NULL', None), ("'',''", None), ("'','x'", 'x'), ("'a  ',' b '", 'a   b '), ("'雪','🙂'", '雪🙂')]:
            case(name, args, expected)
    case('LOCAL', "'a','b'", 'ab::')
    case('LOCAL', 'NULL,NULL', None)
    case('SCOPED', "'outside','!َ'", 'inside!َ:outside')
    case('PAD', '', 'x  y  ')
    for value, expected in [('NULL', '[]'), ('0', '[0]'), ('0.5', '[.5]'), ('-0.5', '[-.5]'), ('12.500', '[12.5]'), ('1e-8', '[.00000001]'), ('123456789012345678901234567890', '[123456789012345678901234567890]')]:
        case('NUMBER', value, expected)
        case('NUMSQL', value, expected)
    for value in ['1e39', '-1e39', '1e40', '-1e40', '1e100', '-1e100', '1e-100', '-1e-100']:
        negative = value.startswith('-')
        exponent = int(value.split('e')[1])
        sign = '-' if negative else ''
        fixed = (sign + '1' + '0' * exponent if exponent >= 0
                 else sign + '.' + '0' * (-exponent - 1) + '1')
        for name, width in [('NUMBER', 100), ('NUMSQL', 40)]:
            if len(fixed) <= width:
                expected = fixed
            else:
                digits = max(2, len(str(abs(exponent))))
                fractional = width - int(negative) - 4 - digits
                expected = sign + '1.' + '0' * fractional + 'E' + ('-' if exponent < 0 else '+') + str(abs(exponent)).zfill(2)
            case(name, value, '[' + expected + ']')
    for value, expected in [("DATE '2024-02-29'", '[29-FEB-24]'), ("DATE '2000-01-01'", '[01-JAN-00]'), ('NULL', '[]')]:
        case('DATE', value, expected)
    for value in ['0.5', '-0.5', '12.500', '1e40', '1e-100', 'NULL']:
        for name, probe in [('TOCHAR', 'NUMBER'), ('TOSQL', 'NUMSQL')]:
            prior = next(row for row in cases if row['sql'] == 'SELECT ' + q(probe) + '(' + value + ') FROM DUAL')
            enclosed = prior['expected'][0][0]
            case(name, value, None if value == 'NULL' else enclosed[1:-1])
    for value, expected in [("DATE '2024-02-29'", '29-FEB-24'), ("DATE '2000-01-01'", '01-JAN-00'), ('NULL', None)]:
        case('TODATE', value, expected)
    case('ONCE', '', '[1]')
    case('TEXT', "RPAD('x',3000,'x'),RPAD('y',2000,'y')", error='ORA-06502')
    case('LEN', "RPAD('x',3000,'x'),RPAD('y',2000,'y')", dict(number='5000'))
    case('NESTSQL', "RPAD('x',3000,'x'),RPAD('y',2000,'y')", error='ORA-06502')
    case('OVER', '32760,7', dict(number='32767'))
    case('OVER', '32760,8', error='ORA-06502')
    case('QUERY', "RPAD('x',3000,'x'),RPAD('y',2000,'y')", error='ORA-01489')
    case('TEXT', "RPAD('x',30000,'x'),RPAD('y',3000,'y')", error='ORA-06502')
    setup = [dict(backend=backend, kind='SEQUENCE', name=q('SEQ'), sql='CREATE SEQUENCE ' + q('SEQ') + ' START WITH 1', inserts=[]) for backend in (0, 1)]
    for name, body in definitions:
        setup.extend(dict(backend=backend, kind='FUNCTION', name=q(name), sql='CREATE FUNCTION ' + q(name) + body, inserts=[]) for backend in (0, 1))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_TEXT').lower(), scope='Routine concatenation NULLs, padding, Unicode, scoped variables, SQL/PLSQL numeric widths, default English DATE format and bounded byte limits; not general NLS, LOBs, binary floating-point or BC dates')
