"""Gregorian/America week truncation with SQL and PL/SQL NULL contracts."""
import datetime
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    owner = identifier(schema)
    names = {kind: f'{owner}.{identifier(prefix + "_" + kind.upper())}'
             for kind in ('clip', 'sql', 'number')}
    definitions = [
        ('clip', '(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS BEGIN RETURN TRUNC(p_date,p_fmt); END;'),
        ('sql', '(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS result DATE; BEGIN SELECT TRUNC(p_date,p_fmt) INTO result FROM DUAL; RETURN result; END;'),
        ('number', '(p_value NUMBER,p_scale NUMBER) RETURN NUMBER IS BEGIN RETURN TRUNC(p_value,p_scale); END;'),
    ]
    setup = [dict(backend=backend, kind='FUNCTION', name=names[kind],
                  sql=f'CREATE FUNCTION {names[kind]}{body}', inserts=[])
             for kind, body in definitions for backend in (0, 1)]
    cases = []
    rows = [
        ('2024-02-29', 'W', '2024-02-29'), ('2024-02-29', 'WW', '2024-02-26'),
        ('2024-02-29', 'D', '2024-02-25'), ('2024-02-29', 'DY', '2024-02-25'),
        ('2024-02-29', 'DAY', '2024-02-25'), ('2024-02-29', 'IW', '2024-02-26'),
        ('2024-02-29', 'IYYY', '2024-01-01'),
        ('2023-01-01', 'W', '2023-01-01'), ('2023-01-01', 'WW', '2023-01-01'),
        ('2023-01-01', 'D', '2023-01-01'), ('2023-01-01', 'IW', '2022-12-26'),
        ('2023-01-01', 'IYYY', '2022-01-03'),
        ('2021-01-01', 'IYYY', '2019-12-30'), ('2020-12-31', 'IYY', '2019-12-30'),
        ('2019-12-30', 'IY', '2019-12-30'),
        ('2024-02-03', 'W', '2024-02-01'), ('2024-02-03', 'WW', '2024-01-29'),
        ('2024-02-03', 'D', '2024-01-28'),
        ('2024-03-01', 'W', '2024-03-01'), ('2024-03-01', 'WW', '2024-02-26'),
        ('2024-03-01', 'D', '2024-02-25'),
    ]
    functions = [('scalar', 'TRUNC'), ('procedure', names['clip']), ('embedded_sql', names['sql'])]
    for index, (stamp, fmt, result) in enumerate(rows):
        expected = canonical(datetime.datetime.fromisoformat(result))
        for label, function in functions:
            cases.append(dict(id=f'{label}_{index}', sql=f"SELECT {function}(TIMESTAMP '{stamp} 12:34:56.9','{fmt}') FROM DUAL", expected=[[expected]]))
    for label, function in functions:
        for index, fmt in enumerate(["'W'", 'NULL']):
            cases.append(dict(id=f'{label}_null_date_{index}', sql=f'SELECT {function}(NULL,{fmt}) FROM DUAL', expected=[[None]]))
        for index, fmt in enumerate(['NULL', "''"]):
            case=dict(id=f'{label}_null_format_{index}', sql=f"SELECT {function}(TIMESTAMP '2024-02-29 12:34:56',{fmt}) FROM DUAL")
            if label == 'procedure':
                case['expected_error'] = dict(oracle='ORA-01899', bicdb='22023')
            else:
                case['expected'] = [[None]]
            cases.append(case)
    for index, args in enumerate(['123.456,NULL', 'NULL,2']):
        cases.append(dict(id=f'numeric_null_{index}', sql=f'SELECT {names["number"]}({args}) FROM DUAL', expected=[[None]]))
    for index, (args, sql_value, procedural_value) in enumerate([
        ("CAST('123456789012345678901234.56789' AS NUMBER),2", '123456789012345678901234.56', '123456789012345678901234.56'),
        ('-123.456,2', '-123.45', '-123.45'), ('1.2,4', '1.2', '1.2'),
        ('123.456,1.9', '123.4', '123.45'), ('123.456,-1.9', '120', '100'),
        ('123.456,-2', '100', '100'),
        ('123.456,1.5', '123.4', '123.45'), ('123.456,1.4', '123.4', '123.4'),
        ('123.456,-1.5', '120', '100'), ('123.456,-1.4', '120', '120'),
        ('123.456,0.5', '123', '123.4'), ('123.456,-0.5', '123', '120'),
    ]):
        for label, function, expected in [('scalar', 'TRUNC', sql_value),
                                           ('procedure', names['number'], procedural_value)]:
            cases.append(dict(id=f'{label}_numeric_{index}',
                              sql=f'SELECT {function}({args}) FROM DUAL',
                              expected=[[dict(number=expected)]]))
    cases.append(dict(id='recovery_after_errors', sql=f"SELECT {names['clip']}(TIMESTAMP '2024-02-29 12:34:56','D') FROM DUAL", expected=[[canonical(datetime.datetime(2024, 2, 25))]]))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_CLIP').lower(),
                scope='Gregorian/America week and ISO-year values; SQL versus PL/SQL NULL-format errors; not alternate NLS territories or full Oracle error mapping')
