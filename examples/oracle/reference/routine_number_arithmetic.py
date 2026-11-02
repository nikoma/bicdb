"""Exact Oracle NUMBER arithmetic expectations from an independent reference."""
import re
import json
from pathlib import Path
from routine_namespaces import identifier


def fixture(schema, prefix, source_only=False):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    setup, cases = [], []
    # Every operand has at most 38 coefficient digits and is representable.
    operands = [
        ('1', '2'), ('-1', '2'), ('0', '0'), ('0.5', '-0.5'),
        ('99999999999999999999999999999999999999', '1'),
        ('12345678901234567890123456789012345678', '98765432109876543210987654321098765432'),
        ('-12345678901234567890123456789012345678', '98765432109876543210987654321098765432'),
        ('1e100', '1'), ('1e-100', '1'), ('1e-65', '1e-65'),
        ('1e-130', '0.1'), ('-1e-130', '0.1'),
        ('9.9999999999999999999999999999999999999e125', '1'),
        ('0.12345678901234567890123456789012345678', '0.98765432109876543210987654321098765432'),
        ('9.9999999999999999999999999999999999999e125', '2'),
    ]
    for suffix, operator in [('ADD', '+'), ('SUBTRACT', '-'), ('MULTIPLY', '*')]:
        for context in ('PLSQL', 'SQL'):
            name = q(suffix + '_' + context)
            operation = 'RETURN p_left' + operator + 'p_right;' if context == 'PLSQL' else 'SELECT p_left' + operator + 'p_right INTO result FROM DUAL; RETURN result;'
            sql = f'CREATE FUNCTION {name}(p_left NUMBER,p_right NUMBER) RETURN NUMBER IS result NUMBER; BEGIN {operation} END;'
            setup.extend(dict(backend=backend, kind='FUNCTION', name=name, sql=sql, inserts=[]) for backend in (0, 1))
            for index, (left, right) in enumerate(operands):
                cases.append(dict(id=suffix.lower() + '_' + context.lower() + '_' + str(index),
                                  sql=f'SELECT {name}({left},{right}) FROM DUAL'))
    if not source_only:
        reference = json.loads(Path(__file__).with_name('number-arithmetic-expectations.json').read_text())
        expected = {row['id']: row for row in reference['cases']}
        for case in cases:
            row = expected[case['id']]
            if 'error' in row:
                case['expected_error'] = dict(oracle=row['error']['code'], bicdb='22003')
            else:
                case['expected'] = row['rows']
    # Construct NUMBER infinities through PL/SQL arithmetic. The independent
    # reference records TO_CHAR results, avoiding a finite numeric sentinel.
    inf_name = q('INFINITY')
    inf_ddl = f'CREATE FUNCTION {inf_name}(p_value NUMBER) RETURN NUMBER IS BEGIN RETURN p_value*2; END;'
    setup.extend(dict(backend=backend, kind='FUNCTION', name=inf_name, sql=inf_ddl, inserts=[]) for backend in (0, 1))
    operators = {'+': 'ADD', '-': 'SUBTRACT', '*': 'MULTIPLY', '/': 'DIVIDE'}
    for operator, suffix in operators.items():
        for context in ('plsql', 'sql'):
            name = q('INF_' + suffix + '_' + context.upper())
            operation = f'result:=p_left{operator}p_right;' if context == 'plsql' else f'SELECT p_left{operator}p_right INTO result FROM DUAL;'
            sql = f'CREATE FUNCTION {name}(p_left NUMBER,p_right NUMBER) RETURN VARCHAR2 IS result NUMBER; BEGIN {operation} RETURN TO_CHAR(result); END;'
            setup.extend(dict(backend=backend, kind='FUNCTION', name=name, sql=sql, inserts=[]) for backend in (0, 1))
    infinity = json.loads(Path(__file__).with_name('number-infinity-expectations.json').read_text())
    for index, row in enumerate(infinity['cases']):
        arguments = [f'{inf_name}(9e125)' if value == 'inf' else f'{inf_name}(-9e125)' if value == '-inf' else value for value in (row['left'], row['right'])]
        name = q('INF_' + operators[row['operator']] + '_' + row['context'].upper())
        case = dict(id='infinity_' + str(index), sql=f'SELECT {name}({arguments[0]},{arguments[1]}) FROM DUAL')
        if 'error' in row:
            case['expected_error'] = dict(oracle=row['error'], bicdb='22012' if row['error'] == 'ORA-01476' else '22003')
        else:
            case['expected'] = [[row['text']]]
        cases.append(case)
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_ADD_PLSQL').lower(),
                scope='Resolved version-8 NUMBER addition, subtraction and multiplication in SQL/procedural contexts; representable operands, base-100 rounding, cancellation, underflow, and reference NUMBER infinity operations. Not unconstrained input conversion or binary floating point.')
