"""AL32UTF8 CHR and nested default TO_CHAR, in SQL and versioned routines."""
import json
from pathlib import Path
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    source = json.loads(Path(__file__).with_name('chr-reference.json').read_text())
    definitions = [
        ('VALUE', '(n NUMBER)', 'BEGIN RETURN CHR(n); END;'),
        ('DEFAULT', '(n NUMBER)', 'BEGIN RETURN TO_CHAR(CHR(n)); END;'),
        ('NESTED', '(n NUMBER,p_label VARCHAR2)', "v VARCHAR2(100); BEGIN SELECT COALESCE(p_label,TO_CHAR(CHR(n))) INTO v FROM DUAL; RETURN '['||v||']'; END;"),
        ('SHORT', '(p_label VARCHAR2)', 'BEGIN RETURN COALESCE(p_label,CHR(-1)); END;'),
    ]
    setup = []
    for backend in (0, 1):
        for suffix, arguments, body in definitions:
            name = q(suffix)
            setup.append(dict(backend=backend,kind='FUNCTION',name=name,
                              sql=f'CREATE FUNCTION {name}{arguments} RETURN VARCHAR2 IS {body}',inserts=[]))
    cases = []
    for index, row in enumerate(source['valid_character_cases']):
        n = row['input']
        value = row['rows']['rows'][0][0]
        for label, expression, expected in [
            ('sql', f'CHR({n})', value),
            ('to_char', f'TO_CHAR(CHR({n}))', value),
            ('function', f'{q("VALUE")}({n})', value),
            ('default_function', f'{q("DEFAULT")}({n})', value),
            ('nested_function', f'{q("NESTED")}({n},NULL)', '[' + value + ']'),
        ]:
            cases.append(dict(id=f'{label}_{index}',sql=f'SELECT {expression} FROM DUAL',expected=[[expected]]))
    for index, expression in enumerate(['NULL', "''", '65.4', '65.5', '65.9', "' 65.9 '", '6.59e1']):
        value = None if index < 2 else 'A'
        for label, sql in [('sql', f'CHR({expression})'), ('routine', f'{q("DEFAULT")}({expression})')]:
            cases.append(dict(id=f'conversion_{label}_{index}',sql=f'SELECT {sql} FROM DUAL',expected=[[value]]))
    for index, (expression, oracle_code, pg_code) in enumerate([
        ('-1','ORA-01426','22003'), ('-0.1','ORA-01426','22003'),
        ('0.1','ORA-01426','22003'), ('1e-50','ORA-01426','22003'),
        ('4294967296','ORA-01426','22003'), ('1e126','ORA-01426','22003'),
        ("'invalid'",'ORA-01722','22P02'), ("'1,000'",'ORA-01722','22P02'),
    ]):
        cases.append(dict(id=f'number_error_{index}',sql=f'SELECT CHR({expression}) FROM DUAL',
                          expected_error=dict(oracle=oracle_code,bicdb=pg_code)))
    for index, (expression, expected) in enumerate([
        ("TO_CHAR('')",None), ('TO_CHAR(NULL)',None),
        ("COALESCE(CHR(65),CHR(-1))",'A'),
        ("DECODE(1,1,CHR(65),CHR(-1))",'A'),
        (f'{q("NESTED")}(65,\'prefix\')','[prefix]'),
    ]):
        cases.append(dict(id=f'nesting_{index}',sql=f'SELECT {expression} FROM DUAL',expected=[[expected]]))
    cases.append(dict(id='procedural_short_circuit',sql=f'SELECT {q("SHORT")}(\'prefix\') FROM DUAL',expected=[['prefix']]))
    cases.append(dict(id='procedural_selected_error',sql=f'SELECT {q("SHORT")}(NULL) FROM DUAL',expected_error=dict(oracle='ORA-01426',bicdb='22003')))
    return dict(setup=setup,cases=cases,compiler_probe=(prefix + '_VALUE').lower(),
                scope='Fixed AL32UTF8 complete characters, truncating numeric input, error codes, nested default TO_CHAR and stored routines. NUL transport, indeterminate invalid code points, NCHAR_CS and other database character sets remain unsupported/unqualified.')
