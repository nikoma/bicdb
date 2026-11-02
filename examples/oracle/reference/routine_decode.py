"""Oracle DECODE in stored SQL; expectations come from the scalar source probe."""
import json
from pathlib import Path
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    setup, cases = [], []
    definitions = []
    source = json.loads(Path(__file__).with_name('decode-cases.json').read_text())
    for index, case in enumerate(source['cases']):
        # Oracle diagnoses incompatible types during SQL compilation. This
        # invalid definition is asserted separately as a scalar SQL error.
        if case['id'] == 'number_date_type':
            continue
        result_type = {'DB_TYPE_NUMBER': 'NUMBER', 'DB_TYPE_VARCHAR': 'VARCHAR2',
                       'DB_TYPE_DATE': 'DATE', 'DB_TYPE_TIMESTAMP': 'TIMESTAMP'}.get(case.get('oracle_type'), 'NUMBER')
        local_type = result_type + '(4000)' if result_type == 'VARCHAR2' else result_type
        suffix = 'D' + str(index)
        definitions.append((suffix, f'CREATE FUNCTION {q(suffix)}(p_unused NUMBER) RETURN {result_type} IS v {local_type}; BEGIN {case["sql"].replace(" FROM DUAL", " INTO v FROM DUAL")}; RETURN v; END;'))
        row = dict(id='stored_' + case['id'], sql=f'SELECT {q(suffix)}(0) FROM DUAL')
        for field in ('expected', 'expected_error'):
            if field in case:
                row[field] = case[field]
        cases.append(row)
    for suffix, kind, search, checks in [
        ('NUMBER', 'NUMBER', "1,'one',NULL,'nil','other'", [('1','one'), ('NULL','nil'), ('2','other')]),
        ('TEXT', 'VARCHAR2', "'001','padded',NULL,'nil','other'", [("'001'",'padded'), ("'1'",'other'), ('NULL','nil')]),
        ('DATE', 'DATE', "DATE '2024-02-29','leap','other'", [("TIMESTAMP '2024-02-29 00:00:00.123456'",'leap'), ("DATE '2000-01-01'",'other')]),
    ]:
        definitions.append((suffix, f'CREATE FUNCTION {q(suffix)}(p_value {kind}) RETURN VARCHAR2 IS v VARCHAR2(4000); BEGIN SELECT DECODE(p_value,{search}) INTO v FROM DUAL; RETURN v; END;'))
        for index, (value, expected) in enumerate(checks):
            cases.append(dict(id=f'formal_{suffix.lower()}_{index}', sql=f'SELECT {q(suffix)}({value}) FROM DUAL', expected=[[expected]]))
    # A user function makes evaluation observable without relying on Oracle's
    # special SQL rules for a naked NEXTVAL expression.
    definitions.append(('TICK', f'CREATE FUNCTION {q("TICK")} RETURN NUMBER IS v NUMBER; BEGIN SELECT {q("SEQ")}.NEXTVAL INTO v FROM DUAL; RETURN v; END;'))
    definitions.append(('ONCE', f"CREATE FUNCTION {q('ONCE')}(p_unused NUMBER) RETURN VARCHAR2 IS v VARCHAR2(20); BEGIN SELECT DECODE({q('TICK')}(),999,'missing',1,'once','later') INTO v FROM DUAL; RETURN v; END;"))
    for index, expected in enumerate(['once', 'later']):
        cases.append(dict(id=f'selector_once_{index}', sql=f'SELECT {q("ONCE")}(0) FROM DUAL', expected=[[expected]]))
    for backend in (0, 1):
        setup.append(dict(backend=backend, kind='SEQUENCE', name=q('SEQ'), sql=f'CREATE SEQUENCE {q("SEQ")} START WITH 1', inserts=[]))
        setup.extend(dict(backend=backend, kind='FUNCTION', name=q(suffix), sql=sql, inserts=[]) for suffix, sql in definitions)
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_NUMBER').lower(),
                scope='Stored SQL DECODE, declared formal types, null equality, nonpadded comparison, lazy search/result conversion, and one selector evaluation; fixed default NLS only, not binary floating point or full Oracle diagnostics')
