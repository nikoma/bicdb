"""Table-column %TYPE names, local constraints and unconstrained arguments."""
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    owner = identifier(schema)
    table = identifier(prefix + '_ANCHORS')
    qualified = f'{owner}.{table}'
    definitions = [
        ('num', f'(p_value NUMBER) RETURN NUMBER IS result {table}."Amount"%TYPE; BEGIN result:=p_value; RETURN result; END;'),
        ('qualified', f'(p_value NUMBER) RETURN NUMBER IS result {qualified}."Amount"%TYPE; BEGIN result:=p_value; RETURN result; END;'),
        ('text', f'(p_value VARCHAR2) RETURN VARCHAR2 IS result {table}."Short.Text"%TYPE; BEGIN result:=p_value; RETURN result; END;'),
        ('empty', f' RETURN NUMBER IS result {table}."Amount"%TYPE; BEGIN RETURN result; END;'),
        ('arg', f'(p_value {qualified}."Short.Text"%TYPE) RETURN VARCHAR2 IS BEGIN RETURN p_value; END;'),
        ('text_return', f'(p_value VARCHAR2) RETURN {qualified}."Short.Text"%TYPE IS BEGIN RETURN p_value; END;'),
        ('number_return', f'(p_value NUMBER) RETURN {qualified}."Amount"%TYPE IS BEGIN RETURN p_value; END;'),
        ('number_arg', f'(p_value {qualified}."Amount"%TYPE) RETURN NUMBER IS BEGIN RETURN p_value; END;'),
        ('handled', f'(p_value NUMBER) RETURN {qualified}."Amount"%TYPE IS BEGIN RETURN p_value; EXCEPTION WHEN OTHERS THEN RETURN 0; END;'),
        ('positive', f'(p_value NUMBER) RETURN {qualified}."Amount"%TYPE IS BEGIN RETURN CASE WHEN p_value<0 THEN -p_value ELSE +p_value END; END;'),
        ('literal', f' RETURN {qualified}."Long.Amount"%TYPE IS BEGIN RETURN 123456789012345678901234.56789; END;'),
    ]
    setup = [dict(backend=backend, kind='TABLE', name=qualified,
                  sql=f'''CREATE TABLE {qualified}("Amount" NUMBER(5,2) DEFAULT 1 NOT NULL,
                          "Short.Text" VARCHAR2(3 CHAR) DEFAULT 'x' NOT NULL,
                          "Long.Amount" NUMBER(38,2) DEFAULT 1 NOT NULL)''', inserts=[])
             for backend in (0, 1)]
    names = {}
    for kind, body in definitions:
        name = f'{owner}.{identifier(prefix + "_" + kind.upper())}'
        names[kind] = name
        setup.extend(dict(backend=backend, kind='FUNCTION', name=name,
                          sql=f'CREATE FUNCTION {name}{body}', inserts=[])
                     for backend in (0, 1))
    cases = []
    for kind in ('num', 'qualified'):
        for index, (value, expected) in enumerate([
            ('12.345', '12.35'), ('-12.345', '-12.35'),
            ('999.994', '999.99'), ('-999.994', '-999.99'), ('0.005', '0.01'),
        ]):
            cases.append(dict(id=f'{kind}_{index}',
                              sql=f"SELECT {names[kind]}(CAST('{value}' AS NUMBER)) FROM DUAL",
                              expected=[[dict(number=expected)]]))
        cases.append(dict(id=f'{kind}_null', sql=f'SELECT {names[kind]}(NULL) FROM DUAL', expected=[[None]]))
        cases.append(dict(id=f'{kind}_overflow', sql=f'SELECT {names[kind]}(1000) FROM DUAL',
                          expected_error=dict(oracle='ORA-06502', bicdb='22003')))
    for index, value in enumerate(['abc', 'a  ', 'ééé']):
        cases.append(dict(id=f'text_{index}', sql=f"SELECT {names['text']}('{value}') FROM DUAL", expected=[[value]]))
    cases.extend([
        dict(id='text_null', sql=f"SELECT {names['text']}(NULL) FROM DUAL", expected=[[None]]),
        dict(id='text_overflow', sql=f"SELECT {names['text']}('abcd') FROM DUAL",
             expected_error=dict(oracle='ORA-06502', bicdb='22001')),
        dict(id='text_unicode_overflow', sql=f"SELECT {names['text']}('éééé') FROM DUAL",
             expected_error=dict(oracle='ORA-06502', bicdb='22001')),
        dict(id='not_null_and_default_not_inherited', sql=f"SELECT {names['empty']}() FROM DUAL", expected=[[None]]),
        dict(id='argument_size_not_inherited', sql=f"SELECT {names['arg']}('abcdef') FROM DUAL", expected=[['abcdef']]),
        dict(id='argument_null', sql=f"SELECT {names['arg']}(NULL) FROM DUAL", expected=[[None]]),
        dict(id='return_text_size_not_inherited', sql=f"SELECT {names['text_return']}('abcdef') FROM DUAL", expected=[['abcdef']]),
        dict(id='return_text_null', sql=f"SELECT {names['text_return']}(NULL) FROM DUAL", expected=[[None]]),
        dict(id='return_numeric_overflow', sql=f"SELECT {names['number_return']}(CAST('1000.123' AS NUMBER)) FROM DUAL", expected_error=dict(oracle='ORA-06502', bicdb='22003')),
        dict(id='return_numeric_rounding', sql=f"SELECT {names['number_return']}(CAST('12.345' AS NUMBER)) FROM DUAL", expected=[[dict(number='12.35')]]),
        dict(id='return_numeric_negative_rounding', sql=f"SELECT {names['number_return']}(CAST('-12.345' AS NUMBER)) FROM DUAL", expected=[[dict(number='-12.35')]]),
        dict(id='return_numeric_rounding_overflow', sql=f"SELECT {names['number_return']}(CAST('999.995' AS NUMBER)) FROM DUAL", expected_error=dict(oracle='ORA-06502', bicdb='22003')),
        dict(id='case_negative_return', sql=f"SELECT {names['positive']}(CAST('-12.345' AS NUMBER)) FROM DUAL", expected=[[dict(number='12.35')]]),
        dict(id='case_positive_return', sql=f"SELECT {names['positive']}(CAST('12.345' AS NUMBER)) FROM DUAL", expected=[[dict(number='12.35')]]),
        dict(id='case_null_return', sql=f"SELECT {names['positive']}(NULL) FROM DUAL", expected=[[None]]),
        dict(id='numeric_return_handler_rounding', sql=f"SELECT {names['handled']}(CAST('12.345' AS NUMBER)) FROM DUAL", expected=[[dict(number='12.35')]]),
        dict(id='numeric_return_handler_overflow', sql=f"SELECT {names['handled']}(1000) FROM DUAL", expected=[[dict(number='0')]]),
        dict(id='numeric_literal_return', sql=f"SELECT {names['literal']}() FROM DUAL", expected=[[dict(number='123456789012345678901234.57')]]),
        dict(id='return_numeric_null', sql=f"SELECT {names['number_return']}(NULL) FROM DUAL", expected=[[None]]),
        dict(id='recovery_after_errors', sql=f"SELECT {names['num']}(12.345) FROM DUAL", expected=[[dict(number='12.35')]]),
    ])
    for index, value in enumerate(['12.345', '1000.123', '999.995']):
        cases.append(dict(id=f'number_argument_{index}', sql=f"SELECT {names['number_arg']}(CAST('{value}' AS NUMBER)) FROM DUAL", expected=[[dict(number=value)]]))
    cases.append(dict(id='number_argument_null', sql=f"SELECT {names['number_arg']}(NULL) FROM DUAL", expected=[[None]]))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_NUM').lower(),
                scope='Table-column %TYPE local precision and character length, quoted names and argument sizing; not logical temporal provenance, variable anchors, DDL lifecycle or cross-schema privileges')
