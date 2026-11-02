"""Eager Oracle NVL with declared SQL and procedural operand types.

Expected values are frozen from an independent Oracle source run. The runner
checks them again on Oracle before accepting any BicDB comparison.
"""
import json
import datetime
import decimal
from pathlib import Path
import re

from routine_namespaces import identifier
from compare import canonical


def compare_bindings(oracle, bicdb):
    """Exercise actual Oracle input types and PostgreSQL Parse/Bind inference."""
    import oracledb
    stamp = datetime.datetime(2024, 2, 29, 12, 34, 56, 123456)
    cases = [
        ('bound_timestamp', "NVL('',{bind})", stamp, oracledb.DB_TYPE_TIMESTAMP,
         '29-FEB-24 12.34.56.123456000 PM', None),
        ('bound_number', "NVL('',{bind})", decimal.Decimal('42.5'), oracledb.DB_TYPE_NUMBER,
         '42.5', None),
        ('bound_null_character', "NVL({bind},7)", None, oracledb.DB_TYPE_VARCHAR,
         '7', None),
        ('bound_timestamp_text', "NVL(CAST(NULL AS TIMESTAMP),{bind})",
         '29-FEB-24 12.34.56.123456 PM', oracledb.DB_TYPE_VARCHAR, stamp, None),
        ('bound_timestamp_invalid', "NVL(CAST(NULL AS TIMESTAMP),{bind})",
         'invalid', oracledb.DB_TYPE_VARCHAR, None, dict(oracle='ORA-01858', bicdb='22007')),
        ('bound_unused_number_error', "NVL({bind},'bad')", decimal.Decimal(1),
         oracledb.DB_TYPE_NUMBER, None, dict(oracle='ORA-01722', bicdb='22P02')),
    ]
    results = []
    for name, expression, value, source_type, expected, errors in cases:
        row = dict(id=name)
        for label, connection, bind in [('oracle', oracle, ':1'), ('bicdb', bicdb, '%s')]:
            try:
                with connection.cursor() as cursor:
                    if label == 'oracle':
                        cursor.setinputsizes(source_type)
                    cursor.execute('SELECT ' + expression.format(bind=bind) + ' FROM DUAL', [value])
                    row[label] = dict(rows=[[canonical(item) for item in values]
                                            for values in cursor.fetchall()])
            except Exception as error:
                detail = error.args[0] if error.args else None
                row[label] = dict(error=type(error).__name__, code=getattr(error, 'sqlstate', None)
                                  or getattr(detail, 'full_code', None))
        if errors:
            row['expected_error'] = errors
            row['matched'] = all(row[label].get('code') == code for label, code in errors.items())
        else:
            row['expected'] = [[canonical(expected)]]
            row['matched'] = row['oracle'].get('rows') == row['bicdb'].get('rows') == row['expected']
        results.append(row)
    return results


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    source = json.loads(Path(__file__).with_name('nvl-reference.json').read_text())
    setup, cases = [], []
    table = q('ROWS')
    for backend in (0, 1):
        setup.append(dict(backend=backend, kind='SEQUENCE', name=q('SEQ'),
                          sql=f'CREATE SEQUENCE {q("SEQ")} START WITH 1', inserts=[]))
        setup.append(dict(backend=backend, kind='TABLE', name=table,
                          sql=f'CREATE TABLE {table}(id NUMBER,d DATE,t0 TIMESTAMP(0),t3 TIMESTAMP(3),n NUMBER(5,2),label VARCHAR2(8),c CHAR(3))',
                          inserts=[f"INSERT INTO {table} VALUES(1,TIMESTAMP '2024-02-29 12:34:56',TIMESTAMP '2024-02-29 12:34:56',TIMESTAMP '2024-02-29 12:34:56.123456',12.34,'first','A')",
                                   f'INSERT INTO {table} VALUES(2,NULL,NULL,NULL,NULL,NULL,NULL)']))
    definitions = [
        ('NUM', '(p_number NUMBER,p_text VARCHAR2)', 'NUMBER', 'BEGIN RETURN NVL(p_number,p_text); END;'),
        ('TEXT', '(p_text VARCHAR2,p_number NUMBER)', 'VARCHAR2', 'BEGIN RETURN NVL(p_text,p_number); END;'),
        ('DATEVALUE', '(p_date DATE,p_stamp TIMESTAMP)', 'DATE', 'BEGIN RETURN NVL(p_date,p_stamp); END;'),
        ('STAMP', '(p_stamp TIMESTAMP)', 'VARCHAR2', "BEGIN RETURN NVL('',p_stamp); END;"),
        ('SQLSTAMP', '(p_stamp TIMESTAMP)', 'VARCHAR2', "v VARCHAR2(100); BEGIN SELECT NVL('',p_stamp) INTO v FROM DUAL; RETURN v; END;"),
        ('LOCALSTAMP', '', 'VARCHAR2', "v TIMESTAMP(3):=TIMESTAMP '2024-02-29 12:34:56.123456'; BEGIN RETURN NVL('',v); END;"),
        ('RECORD', '', 'VARCHAR2', f"CURSOR c IS SELECT t3 FROM {table} WHERE id=1; BEGIN FOR r IN c LOOP RETURN NVL('',r.t3); END LOOP; RETURN NULL; END;"),
        ('SQLRECORD', '', 'VARCHAR2', f"v VARCHAR2(100); CURSOR c IS SELECT t3 FROM {table} WHERE id=1; BEGIN FOR r IN c LOOP SELECT NVL('',r.t3) INTO v FROM DUAL; RETURN v; END LOOP; RETURN NULL; END;"),
        ('RETURNSTAMP', '', table + '.t3%TYPE', "BEGIN RETURN TIMESTAMP '2024-02-29 12:34:56.123456'; END;"),
        ('DIRECTRETURN', '', 'VARCHAR2', f"BEGIN RETURN NVL('',{q('RETURNSTAMP')}()); END;"),
        ('EAGER', '(p_number NUMBER)', 'NUMBER', 'BEGIN RETURN NVL(p_number,1/0); END;'),
        ('CHARVALUE', '', 'VARCHAR2', "v CHAR(3):='A'; BEGIN RETURN NVL(v,'longer'); END;"),
        ('CHARNIL', '', 'VARCHAR2', "v CHAR(3); BEGIN RETURN NVL(v,'longer'); END;"),
        ('CONTEXT', '', 'VARCHAR2', 'BEGIN RETURN NVL(\'\',12345678901234567890123456789012345678901234567890123456789012345678901234567890); END;'),
        ('TICK', '', 'NUMBER', f'v NUMBER; BEGIN SELECT {q("SEQ")}.NEXTVAL INTO v FROM DUAL; RETURN v; END;'),
    ]
    for backend in (0, 1):
        for suffix, arguments, result, body in definitions:
            setup.append(dict(backend=backend, kind='FUNCTION', name=q(suffix),
                              sql=f'CREATE FUNCTION {q(suffix)}{arguments} RETURN {result} IS {body}', inserts=[]))
    for index, row in enumerate(source['cases']):
        case = dict(id=f'scalar_{index}', sql=f'SELECT NVL({row["first"]},{row["second"]}) FROM DUAL')
        if 'rows' in row['oracle']:
            case['expected'] = row['oracle']['rows']
        else:
            code = row['oracle']['code']
            case['expected_error'] = dict(oracle=code, bicdb={'ORA-01722':'22P02', 'ORA-00932':'42804', 'ORA-01858':'22007'}[code])
        cases.append(case)
    expressions = [
        ('unused_division', 'NVL(1,1/0)'),
        ('unused_character_error', "NVL('A',CHR(-1))"),
        ('coalesce_lazy', "COALESCE('A',CHR(-1))"),
        ('nested_coalesce', "NVL(COALESCE('','x'),7)"),
        ('nested_nvl', "NVL('',NVL('',7))"),
        ('char_padding', "NVL(CAST('A' AS CHAR(3)),'longer')"),
        ('char_null', "NVL(CAST(NULL AS CHAR(3)),'longer')"),
        ('number_scale', 'NVL(CAST(NULL AS NUMBER(5,2)),12.3456)'),
        ('timestamp_literal', "NVL('',TIMESTAMP '2024-02-29 12:34:56.123456')"),
        ('timestamp_precision0', "NVL('',CAST(TIMESTAMP '2024-02-29 12:34:56' AS TIMESTAMP(0)))"),
        ('timestamp_precision3', "NVL('',CAST(TIMESTAMP '2024-02-29 12:34:56.123456' AS TIMESTAMP(3)))"),
        ('timestamp_null', "NVL('',CAST(NULL AS TIMESTAMP(3)))"),
        ('timestamp_text', "NVL(CAST(NULL AS TIMESTAMP),'29-FEB-24 12.34.56.123456 PM')"),
        ('timestamp_constant_text', "NVL(TIMESTAMP '2024-02-29 12:34:56.123456','29-FEB-24 12.34.56.123456 PM')"),
        ('timestamp_date_only', "NVL(CAST(NULL AS TIMESTAMP),'29-FEB-24')"),
        ('timestamp_bad_literal', "NVL(CAST(NULL AS TIMESTAMP),'invalid')"),
        ('timestamp_bad_typed', "NVL(CAST(NULL AS TIMESTAMP),CAST('invalid' AS VARCHAR2(100)))"),
        ('date_fraction', "NVL(CAST(NULL AS DATE),TIMESTAMP '2024-02-29 12:34:56.999999')"),
        ('stamp_date_first', "NVL(CAST(NULL AS TIMESTAMP(3)),DATE '2024-02-29')"),
        ('stamp_max_precision', "NVL('',NVL(CAST(NULL AS TIMESTAMP(3)),TIMESTAMP '2024-02-29 12:34:56.123456'))"),
        ('pl_number_value', f"{q('NUM')}(1,'2')"),
        ('pl_number_bad', f"{q('NUM')}(1,'bad')"),
        ('pl_number_null', f"{q('NUM')}(NULL,'2')"),
        ('pl_text_value', f"{q('TEXT')}('A',7)"),
        ('pl_text_null', f"{q('TEXT')}(NULL,7)"),
        ('pl_date', f"{q('DATEVALUE')}(NULL,TIMESTAMP '2024-02-29 12:34:56.999999')"),
        ('pl_stamp', f"{q('STAMP')}(TIMESTAMP '2024-02-29 12:34:56.123456')"),
        ('sql_stamp', f"{q('SQLSTAMP')}(TIMESTAMP '2024-02-29 12:34:56.123456')"),
        ('pl_local_stamp', f"{q('LOCALSTAMP')}()"),
        ('pl_record_stamp', f"{q('RECORD')}()"),
        ('sql_record_stamp', f"{q('SQLRECORD')}()"),
        ('anchored_stamp', f"{q('RETURNSTAMP')}()"),
        ('sql_return_stamp', f"NVL('',{q('RETURNSTAMP')}())"),
        ('pl_return_stamp', f"{q('DIRECTRETURN')}()"),
        ('pl_eager', f"{q('EAGER')}(1)"),
        ('pl_char', f"{q('CHARVALUE')}()"),
        ('pl_char_null', f"{q('CHARNIL')}()"),
        ('pl_number_context', f"{q('CONTEXT')}()"),
        ('eager_once_1', f"NVL(1,{q('TICK')}())"),
        ('eager_once_2', f"NVL(1,{q('TICK')}())"),
        ('eager_once_observed', f"NVL(NULL,{q('TICK')}())"),
        ('first_once', f"NVL({q('TICK')}(),0)"),
        ('first_once_observed', f"NVL(NULL,{q('TICK')}())"),
    ]
    extra = [(name, f'SELECT {expression} FROM DUAL') for name, expression in expressions]
    for name, expression in [('column_stamp0', "NVL('',t0)"), ('column_stamp3', "NVL('',t3)"),
                             ('column_stamp_text', "NVL(t3,'29-FEB-24 12.34.56.123456 PM')"),
                             ('column_date', "NVL('',d)"), ('column_number', "NVL('',n)"),
                             ('column_char', "NVL(c,'longer')"), ('column_numeric', "NVL(n,'123.456')"),
                             ('column_date_delta', "NVL(d,DATE '2024-01-01')-DATE '2024-02-28'"),
                             ('column_date_trunc', 'TRUNC(NVL(d,DATE \'2024-01-01\'))')]:
        for row in (1, 2):
            extra.append((f'{name}_{row}', f'SELECT {expression} FROM {table} WHERE id={row}'))
    extra += [('cte_stamp', f"WITH chosen AS (SELECT t3 AS stamp FROM {table} WHERE id=1) SELECT NVL('',stamp) FROM chosen"),
              ('derived_stamp', f"SELECT NVL('',chosen.stamp) FROM (SELECT t3 AS stamp FROM {table} WHERE id=1) chosen")]
    for name, sql in extra:
        case = dict(id=name, sql=sql)
        # Source capture populates these once; absence fails the public runner.
        case.update(source.get('fixture_expectations', {}).get(name, {}))
        cases.append(case)
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_NUM').lower(),
                projection_types=dict(sql=f"SELECT NVL(n,7),NVL('',n),NVL(d,DATE '2024-01-01'),NVL(t3,TIMESTAMP '2024-01-01 00:00:00'),NVL(c,'fallback') FROM {table} WHERE id=1",
                                      oracle=['DB_TYPE_NUMBER','DB_TYPE_VARCHAR','DB_TYPE_DATE','DB_TYPE_TIMESTAMP','DB_TYPE_VARCHAR'], bicdb=[1700,25,1114,1114,25]),
                scope='Eager NVL under fixed preview NLS, NUMBER/character/DATE/TIMESTAMP type selection, column and routine precision, padding and independent COALESCE behavior. Binary floating point, other NLS settings and nonzero submicrosecond values remain unsupported/unqualified.')
