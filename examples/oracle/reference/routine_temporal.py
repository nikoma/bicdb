"""Generated logical DATE/TIMESTAMP routine values for an Oracle reference."""
import datetime
import decimal
import re

from compare import canonical
from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('invalid fixture prefix')
    owner = identifier(schema)
    table = identifier(prefix + '_DAYS')
    setup = []
    for backend in (0, 1):
        name = f'{owner}.{table}'
        setup.append(dict(backend=backend, kind='TABLE', name=name,
                          sql=f'CREATE TABLE {name}(k NUMBER,days NUMBER)',
                          inserts=[f'INSERT INTO {name} VALUES({row})' for row in
                                   ['1,2', '2,-1', '3,0.25', '4,NULL']]))
    stamp = "TIMESTAMP '2024-02-29 23:59:59.9'"
    midnight = "TIMESTAMP '2024-03-01 00:00:00'"
    def date(value): return datetime.datetime.fromisoformat(value)
    definitions = [
        ('shift', 'p_date DATE,p_days NUMBER', 'DATE',
         'BEGIN RETURN p_date+p_days; END;', [
             (f'{stamp},0', date('2024-02-29 23:59:59')),
             (f'{stamp},0.25', date('2024-03-01 05:59:59')),
             (f'{midnight},-1', date('2024-02-29 00:00:00')),
             (f'{midnight},NULL', None), ('NULL,1', None),
             (f"{midnight},CAST('0.000005787037037037037038' AS NUMBER)", date('2024-03-01 00:00:01')),
             (f'{midnight},0.000005787037037037037038', date('2024-03-01 00:00:01')),
             (f'{stamp},0', date('2024-02-29 23:59:59')),
         ]),
        ('commute', 'p_date DATE,p_days NUMBER', 'DATE',
         'BEGIN RETURN p_days+p_date; END;', [(f'{stamp},0.25', date('2024-03-01 05:59:59')), ('NULL,1', None)]),
        ('subtract', 'p_date DATE,p_days NUMBER', 'DATE',
         'BEGIN RETURN p_date-p_days; END;', [(f'{midnight},0.25', date('2024-02-29 18:00:00')), (f'{midnight},-1', date('2024-03-02 00:00:00'))]),
        ('delta', 'p_left DATE,p_right DATE', 'NUMBER',
         'BEGIN RETURN p_left-p_right; END;', [
             (f"TIMESTAMP '2024-03-01 00:00:01',{midnight}", decimal.Decimal('0.00001157407407407407407407407407407407407407')),
             (f"{midnight},TIMESTAMP '2024-03-01 00:00:01'", decimal.Decimal('-0.00001157407407407407407407407407407407407407')),
             (f'{stamp},{stamp}', 0), ('NULL,NULL', None),
         ]),
        ('stamp_delta', 'p_left TIMESTAMP,p_right TIMESTAMP', 'INTERVAL DAY TO SECOND',
         'BEGIN RETURN p_left-p_right; END;', [
             ("TIMESTAMP '2024-03-01 00:00:01.123456',TIMESTAMP '2024-03-01 00:00:00.123455'", datetime.timedelta(microseconds=1_000_001)),
             ("TIMESTAMP '2024-03-01 00:00:00.5',TIMESTAMP '2024-03-01 00:00:01'", datetime.timedelta(microseconds=-500_000)),
         ]),
        ('mixed', 'p_left DATE,p_right TIMESTAMP', 'INTERVAL DAY TO SECOND',
         'BEGIN RETURN p_left-p_right; END;', [
             ("TIMESTAMP '2024-03-01 00:00:01',TIMESTAMP '2024-03-01 00:00:00.5'", datetime.timedelta(microseconds=500_000)),
         ]),
        ('cast_date', 'p_stamp TIMESTAMP', 'DATE',
         'BEGIN RETURN CAST(p_stamp AS DATE); END;', [(stamp, date('2024-02-29 23:59:59')), ('NULL', None)]),
        ('local_date', 'p_stamp TIMESTAMP', 'DATE',
         'result DATE:=p_stamp; BEGIN RETURN result; END;', [(stamp, date('2024-02-29 23:59:59'))]),
        ('round_stamp', 'p_stamp TIMESTAMP', 'TIMESTAMP',
         'result TIMESTAMP(0):=p_stamp; BEGIN RETURN result; END;', [(stamp, date('2024-03-01 00:00:00'))]),
        ('literal', '', 'DATE', 'BEGIN RETURN DATE \'2024-02-29\'+1; END;', [('', date('2024-03-01 00:00:00'))]),
        ('row_shift', 'p_date DATE,p_key NUMBER', 'DATE',
         f'result DATE:=p_date; CURSOR c IS SELECT * FROM {table} WHERE k=p_key; BEGIN FOR r IN c LOOP result:=result+r.days; END LOOP; RETURN result; END;', [
             (f'{midnight},1', date('2024-03-03 00:00:00')),
             (f'{midnight},2', date('2024-02-29 00:00:00')),
             (f'{midnight},3', date('2024-03-01 06:00:00')),
             (f'{midnight},4', None),
             (f'{midnight},9', date('2024-03-01 00:00:00')),
         ]),
    ]
    cases = []
    for suffix, args, result, body, values in definitions:
        name = f'{owner}.{identifier(prefix + "_" + suffix.upper())}'
        parameters = f'({args})' if args else ''
        for backend in (0, 1):
            setup.append(dict(backend=backend, kind='FUNCTION', name=name,
                              sql=f'CREATE FUNCTION {name}{parameters} RETURN {result} IS {body}', inserts=[]))
        for index, (arguments, expected) in enumerate(values):
            cases.append(dict(id=f'{suffix}_{index}', sql=f'SELECT {name}({arguments}) FROM DUAL',
                              expected=[[canonical(expected)]]))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_SHIFT').lower(),
                scope='Generated DATE/TIMESTAMP routine values; no timezone, >6-digit timestamp or full Oracle type conformance claim')
