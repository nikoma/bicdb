"""Frozen Oracle column storage and %TYPE expectations, including PG COPY input."""
import json
from pathlib import Path
import re

from routine_namespaces import identifier


def fixture(schema, prefix):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,31}', prefix):
        raise ValueError('fixture prefix must be a short uppercase identifier')
    q = lambda suffix: identifier(schema) + '.' + identifier(prefix + '_' + suffix)
    reference = json.loads(Path(__file__).with_name('column-types-reference.json').read_text())
    inputs = [row['input'] for row in reference['cases'] if row['anchor'] == 'd']
    declaration = f'CREATE TABLE {q("ROWS")}(id NUMBER PRIMARY KEY,d DATE,t TIMESTAMP,t0 TIMESTAMP(0),t3 TIMESTAMP(3),"Quoted.Date" DATE,z TIMESTAMP(6) WITH TIME ZONE)'
    expected, copies, inserts = [], [], []
    for index, (stamp, row) in enumerate(zip(inputs, reference['storage_rows'])):
        value = "TIMESTAMP '" + stamp + "'"
        inserts.append(f'INSERT INTO {q("ROWS")} VALUES({index},{value},{value},{value},{value},{value},NULL)')
        copies.append([dict(number=str(index))] + [dict(timestamp=stamp)] * 5 + [None])
        expected.append(row + [row[1], None])
    setup, cases = [], []
    for backend in (0, 1):
        setup.append(dict(backend=backend, kind='TABLE', name=q('ROWS'), sql=declaration,
                          inserts=inserts if backend == 0 else [],
                          **(dict(copy_rows=copies) if backend == 1 else {})))
        for column in ('d', 't', 't0', 't3'):
            suffix = column.upper()
            sql = f'CREATE FUNCTION {q(suffix)}(p_stamp TIMESTAMP) RETURN TIMESTAMP IS v {q("ROWS")}.{column}%TYPE; BEGIN v:=p_stamp; RETURN v; END;'
            setup.append(dict(backend=backend, kind='FUNCTION', name=q(suffix), sql=sql, inserts=[]))
        sql = f'CREATE FUNCTION {q("QUOTED")}(p_stamp TIMESTAMP) RETURN TIMESTAMP IS v {q("ROWS")}."Quoted.Date"%TYPE; BEGIN v:=p_stamp; RETURN v; END;'
        setup.append(dict(backend=backend, kind='FUNCTION', name=q('QUOTED'), sql=sql, inserts=[]))
    cases.append(dict(id='stored_column_values', sql=f'SELECT id,d,t,t0,t3,"Quoted.Date",z FROM {q("ROWS")} ORDER BY id', expected=expected))
    for index, row in enumerate(reference['cases']):
        cases.append(dict(id='anchor_' + row['anchor'] + '_' + str(index),
                          sql=f'SELECT {q(row["anchor"].upper())}(TIMESTAMP \'{row["input"]}\') FROM DUAL', expected=row['rows']))
    for index, row in enumerate(reference['cases'][:len(inputs)]):
        cases.append(dict(id='quoted_anchor_' + str(index), sql=f'SELECT {q("QUOTED")}(TIMESTAMP \'{row["input"]}\') FROM DUAL', expected=row['rows']))
    return dict(setup=setup, cases=cases, compiler_probe=(prefix + '_D').lower(),
                temporal_metadata=dict(relation=q('ROWS'), owner=schema, table=prefix + '_ROWS',
                    expected={'D':'DATE','T':'TIMESTAMP(6)','T0':'TIMESTAMP(0)','T3':'TIMESTAMP(3)','Quoted.Date':'DATE','Z':'TIMESTAMP(6) WITH TIME ZONE'}),
                projection_types=dict(sql=f'SELECT id,d,t,t0,t3,"Quoted.Date",z FROM {q("ROWS")} WHERE 1=0',
                    oracle=['DB_TYPE_NUMBER','DB_TYPE_DATE','DB_TYPE_TIMESTAMP','DB_TYPE_TIMESTAMP','DB_TYPE_TIMESTAMP','DB_TYPE_DATE','DB_TYPE_TIMESTAMP_TZ'],
                    bicdb=[1700,1114,1114,1114,1114,1114,1184]),
                scope='Oracle DATE versus TIMESTAMP precision in PostgreSQL COPY storage, anchored locals, quoted columns, logical metadata and unchanged native transport OIDs. TIMESTAMP WITH TIME ZONE declaration and NULL metadata only; region behavior and mutable NLS unqualified.')
