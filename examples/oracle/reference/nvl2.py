"""Typed, eager Oracle NVL2 with independent PostgreSQL CASE controls.

Nonce objects are removed. Expectations qualify selected NUMBER, character,
DATE/TIMESTAMP, prepared and procedural controls, not full NVL2/NLS conformance.
"""
import datetime
import decimal
import uuid
from row_lock_waits import main as comparison_main


def canonical(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat(timespec='microseconds')
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time()).isoformat(timespec='microseconds')
    if isinstance(value, (int, float, decimal.Decimal)):
        with decimal.localcontext() as context:
            context.prec = 100
            return {'number': str(decimal.Decimal(str(value)).normalize())}
    return value


def error_kind(error):
    code = getattr(error.args[0], 'code', None) if error.args else None
    state = getattr(error, 'sqlstate', None)
    if code == 1476 or state == '22012': return 'division'
    if code == 1722 or state == '22P02': return 'number'
    if code == 932 or state == '42804': return 'type'
    if code in [1858, 1861, 1843] or state == '22007': return 'date'
    return 'unexpected'


def observe(factory, oracle):
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        driver = type(conn).__module__.startswith('oracledb')
        report['server_version'] = conn.version if driver else conn.info.server_version
        prefix = 'BICDB_N2_' + uuid.uuid4().hex[:10].upper()
        table = prefix + 'T'
        made = []

        def execute(sql, parameters=None):
            with conn.cursor() as q:
                if driver: q.execute(sql, parameters)
                else: q.execute(sql, parameters, prepare=True)
                return q.fetchall() if q.description else None

        def check(label, sql, expected=None, error=None, parameters=None):
            row = dict(id=label, expected=dict(value=canonical(expected), error=error))
            try:
                value = execute(sql, parameters)[0][0]
                actual = dict(value=canonical(value), error=None)
            except Exception as e:
                actual = dict(value=None, error=error_kind(e))
                row['detail'] = str(e)[:500]
            row.update(actual=actual, qualified=actual == row['expected'])
            report['cases'].append(row)

        def expression(flag, yes, no):
            return f'NVL2({flag},{yes},{no})' if oracle else f'CASE WHEN {flag} IS NOT NULL THEN {yes} ELSE {no} END'

        def select(expr, ident=1): return f'SELECT {expr} FROM {table} WHERE id={ident}'
        try:
            number, text = ('NUMBER', 'VARCHAR2(20)') if driver else ('NUMERIC', 'VARCHAR(20)')
            execute(f'CREATE TABLE {table}(id {number} PRIMARY KEY,flag {number},amount {number},label {text},day_value DATE,stamp TIMESTAMP(6))')
            made.append(('TABLE', table))
            execute(f"INSERT INTO {table} VALUES(1,1,7,'bad',DATE '2024-02-29',TIMESTAMP '2024-02-29 12:34:56.125')")
            execute(f"INSERT INTO {table} VALUES(2,NULL,NULL,'8',NULL,NULL)")
            for label, flag, yes, no, expected, ident in [
                ('nonnull', 'flag', '7', '8', 7, 1), ('null', 'flag', '7', '8', 8, 2),
                ('zero_nonnull', '0', '7', '8', 7, 1), ('text_condition', "'flag'", '7', '8', 7, 1),
                ('null_yes', 'flag', 'NULL', '8', None, 1), ('null_yes_fallback', 'flag', 'NULL', '8', 8, 2),
                ('null_no', 'flag', '7', 'NULL', None, 2),
                ('column_values', 'flag', 'amount', '8', 7, 1),
                ('number_conversion', 'flag', '7', f'CAST(label AS {number})', 8, 2),
                ('nested', 'flag', expression('NULL','8','9'), '10', 9, 1),
            ]: check(label, select(expression(flag,yes,no),ident), expected)
            check('character_number', select(expression('NULL', "'x'", "42" if oracle else 'CAST(42 AS TEXT)')), '42')
            empty = "''" if oracle else 'CAST(NULL AS VARCHAR(20))'
            check('empty_yes_character', select(expression('NULL',empty,"42" if oracle else 'CAST(42 AS TEXT)')), '42')
            check('empty_condition', select(expression(empty,'7','8')), 8)
            check('padded_character', select(expression('1',"CAST('a' AS CHAR(3))",'NULL')), 'a  ')
            for label, flag, yes, no, ident, error, pg_value in [
                ('eager_unused_no', 'flag', '7', '1/(id-id)', 1, 'division', 7),
                ('eager_unused_yes', 'flag', '1/(id-id)', '7', 2, 'division', 7),
                ('unused_number_conversion', 'flag', '7', f'CAST(label AS {number})', 1, 'number', 7),
                ('selected_number_conversion', 'NULL', '7', f'CAST(label AS {number})', 1, 'number', None),
            ]:
                wanted_error = error if oracle or label == 'selected_number_conversion' else None
                check(label, select(expression(flag,yes,no),ident), None if wanted_error else pg_value, wanted_error)
            dt = datetime.datetime
            check('date_values', select(expression('flag','day_value',"DATE '2000-01-01'")), dt(2024,2,29))
            check('date_timestamp', select(expression('NULL','day_value','stamp' if oracle else "date_trunc('second',stamp)")), dt(2024,2,29,12,34,56))
            check('timestamp_date', select(expression('flag','stamp','day_value')), dt(2024,2,29,12,34,56,125000))
            check('date_arithmetic', select('('+expression('flag','day_value',"DATE '2000-01-01'")+")-DATE '2024-02-28'"), 1)
            check('scalar_subquery', select(expression('flag',f'(SELECT amount FROM {table} WHERE id=2)','8')), None)
            check('cte_scope', f'WITH local_rows AS (SELECT * FROM {table}) SELECT '+expression('flag','amount','8')+' FROM local_rows WHERE id=2', 8)
            bind = ':flag' if driver else '%s'
            for value in [1,None]:
                check('prepared_'+('nonnull' if value else 'null'), select(expression(f'CAST({bind} AS {number})','7','8')), 7 if value else 8, parameters={'flag':value} if driver else (value,))

            check('quoted_empty_character', select('"NVL2"(NULL,\'\',42)' if oracle else expression('NULL','CAST(NULL AS VARCHAR(20))','CAST(42 AS TEXT)')), '42')
            check('quoted_date_timestamp', select('"NVL2"(NULL,DATE \'2024-01-01\',stamp)' if oracle else expression('NULL',"DATE '2024-01-01'","date_trunc('second',stamp)")), dt(2024,2,29,12,34,56))
            check('quoted_upper_builtin', select('"NVL2"(flag,7,8)' if oracle else expression('flag','7','8')), 7)
            check('empty_yes_null_value', select(expression('1',empty,"42" if oracle else 'CAST(42 AS TEXT)')), None)
            yes, no = ("N'α'", "N'β'") if oracle else ("'α'", "'β'")
            check('national_nonnull', select(expression('flag',yes,no)), 'α')
            check('national_null', select(expression('flag',yes,no),2), 'β')
            yes, no = ("N'😀'", "N'𐐷'") if oracle else ("'😀'", "'𐐷'")
            check('national_length', select('LENGTH('+expression('flag',yes,no)+')'), 2 if oracle else 1)
            yes = "CAST(TIMESTAMP '2024-01-01 00:00:00.12' AS TIMESTAMP(2))"
            no = "TIMESTAMP '2024-02-29 12:34:56.123456'"
            check('timestamp_precision', select(expression('NULL',yes,no)), dt(2024,2,29,12,34,56,123456))
            no = 'label' if oracle else 'CAST(label AS DATE)'
            check('unused_date_conversion', select(expression('flag','day_value',no)), None if oracle else dt(2024,2,29), 'date' if oracle else None)
            no = "'29-FEB-24 12.34.56 PM'" if oracle else "TIMESTAMP '2024-02-29 12:34:56'"
            check('constant_timestamp_character', select(expression('flag',"TIMESTAMP '2024-01-01 00:00:00'",no)), None if oracle else dt(2024,1,1), 'type' if oracle else None)

            check('shared_nvl_national', select("NVL(N'α',N'β')" if oracle else "COALESCE('α','β')"), 'α')
            check('national_literal_length', select("LENGTH(N'😀')" if oracle else "LENGTH('😀')"), 2 if oracle else 1)
            check('national_literal_padding', select("CASE WHEN N'a'=N'a ' THEN 1 ELSE 0 END" if oracle else "CASE WHEN 'a'='a ' THEN 1 ELSE 0 END"), 1 if oracle else 0)
            check('national_empty_length', select("LENGTH(N'')" if oracle else "LENGTH(CAST(NULL AS TEXT))"), None)
            check('national_empty_nvl', select("NVL(N'',7)" if oracle else "COALESCE(CAST(NULL AS TEXT),CAST(7 AS TEXT))"), '7')

            for mode in ['sql','procedure','embedded_sql']:
                for label, flag, yes, no, wanted, calls in [
                    ('unused_no','1','f()','f()',1,2 if oracle else 1),
                    ('unused_yes','NULL','f()','f()',2 if oracle else 1,2 if oracle else 1),
                    ('all_arguments','f()','f()','f()',2,3 if oracle else 2),
                ]:
                    seq, fn, caller = [prefix+uuid.uuid4().hex[:4].upper()+part for part in ['S','F','C']]
                    execute(f'CREATE SEQUENCE {seq} START WITH 1');made.append(('SEQUENCE',seq))
                    definition = (f'CREATE FUNCTION {fn} RETURN INTEGER IS BEGIN RETURN {seq}.NEXTVAL; END;' if oracle else f"CREATE FUNCTION {fn}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RETURN nextval('{seq}'); END$$")
                    execute(definition);made.append(('FUNCTION',fn+('' if driver else '()')))
                    expr = expression(flag,yes,no).replace('f()',fn+'()')
                    if mode!='sql':
                        body = ('RETURN '+expr+';' if mode=='procedure' else 'SELECT '+expr+' INTO n'+(' FROM DUAL' if oracle else '')+'; RETURN n;')
                        definition = (f'CREATE FUNCTION {caller} RETURN INTEGER IS n INTEGER; BEGIN {body} END;' if oracle else f'CREATE FUNCTION {caller}() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE n INTEGER; BEGIN {body} END$$')
                        execute(definition);made.append(('FUNCTION',caller+('' if driver else '()')));expr=caller+'()'
                    check(mode+'_'+label, 'SELECT '+expr+(' FROM DUAL' if oracle else ''), wanted)
                    current = seq+'.CURRVAL FROM DUAL' if oracle else "currval('"+seq+"')"
                    check(mode+'_'+label+'_calls','SELECT '+current,calls)
        finally:
            for kind,name in reversed(made):
                try: execute('DROP '+kind+' '+name+(' PURGE' if driver and kind=='TABLE' else ''))
                except Exception as e: report['cleanup_errors'].append(dict(kind=kind,detail=str(e)[:300]))
    return report


def run(source_factory,target_factory):
    with source_factory() as conn: oracle=type(conn).__module__.startswith('oracledb')
    source,target=observe(source_factory,oracle),observe(target_factory,oracle)
    cases=[dict(id=a['id'],source=a,bicdb=b,source_qualified=a['qualified'],matched=a['qualified'] and b['qualified']) for a,b in zip(source['cases'],target['cases'],strict=True)]
    cleanup=source['cleanup_errors']+target['cleanup_errors']
    return dict(format=1,application_qualified=False,reference_version=source['server_version'],cases=cases,total=len(cases),matched=sum(c['matched'] for c in cases),source_qualified=all(c['source_qualified'] for c in cases),cleanup_errors=cleanup,success=all(c['matched'] for c in cases) and not cleanup)


if __name__=='__main__': raise SystemExit(comparison_main(runner=run,description=__doc__))
