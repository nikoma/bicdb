"""Implicit RAW character conversion and independent PostgreSQL binary controls.

Random values are checked by shape, length and volatility, never by matching
random bytes across servers. Nonce routines are removed; no application objects
are modified. This fixture does not qualify RAW columns or general LOB support.
"""
import re
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main


def observe(factory, oracle):
    routine = 'BICDB_RAW_' + uuid.uuid4().hex[:12].upper()
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        oracle_driver = type(conn).__module__.startswith('oracledb')
        report['server_version'] = conn.version if oracle_driver else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                if oracle_driver:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, prepare=True)
                return cursor.fetchall() if cursor.description else None

        def check(name, sql, expected=None, shape=None):
            row = dict(id=name, expected=expected, shape=shape)
            try:
                rows = execute(sql)
                if shape and shape.startswith('raw'):
                    values = [bytes(r[0]) for r in rows]
                    actual = [dict(length=len(v), binary=True) for v in values]
                    passed = len(values) == 1 and len(values[0]) == int(shape[3:])
                elif shape:
                    actual = [[canonical(v) for v in r] for r in rows]
                    passed = len(rows) == 1 and bool(re.fullmatch(shape, rows[0][0]))
                else:
                    actual = [[canonical(v) for v in r] for r in rows] if rows is not None else None
                    passed = actual == expected
                row.update(actual=actual, qualified=passed)
            except Exception as error:
                row.update(exception=type(error).__name__, sqlstate=getattr(error, 'sqlstate', None),
                           detail=str(error)[:500], qualified=False)
            report['cases'].append(row)

        created = False
        try:
            if oracle:
                check('raw_bytes', 'SELECT SYS_GUID() FROM DUAL', shape='raw16')
                check('lower_raw', 'SELECT LOWER(SYS_GUID()) FROM DUAL', shape='[0-9a-f]{32}')
                check('upper_raw', 'SELECT UPPER(SYS_GUID()) FROM DUAL', shape='[0-9A-F]{32}')
                check('cast_raw', 'SELECT CAST(SYS_GUID() AS VARCHAR2(32)) FROM DUAL', shape='[0-9A-F]{32}')
                expressions = [('length_raw', 'LENGTH(SYS_GUID())', 32),
                               ('lengthb_raw', 'LENGTHB(SYS_GUID())', 32),
                               ('substring_raw', 'LENGTH(SUBSTR(SYS_GUID(),1,8))', 8),
                               ('trim_raw', 'LENGTH(TRIM(SYS_GUID()))', 32),
                               ('pattern_raw', "LENGTH(REGEXP_REPLACE(SYS_GUID(),'[0-9A-F]','x'))", 32),
                               ('concat_raw', "LENGTH(SYS_GUID()||'z')", 33),
                               ('volatile_raw', 'CASE WHEN SYS_GUID()=SYS_GUID() THEN 1 ELSE 0 END', 0)]
                for name, expression, want in expressions:
                    check(name, 'SELECT ' + expression + ' FROM DUAL', [[canonical(want)]])
                check('ordinary_prefixed_text', r"SELECT LOWER('\xAB00FF') FROM DUAL", [[r'\xab00ff']])
                execute(f"CREATE FUNCTION {routine} RETURN VARCHAR2 IS guid VARCHAR2(32); BEGIN guid:=LOWER(SYS_GUID()); RETURN SUBSTR(guid,1,8)||'-'||SUBSTR(guid,9,4)||'-'||SUBSTR(guid,13,4)||'-'||SUBSTR(guid,17,4)||'-'||SUBSTR(guid,21,12); END;")
                created = True
                check('procedural_guid_buffer', f'SELECT {routine}() FROM DUAL', shape='[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}')
            else:
                for name, sql, shape in [
                    ('random_binary_result', 'SELECT gen_random_bytes(16)', 'raw16'),
                    ('convert_to_binary_result', "SELECT convert_to('ab','UTF8')", 'raw2'),
                    ('convert_binary_result', "SELECT convert(decode('4142','hex'),'UTF8','UTF8')", 'raw2'),
                    ('digest_binary_result', "SELECT digest('abc','sha256')", 'raw32'),
                    ('hmac_binary_result', "SELECT hmac('abc','key','sha256')", 'raw32'),
                ]:
                    check(name, sql, shape=shape)
                for name, expression, want in [
                    ('bytea_text', "CAST(decode('ab00ff','hex') AS TEXT)", r'\xab00ff'),
                    ('bytea_lower_text', "LOWER(CAST(decode('AB00FF','hex') AS TEXT))", r'\xab00ff'),
                    ('bytea_upper_text', "UPPER(CAST(decode('ab00ff','hex') AS TEXT))", r'\XAB00FF'),
                    ('binary_length', "OCTET_LENGTH(decode('ab00ff','hex'))", canonical(3)),
                    ('hex_encoding', "encode(decode('ab00ff','hex'),'hex')", 'ab00ff'),
                    ('ordinary_prefixed_text', r"LOWER('\xAB00FF')", r'\xab00ff'),
                    ('empty_binary', "OCTET_LENGTH(decode('','hex'))", canonical(0)),
                    ('binary_null', "CAST(CAST(NULL AS BYTEA) AS TEXT)", None),
                ]:
                    check(name, 'SELECT ' + expression, [[want]])
        finally:
            if created:
                try:
                    execute('DROP FUNCTION ' + routine + ('' if oracle_driver else '()'))
                except Exception as error:
                    report['cleanup_errors'].append(dict(kind='FUNCTION', exception=type(error).__name__))
    return report


def run(source_factory, target_factory):
    with source_factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False, reference_version=source['server_version'],
                cases=cases, total=len(cases), matched=sum(c['matched'] for c in cases),
                source_qualified=all(c['source_qualified'] for c in cases), cleanup_errors=cleanup,
                success=len(cases) == 13 and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
