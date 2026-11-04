"""Runtime BETWEEN evaluation in SQL and procedural language contexts.

Oracle SQL and PostgreSQL repeat the tested expression; Oracle PL/SQL retains
it. Nonce sequences observe evaluation counts without changing application
records. This does not qualify every planner-time constant-folding behavior.
"""
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main

CASES = [
    ('equal', 'f() BETWEEN 1 AND 1', (0, 2), (1, 1)),
    ('inside', 'f() BETWEEN 0 AND 100', (1, 2), (1, 1)),
    ('lower_false', 'f() BETWEEN 5 AND 100', (0, 1), (0, 1)),
    ('false_skips_high', 'f() BETWEEN 5 AND f()+100', (0, 1), (0, 1)),
    ('order_both_bounds', 'f() BETWEEN f()-2 AND f()', (1, 4), (1, 3)),
    ('false_both_bounds', 'f() BETWEEN f()+10 AND f()', (0, 2), (0, 2)),
    ('not_between', 'f() NOT BETWEEN 1 AND 1', (1, 2), (0, 1)),
    ('runtime_null_low', 'f() BETWEEN lo AND f()+100', (2, 3), (2, 2)),
    ('runtime_null_high', 'f() BETWEEN 0 AND hi', (2, 2), (2, 1)),
    ('runtime_both_null', 'f() BETWEEN lo AND hi', (2, 2), (2, 1)),
]


def observe(factory, oracle):
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

        matrix = [('sql', case) for case in CASES[:7]] + [('procedure', case) for case in CASES]
        matrix += [('embedded_sql', CASES[0]), ('anonymous', CASES[0])]
        if not oracle:
            matrix += [('sql', ('tuple', '(f(),0) BETWEEN (1,0) AND (1,0)', (0, 2), (0, 2)))]
        for mode, (label, template, sql_expected, plsql_expected) in matrix:
            prefix = 'BICDB_BE_' + uuid.uuid4().hex[:12].upper()
            seq, fn, caller = [prefix + part for part in ['S', 'F', 'C']]
            made = []
            procedural = oracle and mode in ['procedure', 'anonymous']
            want, calls = plsql_expected if procedural else sql_expected
            if mode == 'anonymous':
                want = None
            row = dict(id=mode + '_' + label, expected=dict(value=canonical(want), sequence=canonical(calls)))
            try:
                execute(f'CREATE SEQUENCE {seq} START WITH 1')
                made.append(('SEQUENCE', seq))
                definition = (f'CREATE FUNCTION {fn} RETURN INTEGER IS BEGIN RETURN {seq}.NEXTVAL; END;' if oracle else
                              f"CREATE FUNCTION {fn}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RETURN nextval('{seq}'); END$$")
                execute(definition)
                made.append(('FUNCTION', fn + ('' if oracle_driver else '()')))
                expression = template.replace('f()', fn + '()')
                if mode == 'procedure':
                    body = 'b:=' + expression + '; IF b THEN RETURN 1; ELSIF NOT b THEN RETURN 0; ELSE RETURN 2; END IF;'
                    definition = (f'CREATE FUNCTION {caller} RETURN INTEGER IS lo INTEGER:=NULL; hi INTEGER:=NULL; b BOOLEAN; BEGIN {body} END;' if oracle else
                                  f'CREATE FUNCTION {caller}() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE lo INTEGER:=NULL; hi INTEGER:=NULL; b BOOLEAN; BEGIN {body} END$$')
                elif mode == 'embedded_sql':
                    body = 'SELECT CASE WHEN ' + expression + ' THEN 1 ELSE 0 END INTO n' + (' FROM DUAL' if oracle else '') + '; RETURN n;'
                    definition = (f'CREATE FUNCTION {caller} RETURN INTEGER IS n INTEGER; BEGIN {body} END;' if oracle else
                                  f'CREATE FUNCTION {caller}() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE n INTEGER; BEGIN {body} END$$')
                if mode in ['procedure', 'embedded_sql']:
                    execute(definition)
                    made.append(('FUNCTION', caller + ('' if oracle_driver else '()')))
                    statement = 'SELECT ' + caller + '()' + (' FROM DUAL' if oracle else '')
                elif mode == 'anonymous':
                    body = 'b:=' + expression + '; IF b THEN NULL; END IF;'
                    statement = 'DECLARE b BOOLEAN; BEGIN ' + body + ' END;' if oracle else 'DO $$DECLARE b BOOLEAN; BEGIN ' + body + ' END$$'
                else:
                    statement = 'SELECT CASE WHEN ' + expression + ' THEN 1 ELSE 0 END' + (' FROM DUAL' if oracle else '')
                rows = execute(statement)
                actual = dict(value=canonical(rows[0][0] if rows is not None else None))
                current = seq + '.CURRVAL FROM DUAL' if oracle else "currval('" + seq + "')"
                actual['sequence'] = canonical(execute('SELECT ' + current)[0][0])
                row.update(actual=actual, qualified=actual == row['expected'])
            except Exception as error:
                row.update(exception=type(error).__name__, sqlstate=getattr(error, 'sqlstate', None),
                           detail=str(error)[:600], qualified=False)
            finally:
                for kind, name in reversed(made):
                    try:
                        execute('DROP ' + kind + ' ' + name)
                    except Exception as error:
                        report['cleanup_errors'].append(dict(kind=kind, exception=type(error).__name__, detail=str(error)[:300]))
            report['cases'].append(row)
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
                success=len(cases) == (19 if oracle else 20) and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
