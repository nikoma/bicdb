"""Typed BETWEEN boundaries, character controls, NULL logic and evaluation count.

PostgreSQL controls include unknown ISO timestamp literals. Oracle controls use
explicit datetime literals and do not qualify implicit NLS conversions. Nonce
objects are removed; application data and configuration are not modified.
"""
import uuid
from join_wildcards import canonical
from row_lock_waits import main as comparison_main


def observe(factory, oracle):
    nonce = 'BICDB_BT_' + uuid.uuid4().hex[:12].upper()
    table, sequence, routine, counter = [nonce + part for part in ['T', 'S', 'F', 'C']]
    report = dict(cases=[], cleanup_errors=[])
    created = []
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
                return [[canonical(v) for v in row] for row in cursor.fetchall()] if cursor.description else None

        def check(name, sql, expected):
            row = dict(id=name, expected=expected)
            try:
                actual = execute(sql)
                row.update(actual=actual, qualified=actual == expected)
            except Exception as error:
                row.update(exception=type(error).__name__, sqlstate=getattr(error, 'sqlstate', None),
                           detail=str(error)[:500], qualified=False)
            report['cases'].append(row)

        def select(expression):
            return 'SELECT ' + expression + (' FROM DUAL' if oracle else '')

        lower = "TIMESTAMP '2026-11-01 00:00:00.0'" if oracle else "'2026-11-01 00:00:00.0'"
        upper = "TIMESTAMP '2026-11-30 00:00:00.000000'" if oracle else "'2026-11-30 00:00:00.000000'"
        bounds = ' BETWEEN ' + lower + ' AND ' + upper
        yes = [[canonical(i)] for i in [1, 2, 3]]
        number = lambda n: [[canonical(n)]]
        try:
            execute(f'CREATE TABLE {table}(id INTEGER PRIMARY KEY,happened TIMESTAMP(6),label VARCHAR(40))')
            created.append(('TABLE', table))
            for key, day in [(1, '2026-11-01'), (2, '2026-11-04'), (3, '2026-11-30'), (4, '2026-12-01')]:
                execute(f"INSERT INTO {table} VALUES({key},TIMESTAMP '{day} 00:00:00','{day} 00:00:00')")
            execute(f'INSERT INTO {table} VALUES(5,NULL,NULL)')
            check('lower_scalar', select("CASE WHEN TIMESTAMP '2026-11-01 00:00:00'" + bounds + ' THEN 1 ELSE 0 END'), number(1))
            check('upper_scalar', select("CASE WHEN TIMESTAMP '2026-11-30 00:00:00'" + bounds + ' THEN 1 ELSE 0 END'), number(1))
            check('row_predicate', f'SELECT id FROM {table} WHERE happened' + bounds + ' ORDER BY id', yes)
            check('negated_predicate', f'SELECT id FROM {table} WHERE happened NOT' + bounds + ' ORDER BY id', number(4))
            truncate = 'TRUNC(happened)' if oracle else "date_trunc('day',happened)"
            check('truncated_predicate', f'SELECT id FROM {table} WHERE ' + truncate + bounds + ' ORDER BY id', yes)
            check('character_control', f"SELECT id FROM {table} WHERE label BETWEEN '2026-11-01 00:00:00.0' AND '2026-11-30 00:00:00.000000' ORDER BY id", [[canonical(i)] for i in [2, 3]])
            check('projected_case', f'SELECT id,CASE WHEN happened' + bounds + f' THEN 1 ELSE 0 END FROM {table} ORDER BY id', [[canonical(i), canonical(int(i <= 3))] for i in range(1, 6)])
            check('aggregate_having', f'SELECT id FROM {table} GROUP BY id HAVING MAX(happened)' + bounds + ' ORDER BY id', yes)
            check('joined_predicate', f'SELECT a.id FROM {table} a JOIN {table} b ON a.id=b.id WHERE a.happened' + bounds + ' ORDER BY a.id', yes)
            check('cte_predicate', f'WITH picked AS (SELECT id,happened FROM {table}) SELECT id FROM picked WHERE happened' + bounds + ' ORDER BY id', yes)
            check('scalar_subquery', f'SELECT a.id FROM {table} a WHERE (SELECT b.happened FROM {table} b WHERE b.id=a.id)' + bounds + ' ORDER BY a.id', yes)
            for name, value, expected in [('null_and_false', '2026-12-01', 0), ('null_and_true', '2026-11-01', 2)]:
                predicate = f"TIMESTAMP '{value} 00:00:00' BETWEEN CAST(NULL AS TIMESTAMP) AND TIMESTAMP '2026-11-30 00:00:00'"
                check(name, select('CASE WHEN ' + predicate + ' THEN 1 WHEN (' + predicate + ') IS NULL THEN 2 ELSE 0 END'), number(expected))
            declaration = (f'CREATE FUNCTION {routine}(p_moment TIMESTAMP) RETURN INTEGER IS BEGIN IF p_moment' + bounds + ' THEN RETURN 1; ELSE RETURN 0; END IF; END;' if oracle else
                           f'CREATE FUNCTION {routine}(p_moment TIMESTAMP) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN IF p_moment' + bounds + ' THEN RETURN 1; ELSE RETURN 0; END IF; END$$')
            execute(declaration)
            created.append(('FUNCTION', routine + ('' if oracle_driver else '(timestamp)')))
            check('stored_parameter', select(f"{routine}(TIMESTAMP '2026-11-01 00:00:00')"), number(1))
            execute(f'CREATE SEQUENCE {sequence} START WITH 1')
            created.append(('SEQUENCE', sequence))
            next_value = sequence + '.NEXTVAL' if oracle else "nextval('" + sequence + "')"
            declaration = (f'CREATE FUNCTION {counter} RETURN INTEGER IS BEGIN IF {next_value} BETWEEN 1 AND 1 THEN RETURN 1; ELSE RETURN 0; END IF; END;' if oracle else
                           f'CREATE FUNCTION {counter}() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN IF {next_value} BETWEEN 1 AND 1 THEN RETURN 1; ELSE RETURN 0; END IF; END$$')
            execute(declaration)
            created.append(('FUNCTION', counter + ('' if oracle_driver else '()')))
            check('volatile_value_evaluation', select(counter + '()'), number(1 if oracle else 0))
            current = sequence + '.CURRVAL' if oracle else "currval('" + sequence + "')"
            check('sequence_evaluation_count', select(current), number(1 if oracle else 2))
        finally:
            for kind, name in reversed(created):
                try:
                    execute(f'DROP {kind} {name}' + (' PURGE' if oracle_driver and kind == 'TABLE' else ''))
                except Exception as error:
                    report['cleanup_errors'].append(dict(kind=kind, exception=type(error).__name__, detail=str(error)[:300]))
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
                success=len(cases) == 16 and all(c['matched'] for c in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
