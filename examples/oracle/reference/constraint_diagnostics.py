"""Constraint identity survives PostgreSQL transport without losing Oracle operation identity."""
import json
import uuid
from row_lock_waits import main as comparison_main


def observe(factory, oracle_mode):
    suffix = uuid.uuid4().hex.upper()
    quote = lambda value: '"' + value.replace('"', '""') + '"'
    cases = []
    cleanup = []
    report = dict(cases=cases, cleanup_errors=cleanup)
    with factory() as conn:
        native_oracle = type(conn).__module__.startswith('oracledb')
        conn.autocommit = not native_oracle
        report['server_version'] = conn.version if native_oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall() if cursor.description else cursor.rowcount

        owner = execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL" if oracle_mode else 'SELECT current_schema()')[0][0]

        def capture_error(sql, table, constraint, code, message, label):
            item = dict(id=label, qualified=False)
            cases.append(item)
            if not native_oracle:
                execute('BEGIN')
            try:
                execute(sql)
                item['unexpected_success'] = True
            except Exception as error:
                if native_oracle:
                    from oracledb import DatabaseError
                    if not isinstance(error, DatabaseError):
                        raise
                    info = error.args[0]
                    actual = dict(code=info.code, message=info.message)
                    valid = info.code == code and info.message.startswith(message)
                else:
                    import psycopg
                    if not isinstance(error, psycopg.Error):
                        raise
                    diag = error.diag
                    actual = dict(sqlstate=error.sqlstate, message=diag.message_primary,
                                  schema=diag.schema_name, table=diag.table_name,
                                  constraint=diag.constraint_name)
                    valid = (error.sqlstate == '23505' and diag.schema_name == owner
                             and diag.table_name == table and diag.constraint_name == constraint)
                    if oracle_mode:
                        try:
                            detail = json.loads(diag.message_detail or '{}')
                        except ValueError:
                            detail = {}
                        actual['oracle_error_stack'] = detail.get('oracle_error_stack')
                        valid = (valid and (diag.message_primary or '').startswith(message)
                                 and bool(actual['oracle_error_stack'])
                                 and actual['oracle_error_stack'][0].get('code') == -code)
                    else:
                        valid = valid and not (diag.message_primary or '').startswith('ORA-')
                item['actual'] = actual
                item['identity_qualified'] = valid
            finally:
                conn.rollback()
            state = execute(f'SELECT key_value,a,b FROM {quote(table)} ORDER BY key_value,a,b')
            state = [[str(value) if value is not None else None for value in row] for row in state]
            item['rows_after_rollback'] = state
            expected = [['1', '10', '100'], ['2', '20', '200']] if label.startswith(('plain_', 'quoted_')) else [['1', '10', '100'], ['1', '10', '100']]
            item['qualified'] = bool(item.get('identity_qualified')) and state == expected

        for variant in ['plain', 'quoted', 'ddl']:
            table = ('Mixed.Table_' if variant == 'quoted' else 'BICDB_DIAG_') + variant + '_' + suffix
            pk = 'PK_' + variant + '_' + suffix
            uq = ('Mixed.Constraint_' if variant == 'quoted' else 'UQ_') + variant + '_' + suffix
            ix = ('Spaced Index ' if variant == 'quoted' else 'IX_') + variant + '_' + suffix
            try:
                definition = (f'key_value NUMERIC(10,0) CONSTRAINT {quote(pk)} PRIMARY KEY,a NUMERIC(10,0) CONSTRAINT {quote(uq)} UNIQUE,b NUMERIC(10,0)'
                              if variant != 'ddl' else 'key_value NUMERIC(10,0),a NUMERIC(10,0),b NUMERIC(10,0)')
                execute(f'CREATE TABLE {quote(table)}({definition})')
                if variant != 'ddl':
                    execute(f'CREATE UNIQUE INDEX {quote(ix)} ON {quote(table)}(b)')
                execute(f'INSERT INTO {quote(table)} VALUES(1,10,100)')
                execute(f'INSERT INTO {quote(table)} VALUES({"2,20,200" if variant != "ddl" else "1,10,100"})')
                conn.commit()
                if variant == 'ddl':
                    controls = [
                        ('primary_validation', f'ALTER TABLE {quote(table)} ADD CONSTRAINT {quote(pk)} PRIMARY KEY(key_value)', pk, 2437, f'ORA-02437: cannot validate ({owner}.{pk}) - primary key violated'),
                        ('unique_validation', f'ALTER TABLE {quote(table)} ADD CONSTRAINT {quote(uq)} UNIQUE(a)', uq, 2299, f'ORA-02299: cannot validate ({owner}.{uq}) - duplicate keys found'),
                        ('index_build', f'CREATE UNIQUE INDEX {quote(ix)} ON {quote(table)}(b)', ix, 1452, 'ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found'),
                    ]
                else:
                    controls = [(name, sql, constraint, 1, f'ORA-00001: unique constraint ({owner}.{constraint}) violated') for name, sql, constraint in [
                        ('primary_insert', f'INSERT INTO {quote(table)} VALUES(1,30,300)', pk),
                        ('unique_insert', f'INSERT INTO {quote(table)} VALUES(3,10,300)', uq),
                        ('index_insert', f'INSERT INTO {quote(table)} VALUES(3,30,100)', ix),
                        ('primary_update', f'UPDATE {quote(table)} SET key_value=1 WHERE key_value=2', pk),
                        ('unique_update', f'UPDATE {quote(table)} SET a=10 WHERE key_value=2', uq),
                        ('index_update', f'UPDATE {quote(table)} SET b=100 WHERE key_value=2', ix),
                    ]]
                for name, sql, constraint, code, message in controls:
                    capture_error(sql, table, constraint, code, message, variant + '_' + name)
            except Exception as error:
                report.setdefault('execution_errors', []).append(dict(variant=variant, error=type(error).__name__ + ':' + str(error)[:1000]))
            finally:
                conn.rollback()
                try:
                    conn.autocommit = True
                    execute('DROP TABLE ' + quote(table) + (' PURGE' if native_oracle else ''))
                except Exception as error:
                    code = getattr(getattr(error, 'args', [None])[0], 'code', None)
                    if code != 942 and getattr(error, 'sqlstate', None) != '42P01':
                        cleanup.append(str(error)[:300])
                conn.autocommit = not native_oracle
    return report


def run(source_factory, target_factory):
    with source_factory() as conn:
        oracle_mode = type(conn).__module__.startswith('oracledb')
    source = observe(source_factory, oracle_mode)
    target = observe(target_factory, oracle_mode)
    source_cases = {case['id']: case for case in source['cases']}
    target_cases = {case['id']: case for case in target['cases']}
    cases = [dict(id=name, source=case, bicdb=target_cases.get(name), source_qualified=case['qualified'],
                  matched=case['qualified'] and bool(target_cases.get(name, {}).get('qualified')))
             for name, case in source_cases.items()]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False, reference_version=source['server_version'],
                cases=cases, total=15, matched=sum(case['matched'] for case in cases),
                source_qualified=len(source_cases) == 15 and all(case['source_qualified'] for case in cases),
                cleanup_errors=cleanup, source_execution_errors=source.get('execution_errors', []),
                target_execution_errors=target.get('execution_errors', []),
                success=len(source_cases) == len(target_cases) == 15 and all(case['matched'] for case in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
