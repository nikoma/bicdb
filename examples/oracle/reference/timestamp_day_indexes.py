"""Compare datetime day-index build and mutation semantics with Oracle.

Other format masks, zoned timestamps, concurrency, and production maintenance
cost are separate qualification. PostgreSQL date_trunc controls run separately.
"""
import uuid
from row_lock_waits import main as comparison_main


def observe(factory, dialect='oracle'):
    if dialect not in {'oracle', 'postgres'}:
        raise ValueError('expected oracle or postgres reference dialect')
    day = 'TRUNC(seen)' if dialect == 'oracle' else "date_trunc('day',seen)"
    rounded = 'TRUNC(CAST(seen AS TIMESTAMP(0)))' if dialect == 'oracle' else "date_trunc('day',CAST(seen AS TIMESTAMP(0)))"
    report = dict(dialect=dialect, cases=[], cleanup_errors=[])
    with factory() as connection:
        connection.autocommit = True
        oracle = type(connection).__module__.startswith('oracledb')
        with connection.cursor() as cursor:
            for case in ['nonunique_backfill_and_mutation', 'unique_backfill_conflict', 'unique_nulls_and_rollback']:
                table = 'BCD_' + uuid.uuid4().hex[:12].upper()
                index = table + '_D'
                created = False
                item = dict(id=case, qualified=False)
                try:
                    cursor.execute(f'CREATE TABLE {table}(id INTEGER PRIMARY KEY,seen TIMESTAMP)')
                    created = True
                    cursor.execute(f"INSERT INTO {table} VALUES(1,TIMESTAMP '1999-12-31 23:59:59.999999')")
                    cursor.execute(f"INSERT INTO {table} VALUES(2,TIMESTAMP '1999-12-31 00:00:01')")
                    predicate = f"{day}=TIMESTAMP '1999-12-31 00:00:00'"
                    def ids():
                        cursor.execute(f'SELECT id FROM {table} WHERE {predicate} ORDER BY id')
                        return [int(row[0]) for row in cursor.fetchall()]
                    def duplicate(sql, build=False):
                        try: cursor.execute(sql)
                        except Exception as error:
                            code = getattr(error.args[0], 'code', None) if error.args else None
                            if code == (1452 if build else 1) or getattr(error, 'sqlstate', None) == '23505':
                                return True
                            raise
                        return False
                    if case == 'nonunique_backfill_and_mutation':
                        cursor.execute(f'CREATE INDEX {index} ON {table}({day})')
                        actual = [ids()]
                        cursor.execute(f"SELECT id FROM {table} WHERE {rounded}=TIMESTAMP '2000-01-01 00:00:00' ORDER BY id")
                        actual.append([int(row[0]) for row in cursor.fetchall()])
                        cursor.execute(f"UPDATE {table} SET seen=TIMESTAMP '2000-01-01 01:00:00' WHERE id=2")
                        actual.append(ids())
                        cursor.execute(f'DELETE FROM {table} WHERE id=1')
                        actual.append(ids())
                        expected = [[1, 2], [1], [1], []]
                    elif case == 'unique_backfill_conflict':
                        rejected = duplicate(f'CREATE UNIQUE INDEX {index} ON {table}({day})', build=True)
                        cursor.execute(f'DELETE FROM {table} WHERE id=2')
                        cursor.execute(f'CREATE UNIQUE INDEX {index} ON {table}({day})')
                        actual = [rejected, ids()]
                        expected = [True, [1]]
                    else:
                        cursor.execute(f'DELETE FROM {table} WHERE id=2')
                        cursor.execute(f'INSERT INTO {table} VALUES(2,NULL)')
                        cursor.execute(f'INSERT INTO {table} VALUES(3,NULL)')
                        cursor.execute(f'CREATE UNIQUE INDEX {index} ON {table}({day})')
                        rejected = duplicate(f"INSERT INTO {table} VALUES(4,TIMESTAMP '1999-12-31 10:00:00')")
                        connection.autocommit = False
                        cursor.execute('SAVEPOINT before_day_move')
                        cursor.execute(f"UPDATE {table} SET seen=TIMESTAMP '2001-01-01 12:00:00' WHERE id=1")
                        cursor.execute('ROLLBACK TO SAVEPOINT before_day_move')
                        connection.commit()
                        connection.autocommit = True
                        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE seen IS NULL')
                        actual = [rejected, int(cursor.fetchone()[0]), ids()]
                        expected = [True, 2, [1]]
                    if oracle:
                        cursor.execute('SELECT status FROM user_indexes WHERE index_name=:1', [index])
                        ready = cursor.fetchone() == ('VALID',)
                    else:
                        cursor.execute('SELECT i.indisvalid,i.indisready FROM pg_catalog.pg_index i '
                                       'JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE c.relname=%s', [index if dialect == 'oracle' else index.lower()])
                        ready = cursor.fetchone() == (True, True)
                    item.update(actual=actual, expected=expected, executable_index=ready,
                                qualified=actual == expected and ready)
                except Exception as error:
                    item.update(error_type=type(error).__name__, sqlstate=getattr(error, 'sqlstate', None), detail=str(error))
                finally:
                    connection.rollback();connection.autocommit = True
                    if created:
                        try: cursor.execute(f'DROP TABLE {table}' + (' PURGE' if oracle else ''))
                        except Exception as error: report['cleanup_errors'].append(dict(table=table, error_type=type(error).__name__))
                report['cases'].append(item)
    return report


def compare(source_factory, target_factory, dialect):
    source, target = observe(source_factory, dialect), observe(target_factory, dialect)
    return dict(source=source, target=target,
                success=not source['cleanup_errors'] and not target['cleanup_errors']
                and all(item['qualified'] for result in [source, target] for item in result['cases']),
                application_qualified=False)


def run(source_factory, target_factory):
    return compare(source_factory, target_factory, 'oracle')


def run_postgres(source_factory, target_factory):
    return compare(source_factory, target_factory, 'postgres')


if __name__ == '__main__': comparison_main(run)
