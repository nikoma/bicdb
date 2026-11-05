"""Typed timestamp snapshots retain their meaning in queries and mutations."""
import datetime
import decimal
import uuid
from row_lock_waits import main as comparison_main


class TextTimestamp:
    def __init__(self, value, spelling=None):
        self.value = value
        self.spelling = spelling


class InferredTimestamp(TextTimestamp):
    """A JDBC-style text value whose SQL type is inferred from its column."""


def observe(factory, coarse_type=None):
    table = 'BICDB_TM_' + uuid.uuid4().hex[:8].upper()
    old = datetime.datetime(2000, 1, 1, microsecond=123456)
    later = old + datetime.timedelta(seconds=1)
    first = TextTimestamp(old, '2000-01-01 00:00:00.123456+00')
    plain = TextTimestamp(old, '2000-01-01T00:00:00.123456')
    offset = TextTimestamp(old, '2000-01-01 00:00:00.123456+05:30')
    wrong = TextTimestamp(old + datetime.timedelta(microseconds=1), '2000-01-01 00:00:00.123457+00')
    coarse = datetime.datetime(2000, 1, 1)
    coarse_text = coarse.isoformat(timespec='microseconds')
    original = [[1, 'old', old.isoformat(timespec='microseconds'), coarse_text], [2, 'null', None, None]]
    changed = [[1, 'changed', later.isoformat(timespec='microseconds'), coarse_text], original[1]]
    advanced = [[1, 'advanced', later.isoformat(timespec='microseconds'), coarse_text], original[1]]
    coarse_changed = [[1, 'date', old.isoformat(timespec='microseconds'), (coarse + datetime.timedelta(seconds=1)).isoformat(timespec='microseconds')], original[1]]
    update = f"UPDATE {table} SET note='changed',stamp=TIMESTAMP '2000-01-01 00:00:01.123456' WHERE id=1 AND stamp=?"
    remove = f'DELETE FROM {table} WHERE id=1 AND stamp=?'
    instant = "CAST('1999-12-31 23:00:00.123456 +00:00' AS TIMESTAMP WITH TIME ZONE)"
    scenarios = [
        ('select_snapshot', False, f'SELECT id FROM {table} WHERE id=1 AND stamp=?', [first], [[1]], original),
        ('update_fresh_snapshot', False, update, [first], 1, changed),
        ('update_stale_snapshot', True, update, [first], 0, advanced),
        ('delete_matching_snapshot', False, remove, [first], 1, [original[1]]),
        ('delete_stale_snapshot', True, remove, [first], 0, advanced),
        ('fractional_mismatch', False, update, [wrong], 0, original),
        ('reversed_equality', False, update.replace('stamp=?', '?=stamp'), [first], 1, changed),
        ('timestamp_ignores_offset', False, update, [offset], 1, changed),
        ('binary_timestamp_update', False, update, [old], 1, changed),
        ('inferred_timestamp_update', False, update, [InferredTimestamp(old, first.spelling)], 1, changed),
        ('null_snapshot_matches_no_rows', False, f"UPDATE {table} SET note='wrong' WHERE id=2 AND stamp=?", [TextTimestamp(None)], 0, original),
        ('date_column_snapshot', False, f"UPDATE {table} SET note='date',coarse=TIMESTAMP '2000-01-01 00:00:01' WHERE id=1 AND coarse=?", [TextTimestamp(coarse, '2000-01-01 00:00:00+00')], 1, coarse_changed),
        ('inferred_date_column_snapshot', False, f"UPDATE {table} SET note='date',coarse=TIMESTAMP '2000-01-01 00:00:01' WHERE id=1 AND coarse=?", [InferredTimestamp(coarse, '2000-01-01 00:00:00+00')], 1, coarse_changed),
        ('date_column_fractional_guard', False, f"UPDATE {table} SET note='wrong' WHERE id=1 AND coarse=?", [TextTimestamp(coarse + datetime.timedelta(microseconds=1), '2000-01-01 00:00:00.000001+00')], 0, original),
        ('typed_parameter_pair', False, 'SELECT CASE WHEN ?=? THEN 1 ELSE 0 END', [first, plain], [[1]], original),
        ('mixed_wire_parameter_pair', False, 'SELECT CASE WHEN ?=? THEN 1 ELSE 0 END', [first, old], [[1]], original),
        ('zoned_projection', False, f'SELECT stamp={instant} FROM {table} WHERE id=1', [], [[True]], original),
        ('zoned_projection_reversed', False, f'SELECT {instant}=stamp FROM {table} WHERE id=1', [], [[True]], original),
        ('zoned_filter', False, f'SELECT id FROM {table} WHERE stamp={instant}', [], [[1]], original),
        ('zoned_mutation', False, update.replace('stamp=?', f'stamp={instant}'), [], 1, changed),
    ]
    cases = {name: dict(id=name, expected=dict(result=result, rows=rows), qualified=False)
             for name, _, _, _, result, rows in scenarios}
    cases['caller_rollback_restores_snapshot'] = dict(id='caller_rollback_restores_snapshot', expected=original, qualified=False)
    report = dict(cases=list(cases.values()), cleanup_errors=[])
    made = False
    with factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
        coarse_type = coarse_type or ('DATE' if oracle else 'TIMESTAMP(0)')
        assert coarse_type in ('DATE', 'TIMESTAMP(0)')
        report['coarse_column_type'] = coarse_type
        conn.autocommit = not oracle
        report['server_version'] = conn.version if oracle else conn.info.server_version
        if not oracle:
            from psycopg.adapt import Dumper
            from psycopg.pq import Format

            class TimestampTextDumper(Dumper):
                oid = 1114
                format = Format.TEXT

                def dump(self, value):
                    return None if value.value is None else value.spelling.encode()

            conn.adapters.register_dumper(TextTimestamp, TimestampTextDumper)

            class InferredTimestampDumper(TimestampTextDumper):
                oid = 0

            conn.adapters.register_dumper(InferredTimestamp, InferredTimestampDumper)

        def canon(value):
            if isinstance(value, datetime.datetime):
                return value.isoformat(timespec='microseconds')
            if isinstance(value, decimal.Decimal):
                return int(value)
            return value

        def execute(sql, params=()):
            with conn.cursor() as q:
                if oracle:
                    import oracledb
                    parts = sql.split('?')
                    sql = ''.join(part + (':' + str(i + 1) if i < len(parts) - 1 else '')
                                  for i, part in enumerate(parts))
                    if params:
                        q.setinputsizes(*[oracledb.DB_TYPE_TIMESTAMP] * len(params))
                        params = [value.value if isinstance(value, TextTimestamp) else value for value in params]
                    if sql.startswith('SELECT CASE') and ' FROM ' not in sql:
                        sql += ' FROM DUAL'
                    q.execute(sql, params or None)
                else:
                    q.execute(sql.replace('?', '%s'), params or None, prepare=True)
                return ([[canon(value) for value in row] for row in q.fetchall()]
                        if q.description else q.rowcount)

        def state():
            return execute(f'SELECT id,note,stamp,coarse FROM {table} ORDER BY id')

        try:
            if oracle:
                execute("ALTER SESSION SET TIME_ZONE='Europe/Berlin'")
                execute("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'")
            else:
                execute("SET TIME ZONE 'Europe/Berlin'")
            execute(f'CREATE TABLE {table}(id NUMERIC(10,0) PRIMARY KEY,stamp TIMESTAMP(6),note VARCHAR(12),coarse {coarse_type})')
            made = True
            execute(f"INSERT INTO {table} VALUES(1,TIMESTAMP '2000-01-01 00:00:00.123456','old',TIMESTAMP '2000-01-01 00:00:00')")
            execute(f"INSERT INTO {table} VALUES(2,NULL,'null',NULL)")
            conn.commit()
            for name, advance, sql, params, _, _ in scenarios:
                if not oracle:
                    execute('BEGIN')
                if advance:
                    execute(f"UPDATE {table} SET note='advanced',stamp=TIMESTAMP '2000-01-01 00:00:01.123456' WHERE id=1")
                actual = dict(result=execute(sql, params), rows=state())
                cases[name].update(actual=actual, qualified=actual == cases[name]['expected'])
                conn.rollback()
            actual = state()
            cases['caller_rollback_restores_snapshot'].update(actual=actual, qualified=actual == original)
        except Exception as error:
            report['execution_error'] = type(error).__name__ + ':' + str(error)[:1500]
        finally:
            conn.rollback()
            if made:
                try:
                    conn.autocommit = True
                    execute('DROP TABLE ' + table + (' PURGE' if oracle else ''))
                except Exception as error:
                    report['cleanup_errors'].append(str(error)[:300])
    return report


def run(source_factory, target_factory):
    source = observe(source_factory)
    target = observe(target_factory, coarse_type=source['coarse_column_type'])
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False,
                reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(case['matched'] for case in cases),
                source_qualified=all(case['source_qualified'] for case in cases),
                cleanup_errors=cleanup,
                source_execution_error=source.get('execution_error'),
                target_execution_error=target.get('execution_error'),
                coarse_column_type=source['coarse_column_type'],
                success=len(cases) == 21 and all(case['matched'] for case in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
