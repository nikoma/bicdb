#!/usr/bin/env python3
"""Load the pinned Oracle HR adaptation through psycopg's PostgreSQL protocol."""
import argparse
import json
import time
from pathlib import Path
import psycopg

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dsn', help='PostgreSQL DSN for an empty oracle-hr-v1 database')
    args = parser.parse_args()
    fixture = json.loads((ROOT/'migration.json').read_text())
    deadline = time.monotonic() + 30
    while True:
        try:
            connection = psycopg.connect(args.dsn, options='-c bicdb.sql_profile=oracle-hr-v1', autocommit=True)
            break
        except psycopg.OperationalError as error:
            if error.sqlstate != '57P03' or time.monotonic() >= deadline:
                raise
            time.sleep(0.1)
    with connection:
        assert connection.info.parameter_status('bicdb_sql_profile') == 'oracle-hr-v1'
        with connection.cursor() as cursor:
            for index, statement in enumerate(fixture['statements']):
                try: cursor.execute(statement, prepare=False)
                except Exception as error:
                    raise RuntimeError(f'HR migration statement {index} failed') from error
            counts = {}
            for table in ['regions','countries','locations','departments','jobs','employees','job_history']:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                counts[table] = int(cursor.fetchone()[0])
            assert counts == fixture['expected_rows']
            cursor.execute('SELECT first_name FROM employees WHERE employee_id = %s', (100,))
            assert cursor.fetchone()[0] == 'Steven'
            cursor.execute('SELECT COUNT(*) FROM emp_details_view')
            joined = int(cursor.fetchone()[0]); assert joined == 106
            cursor.execute("SELECT NVL(NULL, 'fallback'), 'A' || NULL, DUMMY FROM DUAL")
            assert cursor.fetchone() == ('fallback','A','X')
            with connection.transaction():
                cursor.execute("CALL add_job_history(100, DATE '2020-01-01', DATE '2021-01-01', 'AD_PRES', 90)")
            cursor.execute('SELECT COUNT(*) FROM job_history')
            assert int(cursor.fetchone()[0]) == counts['job_history'] + 1
            try:
                with connection.transaction():
                    cursor.execute("CALL add_job_history(100, DATE '2030-01-01', DATE '2029-01-01', 'AD_PRES', 90)")
            except psycopg.errors.CheckViolation:
                pass
            else:
                raise AssertionError('invalid procedure write was accepted')
            cursor.execute('SELECT COUNT(*) FROM job_history')
            assert int(cursor.fetchone()[0]) == counts['job_history'] + 1
    print(json.dumps(dict(profile='oracle-hr-v1', driver='psycopg', driver_version=psycopg.__version__,
                          source_commit=fixture['source_commit'], migration_statements=len(fixture['statements']),
                          initial_counts=counts, joined_view_rows=joined, procedure_commit=True,
                          procedure_constraint_rollback=True, parameterized_query=True,
                          adaptations=fixture['adaptations'], oracle_database_comparison=False), indent=2))


if __name__ == '__main__': main()
