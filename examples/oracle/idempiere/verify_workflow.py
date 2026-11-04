"""Recheck recorded business-workflow observations without applying repairs.

This checks a qualified fixture's exact records, not arbitrary application
instances. Use the PostgreSQL connection of the corresponding Oracle-profile
fixture, with BICDB_DSN supplied in the environment. Expected SQL is trusted
repository input. Passing does not establish full application compatibility.
"""
import argparse
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path


def checks_from(document):
    checks = []
    for name, item in document.items():
        if isinstance(item, dict) and 'sql' in item and 'rows' in item:
            checks.append((name, item))
    for index, item in enumerate(document.get('preservation_checks', []), 1):
        checks.append((f'preservation-{index}', item))
    if not checks:
        raise ValueError('expected observations contain no SQL checks')
    for name, item in checks:
        if not isinstance(item.get('sql'), str) or not item['sql'].lstrip().upper().startswith('SELECT '):
            raise ValueError('expected SELECT observation required: ' + name)
        if ';' in item['sql']:
            raise ValueError('recorded observation must omit statement separators: ' + name)
        if not isinstance(item.get('rows'), list) or any(not isinstance(row, list) for row in item['rows']):
            raise ValueError('expected row arrays required: ' + name)
    return checks


def rows_match(actual, expected):
    if len(actual) != len(expected):
        return False
    for actual_row, expected_row in zip(actual, expected):
        if len(actual_row) != len(expected_row):
            return False
        for value, want in zip(actual_row, expected_row):
            if isinstance(value, (Decimal, int)) and not isinstance(value, bool):
                if isinstance(want, bool) or want is None:
                    return False
                try:
                    if Decimal(str(value)) != Decimal(str(want)):
                        return False
                except InvalidOperation:
                    return False
            elif value != want:
                return False
    return True


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.expected.resolve() == args.report.resolve():
        parser.error('report must not overwrite expected observations')
    try:
        checks = checks_from(json.loads(args.expected.read_text()))
    except ValueError as error:
        parser.error(str(error))
    report = dict(application_qualified=False, read_only=True, checks=[])
    # Keep the checks in a read-only transaction. checks_from rejects statement
    # separators before connecting; prepared execution is not the SQL validator.
    with psycopg.connect(os.environ['BICDB_DSN'], autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute('BEGIN READ ONLY')
            try:
                for name, item in checks:
                    result = dict(name=name, passed=False)
                    try:
                        cursor.execute(item['sql'], prepare=True)
                        actual = cursor.fetchall()
                        result.update(rows=actual, passed=rows_match(actual, item['rows']))
                    except psycopg.Error as error:
                        result.update(error_type=type(error).__name__, sqlstate=error.sqlstate)
                    report['checks'].append(result)
                    if 'error_type' in result:
                        break  # Preserve the original failure, not aborted-transaction noise.
            finally:
                cursor.execute('ROLLBACK')
    report['expected_checks'] = len(checks)
    report['passed_checks'] = sum(item['passed'] for item in report['checks'])
    report['workflow_state_matches'] = report['passed_checks'] == len(checks)
    args.report.write_text(json.dumps(report, default=str, indent=2) + '\n')
    print(f"{report['passed_checks']}/{len(checks)} workflow state checks passed")
    return 0 if report['workflow_state_matches'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
