#!/usr/bin/env python3
"""Compare the public Oracle fixtures with BicDB over the PostgreSQL protocol.

Connections come from local configuration, never from the result artifact. A
matching result only qualifies that case on the recorded database versions.
Unexpected differences and execution errors make the command fail. Explicit
error assertions require the exact declared code from each backend.
"""
import argparse
import datetime
import decimal
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def canonical(value):
    """Keep numeric precision, nulls and string padding in comparisons."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, decimal.Decimal)):
        number = decimal.Decimal(value)
        # Decimal.normalize() rounds to the process context (usually 28 digits).
        # Formatting and trimming preserve Oracle's full 38-digit precision.
        text = format(number, 'f')
        if '.' in text:
            text = text.rstrip('0').rstrip('.')
        return {'number': '0' if number == 0 else text}
    if isinstance(value, float):
        raise TypeError('floating point needs an explicit comparison policy')
    if isinstance(value, datetime.datetime):
        return {'timestamp': value.isoformat(timespec='microseconds')}
    if isinstance(value, datetime.date):
        return {'date': value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {'interval': {'days': value.days, 'seconds': value.seconds,
                             'microseconds': value.microseconds}}
    if isinstance(value, bytes):
        return {'bytes': value.hex()}
    raise TypeError(f'unsupported result type: {type(value).__name__}')


def outcome(connection, sql, max_rows=5000):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchmany(max_rows + 1)
            if len(rows) > max_rows:
                raise ValueError('fixture exceeded row limit')
            return {'rows': [[canonical(value) for value in row] for row in rows]}
    except Exception as error:
        # Driver messages can include connection information. Publish only the
        # stable error code and class, never exception text or connection DSNs.
        detail = error.args[0] if error.args else None
        return {'error': type(error).__name__,
                'code': getattr(error, 'sqlstate', None) or getattr(detail, 'full_code', None)}


def compare_cases(oracle, bicdb, cases):
    results = []
    for case in cases:
        reference = outcome(oracle, case['sql'])
        actual = outcome(bicdb, case['sql'])
        matched = 'rows' in reference and 'rows' in actual and reference == actual
        if 'expected_error' in case:
            expected = case['expected_error']
            if not isinstance(expected, dict) or set(expected) != {'oracle', 'bicdb'} or not all(isinstance(code, str) and code for code in expected.values()):
                raise ValueError('expected_error must specify nonempty Oracle and BicDB codes')
            matched = ('error' in reference and 'error' in actual
                       and reference.get('code') == expected['oracle']
                       and actual.get('code') == expected['bicdb'])
        results.append(dict(id=case['id'], matched=matched, oracle=reference, bicdb=actual))
    return results


def read_cases(path):
    content = path.read_bytes()
    fixture = json.loads(content)
    if fixture['format'] != 1 or not fixture['cases']:
        raise ValueError('expected a nonempty format-1 fixture')
    ids = [case['id'] for case in fixture['cases']]
    if len(set(ids)) != len(ids):
        raise ValueError('duplicate case ID')
    return fixture, hashlib.sha256(content).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', required=True, type=Path,
                        help='private JSON: host, port, service_name, user, password_file')
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN',
                        help='environment variable containing the BicDB PostgreSQL DSN')
    parser.add_argument('--cases', type=Path, default=ROOT / 'cases.json')
    parser.add_argument('--profile', choices=('oracle-hr-v1', 'oracle-preview-v1'),
                        default='oracle-hr-v1', help='explicit target database SQL profile')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    import oracledb
    import psycopg
    oracledb.defaults.fetch_decimals = True
    fixture, digest = read_cases(args.cases)
    config = json.loads(args.oracle_config.read_text())
    password_path = Path(config['password_file'])
    if not password_path.is_absolute():
        password_path = args.oracle_config.parent / password_path
    with oracledb.connect(user=config['user'], password=password_path.read_text().strip(),
                          host=config['host'], port=config['port'],
                          service_name=config['service_name']) as oracle:
        oracle.call_timeout = 10000
        with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True,
                             options=f'-c bicdb.sql_profile={args.profile}',
                             connect_timeout=10) as bicdb:
            profile = bicdb.info.parameter_status('bicdb_sql_profile')
            if profile != args.profile:
                raise RuntimeError('server did not confirm the requested SQL profile')
            results = compare_cases(oracle, bicdb, fixture['cases'])
            matched = sum(result['matched'] for result in results)
            report = dict(format=1, created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          oracle_database_version=oracle.version, bicdb_profile=profile,
                          oracle_driver_version=oracledb.__version__, pg_driver_version=psycopg.__version__,
                          fixture_sha256=digest, total=len(results), matched=matched,
                          different=len(results) - matched, cases=results,
                          scope='Query values only; not type metadata, errors, transactions, or full conformance')
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'{matched}/{len(results)} query results match; report: {args.output}')
    return 0 if matched == len(results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
