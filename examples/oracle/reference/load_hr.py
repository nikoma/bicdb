#!/usr/bin/env python3
"""Load the same adapted HR fixture into a dedicated, empty Oracle schema."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', type=Path, required=True)
    args = parser.parse_args()
    import oracledb
    config = json.loads(args.oracle_config.read_text())
    if config['user'].upper() in {'SYS', 'SYSTEM', 'PDBADMIN'}:
        raise ValueError('use a dedicated, unprivileged fixture schema')
    password = Path(config['password_file'])
    if not password.is_absolute(): password = args.oracle_config.parent / password
    fixture = json.loads((Path(__file__).resolve().parent.parent / 'hr/migration.json').read_text())
    with oracledb.connect(user=config['user'], password=password.read_text().strip(),
                          host=config['host'], port=config['port'],
                          service_name=config['service_name']) as connection:
        connection.call_timeout = 10000
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM user_objects')
            if cursor.fetchone()[0] != 0:
                raise ValueError('fixture import requires an empty Oracle schema; no objects were removed')
            for index, sql in enumerate(fixture['statements']):
                try:
                    cursor.execute(sql)
                except oracledb.Error as error:
                    # DDL commits independently. Do not pretend this import is
                    # atomic or automatically drop partially imported objects.
                    raise RuntimeError(f'HR import failed at statement {index}; recreate the dedicated schema before retrying') from error
            counts = {}
            for table in fixture['expected_rows']:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                counts[table] = cursor.fetchone()[0]
            if counts != fixture['expected_rows']:
                raise AssertionError('Oracle fixture row counts differ from the manifest')
            cursor.execute("SELECT object_name, object_type FROM user_objects WHERE status = 'INVALID'")
            invalid = cursor.fetchall()
            if invalid:
                raise AssertionError(f'Oracle fixture has invalid objects: {invalid}')
        connection.commit()
        print(json.dumps(dict(oracle_database_version=connection.version, counts=counts,
                              statements=len(fixture['statements']), invalid_objects=invalid,
                              source_commit=fixture['source_commit']), indent=2))


if __name__ == '__main__': main()
