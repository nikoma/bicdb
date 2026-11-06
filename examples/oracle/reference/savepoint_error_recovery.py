"""Observe Oracle missing-savepoint recovery using only an owned nonce table.

No existing application objects are changed. Original rows, later writes and
valid savepoints are checked after ORA-01086, including a fully rolled-back
transaction. PostgreSQL transport RELEASE is deliberately a separate contract.
"""
import re
import uuid

from persistent_blob_ownership import main as reference_main


def observe(factory, repetitions=2, *, table_name=None):
    if not 1 <= repetitions <= 10:
        raise ValueError('repetitions must be between 1 and 10')
    import oracledb
    table = table_name or 'BICDB_SP_' + uuid.uuid4().hex[:16].upper()
    if not re.fullmatch(r'BICDB_SP_[0-9A-F]{16}', table):
        raise ValueError('owned savepoint probe table must be a nonce identifier')
    report = dict(format=1, reference='oracle', cases=[], cleanup_errors=[],
                  original_objects_mutated=False, target_compared=False,
                  application_qualified=False, implementation_qualified=False)
    created = False
    with factory() as connection:
        connection.autocommit = False
        connection.call_timeout = 15000
        report['source_version'] = connection.version
        report['owned_table'] = table
        with connection.cursor() as cursor:
            def rows():
                cursor.execute('SELECT id FROM ' + table + ' ORDER BY id')
                return [int(row[0]) for row in cursor.fetchall()]

            def missing(name):
                try:
                    cursor.execute('ROLLBACK TO SAVEPOINT ' + name)
                except oracledb.Error as error:
                    return error.args[0].code
                return None

            try:
                cursor.execute('CREATE TABLE ' + table + '(id NUMBER PRIMARY KEY)')
                created = True
                for repeat in range(repetitions):
                    cursor.execute('DELETE FROM ' + table)
                    connection.commit()
                    cursor.execute('INSERT INTO ' + table + ' VALUES(1)')
                    cursor.execute('SAVEPOINT retained')
                    cursor.execute('INSERT INTO ' + table + ' VALUES(2)')
                    actual = dict(error_code=missing('missing'), retained=rows())
                    cursor.execute('INSERT INTO ' + table + ' VALUES(3)')
                    actual['later_write'] = rows()
                    cursor.execute('ROLLBACK TO SAVEPOINT retained')
                    actual['rollback_to_valid'] = rows()
                    expected = dict(error_code=1086, retained=[1, 2],
                                    later_write=[1, 2, 3], rollback_to_valid=[1])
                    report['cases'].append(dict(id='missing_retains_transaction', repeat=repeat,
                                                actual=actual, expected=expected, matched=actual == expected))
                    connection.commit()
                    cursor.execute('SAVEPOINT "JDBC_SAVEPOINT_0"')
                    cursor.execute('INSERT INTO ' + table + ' VALUES(2)')
                    connection.rollback()
                    actual = dict(error_code=missing('"JDBC_SAVEPOINT_0"'))
                    cursor.execute('SAVEPOINT next_object')
                    cursor.execute('INSERT INTO ' + table + ' VALUES(4)')
                    actual['later_write'] = rows()
                    expected = dict(error_code=1086, later_write=[1, 4])
                    report['cases'].append(dict(id='expired_allows_new_work', repeat=repeat,
                                                actual=actual, expected=expected, matched=actual == expected))
                    connection.rollback()
            finally:
                connection.rollback()
                if created:
                    try:
                        cursor.execute('DROP TABLE ' + table + ' PURGE')
                        cursor.execute('SELECT COUNT(*) FROM user_tables WHERE table_name=:name', name=table)
                        if cursor.fetchone()[0] != 0:
                            report['cleanup_errors'].append('owned table remains after DROP')
                    except Exception as error:
                        report['cleanup_errors'].append(type(error).__name__)
    report['connection_closed'] = True
    report['total'] = len(report['cases'])
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['success'] = (report['total'] == repetitions * 2
                         and report['matched'] == report['total'] and not report['cleanup_errors'])
    return report


if __name__ == '__main__':
    raise SystemExit(reference_main(observe, __doc__))
