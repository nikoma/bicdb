"""Qualify Oracle FK profile upgrade and old-writer rejection on a fresh database.

Both executables must implement the local oracle_server example's CLI. This
creates only disposable local state; it never opens an application database.
The previous binary must reproduce the former unchanged-key exemption.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

import psycopg


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def port_number():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def command(binary, database, port):
    return [str(binary), str(database), str(port), 'oracle-preview-v1', '8', '4']


@contextmanager
def server(binary, database, log):
    port = port_number()
    with log.open('wb') as output:
        child = subprocess.Popen(command(binary, database, port), stdout=output,
                                 stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError('owned qualification server exited during startup')
                try:
                    connection = psycopg.connect(
                        host='127.0.0.1', port=port, user='bicdb', dbname='bicdb',
                        options='-c bicdb.sql_profile=oracle-preview-v1',
                        autocommit=True, connect_timeout=1)
                    break
                except psycopg.OperationalError as error:
                    if error.sqlstate is not None:
                        raise
                    time.sleep(.1)
            else:
                raise RuntimeError('owned qualification server readiness timed out')
            with connection:
                yield connection
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            if child.returncode != 0:
                raise RuntimeError('owned qualification server did not shut down cleanly')


def expect_row(connection, expected):
    row = connection.execute('SELECT pid,note FROM c WHERE id=1').fetchone()
    if tuple(row) != expected:
        raise AssertionError('foreign-key fixture values changed unexpectedly')


def run(current, previous):
    report = dict(format=1, current_binary_sha256=digest(current),
                  previous_binary_sha256=digest(previous), checks=[],
                  application_qualified=False, success=False)
    with tempfile.TemporaryDirectory(prefix='bicdb-fk-writer-fence-') as name:
        root = Path(name)
        database = root / 'database'
        with server(previous, database, root / 'legacy.log') as connection:
            for sql in [
                'CREATE TABLE p(id INTEGER PRIMARY KEY)',
                'CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER,note INTEGER,payload BLOB)',
                'CREATE TABLE blobs(id INTEGER PRIMARY KEY,payload BLOB)',
                "INSERT INTO blobs VALUES(1,HEXTORAW('00FF'))",
                "INSERT INTO c VALUES(1,999,0,HEXTORAW('11'))",
                'ALTER TABLE c ADD CONSTRAINT legacy_fk FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED NOT VALID',
                'BEGIN', 'UPDATE c SET pid=pid,note=2 WHERE id=1', 'COMMIT',
            ]:
                connection.execute(sql)
            expect_row(connection, (999, 2))
        report['checks'].append('previous_binary_reproduces_unchanged_key_exemption')
        with server(current, database, root / 'upgrade.log') as connection:
            expect_row(connection, (999, 2))
            connection.execute('BEGIN')
            connection.execute("DECLARE d BLOB; BEGIN SELECT payload INTO d FROM c WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW('22')); END;", prepare=False)
            connection.execute('COMMIT')
            payload = connection.execute('SELECT payload FROM c WHERE id=1').fetchone()[0]
            if bytes(payload) != bytes.fromhex('1122'):
                raise AssertionError('native BLOB append changed legacy foreign-key behavior')
            report['checks'].append('native_blob_append_does_not_invent_foreign_key_assignment_intent')
            for sql in ['BEGIN', 'UPDATE c SET note=3 WHERE id=1', 'COMMIT',
                        'BEGIN', 'UPDATE c SET pid=pid,note=4 WHERE id=1']:
                connection.execute(sql)
            try:
                connection.execute('COMMIT')
            except psycopg.Error as error:
                if error.sqlstate != '23503':
                    raise
            else:
                raise AssertionError('explicit unchanged invalid FK committed after upgrade')
            connection.execute('ROLLBACK')
            expect_row(connection, (999, 3))
        report['checks'].append('current_binary_upgrades_legacy_fk_contract_without_row_rewrite')
        with (root / 'rejected.log').open('wb') as output:
            child = subprocess.Popen(command(previous, database, port_number()),
                                     stdout=output, stderr=subprocess.STDOUT)
            try:
                code = child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
                raise AssertionError('previous binary opened the upgraded database')
        rejection = (root / 'rejected.log').read_text(errors='replace')
        if code == 0 or 'database SQL profile does not match server configuration' not in rejection:
            raise AssertionError('previous binary did not reject the durable profile contract')
        report['checks'].append('previous_binary_rejects_upgraded_profile_before_serving')
        with server(current, database, root / 'reopen.log') as connection:
            expect_row(connection, (999, 3))
            payload = connection.execute('SELECT payload FROM c WHERE id=1').fetchone()[0]
            if bytes(payload) != bytes.fromhex('1122'):
                raise AssertionError('native BLOB append did not survive rejection and restart')
            for sql in ['INSERT INTO p VALUES(999)', 'BEGIN',
                        'UPDATE c SET pid=pid,note=4 WHERE id=1', 'COMMIT']:
                connection.execute(sql)
            expect_row(connection, (999, 4))
            payload = connection.execute('SELECT payload FROM blobs WHERE id=1').fetchone()[0]
            if bytes(payload) != bytes.fromhex('00FF'):
                raise AssertionError('BLOB payload changed across profile upgrade/reopen')
            connection.execute('CREATE TABLE more_blobs(id INTEGER PRIMARY KEY,payload BLOB)')
        report['checks'].append('current_binary_reopens_repairs_reference_and_preserves_blob_values')
        # Creating another BLOB table must not downgrade the new profile marker.
        with (root / 'no-downgrade.log').open('wb') as output:
            child = subprocess.Popen(command(previous, database, port_number()),
                                     stdout=output, stderr=subprocess.STDOUT)
            try:
                code = child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
                raise AssertionError('BLOB schema save downgraded the profile contract')
        if code == 0 or 'database SQL profile does not match server configuration' not in (root / 'no-downgrade.log').read_text(errors='replace'):
            raise AssertionError('new profile fence was not retained')
        report['checks'].append('blob_schema_save_retains_old_writer_fence')
    report['success'] = True
    report['disposable_database_removed'] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--current-server', type=Path, required=True)
    parser.add_argument('--previous-server', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = run(args.current_server.resolve(strict=True), args.previous_server.resolve(strict=True))
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(success=report['success'], checks=len(report['checks']))))


if __name__ == '__main__':
    main()
