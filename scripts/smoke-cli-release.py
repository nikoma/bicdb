#!/usr/bin/env python3
"""Exercise the README demo on a packaged CLI, including pgwire and reopen."""
import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('binary', type=Path)
args = parser.parse_args()
binary = str(args.binary.resolve())

def run(*arguments):
    return subprocess.check_output(arguments, text=True, stderr=subprocess.STDOUT, timeout=60).strip()

with tempfile.TemporaryDirectory(prefix='bicdb-release-smoke-') as temporary:
    directory = Path(temporary)
    db = str(directory / 'demo')
    print(run(binary, '--version'))
    run(binary, 'sql', db, """
      CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT NOT NULL);
      INSERT INTO notes VALUES (1, 'Search works offline'), (2, 'Ship fewer services');
      CREATE INDEX notes_search ON notes USING GIN (to_tsvector('english', body));
    """)
    result = run(binary, 'sql', db, '--csv', """
      SELECT id, body FROM notes
      WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'offline');
    """)
    assert result == 'id,body\n1,Search works offline', result
    print('README search after process restart: passed')
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    with (directory / 'server.log').open('w') as log:
        server = subprocess.Popen([binary, 'serve', db, '--host', '127.0.0.1', '--port', str(port)],
                                  stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while True:
                if server.poll() is not None:
                    raise RuntimeError((directory / 'server.log').read_text())
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError('pgwire did not start')
                    time.sleep(0.1)
            result = run('psql', '-X', '-w', '-h', '127.0.0.1', '-p', str(port), '-U', 'bicdb',
                         '-d', 'bicdb', '-At', '-F', ',', '-v', 'ON_ERROR_STOP=1',
                         '-c', 'SELECT * FROM notes ORDER BY id;')
            assert result == '1,Search works offline\n2,Ship fewer services', result
            print('PostgreSQL client reads the same database: passed')
        finally:
            if server.poll() is None:
                os.killpg(server.pid, signal.SIGTERM)
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL)
                    server.wait(timeout=10)
    assert run(binary, 'sql', db, '--csv', 'SELECT COUNT(*) AS total FROM notes;') == 'total\n2'
    print('CLI reopen after server shutdown: passed')
