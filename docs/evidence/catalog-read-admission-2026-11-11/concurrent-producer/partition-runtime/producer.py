"""Observe partition data behavior on an isolated, pinned historical binary.

No application fixture or original test is modified. Observations are retained
even when they contradict Native PostgreSQL; this is not an acceptance test.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

import psycopg

ROOT = Path('/root/bicdb-durability-s3')
SOURCE = 'a68fe5f37d697555d7c337005aac4785e5887f70'
BINARY = ROOT / 'qualification-artifacts-debug' / SOURCE / 'oracle_server'
SHA = '8c2aeb480d1434127817c61e63eeb065b4f6ce8ecf0d4ad3e4b6e29ee34ec50b'
OUT = ROOT / 'diagnostics' / 'partition-runtime-a68-20261112'
OUT.mkdir(parents=True, exist_ok=False)
record = dict(source=SOURCE, status='preflight', application_qualified=False,
              binary=str(BINARY), expected_binary_sha256=SHA,
              profile='postgres', storage_mode='server_paged', fsync=True,
              original_fixture_touched=False, started_unix=time.time(), observations=[])


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save():
    path = OUT / 'result.json'
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(record, indent=2) + '\n')
    tmp.replace(path)


assert sha(BINARY) == SHA
record['producer_sha256'] = sha(__file__)
record['binary_sha256'] = SHA
child = None


def start():
    global child
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    environment = dict(os.environ, BICDB_COMMIT_DURABILITY='all-logs', BICDB_SYNC_OUTBOX='off')
    for key in ['BICDB_PAGE_WAL_UNLOCKED_SYNC', 'BICDB_PAGE_DELTAS']:
        environment.pop(key, None)
    log = (OUT / ('database-' + str(len(record['observations'])) + '.log')).open('xb')
    child = subprocess.Popen([str(BINARY), str(OUT / 'db'), str(port), 'postgres', '32', '4', 'server_paged'],
                             env=environment, stdout=log, stderr=subprocess.STDOUT)
    log.close()
    deadline = time.monotonic() + 30
    while True:
        assert child.poll() is None, 'owned database exited during startup'
        try:
            return psycopg.connect(host='127.0.0.1', port=port, user='bicdb', dbname='bicdb',
                                   autocommit=True, connect_timeout=2,
                                   options='-c bicdb.sql_profile=postgres')
        except psycopg.OperationalError:
            assert time.monotonic() < deadline
            time.sleep(.1)


def stop():
    global child
    if child is not None:
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
                raise AssertionError('owned database failed clean shutdown')
        record.setdefault('database_exits', []).append(child.returncode)
        assert child.returncode == 0, child.returncode
        child = None


def execute(connection, label, sql):
    observation = dict(label=label, sql=sql)
    try:
        cursor = connection.execute(sql)
        observation.update(status='accepted', rows=cursor.fetchall() if cursor.description else [],
                           command=cursor.statusmessage)
    except psycopg.Error as error:
        observation.update(status='rejected', sqlstate=error.sqlstate, error=str(error))
    record['observations'].append(observation)
    save()


save()
try:
    with start() as connection:
        record['status'] = 'observing_live_data'
        for label, sql in [
            ('create_parent', 'CREATE TABLE parent(item_key INT,payload TEXT) PARTITION BY RANGE(item_key)'),
            ('create_child', 'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10)'),
            ('child_ack', "INSERT INTO child VALUES(1,'child-ack')"),
            ('parent_after_child_ack', 'SELECT item_key,payload FROM parent ORDER BY item_key'),
            ('parent_ack', "INSERT INTO parent VALUES(2,'parent-ack')"),
            ('child_after_parent_ack', 'SELECT item_key,payload FROM child ORDER BY item_key'),
            ('bounds_begin', 'BEGIN'),
            ('out_of_bound_insert', "INSERT INTO child VALUES(50,'must-refuse')"),
            ('bounds_rollback', 'ROLLBACK'),
            ('create_default_candidate', 'CREATE TABLE fallback(item_key INT,payload TEXT)'),
            ('attach_default', 'ALTER TABLE parent ATTACH PARTITION fallback DEFAULT'),
            ('parent_after_attach', 'SELECT item_key,payload FROM parent ORDER BY item_key'),
            ('child_after_attach', 'SELECT item_key,payload FROM child ORDER BY item_key'),
        ]:
            execute(connection, label, sql)
    stop()
    for reopen in range(2):
        with start() as connection:
            for relation in ['parent', 'child']:
                execute(connection, 'reopen_' + str(reopen + 1) + '_' + relation,
                        'SELECT item_key,payload FROM ' + relation + ' ORDER BY item_key')
        stop()
    record.update(status='observations_collected', processes_stopped=True, finished_unix=time.time())
    save()
finally:
    stop()
    record['processes_stopped'] = child is None
    save()
print(json.dumps(record), flush=True)
