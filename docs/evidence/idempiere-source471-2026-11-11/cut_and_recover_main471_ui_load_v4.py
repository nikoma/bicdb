"""Kill only the owned ERP database and capture two recovered business rowsets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument('--prefix', required=True)
parser.add_argument('--verify-only', action='store_true')
args = parser.parse_args()
assert args.prefix.startswith('BicdbLoad471') and args.prefix.isalnum()
base = Path('/root/.local/state/bicdb/idempiere-reference/main471-ui-4lhe0gou')
live = json.loads((base / 'live.json').read_text())
source = 'b0e2498842be7b01e355bb8da1cc249ff16be056'
assert live['source'] == source and not live.get('processes_stopped')
binary = Path(live['binary'])
with binary.open('rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
assert digest == live['binary_sha256'] == 'e23d7c1a078b7ce6722237f35cfa67a822727d9762c928c808f3ce2767c543c2'
output = base / ('multiuser-crash-load-v4-preflight' if args.verify_only else 'multiuser-crash-load-v4')
output.mkdir()
receipt = output / 'result.json'
record = {'source': source, 'binary_sha256': digest, 'prefix': args.prefix,
          'status': 'validating_owned_process', 'captures': [], 'physical_power_loss': False}
def save():
    temporary = receipt.with_suffix('.tmp')
    with temporary.open('w') as file:
        file.write(json.dumps(record, indent=2, default=str) + chr(10))
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(receipt)
save()
process_id = live['database_pid']
process = Path('/proc') / str(process_id)
command = (process / 'cmdline').read_bytes().split(bytes([0]))
assert str(binary).encode() in command and str(base / 'db').encode() in command
assert 'bicdb-main471-ui-instance.service' in (process / 'cgroup').read_text()
assert hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal')
process_start_ticks = (process / 'stat').read_text().rsplit(')', 1)[1].split()[19]
pidfd = os.pidfd_open(process_id)
try:
    with (process / 'exe').open('rb') as executable:
        assert hashlib.file_digest(executable, 'sha256').hexdigest() == digest
    environment = dict(item.split(b'=', 1) for item in (process / 'environ').read_bytes().split(bytes([0])) if b'=' in item)
    assert environment.get(b'BICDB_COMMIT_DURABILITY') == b'all-logs'
    assert environment.get(b'BICDB_SYNC_OUTBOX') == b'off'
    for flag in [b'BICDB_PAGE_WAL_UNLOCKED_SYNC', b'BICDB_PAGE_DELTAS']:
        assert environment.get(flag, b'0') in [b'0', b'off', b'false', b'']
    assert (process / 'stat').read_text().rsplit(')', 1)[1].split()[19] == process_start_ticks
    record.update(database_pid=process_id, process_start_ticks=process_start_ticks,
                  provenance_verified_unix=time.time(), planned_signal='SIGKILL', pidfd_bound=True,
                  actual_loaded_executable_sha256=digest, commit_durability='all-logs',
                  sync_outbox='off', experimental_page_flags_enabled=False)
    save()
    if args.verify_only:
        record.update(status='owned_process_provenance_verified_without_signal', process_crash_performed=False)
        save()
        print(json.dumps(record, default=str), flush=True)
        raise SystemExit(0)
    record.update(cut_unix=time.time(), signal='SIGKILL')
    save()
    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
    record['signal_sent_unix'] = time.time()
    save()
    print(json.dumps(dict(event='owned_process_sigkill_sent', source=source,
                          database_pid=process_id, signal_sent_unix=record['signal_sent_unix'])), flush=True)
finally:
    os.close(pidfd)
record['status'] = 'waiting_owned_ui_shutdown'
save()
deadline = time.monotonic() + 90
while True:
    state = subprocess.check_output(
        ['systemctl', 'show', 'bicdb-main471-ui-instance', '-p', 'ActiveState', '--value'],
        text=True).strip()
    if state not in ['active', 'activating', 'reloading', 'deactivating']:
        break
    assert time.monotonic() < deadline
    time.sleep(.25)
closed = json.loads((base / 'live.json').read_text())
assert closed['processes_stopped']
assert next(item for item in closed['shutdown_results'] if item['role'] == 'database')['returncode'] == -9
record['original_process_shutdown'] = closed['shutdown_results']
record['status'] = 'recovering'
save()
for attempt in [1, 2]:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    environment = {**os.environ, 'BICDB_COMMIT_DURABILITY': 'all-logs', 'BICDB_SYNC_OUTBOX': 'off'}
    for flag in ['BICDB_PAGE_WAL_UNLOCKED_SYNC', 'BICDB_PAGE_DELTAS']:
        environment.pop(flag, None)
    with (output / f'reopen-{attempt}.log').open('xb') as log:
        child = subprocess.Popen(
            [str(binary), 'serve-pg', str(base / 'db'), '--host', '127.0.0.1',
             '--port', str(port), '--sql-profile', 'oracle-preview-v1',
             '--storage-sync', 'durable', '--max-connections', '32', '--max-active-queries', '16'],
            stdout=log, stderr=subprocess.STDOUT, env=environment, start_new_session=True)
    capture = {'reopen': attempt}
    record['captures'].append(capture)
    save()
    try:
        deadline = time.monotonic() + 90
        while True:
            assert child.poll() is None, f'recovery server exited {child.returncode}'
            try:
                connection = psycopg.connect(
                    host='127.0.0.1', port=port, user='IDEMPIERE_QUAL', dbname='bicdb',
                    options='-c bicdb.sql_profile=oracle-preview-v1',
                    autocommit=True, connect_timeout=1)
                break
            except psycopg.OperationalError:
                assert time.monotonic() < deadline
                time.sleep(.2)
        with connection:
            cursor = connection.execute(
                "SELECT C_Order_ID,DocumentNo,Description,CreatedBy,AD_Org_ID,C_BPartner_ID,"
                "DocStatus,Processed,TotalLines,GrandTotal FROM IDEMPIERE_QUAL.C_Order "
                "WHERE Description LIKE %s ORDER BY Description", [args.prefix + '%'])
            capture['columns'] = [column.name for column in cursor.description]
            capture['rows'] = cursor.fetchall()
            capture['prior_invoice'] = connection.execute(
                "SELECT i.DocumentNo,i.DocStatus,i.Processed,i.Posted,i.GrandTotal,i.IsPaid "
                "FROM IDEMPIERE_QUAL.C_Invoice i JOIN IDEMPIERE_QUAL.C_Order o ON i.C_Order_ID=o.C_Order_ID "
                "WHERE o.Description=%s", ['BicDB main471 browser acceptance 20261111']).fetchall()
            capture['prior_payment'] = connection.execute(
                "SELECT p.DocumentNo,p.DocStatus,p.Processed,p.Posted,p.PayAmt,p.DiscountAmt,p.IsAllocated "
                "FROM IDEMPIERE_QUAL.C_Payment p JOIN IDEMPIERE_QUAL.C_Invoice i ON p.C_Invoice_ID=i.C_Invoice_ID "
                "JOIN IDEMPIERE_QUAL.C_Order o ON i.C_Order_ID=o.C_Order_ID "
                "WHERE o.Description=%s", ['BicDB main471 browser acceptance 20261111']).fetchall()
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGINT)
            child.wait(timeout=45)
        capture['process_exit'] = child.returncode
        # Numeric SQL values are retained as text for exact external decimal checks.
        save()
        assert child.returncode == 0
record.update(status='captured_two_reopens', finished_unix=time.time())
save()
print(json.dumps(record, default=str))
