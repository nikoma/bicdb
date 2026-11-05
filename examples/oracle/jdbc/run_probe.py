#!/usr/bin/env python3
"""Run the JDBC adapter against new, disposable loopback databases."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import zipfile


MODULE = Path(__file__).resolve().parent
ROOT = MODULE.parents[2]


def available_mib():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) / 1024
    raise RuntimeError('Linux MemAvailable is required for the memory guard')


def stop(child):
    if child.poll() is not None:
        return child.returncode
    child.send_signal(signal.SIGINT)
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
    return child.returncode


def signal_owned_group(child, sig):
    try:
        os.killpg(child.pid, sig)
    except ProcessLookupError:
        pass


def guarded(command, minimum_mib, timeout=900, **kwargs):
    if available_mib() < minimum_mib:
        raise RuntimeError('Insufficient available memory to start the owned command')
    with subprocess.Popen(command, start_new_session=True, **kwargs) as child:
        deadline = time.monotonic() + timeout
        try:
            while child.poll() is None:
                if available_mib() < minimum_mib:
                    signal_owned_group(child, signal.SIGTERM)
                    raise RuntimeError('Owned command interrupted because memory headroom fell below the limit')
                if time.monotonic() > deadline:
                    raise TimeoutError('Owned command exceeded its execution deadline')
                time.sleep(0.2)
            if child.returncode:
                raise subprocess.CalledProcessError(child.returncode, command)
        finally:
            if child.poll() is None:
                signal_owned_group(child, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    signal_owned_group(child, signal.SIGKILL)
                    child.wait()


def sha256(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def manifest(path):
    with zipfile.ZipFile(path) as jar:
        text = jar.read('META-INF/MANIFEST.MF').decode().replace('\r\n ', '')
    return dict(line.split(': ', 1) for line in text.splitlines() if ': ' in line)


def run(args):
    if args.build_server:
        env = {**os.environ, 'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG': '0'}
        guarded(['cargo', 'build', '--locked', '-p', 'bicdb-pgwire', '--example', 'oracle_server'], args.minimum_free_mib, timeout=3600, cwd=ROOT, env=env)
    if not args.skip_build:
        guarded(['mvn', '-B', '-f', str(MODULE / 'pom.xml'), 'package'], args.minimum_free_mib,
                cwd=ROOT, env={**os.environ, 'MAVEN_OPTS': os.environ.get('MAVEN_OPTS', '') + ' -Xmx512m'})
    binary = args.server.resolve()
    adapter = MODULE / 'target/oracle-jdbc-0.1.0-SNAPSHOT.jar'
    postgres = args.postgres_jar.resolve()
    for path in [binary, adapter, postgres]:
        if not path.is_file():
            raise ValueError(f'Missing input artifact: {path}')
    version = manifest(postgres).get('Implementation-Version')
    if version != '42.7.8':
        raise ValueError('This qualification requires PostgreSQL JDBC 42.7.8')
    if manifest(adapter).get('Bundle-SymbolicName') != 'org.bicdb.jdbc.oracle':
        raise ValueError('The adapter artifact has the wrong bundle identity')
    guarded([sys.executable, '-m', 'unittest', 'discover', '-s', str(MODULE.parent / 'idempiere'),
             '-p', 'test_prepare_provider.py'], args.minimum_free_mib, cwd=ROOT,
            env={**os.environ, 'POSTGRES_JDBC_JAR': str(postgres), 'BICDB_ORACLE_ADAPTER_JAR': str(adapter),
                 'PYTHONDONTWRITEBYTECODE': '1'})
    report = dict(format=1, success=False, application_qualified=False, source_databases_mutated=False,
                  server_sha256=sha256(binary), adapter_sha256=sha256(adapter),
                  postgres_jdbc_version=version, postgres_jdbc_sha256=sha256(postgres), modes=[])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    for mode in ['extended', 'simple', 'extendedCacheEverything', 'postgres_profile']:
        with tempfile.TemporaryDirectory(prefix='bicdb-jdbc-probe-') as tmp:
            directory = Path(tmp)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
            profile = 'postgres' if mode == 'postgres_profile' else 'oracle-preview-v1'
            with (directory / 'server.log').open('w') as log:
                server = subprocess.Popen([str(binary), str(directory / 'db'), str(port), profile, '64', '64'], stdout=log, stderr=subprocess.STDOUT)
                item = None
                try:
                    deadline = time.monotonic() + 30
                    while True:
                        if server.poll() is not None:
                            raise RuntimeError('Disposable server stopped before readiness')
                        try:
                            with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                                break
                        except OSError:
                            if time.monotonic() >= deadline:
                                raise RuntimeError('Disposable server readiness timed out')
                            time.sleep(0.1)
                    query_mode = 'extended' if profile == 'postgres' else mode
                    url = (f'jdbc:bicdb:oracle://127.0.0.1:{port}/bicdb?stringtype=unspecified'
                           f'&reWriteBatchedInserts=true&preferQueryMode={query_mode}')
                    env = {**os.environ, 'BICDB_JDBC_USER': 'bicdb'}
                    env.pop('BICDB_JDBC_PASSWORD', None)
                    env.pop('BICDB_JDBC_EXPECT_PROFILE_REJECTION', None)
                    if profile == 'postgres':
                        env['BICDB_JDBC_EXPECT_PROFILE_REJECTION'] = '1'
                    else:
                        url += '&options=-c%20bicdb.sql_profile%3Doracle-preview-v1'
                    classpath = os.pathsep.join(map(str, [adapter, MODULE / 'target/test-classes', postgres]))
                    output = directory / 'probe.log'
                    with output.open('w') as capture:
                        try:
                            guarded(['java', '-Xmx256m', '-cp', classpath, 'org.bicdb.jdbc.oracle.OracleAdapterProbe', url],
                                    args.minimum_free_mib, timeout=120, env=env, stdout=capture, stderr=subprocess.STDOUT)
                        except BaseException:
                            capture.flush()
                            print(output.read_text(), file=sys.stderr, flush=True)
                            raise
                    text = output.read_text()
                    checks = re.findall(r'^passed=(.+)$', text, re.MULTILINE)
                    totals = re.findall(r'^checks=(\d+)$', text, re.MULTILINE)
                    if len(totals) != 1 or int(totals[0]) != len(checks):
                        raise RuntimeError('Probe did not report all executed checks')
                    cleanup = 'cleanup=true' in text
                    if profile != 'postgres' and not cleanup:
                        raise RuntimeError('Nonce-table cleanup was not confirmed')
                    item = dict(mode=mode, profile=profile, checks=checks, total=len(checks),
                                nonce_table_created=profile != 'postgres', cleanup_complete=cleanup or profile == 'postgres')
                    report['modes'].append(item)
                    args.output.write_text(json.dumps(report, indent=2) + '\n')
                    print(f'{mode}: {len(checks)} checks passed', flush=True)
                finally:
                    result = stop(server)
                    if item is not None:
                        item['server_exit_code'] = result
                        args.output.write_text(json.dumps(report, indent=2) + '\n')
                    if result != 0 and sys.exc_info()[0] is None:
                        raise RuntimeError(f'Disposable server did not stop cleanly: {result}')
    report.update(success=True, total=sum(item['total'] for item in report['modes']))
    args.output.write_text(json.dumps(report, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = Path(os.environ.get('CARGO_TARGET_DIR', str(ROOT / 'target')))
    if not target.is_absolute():
        target = ROOT / target
    parser.add_argument('--server', type=Path, default=target / 'debug/examples/oracle_server')
    parser.add_argument('--postgres-jar', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--build-server', action='store_true')
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--minimum-free-mib', type=int, default=1536)
    args = parser.parse_args()
    if args.minimum_free_mib <= 0:
        parser.error('--minimum-free-mib must be positive')
    def interrupted(signum, frame):
        raise InterruptedError('Disposable probe runner was interrupted')
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        run(args)
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == '__main__':
    main()
