#!/usr/bin/env python3
"""Start a loopback-only Field Notebook with its own RF=1 event cluster.

Uses existing trusted binaries and new local directories. Ctrl-C stops only
these children; their data and logs remain available at the printed location.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def unused_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bicdb', default='bicdb')
    parser.add_argument('--bic', default='bic')
    parser.add_argument('--port', type=int, default=8788)
    parser.add_argument('--data-directory', type=Path, help='new directory; existing paths are never reused')
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error('port must be between 1024 and 65535')
    binaries = {}
    for name in ['bicdb', 'bic']:
        path = shutil.which(getattr(args, name))
        if not path:
            parser.error(f'{name} binary not found; build it first or pass its path')
        binaries[name] = str(Path(path).resolve())
    root = args.data_directory.resolve() if args.data_directory else Path(tempfile.mkdtemp(prefix='bicdb-notebook-demo-'))
    if args.data_directory:
        root.mkdir(mode=0o700, exist_ok=False)
    processes = []
    base = f'http://127.0.0.1:{args.port}'
    config = root / 'events.json'
    with config.open('x') as stream:
        os.chmod(config, 0o600)
        json.dump(dict(format=1, member_root=str(root / 'cluster'), applications={'notebook': 'demo-notebook'}), stream)

    def start(arguments, name):
        with (root / f'{name}.log').open('xb') as log:
            child = subprocess.Popen(arguments, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        processes.append(child)
        return child

    try:
        subprocess.run([binaries['bicdb'], 'cluster', 'init', str(root / 'cluster'),
                        '--cluster-id', 'notebook-demo', '--node-id', 'local',
                        '--address', f'127.0.0.1:{unused_port()}', '--replication-factor', '1',
                        '--initial-ranges', '4', '--application-events'], check=True, capture_output=True)
        start([binaries['bicdb'], 'serve', str(root / 'cluster'), '--host', '127.0.0.1',
               '--port', str(unused_port())], 'cluster')
        platform = start([binaries['bic'], '--data', str(root / 'platform'), 'serve',
                          '--listen', f'127.0.0.1:{args.port}', '--events-config', str(config)], 'platform')
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 30
        while True:
            if platform.poll() is not None:
                raise RuntimeError(f'platform exited; inspect {root}/platform.log')
            try:
                with client.open(base + '/health', timeout=1):
                    break
            except (urllib.error.URLError, TimeoutError):
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'platform did not become ready; inspect {root}/platform.log')
                time.sleep(0.1)
        subprocess.run([binaries['bic'], '--data', str(root / 'platform'), '--endpoint', base,
                        'deploy', str(Path(__file__).with_name('bic.json').resolve())], check=True)
        print(f'Open {base}/apps/notebook/\nData and logs: {root}\nCtrl-C stops this demo.', flush=True)
        while True:
            if any(child.poll() is not None for child in processes):
                raise RuntimeError(f'a demo service exited; inspect logs in {root}')
            time.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        for child in reversed(processes):
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        print(f'Demo data retained at {root}', flush=True)


if __name__ == '__main__':
    main()
