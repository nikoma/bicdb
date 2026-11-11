#!/usr/bin/env python3
"""Read-only, bounded diagnostics for an owned reproduction controller/process tree.
No ptrace, profiling, signals, process-memory reads or unrelated socket entries.
Private output can include task paths and network addresses; do not publish raw.
"""
import argparse, json, os, pathlib, time

def read(path, limit=65536):
    try:
        with pathlib.Path(path).open('rb') as stream:
            data = stream.read(limit + 1)
        return {'text': data[:limit].decode(errors='replace'), 'truncated': len(data) > limit}
    except OSError as error:
        return {'error': str(error)}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('controller_directory', type=pathlib.Path)
    p.add_argument('--output', type=pathlib.Path, required=True)
    p.add_argument('--force', action='store_true')
    p.add_argument('--minimum-operation-age-seconds', type=int, default=30)
    a = p.parse_args()
    identity = json.loads((a.controller_directory / 'controller-identity.json').read_text())
    progress = json.loads((a.controller_directory / 'progress.json').read_text()) if (a.controller_directory / 'progress.json').exists() else {}
    now = time.monotonic_ns()
    old = [v for v in progress.get('cursors', {}).values() if v.get('state') == 'request' and now - v.get('started_monotonic_ns', now) > a.minimum_operation_age_seconds * 10 ** 9]
    if not a.force and (not old):
        p.exit(0, 'no old active operation; no diagnostic snapshot collected\n')
    report = {'started_unix_ns': time.time_ns(), 'scope': 'only owned controller/SSH children; shared-host aggregate CPU/pressure/disk counters are not isolated workload cost', 'progress': progress, 'processes': []}
    inodes = set()
    for entry in [identity, *identity.get('workers', [])]:
        pid = entry['pid']
        root = pathlib.Path('/proc') / str(pid)
        row = {'pid': pid}
        try:
            stat = (root / 'stat').read_text().split()
            if stat[21] != entry['start_ticks']:
                row['ownership_match'] = False
                report['processes'].append(row)
                continue
            row['ownership_match'] = True
            row['files'] = {name: read(root / name) for name in ['stat', 'status', 'io', 'wchan', 'syscall', 'smaps_rollup', 'schedstat']}
            row['threads'] = []
            for task in list((root / 'task').iterdir())[:256]:
                row['threads'].append({'tid': task.name, **{name: read(task / name, 16384) for name in ['stat', 'status', 'wchan', 'syscall']}})
            row['fds'] = {}
            for fd in list((root / 'fd').iterdir())[:2048]:
                try:
                    target = os.readlink(fd)
                    row['fds'][fd.name] = target
                    if target.startswith('socket:['):
                        inodes.add(target[8:-1])
                except OSError as error:
                    row['fds'][fd.name] = {'error': str(error)}
        except OSError as error:
            row['error'] = str(error)
        report['processes'].append(row)
    report['socket_entries'] = {}
    for name in ['tcp', 'tcp6', 'udp', 'udp6', 'unix']:
        lines = pathlib.Path('/proc/net', name).read_text().splitlines()
        selected = []
        for line in lines[1:]:
            parts = line.split()
            inode = parts[6] if name == 'unix' and len(parts) > 6 else parts[9] if len(parts) > 9 else None
            if inode in inodes:
                selected.append(line)
        report['socket_entries'][name] = {'header': lines[0] if lines else '', 'owned_entries': selected}
    report['host_aggregate'] = {name: read('/proc/' + name) for name in ['meminfo', 'stat', 'loadavg', 'pressure/cpu', 'pressure/io', 'pressure/memory']}
    filesystem = os.statvfs(a.controller_directory)
    device = os.stat(a.controller_directory).st_dev
    major, minor = (os.major(device), os.minor(device))
    report['controller_filesystem'] = {'free_bytes': filesystem.f_bavail * filesystem.f_frsize, 'device_major': major, 'device_minor': minor, 'diskstats': [line for line in pathlib.Path('/proc/diskstats').read_text().splitlines() if line.split()[:2] == [str(major), str(minor)]]}
    report['completed_unix_ns'] = time.time_ns()
    a.output.parent.mkdir(mode=448, parents=True, exist_ok=True)
    fd = os.open(a.output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
    with os.fdopen(fd, 'w') as stream:
        json.dump(report, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({'output': str(a.output), 'owned_processes': sum((v.get('ownership_match', False) for v in report['processes'])), 'started_unix_ns': report['started_unix_ns'], 'completed_unix_ns': report['completed_unix_ns']}))
if __name__ == '__main__':
    main()
