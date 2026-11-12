#!/usr/bin/env python3
"""Test-only stopped DB copier. No engine opens, original fsyncs or reconciliation.

Payload starts in private quarantine OUTSIDE the requested artifact subtree.
Only a completely copied/fsynced tree is published. Caps are checked between
bounded chunks/entries; filesystem calls themselves are not hard-I/O bounded.
"""
import argparse
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time
import uuid


@dataclasses.dataclass(frozen=True)
class Caps:
    max_bytes: int = 512 * 1024 * 1024
    max_seconds: int = 30
    min_free_bytes: int = 128 * 1024 * 1024
    max_entries: int = 16384

    @classmethod
    def environment(cls):
        def value(name, default, minimum, maximum):
            raw = os.environ.get(name, str(default))
            if not raw.isascii() or not raw.isdecimal():
                raise ValueError('invalid declared custody cap: ' + name)
            number = int(raw)
            if not minimum <= number <= maximum:
                raise ValueError('declared custody cap outside limit: ' + name)
            return number
        return cls(value('BICDB_TEST_CLI_DB_MAX_BYTES', cls.max_bytes, 1, 8 * 1024**3),
                   value('BICDB_TEST_CLI_DB_MAX_SECONDS', cls.max_seconds, 1, 120),
                   value('BICDB_TEST_CLI_DB_MIN_FREE_BYTES', cls.min_free_bytes, 16 * 1024**2, 64 * 1024**3),
                   value('BICDB_TEST_CLI_DB_MAX_ENTRIES', cls.max_entries, 1, 65536))


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)  # Destination only. Never called with source directories.
    finally:
        os.close(fd)


def secure_directory(path):
    """Create destination directories, refusing every symlink ancestor."""
    path = Path(path).absolute()
    if '..' in path.parts:
        raise ValueError('destination traversal refused')
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            current.mkdir(mode=0o700)
        except FileExistsError:
            pass
        mode = current.lstat().st_mode
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise ValueError('non-directory/symlink destination ancestor')
    return path


def open_directory(path):
    """Anchor every source component with openat + O_NOFOLLOW."""
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('source must be an absolute nontraversing path')
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=fd)
            os.close(fd)
            fd = following
        return fd
    except BaseException:
        os.close(fd)
        raise


def metadata(info):
    return {'device': info.st_dev, 'inode': info.st_ino,
            'logical_bytes': info.st_size, 'mtime_ns': info.st_mtime_ns,
            'ctime_ns': info.st_ctime_ns, 'mode': stat.S_IMODE(info.st_mode)}


def tls_name(name):
    lower = name.lower()
    return lower.endswith(('.key', '.pem', '.p12', '.pfx', '.der', '.crt', '.cer')) or lower in ('id_rsa', 'id_ed25519')


class Copier:
    def __init__(self, destination, caps, started):
        self.destination = destination
        self.caps = caps
        self.deadline = started + caps.max_seconds
        self.bytes = 0
        self.entries = []

    def check(self, upcoming=0):
        if time.monotonic() >= self.deadline:
            raise TimeoutError('declared custody time cap exceeded')
        if self.bytes + upcoming > self.caps.max_bytes:
            raise ValueError('declared logical byte cap exceeded')
        if shutil.disk_usage(self.destination).free < self.caps.min_free_bytes + upcoming:
            raise ValueError('declared destination free-disk floor reached')

    def tree(self, source_fd, relative):
        self.check()
        for name in sorted(os.listdir(source_fd)):
            self.check()
            if name in ('.', '..') or '/' in name or '\\' in name:
                raise ValueError('invalid child path')
            if len(self.entries) >= self.caps.max_entries:
                raise ValueError('declared entry cap exceeded')
            path = relative / name
            before = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
            row = {'path': str(path), 'source': metadata(before),
                   'type': 'directory' if stat.S_ISDIR(before.st_mode) else 'regular_file',
                   'status': 'pending', 'durable_copy': False}
            self.entries.append(row)
            if stat.S_ISLNK(before.st_mode) or not (stat.S_ISDIR(before.st_mode) or stat.S_ISREG(before.st_mode)):
                row['type'] = 'refused_nonregular_or_symlink'
                raise ValueError('refuse symlink or nonregular source: ' + str(path))
            if tls_name(name):
                raise ValueError('refuse TLS/key/certificate filename: ' + str(path))
            if stat.S_ISREG(before.st_mode) and before.st_nlink != 1:
                raise ValueError('refuse hardlinked source file: ' + str(path))
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            child = os.open(name, flags, dir_fd=source_fd)
            try:
                opened = os.fstat(child)
                if metadata(opened) != metadata(before):
                    raise ValueError('source identity changed before copy')
                target = self.destination / path
                if stat.S_ISDIR(opened.st_mode):
                    target.mkdir(mode=0o700)
                    self.tree(child, path)
                    sync_directory(target)
                    row.update(status='copied', durable_copy=True, destination_mode='0700')
                else:
                    self.file(child, target, row)
                if metadata(os.fstat(child)) != metadata(before):
                    raise ValueError('stopped source changed while copying')
            finally:
                os.close(child)

    def file(self, source_fd, target, row):
        expected = row['source']['logical_bytes']
        self.check(expected)
        digest = hashlib.sha256()
        copied = 0
        tail = b''
        try:
            with target.open('xb') as output:
                os.chmod(target, 0o600)
                while True:
                    self.check()
                    chunk = os.read(source_fd, 64 * 1024)
                    self.check(len(chunk))
                    if not chunk:
                        break
                    if b'PRIVATE KEY-----' in tail + chunk:
                        raise ValueError('refuse private-key material in source file')
                    tail = chunk[-64:]
                    output.write(chunk)
                    digest.update(chunk)
                    self.bytes += len(chunk)
                    copied += len(chunk)
                    if copied > expected:
                        raise ValueError('stopped source grew during copy')
                if copied != expected:
                    raise ValueError('stopped source length differs')
                output.flush()
                os.fsync(output.fileno())
                row.update(status='copied', durable_copy=True, destination_mode='0600')
                self.check()
        except BaseException:
            row.update(status='partial_or_refused', durable_copy=False)
            # Never publish a suspected private-key file, even as a partial
            # payload. Quarantine is outside artifact upload in all cases.
            if target.exists():
                target.unlink()
            raise
        finally:
            row.update(copied_bytes=copied, copied_sha256=digest.hexdigest())


def write_manifest(destination, value):
    content = (json.dumps(value, indent=2) + '\n').encode()
    path = destination / 'manifest.json.tmp'
    with path.open('wb') as output:
        os.chmod(path, 0o600)
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    os.replace(path, destination / 'manifest.json')
    sync_directory(destination)
    return hashlib.sha256(content).hexdigest()


def validate_request(request):
    fixture = Path(request['fixture_root'])
    roots = [Path(root) for root in request['owned_database_roots']]
    if not 1 <= len(roots) <= 16 or len(set(roots)) != len(roots):
        raise ValueError('explicit owned database root inventory missing/bounded')
    for root in roots:
        name = root.name
        if root.parent != fixture or not name.startswith('db') or not name[2:].isdigit() or not 0 <= int(name[2:]) <= 15:
            raise ValueError('owned root must be explicit fixture/dbN')
    current = request['actual_children_waited']
    if not current or any(row.get('wait_ok') is not True for row in current):
        raise ValueError('ALL actual child waits required before copying')
    waited = {row['pid'] for row in current}
    waited.update(row['pid'] for row in request['prior_test_exits'] if 'exit' in row)
    spawned = {row['pid'] for row in request['actual_spawn_records']}
    if not spawned or not spawned <= waited:
        raise ValueError('actual spawned child not actually waited')
    return fixture, roots


def capture(request, caps):
    started = time.monotonic()
    fixture, roots = validate_request(request)
    destination_root = Path(request['destination_root']).absolute()
    if destination_root.resolve().is_relative_to(fixture.resolve()) or fixture.resolve().is_relative_to(destination_root.resolve()):
        raise ValueError('artifact destination overlaps original fixture')
    identifier = str(uuid.uuid4())
    destination = secure_directory(destination_root / ('database-failure-' + identifier))
    # Keep every unsuccessful payload outside the artifact root. No promotion
    # or runtime restore path consumes this directory.
    quarantine = destination_root.parent / ('.native-cli-db-quarantine-' + identifier)
    if quarantine.resolve().is_relative_to(fixture.resolve()):
        raise ValueError('quarantine overlaps original fixture')
    quarantine.mkdir(mode=0o700)
    manifest = dict(version=1, status='PARTIAL', complete=False,
                    reason='copy not completed; private quarantine only',
                    source_revision=request.get('source_revision'), test=request.get('test'),
                    actual_children_waited=request['actual_children_waited'],
                    prior_test_exits=request['prior_test_exits'],
                    actual_spawn_records=request['actual_spawn_records'],
                    explicit_owned_roots=[str(root) for root in roots],
                    declared_caps=dataclasses.asdict(caps),
                    quarantine_path=str(quarantine), payload_published=False,
                    no_engine_open_repair_or_original_fsync=True,
                    source_powerloss_or_ack_reconciliation_proven=False,
                    hard_kernel_io_deadline_proven=False,
                    logical_byte_accounting='regular file lengths; directory metadata is not file payload',
                    entries=[])
    write_manifest(destination, manifest)
    copier = Copier(quarantine, caps, started)
    try:
        for root in roots:
            copier.check()
            if len(copier.entries) >= caps.max_entries:
                raise ValueError('declared entry cap exceeded')
            fd = open_directory(root)
            try:
                target = quarantine / root.name
                target.mkdir(mode=0o700)
                copier.entries.append({'path': root.name, 'type': 'directory',
                                       'source': metadata(os.fstat(fd)), 'status': 'pending',
                                       'durable_copy': False})
                row = copier.entries[-1]
                before = metadata(os.fstat(fd))
                copier.tree(fd, Path(root.name))
                if metadata(os.fstat(fd)) != before:
                    raise ValueError('stopped root changed while copying')
                sync_directory(target)
                row.update(status='copied', durable_copy=True, destination_mode='0700')
            finally:
                os.close(fd)
        sync_directory(quarantine)
        copier.check()
        os.rename(quarantine, destination / 'database-snapshot')
        sync_directory(destination)
        manifest.update(status='COPIED_STOPPED_OWNED_ROOTS', complete=True,
                        reason=None, payload_published=True, quarantine_path=None)
    except BaseException as error:
        snapshot = destination / 'database-snapshot'
        rollback_error = None
        if snapshot.exists():
            try:
                os.rename(snapshot, quarantine)
                sync_directory(destination)
            except OSError as failure:
                rollback_error = str(failure)
        manifest.update(status='PARTIAL', complete=False, reason=str(error),
                        payload_published=snapshot.exists(),
                        quarantine_path=str(quarantine) if quarantine.exists() else None,
                        publication_rollback_error=rollback_error)
        for row in copier.entries:
            if row['status'] == 'pending':
                row['status'] = 'partial_or_refused'
    manifest.update(entries=copier.entries, streamed_logical_bytes=copier.bytes)
    digest = write_manifest(destination, manifest)
    print('NATIVE_CLI_DATABASE_FAILURE_CUSTODY ' + json.dumps({
        'path': str(destination), 'status': manifest['status'],
        'manifest_sha256': digest, 'complete': manifest['complete'],
        'no_ack_reconciliation_claim': True}), flush=True)
    return destination, manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--request', type=Path, required=True)
    args = parser.parse_args()
    if args.request.stat().st_size > 256 * 1024:
        raise ValueError('bounded custody request exceeded')
    request = json.loads(args.request.read_text())
    destination, manifest = capture(request, Caps.environment())
    return 0 if manifest['complete'] else 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('NATIVE_CLI_DATABASE_CUSTODY_PARTIAL ' + str(error), file=sys.stderr)
        sys.exit(2)
