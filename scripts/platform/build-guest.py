#!/usr/bin/env python3
"""Build a minimal read-only Node guest from explicitly selected host binaries.

No network downloads or application hooks run. The builder requires Linux,
Python 3, ldd and mkfs.ext4. Kernel and VMM installation are operator tasks.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--node', required=True, type=Path)
parser.add_argument('--busybox', default='/usr/bin/busybox', type=Path)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--size-mb', default=512, type=int)
args = parser.parse_args()
if args.output.exists():
    parser.error('output already exists')
if not 256 <= args.size_mb <= 4096:
    parser.error('size-mb must be between 256 and 4096')
version = subprocess.check_output([str(args.node), '--version'], text=True).strip()
if int(version.removeprefix('v').split('.')[0]) < 22:
    parser.error('Node.js 22 or newer is required')

with tempfile.TemporaryDirectory(prefix='bic-guest-') as directory:
    root = Path(directory)
    for name in ['app', 'bin', 'dev', 'etc', 'proc', 'run', 'sbin', 'sys', 'tmp', 'usr/bin']:
        (root / name).mkdir(parents=True, exist_ok=True)

    def copy_binary(source, destination):
        shutil.copy2(source, root / destination)
        result = subprocess.run(['ldd', str(source)], text=True, capture_output=True)
        if result.returncode and 'not a dynamic executable' not in result.stderr:
            raise RuntimeError(result.stderr)
        for path in re.findall(r'(?:=>\s+|^\s*)(/[^\s]+)', result.stdout, re.MULTILINE):
            target = root / path.removeprefix('/')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)

    copy_binary(args.node.resolve(), 'usr/bin/node')
    copy_binary(args.busybox.resolve(), 'bin/busybox')
    for name in ['sh', 'mount', 'poweroff']:
        (root / 'bin' / name).symlink_to('busybox')
    init = root / 'sbin/bic-init'
    init.write_text('''#!/bin/sh
set -eu
/bin/mount -t proc proc /proc
/bin/mount -t sysfs sysfs /sys
bic_dev_mounted=0
while read -r bic_source bic_target bic_type bic_options; do
  if [ "$bic_target" = /dev ]; then bic_dev_mounted=1; break; fi
done < /proc/mounts
if [ "$bic_dev_mounted" = 0 ]; then /bin/mount -t devtmpfs devtmpfs /dev; fi
/bin/mount -t tmpfs -o mode=1777,nosuid,nodev tmpfs /tmp
/bin/mount -t tmpfs -o nosuid,nodev tmpfs /run
/bin/mount -t ext4 -o ro,nosuid,nodev /dev/vdb /app
exec /usr/bin/node /app/guest-launch.mjs
''')
    init.chmod(0o755)
    (root / 'etc/passwd').write_text('root:x:0:0:root:/root:/bin/sh\napp:x:1000:1000:Application:/tmp:/bin/sh\n')
    (root / 'etc/group').write_text('root:x:0:\napp:x:1000:\n')
    (root / 'etc/nsswitch.conf').write_text('passwd: files\ngroup: files\nhosts: files\n')
    root.chmod(0o755)
    for directory in root.rglob('*'):
        if directory.is_dir():
            directory.chmod(0o755)
    with args.output.open('xb') as image:
        image.truncate(args.size_mb * 1024 * 1024)
    try:
        subprocess.run(['mkfs.ext4', '-q', '-F', '-d', str(root), str(args.output)], check=True)
        digest = hashlib.file_digest(args.output.open('rb'), 'sha256').hexdigest()
        print(json.dumps({'rootfs': str(args.output.resolve()), 'rootfs_sha256': digest, 'node_version': version}))
    except BaseException:
        args.output.unlink(missing_ok=True)
        raise
