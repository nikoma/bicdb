#!/usr/bin/env python3
"""Fetch and verify the pinned upstream iDempiere Oracle seed outside the checkout."""
import argparse
import hashlib
import json
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MANIFEST = json.loads((ROOT / 'seed.json').read_text())


def verify(path, size, sha256):
    if path.stat().st_size != size:
        raise ValueError(f'{path.name}: unexpected byte count')
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    if digest != sha256:
        raise ValueError(f'{path.name}: SHA-256 does not match the pinned seed')


def prepare(directory):
    directory = directory.resolve()
    if directory.is_relative_to(ROOT.parents[2]):
        raise ValueError('keep upstream application binaries outside the BicDB checkout')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    archive_path = directory / 'Adempiere.jar'
    if not archive_path.exists():
        url = ('https://raw.githubusercontent.com/idempiere/binary.file/' + MANIFEST['commit']
               + '/' + MANIFEST['path'])
        created = False
        try:
            with urllib.request.urlopen(url, timeout=30) as source, archive_path.open('xb') as target:
                created = True
                count = 0
                while chunk := source.read(1024 * 1024):
                    count += len(chunk)
                    if count > MANIFEST['archive_bytes']:
                        raise ValueError('upstream archive exceeded the pinned size')
                    target.write(chunk)
            verify(archive_path, MANIFEST['archive_bytes'], MANIFEST['archive_sha256'])
        except Exception:
            if created:
                archive_path.unlink(missing_ok=True)
            raise
    verify(archive_path, MANIFEST['archive_bytes'], MANIFEST['archive_sha256'])
    dump_path = directory / MANIFEST['dump_name']
    if not dump_path.exists():
        with zipfile.ZipFile(archive_path) as archive:
            entry = archive.getinfo(MANIFEST['dump_name'])
            if entry.file_size != MANIFEST['dump_bytes']:
                raise ValueError('dump length differs from pinned size')
            created = False
            try:
                with archive.open(entry) as source, dump_path.open('xb') as target:
                    created = True
                    count = 0
                    while chunk := source.read(1024 * 1024):
                        count += len(chunk)
                        if count > MANIFEST['dump_bytes']:
                            raise ValueError('dump exceeded the pinned size')
                        target.write(chunk)
                verify(dump_path, MANIFEST['dump_bytes'], MANIFEST['dump_sha256'])
            except Exception:
                if created:
                    dump_path.unlink(missing_ok=True)
                raise
    verify(dump_path, MANIFEST['dump_bytes'], MANIFEST['dump_sha256'])
    return dict(manifest=MANIFEST, archive=str(archive_path), dump=str(dump_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory), indent=2))


if __name__ == '__main__': main()
