#!/usr/bin/env python3
"""Create a public logical-evidence projection without fixture DB/key custody.

The original archive remains untouched. Excluded members are only streamed to
SHA256; their contents are never decoded, printed or copied to the projection.
"""
import argparse
import hashlib
import json
import pathlib
import re
import zipfile

ROOT_ALLOWED = {'source-revision.txt', 'rustc.txt', 'selected-tests.log', 'tests.log'}
CASE_ALLOWED = {
    'ack-ledger.json', 'fence-ledger.json', 'remote-old-ledger.json',
    'source-frame-ledger.json', 'source-frames-result.json',
    'source-constraints-result.json', 'old-sink-control-intent.json',
    'old-sink-control-result.json', 'terminal.json', 'cleanup.json',
    'evidence-errors.json', 'execution-identity.json',
    'first-app-rpc.jsonl', 'first-app-pre-probe.json', 'first-app-pre-append.json',
}
CASE_PATTERN = re.compile(r'fault-state/native-registered-fifth-image-[0-9a-f-]+/([^/]+)$')
NO_DOMAIN = re.compile(r'no-domain-[0-9]+-(point|member|schema|hook)\.json$')
TRANSFER = re.compile(r'registered-transfer-[0-9]+\.json$')
FIRST_APP_AFTER = re.compile(r'first-app-after-append-[0-9]+\.json$')
PRIVATE_PEM = re.compile(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----')
LIMIT = 1024 * 1024 * 1024


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(65536), b''):
            value.update(chunk)
    return value.hexdigest()


def allowed(name):
    if name in ROOT_ALLOWED:
        return True
    match = CASE_PATTERN.fullmatch(name)
    return bool(match and (match[1] in CASE_ALLOWED or NO_DOMAIN.fullmatch(match[1]) or TRANSFER.fullmatch(match[1]) or FIRST_APP_AFTER.fullmatch(match[1])))


def project(source, expected_zip, expected_source, destination, manifest):
    if len({source.resolve(), destination.resolve(), manifest.resolve()}) != 3:
        raise ValueError('original, projection and manifest must be distinct paths')
    if digest(source) != expected_zip:
        raise ValueError('original ZIP digest differs from pinned expected digest')
    omitted, included = [], []
    with zipfile.ZipFile(source) as original:
        infos = original.infolist()
        names = [info.filename for info in infos]
        if len(set(names)) != len(names) or sum(i.file_size for i in infos) > LIMIT:
            raise ValueError('duplicate members or archive size outside bounded projection')
        for name in names:
            path = pathlib.PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts:
                raise ValueError('unsafe original member name')
        revision = original.read('source-revision.txt').decode().strip()
        if revision != expected_source:
            raise ValueError('original archive is not the pinned source revision')
        with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as public:
            for info in sorted(infos, key=lambda i: i.filename):
                value = hashlib.sha256()
                permitted = allowed(info.filename) and not info.is_dir()
                content = bytearray() if permitted else None
                with original.open(info) as member:
                    for chunk in iter(lambda: member.read(65536), b''):
                        value.update(chunk)
                        if content is not None:
                            content.extend(chunk)
                identity = {'path': info.filename, 'bytes': info.file_size, 'sha256': value.hexdigest()}
                if not permitted:
                    identity['reason'] = ('entire fixture DB/custody/material subtree omitted'
                        if '/custody/' in info.filename or info.filename.endswith('/custody-manifest.json')
                        else 'outside explicit public logical-ledger/report/toolchain whitelist')
                    omitted.append(identity)
                    continue
                # No excluded material is inspected here. Allowed textual
                # evidence still refuses accidental literal private PEM data.
                if PRIVATE_PEM.search(content):
                    raise ValueError('private PEM marker in whitelisted logical member: ' + info.filename)
                content.decode('utf-8')
                output_info = zipfile.ZipInfo(info.filename, (1980, 1, 1, 0, 0, 0))
                output_info.compress_type = zipfile.ZIP_DEFLATED
                output_info.external_attr = 0o100644 << 16
                public.writestr(output_info, content)
                included.append(identity)
    with zipfile.ZipFile(destination) as public:
        if any(not allowed(name) or '/custody/' in name for name in public.namelist()):
            raise ValueError('projection contains an excluded member')
    result = {
        'format_version': 1,
        'source_commit': expected_source,
        'projector_sha256': digest(pathlib.Path(__file__)),
        'original_zip_sha256': expected_zip,
        'original_zip_bytes': source.stat().st_size,
        'original_fixture_db_custody_present': any('/custody/' in n for n in names),
        'original_archive_unchanged': digest(source) == expected_zip,
        'projection_zip_sha256': digest(destination),
        'projection_zip_bytes': destination.stat().st_size,
        'projection_fixture_db_custody_present': False,
        'projection_policy': 'Only explicit logical JSON ledgers/reports and source/toolchain/test logs; entire DB/custody trees and custody manifests excluded',
        'qualification': 'No cold replay, physical HA, target installation, serving or production qualification is implied by retained custody or this projection',
        'included_members': included,
        'omitted_members': omitted,
    }
    manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('original', type=pathlib.Path)
    parser.add_argument('--expected-zip-sha256', required=True)
    parser.add_argument('--expected-source', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--manifest', required=True, type=pathlib.Path)
    args = parser.parse_args()
    result = project(args.original, args.expected_zip_sha256, args.expected_source,
                     args.output, args.manifest)
    print(json.dumps({k: result[k] for k in ('source_commit', 'original_zip_sha256',
        'original_fixture_db_custody_present', 'projection_zip_sha256',
        'projection_zip_bytes', 'projection_fixture_db_custody_present')}))
    print(json.dumps({'included_count': len(result['included_members']),
                      'omitted_count': len(result['omitted_members'])}))


if __name__ == '__main__':
    main()
