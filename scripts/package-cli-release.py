#!/usr/bin/env python3
"""Package the Linux CLI, notices, and locked registry sources; never compile."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--metadata', type=Path, required=True,
                        help='cargo metadata --locked --format-version 1 --filter-platform x86_64-unknown-linux-gnu')
    parser.add_argument('--extra-notices', type=Path, required=True,
                        help='versioned upstream notices for crates omitting them; include provenance.json')
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    version = tomllib.loads((root / 'crates/bicdb-cli/Cargo.toml').read_text())['package']['version']
    if not re.fullmatch(r'[0-9a-f]{40}', args.revision):
        parser.error('full source commit required')
    actual = subprocess.check_output([str(args.binary.resolve()), '--version'], text=True).strip()
    if actual != f'bicdb {version}':
        raise RuntimeError(f'binary version does not match source: {actual}')
    metadata = json.loads(args.metadata.read_text())
    packages = {p['id']: p for p in metadata['packages']}
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    pending = [p['id'] for p in packages.values() if p['name'] == 'bicdb-cli']
    selected = set()
    while pending:
        pid = pending.pop()
        if pid not in selected:
            selected.add(pid)
            pending.extend(nodes[pid]['dependencies'])
    locked = {(p['name'], p['version']): p for p in tomllib.loads((root / 'Cargo.lock').read_text())['package']}
    args.output.mkdir(parents=True, exist_ok=True)
    name = f'bicdb-{version}-linux-x86_64'
    destination = args.output / f'{name}.tar.gz'
    if destination.exists():
        raise RuntimeError(f'refusing to overwrite {destination}')
    with tempfile.TemporaryDirectory(prefix='bicdb-release-') as tmp:
        stage = Path(tmp) / name
        stage.mkdir()
        shutil.copy2(args.binary, stage / 'bicdb')
        subprocess.run(['strip', '--strip-debug', str(stage / 'bicdb')], check=True)
        for f in ['LICENSE', 'LICENSE-SCOPE.md', 'NOTICE', 'THIRD_PARTY_NOTICES.md', 'Cargo.lock']:
            shutil.copy2(root / f, stage / f)
        shutil.copytree(root / 'LICENSES', stage / 'LICENSES')
        shutil.copytree(root / 'licensing', stage / 'licensing')
        notices = stage / 'third-party'
        notices.mkdir()
        index = []
        for pid in sorted(selected):
            package = packages[pid]
            if not package['source']:
                source = Path(package['manifest_path']).parent
                for file in source.glob('*'):
                    if file.is_file() and file.name in ('LICENSE', 'LICENSE-SCOPE.md', 'NOTICE', 'THIRD_PARTY_NOTICES.md'):
                        dest = stage / 'component-notices' / package['name'] / file.name
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(file, dest)
                continue
            if not package['source'].startswith('registry+'):
                raise RuntimeError(f'non-registry dependency requires explicit source packaging: {pid}')
            source = Path(package['manifest_path']).parent
            folder = notices / f"{package['name']}-{package['version']}"
            folder.mkdir()
            # Registry archives retain source, inline notices, and license files,
            # including source for any weak-copyleft component. Check the lock hash.
            cache = source.parent.parent.parent / 'cache' / source.parent.name / f'{source.name}.crate'
            digest = hashlib.sha256(cache.read_bytes()).hexdigest()
            if digest != locked[(package['name'], package['version'])]['checksum']:
                raise RuntimeError(f'registry source checksum mismatch: {pid}')
            shutil.copy2(cache, folder / cache.name)
            copied = []
            for file in source.rglob('*'):
                if file.is_file() and file.name.lower().startswith(('license', 'licence', 'copying', 'notice', 'copyright', 'authors')):
                    relative = file.relative_to(source)
                    dest = folder / 'notices' / relative
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(file, dest)
                    copied.append(str(relative))
            extra = args.extra_notices / folder.name
            if extra.exists():
                shutil.copytree(extra, folder / 'upstream-notices')
            if not copied and not extra.exists():
                raise RuntimeError(f'missing upstream license text: {folder.name}')
            index.append({k: package.get(k) for k in ('name', 'version', 'license', 'authors', 'repository')}
                         | {'source_sha256': digest, 'notice_files': copied})
        (notices / 'index.json').write_text(json.dumps(index, indent=2) + '\n')
        (stage / 'BUILD.json').write_text(json.dumps({
            'version': version, 'revision': args.revision,
            'target': 'x86_64-unknown-linux-gnu', 'features': [],
            'binary_sha256': hashlib.sha256((stage / 'bicdb').read_bytes()).hexdigest(),
            'dependencies': len(index),
        }, indent=2) + '\n')
        (stage / 'README.txt').write_text(f'''BicDB {version} — Linux x86-64 beta

Run ./bicdb --version, then follow the repository five-minute quickstart.
Validated on Ubuntu 24.04 x86-64; requires glibc 2.39 and OpenSSL 3 shared
libraries. Install libssl3t64 and libgcc-s1 on Ubuntu if absent.
No installer, daemon registration, or automatic data migration is performed.

Source: https://github.com/nikoma/bicdb/tree/{args.revision}
Build: cargo build --locked --release -p bicdb-cli (Rust 1.96.0)
Debug information was stripped from this copy. Default CLI features only.

LICENSE and LICENSE-SCOPE.md govern BicDB. Independent dependencies retain
their own terms. third-party/ contains notices and checksum-verified registry
source archives from the CLI dependency closure, including build dependencies.
These archives preserve upstream source and notices; the index records their
license declarations, not a claim that every listed component is linked.
''')
        with tarfile.open(destination, 'w:gz', compresslevel=6) as archive:
            archive.add(stage, arcname=name)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    (args.output / 'SHA256SUMS').write_text(f'{digest}  {destination.name}\n')
    print(destination)


if __name__ == '__main__':
    main()
