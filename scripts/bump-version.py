#!/usr/bin/env python3
"""Synchronize the BicDB source version without changing dependency resolution.

Published download URLs and historical evidence remain pinned to their releases.
Use --check for a read-only consistency check. Coordinate the next version against
remote main immediately before merging; parallel branches must not reuse it.
"""
import argparse
from pathlib import Path
import re
import subprocess
import tomllib


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version', nargs='?')
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    root = args.repo.resolve()
    core = tomllib.loads((root/'crates/bicdb-core/Cargo.toml').read_text())
    current = core['package']['version']
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(-[A-Za-z0-9.-]+)?', current)
    require(match is not None, 'unsupported current version: '+current)
    require(args.check != bool(args.version), 'supply a new version, or use --check')
    target = args.version or current
    target_match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(-[A-Za-z0-9.-]+)?', target)
    require(target_match is not None, 'invalid target version: '+target)
    if not args.check:
        require(tuple(map(int,target_match.groups()[:3])) > tuple(map(int,match.groups()[:3])),
                'new version must be newer than '+current)
    tracked = subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')
    # Source copies in evidence packets are immutable historical inputs, not
    # live manifests or lockfiles. Keep both validation and mutation away from
    # them: a copied Core manifest can legitimately require an older BicDB.
    tracked = [name for name in tracked
               if not name.startswith('docs/evidence/')]
    manifests = [root/name for name in tracked if name.endswith('Cargo.toml')]
    release_names = set()
    planned = {}
    for path in manifests:
        before = path.read_text()
        data = tomllib.loads(before)
        package = data.get('package',{})
        if path.relative_to(root).parts[0]=='crates' and package.get('name','').startswith('bicdb-'):
            require(package.get('version')==current, 'mixed package version: '+str(path))
            release_names.add(package['name'])
        after = before
        for prefix in ('','^','~','='):
            after = after.replace('"'+prefix+current+'"','"'+prefix+target+'"')
        if after!=before:
            planned[path]=after
    require(bool(release_names), 'no release packages found')
    def check_internal_requirements(value, path):
        if isinstance(value,dict):
            for name,spec in value.items():
                if name in release_names and isinstance(spec,dict) and 'version' in spec:
                    require(spec['version'] in {prefix+current for prefix in ('','^','~','=')},
                            'mixed internal dependency version: '+str(path)+' '+name)
                check_internal_requirements(spec,path)
        elif isinstance(value,list):
            for item in value:
                check_internal_requirements(item,path)
    for path in manifests:
        check_internal_requirements(tomllib.loads(path.read_text()),path)
    for name in tracked:
        if not name.endswith('Cargo.lock'):
            continue
        path=root/name
        text=path.read_text()
        blocks=re.split(r'(?=^\[\[package\]\])',text,flags=re.M)
        seen=set()
        for index,block in enumerate(blocks):
            entries=tomllib.loads(block).get('package',[])
            if not entries:
                continue
            package=entries[0]
            if package['name'] in release_names and 'source' not in package:
                require(package['version']==current, 'mixed lockfile version: '+str(path)+' '+package['name'])
                seen.add(package['name'])
                blocks[index]=re.sub(r'^version = "'+re.escape(current)+r'"$',
                    'version = "'+target+'"',block,count=1,flags=re.M)
        if name=='Cargo.lock':
            require(seen==release_names, 'workspace package/lockfile membership mismatch')
        after=''.join(blocks)
        for package in release_names:
            after=after.replace('"'+package+' '+current+'"','"'+package+' '+target+'"')
        if after!=text:
            planned[path]=after
    doc=root/'POSTGRES_COMPATIBILITY.md'
    text=doc.read_text()
    require('Current source version: **BicDB '+current+'**' in text,
            'source compatibility document version is inconsistent')
    updated=text.replace('Current source version: **BicDB '+current+'**',
                         'Current source version: **BicDB '+target+'**',1)
    if updated!=text:
        planned[doc]=updated
    for path,text in planned.items():
        path.write_text(text)
    print(('Checked ' if args.check else 'Updated ')+str(len(release_names))+
          ' packages: '+current+(' -> '+target if not args.check else '')+
          '; '+str(len(planned))+' tracked files changed')


if __name__=='__main__':
    main()
