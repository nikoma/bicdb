#!/usr/bin/env python3
"""Copy original application resources into a fresh private iDempiere home.

The OSGi product and its source feature layout are separate inputs. Tests and
processes that resolve files through IDEMPIERE_HOME need the feature resources
there too. No application classes, assertions, schema or data are rewritten.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import stat
import subprocess

FEATURE = 'org.adempiere.server-feature'
RESOURCE_DIRECTORIES = ('data', 'utils.unix', 'utils.windows')
MAX_RESOURCE_BYTES = 128 * 1024 * 1024


def inventory(upstream):
    upstream = Path(upstream).resolve()
    feature = upstream / FEATURE
    if not (feature / 'data').is_dir():
        raise ValueError('upstream feature data directory is required')
    files = {}
    total = 0
    for name in RESOURCE_DIRECTORIES:
        root = feature / name
        if not root.exists():
            continue
        for path in [root, *sorted(root.rglob('*'))]:
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise ValueError('application resources must be regular files/directories')
            if stat.S_ISDIR(mode):
                continue
            total += path.stat().st_size
            if total > MAX_RESOURCE_BYTES or len(files) >= 20000:
                raise ValueError('application resource copy exceeds its bounded inventory')
            files[path.relative_to(upstream).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


def prepare(upstream, home):
    upstream, home = Path(upstream).resolve(), Path(home).resolve()
    if home == upstream or upstream in home.parents or home in upstream.parents:
        raise ValueError('private application home must be separate from upstream')
    if home.exists():
        raise ValueError('private application home must be new')
    before = inventory(upstream)
    home.mkdir(parents=True)
    for name in RESOURCE_DIRECTORIES:
        source = upstream / FEATURE / name
        if source.exists():
            shutil.copytree(source, home / FEATURE / name)
    after = inventory(home)
    if before != after or inventory(upstream) != before:
        raise ValueError('resource copy differs from upstream; preserve both inputs for inspection')
    return dict(format=1, resource_files=before, file_count=len(before),
                copied_resources_verified=True, upstream_resources_unchanged=True,
                application_classes_changed=False, test_assertions_changed=False,
                application_qualified=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--upstream-commit', required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    upstream = args.upstream.resolve()
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=upstream, text=True).strip()
    if commit != args.upstream_commit:
        parser.error('upstream checkout differs from the required commit')
    relative = [FEATURE + '/' + name for name in RESOURCE_DIRECTORIES]
    subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', *relative], cwd=upstream, check=True)
    tracked = set(subprocess.check_output(['git', 'ls-files', '-z', '--', *relative], cwd=upstream).decode().split('\0')) - {''}
    if set(inventory(upstream)) != tracked:
        parser.error('resource inventory differs from the tracked upstream files')
    report = args.report.resolve()
    if report.exists() or report == upstream or upstream in report.parents:
        parser.error('report must be new and outside the upstream checkout')
    if report == args.home.resolve() or args.home.resolve() in report.parents:
        parser.error('report must be outside the new application home')
    result = prepare(upstream, args.home)
    result['upstream_commit'] = commit
    report.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
