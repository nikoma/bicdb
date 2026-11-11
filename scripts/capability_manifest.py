#!/usr/bin/env python3
"""Bind conservative capability declarations to exact release bytes; fail closed."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib

FORMAT = 'bicdb.release-capabilities/v1'
LEVELS = ('implemented', 'tested', 'qualified', 'deployed')
MODES = ('legacy_file_shipping', 'native_streaming_ha', 'distributed_ranges', 'cell_ha', 'application_fleet')


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_catalog(catalog, root):
    require(catalog.get('format') == 'bicdb.capability-catalog/v1', 'unsupported catalog format')
    require(set(catalog.get('modes', {})) == set(MODES), 'catalog must declare every deployment mode')
    for capabilities in catalog['modes'].values():
        require(bool(capabilities), 'empty deployment mode')
        for name, claim in capabilities.items():
            require(re.fullmatch(r'[a-z][a-z0-9_]*', name) is not None, 'invalid capability name')
            require(claim.get('status') in ('supported', 'unsupported', 'conditional'), 'invalid implementation status')
            require(bool(claim.get('summary')), 'missing scope summary')
            require(bool(claim.get('source_paths')), 'source evidence required')
            for relative in claim['source_paths'] + claim.get('test_paths', []):
                path = Path(relative)
                require(not path.is_absolute() and '..' not in path.parts, 'unsafe evidence path')
                resolved = (root / path).resolve()
                require(resolved.is_relative_to(root.resolve()) and (root / path).is_file()
                        and not any(parent.is_symlink() for parent in [root / path, *(root / path).parents] if parent != root.parent),
                        'missing regular in-tree evidence file: ' + relative)
            if claim['status'] == 'conditional':
                require(bool(claim.get('conditions')), 'conditional capability requires conditions')


def committed_evidence(root, revision, relative):
    entry = subprocess.check_output(['git', '-C', str(root), 'ls-tree', '-z', revision, '--', relative])
    require(entry.count(b'\0') == 1, 'evidence must be one committed source file: ' + relative)
    header, name = entry.rstrip(b'\0').split(b'\t', 1)
    mode, kind, _ = header.split()
    require(name.decode() == relative and mode in (b'100644', b'100755') and kind == b'blob',
            'evidence must be a regular committed blob: ' + relative)
    data = subprocess.check_output(['git', '-C', str(root), 'cat-file', 'blob', revision + ':' + relative])
    return {'kind': 'source', 'path': relative, 'sha256': hashlib.sha256(data).hexdigest()}


def build_manifest(root, binary, revision, target='x86_64-unknown-linux-gnu'):
    require(not Path(binary).is_symlink(), 'binary symlink is not a release artifact')
    root, binary = Path(root), Path(binary).resolve()
    require(re.fullmatch(r'[0-9a-f]{40}', revision) is not None, 'full source revision required')
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    require(head == revision, 'revision must equal checkout HEAD')
    subprocess.run(['git', '-C', str(root), 'diff', '--quiet', 'HEAD', '--'], check=True)
    catalog_path = root / 'capabilities/catalog.json'
    # Untracked declarations cannot silently become release authority.
    subprocess.run(['git', '-C', str(root), 'ls-files', '--error-unmatch', 'capabilities/catalog.json'],
                   check=True, stdout=subprocess.DEVNULL)
    version = tomllib.loads((root / 'crates/bicdb-cli/Cargo.toml').read_text())['package']['version']
    require(binary.is_file() and not Path(binary).is_symlink(), 'regular binary required')
    actual = subprocess.check_output([str(binary), '--version'], text=True, timeout=30).strip()
    require(actual == 'bicdb ' + version, 'binary/source version mismatch')
    catalog_identity = committed_evidence(root, revision, 'capabilities/catalog.json')
    catalog = json.loads(subprocess.check_output(
        ['git', '-C', str(root), 'cat-file', 'blob', revision + ':capabilities/catalog.json']))
    validate_catalog(catalog, root)
    for capabilities in catalog['modes'].values():
        for claim in capabilities.values():
            for path in claim['source_paths'] + claim.get('test_paths', []):
                committed_evidence(root, revision, path)
    modes = {}
    for mode, capabilities in catalog['modes'].items():
        modes[mode] = {}
        for name, claim in capabilities.items():
            modes[mode][name] = {
                'summary': claim['summary'],
                'conditions': claim.get('conditions', []),
                'implemented': {'status': claim['status'], 'evidence': [
                    committed_evidence(root, revision, path)
                    for path in claim['source_paths']]},
                # Test definitions are discoverability, not execution evidence.
                'test_definitions': claim.get('test_paths', []),
                **{level: {'status': 'not_established', 'evidence': []}
                   for level in LEVELS[1:]},
            }
    return {
        'format': FORMAT,
        'artifact': {'component': 'bicdb-cli', 'entrypoint': 'bicdb',
                     'version': version, 'source_revision': revision,
                     'binary_sha256': sha256(binary), 'target': target,
                     'source_build_binding': 'operator_asserted_not_attested',
                     'target_binding': 'operator_asserted_not_inspected'},
        'catalog_sha256': catalog_identity['sha256'],
        'scope': 'release_declarations_only_not_live_configuration_or_deployment_authority',
        'modes': modes,
    }


def validate_manifest(manifest):
    require(manifest.get('format') == FORMAT, 'unsupported manifest format')
    artifact = manifest.get('artifact', {})
    require(artifact.get('component') == 'bicdb-cli' and artifact.get('entrypoint') == 'bicdb', 'unsupported artifact component')
    require(artifact.get('target_binding') == 'operator_asserted_not_inspected', 'unsupported target provenance')
    require(re.fullmatch(r'[0-9a-f]{40}', artifact.get('source_revision', '')) is not None, 'invalid source revision')
    require(re.fullmatch(r'[0-9a-f]{64}', artifact.get('binary_sha256', '')) is not None, 'invalid binary digest')
    require(bool(artifact.get('version')) and bool(artifact.get('target')), 'missing artifact identity')
    require(artifact.get('source_build_binding') == 'operator_asserted_not_attested', 'unsupported source provenance')
    require(manifest.get('scope') == 'release_declarations_only_not_live_configuration_or_deployment_authority', 'invalid scope')
    require(set(manifest.get('modes', {})) == set(MODES), 'invalid mode set')
    require(re.fullmatch(r'[0-9a-f]{64}', manifest.get('catalog_sha256', '')) is not None, 'invalid catalog digest')
    for capabilities in manifest['modes'].values():
        require(bool(capabilities), 'empty mode')
        for claim in capabilities.values():
            require(bool(claim.get('summary')), 'missing claim summary')
            require(isinstance(claim.get('conditions'), list), 'invalid conditions')
            implementation = claim.get('implemented', {})
            require(implementation.get('status') in ('supported', 'unsupported', 'conditional'), 'invalid implementation status')
            require(bool(implementation.get('evidence')), 'missing source evidence')
            if implementation['status'] == 'conditional':
                require(bool(claim['conditions']), 'missing implementation conditions')
            for evidence in implementation['evidence']:
                require(evidence.get('kind') == 'source', 'invalid source evidence kind')
                path = Path(evidence.get('path', ''))
                require(bool(path.parts) and not path.is_absolute() and '..' not in path.parts, 'unsafe source evidence path')
                require(re.fullmatch(r'[0-9a-f]{64}', evidence.get('sha256', '')) is not None, 'invalid source evidence digest')
            # v1 does not accept supplied/unsigned qualification upgrades.
            for level in LEVELS[1:]:
                require(claim.get(level) == {'status': 'not_established', 'evidence': []},
                        'v1 cannot assert executed tests, qualification or deployment')


def admission(manifest, mode, requirements, level):
    validate_manifest(manifest)
    require(mode in MODES and level in LEVELS, 'unknown deployment mode or evidence level')
    require(bool(requirements), 'at least one required capability is necessary')
    denied = []
    for name in requirements:
        claim = manifest['modes'][mode].get(name)
        if claim is None or claim['implemented']['status'] != 'supported' or claim[level]['status'] != 'supported':
            denied.append(name)
    return {'allowed': not denied, 'mode': mode, 'level': level, 'denied': denied,
            'scope': 'artifact_capability_preflight_only_runtime_policy_still_required'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    generate = sub.add_parser('generate')
    generate.add_argument('--binary', type=Path, required=True)
    generate.add_argument('--revision', required=True)
    generate.add_argument('--output', type=Path, required=True)
    verify = sub.add_parser('check')
    verify.add_argument('--manifest', type=Path, required=True)
    verify.add_argument('--binary', type=Path, required=True)
    verify.add_argument('--revision', required=True, help='expected source revision from trusted release provenance')
    verify.add_argument('--expected-sha256', required=True, help='manifest digest from trusted publication, not the adjacent untrusted file')
    verify.add_argument('--mode', choices=MODES, required=True)
    verify.add_argument('--require', dest='requirements', action='append', required=True)
    verify.add_argument('--level', choices=LEVELS, default='qualified')
    args = parser.parse_args()
    try:
        if args.command == 'generate':
            manifest = build_manifest(Path(__file__).resolve().parent.parent, args.binary, args.revision)
            with args.output.open('x') as output:
                output.write(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
            print(args.output)
            return 0
        require(sha256(args.manifest) == args.expected_sha256, 'manifest differs from trusted digest')
        manifest = json.loads(args.manifest.read_text())
        validate_manifest(manifest)
        require(manifest['artifact']['source_revision'] == args.revision, 'unexpected source revision')
        require(sha256(args.binary) == manifest['artifact']['binary_sha256'], 'binary digest mismatch')
        decision = admission(manifest, args.mode, args.requirements, args.level)
        print(json.dumps(decision, sort_keys=True))
        return 0 if decision['allowed'] else 2
    except (ValueError, OSError, subprocess.SubprocessError, KeyError, TypeError, AttributeError) as error:
        print(json.dumps({'allowed': False, 'error': str(error)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
