#!/usr/bin/env python3
"""Compare completed BicDB and saved-export Oracle orphan inventories offline."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from foreign_keys import declarations, save_report


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError('orphan count must be a nonnegative integer')
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def require_digest(value):
    if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('inventory requires a SHA-256 digest')


def inventory_counts(report, manifest, digest, plan, reference=False):
    if not isinstance(report, dict):
        raise ValueError('inventory must be an object')
    phase = 'oracle-export-reference' if reference else 'foreign-key-orphans'
    if (type(report.get('format')) is not int or report['format'] != 1 or
            report.get('phase') != phase or report.get('manifest_sha256') != digest or
            report.get('source_schema') != manifest['source_schema']):
        raise ValueError('inventory identity differs from the selected export')
    flags = ['success', 'inventory_completed', 'validated_relationships_clean']
    flags += ['reference_rows_equal_export', 'cleanup_completed'] if reference else ['writers_quiesced_asserted']
    if any(report.get(flag) is not True for flag in flags):
        raise ValueError('inventory is incomplete, unsuccessful, or lacks required verification')
    require_digest(report.get('target_sha256'))
    if reference:
        if report.get('data_replay_only') is not True or report.get('cleanup_errors') != []:
            raise ValueError('reference must be a cleaned saved-export data replay')
        tables = {item['name']: item for item in manifest['tables']}
        verified = report.get('verified_tables')
        if not isinstance(verified, list) or len(verified) != len(tables):
            raise ValueError('reference must verify every exported table')
        seen = set()
        for item in verified:
            name = item.get('table') if isinstance(item, dict) else None
            if name not in tables or name in seen or count(item.get('rows')) != tables[name]['rows']:
                raise ValueError('reference table coverage or row counts differ from export')
            require_digest(item.get('sha256'))
            seen.add(name)
    else:
        require_digest(report.get('migration_report_sha256'))
    if not reference and report.get('profile') != 'oracle-preview-v1':
        raise ValueError('target inventory must use oracle-preview-v1')
    if count(report.get('source_foreign_keys')) != len(plan):
        raise ValueError('inventory relationship count differs from export')
    entries = report.get('completed')
    if not isinstance(entries, list) or len(entries) != len(plan):
        raise ValueError('inventory must contain every source relationship exactly once')
    expected = {canonical(item): item for item in plan}
    counts = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('invalid inventory entry')
        key = canonical(entry.get('declaration'))
        if key not in expected or key in counts:
            raise ValueError('inventory has a changed, duplicate, or unknown declaration')
        value = count(entry.get('orphan_rows'))
        if expected[key]['validated'] and value:
            raise ValueError('validated relationship has unmatched rows')
        counts[key] = value
    if not reference:
        affected = {item[key] for item in plan for key in ['table', 'parent_table']}
        fingerprints = report.get('table_fingerprints')
        if not isinstance(fingerprints, dict) or set(fingerprints) != affected:
            raise ValueError('target inventory lacks affected-table fingerprints')
        for item in fingerprints.values():
            if (not isinstance(item, dict) or not isinstance(item.get('before'), dict) or
                    canonical(item['before']) != canonical(item.get('after'))):
                raise ValueError('target inventory does not preserve its row fingerprints')
            count(item['before'].get('rows'))
            require_digest(item['before'].get('multiset_sha256'))
    return counts


def compare(args):
    manifest_path = args.snapshot/'manifest.json'
    inputs = [manifest_path, args.reference_report, args.target_report]
    if args.report.resolve() in {path.resolve() for path in inputs}:
        raise ValueError('comparison report must not overwrite an input')
    report = dict(format=1, phase='foreign-key-orphan-comparison', success=False,
                  comparison_completed=False, counts_match=False, compared_relationships=0,
                  mismatches=[], data_compared_to_source=False, live_source_compared=False,
                  application_qualified=False)
    # Invalidate a prior success even if this attempt fails while reading inputs.
    save_report(args.report, report)
    try:
        raw = [path.read_bytes() for path in inputs]
        manifest, reference, target = [json.loads(value) for value in raw]
        digest = hashlib.sha256(raw[0]).hexdigest()
        report.update(manifest_sha256=digest, source_schema=manifest['source_schema'],
                      reference_report_sha256=hashlib.sha256(raw[1]).hexdigest(),
                      target_report_sha256=hashlib.sha256(raw[2]).hexdigest())
        plan = declarations(manifest)
        source_counts = inventory_counts(reference, manifest, digest, plan, reference=True)
        target_counts = inventory_counts(target, manifest, digest, plan)
        for item in plan:
            key = canonical(item)
            if source_counts[key] != target_counts[key]:
                report['mismatches'].append(dict(declaration=item, reference_orphan_rows=source_counts[key],
                                                 target_orphan_rows=target_counts[key]))
        report.update(comparison_completed=True, compared_relationships=len(plan),
                      counts_match=not report['mismatches'], success=not report['mismatches'])
    finally:
        save_report(args.report, report)
    if not report['success']:
        raise ValueError('target orphan counts differ from saved-export Oracle reference')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--reference-report', type=Path, required=True)
    parser.add_argument('--target-report', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    compare(parser.parse_args())


if __name__ == '__main__':
    main()
