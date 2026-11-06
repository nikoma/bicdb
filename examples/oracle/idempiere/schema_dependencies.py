"""Capture source view dependency order after verifying saved code identities.

Use a trusted snapshot and its Oracle owner connection while source DDL is
quiesced. Oracle dictionary dependencies do not cover dynamic SQL, runtime
privileges, or semantic equivalence on the destination.
"""
import argparse
import hashlib
import json
from pathlib import Path

from import_bicdb import save_report
from schema_code import code_plan


def view_order(manifest, dependencies):
    names = [view['name'] for view in manifest['views']]
    if len(names) != len(set(names)):
        raise ValueError('duplicate source views')
    needed = {name: set() for name in names}
    for item in dependencies:
        if item['kind'] != 'VIEW' or item['name'] not in needed:
            continue
        if item['link'] is not None:
            raise ValueError('remote view dependencies require a separate migration policy')
        if item['referenced_owner'] == manifest['source_schema'] and item['referenced_kind'] == 'VIEW':
            reference = item['referenced_name']
            if reference not in needed:
                raise ValueError('source view dependency missing from snapshot: ' + reference)
            needed[item['name']].add(reference)
    order = []
    while len(order) < len(names):
        ready = [name for name in names if name not in order and needed[name].issubset(order)]
        if not ready:
            raise ValueError('cyclic source view dependencies require explicit reconciliation')
        order.extend(ready)
    return order


def capture(cursor, manifest):
    code_plan(manifest)  # Validate exported declaration identities and projections.
    expected = {(item['kind'], item['name']): item['ddl'] for item in manifest['routines']}
    cursor.execute("SELECT name,type,line,text FROM user_source WHERE type IN ('FUNCTION','PROCEDURE') ORDER BY type,name,line")
    source = {}
    for name, kind, line, text in cursor:
        key = (kind, name)
        if key not in expected:
            raise ValueError('live source has a routine outside the snapshot')
        lines = source.setdefault(key, [])
        if isinstance(line, bool) or line != len(lines) + 1:
            raise ValueError('live routine source lines are incomplete or unordered')
        if text is not None and not isinstance(text, str):
            raise ValueError('live routine source is not text')
        lines.append('\n' if text is None else text)
    if set(source) != set(expected) or any('CREATE OR REPLACE ' + ''.join(lines) != expected[key]
                                          for key, lines in source.items()):
        raise ValueError('live routine source differs from saved snapshot')
    cursor.execute('SELECT view_name,text,read_only FROM user_views ORDER BY view_name')
    actual = {}
    for name, query, readonly in cursor:
        if name in actual:
            raise ValueError('duplicate live source view')
        actual[name] = (query, readonly)
    saved = {item['name']: (item['query'], item['read_only']) for item in manifest['views']}
    if actual != saved:
        raise ValueError('live view source differs from saved snapshot')
    cursor.execute('SELECT name,type,referenced_owner,referenced_name,referenced_type,'
                   'referenced_link_name,dependency_type FROM user_dependencies '
                   'ORDER BY type,name,referenced_owner,referenced_type,referenced_name')
    fields = ['name', 'kind', 'referenced_owner', 'referenced_name', 'referenced_kind', 'link', 'dependency_kind']
    dependencies = [dict(zip(fields, row)) for row in cursor]
    return dict(format=1, source_schema=manifest['source_schema'],
                code_identities_verified=True, source_ddl_quiesced_confirmed=True,
                view_order=view_order(manifest, dependencies), dependencies=dependencies,
                static_view_dependency_order_captured=True,
                dynamic_dependencies_qualified=False, target_view_types_qualified=False,
                target_execution_qualified=False, application_qualified=False)


def ordered_views(manifest, report, manifest_sha256):
    if (report.get('format') != 1 or report.get('schema_manifest_sha256') != manifest_sha256
            or report.get('source_schema') != manifest['source_schema']
            or report.get('code_identities_verified') is not True
            or report.get('source_ddl_quiesced_confirmed') is not True
            or report.get('static_view_dependency_order_captured') is not True):
        raise ValueError('dependency report does not qualify this source snapshot')
    expected = view_order(manifest, report['dependencies'])
    if report.get('view_order') != expected:
        raise ValueError('dependency report view order differs from captured edges')
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--source-ddl-quiesced', action='store_true', required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        parser.error('preserve the existing dependency report; choose a new path')
    content = (args.snapshot/'manifest.json').read_bytes()
    manifest = json.loads(content)
    config = json.loads(args.oracle_config.read_text())
    if config['user'] != manifest['source_schema']:
        raise ValueError('connect as the exact exported source schema owner')
    password = Path(config['password_file'])
    if not password.is_absolute(): password = args.oracle_config.parent/password
    import oracledb
    oracledb.defaults.fetch_lobs = False
    with oracledb.connect(user=config['user'], password=password.read_text().strip(),
                          host=config['host'], port=config['port'], service_name=config['service_name']) as connection:
        connection.call_timeout = 30000
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION READ ONLY')
            report = capture(cursor, manifest)
        report['oracle_server_version'] = connection.version
    report['schema_manifest_sha256'] = hashlib.sha256(content).hexdigest()
    save_report(args.report, report)
    print('Captured dependency order for', len(report['view_order']), 'source views; target execution remains unqualified')


if __name__ == '__main__': main()
