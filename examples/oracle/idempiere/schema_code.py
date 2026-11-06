"""Plan stored-code migration from a trusted Oracle export without executing SQL.

Only the routine declaration namespace is adapted. Routine bodies and view
queries remain source text. This plan does not resolve dependencies or prove
that code compiles, executes correctly, or has equivalent ownership privileges.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

from import_bicdb import quote, save_report


_IDENTIFIER = r'(?:"(?:[^"]|"")+"|[A-Za-z][A-Za-z0-9_$#]*)'
_DECLARATION = re.compile(
    r'\A(?P<prefix>\s*CREATE\s+(?:OR\s+REPLACE\s+)?)'
    r'(?P<kind>FUNCTION|PROCEDURE)\s+'
    r'(?P<first>' + _IDENTIFIER + r')'
    r'(?:\s*\.\s*(?P<second>' + _IDENTIFIER + r'))?'
    r'(?=\s|\(|$)', re.IGNORECASE)


def identifier(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('expected a nonempty SQL identifier without NUL')
    return value


def decoded_identifier(value):
    return value[1:-1].replace('""', '"') if value.startswith('"') else value.upper()


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def routine_ddl(schema, routine):
    schema = identifier(schema)
    name = identifier(routine['name'])
    kind = routine['kind']
    ddl = routine['ddl']
    if kind not in {'FUNCTION', 'PROCEDURE'} or not isinstance(ddl, str):
        raise ValueError('only standalone FUNCTION and PROCEDURE source is supported')
    match = _DECLARATION.match(ddl)
    if match is None or match['kind'].upper() != kind:
        raise ValueError('routine declaration does not match the exported kind: ' + name)
    declared_name = match['second'] or match['first']
    if decoded_identifier(declared_name) != name:
        raise ValueError('routine declaration does not match the exported name: ' + name)
    if match['second'] and decoded_identifier(match['first']) != schema:
        raise ValueError('routine declaration belongs to a different source schema: ' + name)
    # Preserve every character after the declared name, including comments,
    # parameters, literals, dynamic SQL, and the original routine body.
    return (ddl[:match.start('first')] + quote(schema) + '.' + quote(name)
            + ddl[match.end('second') if match['second'] else match.end('first'):])


def code_plan(manifest):
    schema = identifier(manifest['source_schema'])
    routines, views = [], []
    seen = set()
    for item in manifest['routines']:
        name = identifier(item['name'])
        if name in seen:
            raise ValueError('duplicate stored object: ' + name)
        seen.add(name)
        ddl = routine_ddl(schema, item)
        routines.append(dict(name=name, kind=item['kind'], ddl=ddl,
                             source_sha256=digest(item['ddl']), ddl_sha256=digest(ddl)))
    columns = {}
    for column in manifest['columns']:
        columns.setdefault(column['table'], []).append(column)
    for item in manifest['views']:
        name = identifier(item['name'])
        if name in seen:
            raise ValueError('duplicate stored object: ' + name)
        seen.add(name)
        labels = sorted(columns.get(name, []), key=lambda c: int(c['position']))
        if not labels or [int(c['position']) for c in labels] != list(range(1, len(labels) + 1)):
            raise ValueError('missing or noncontiguous view column positions: ' + name)
        names = [identifier(column['name']) for column in labels]
        if len(set(names)) != len(names):
            raise ValueError('duplicate view column labels: ' + name)
        if item['read_only'] not in {'Y', 'N'}:
            raise ValueError('unknown view update policy: ' + name)
        query = item['query']
        if not isinstance(query, str) or not query.strip():
            raise ValueError('missing source view query: ' + name)
        # USER_VIEWS.TEXT is a query, without a SQL client terminator. Do not
        # strip punctuation: that could change a literal or trailing comment.
        ddl = ('CREATE VIEW ' + quote(schema) + '.' + quote(name) + '('
               + ','.join(quote(label) for label in names) + ') AS ' + query)
        if item['read_only'] == 'Y':
            ddl += '\nWITH READ ONLY'
        views.append(dict(name=name, columns=names, read_only=item['read_only'],
                          ddl=ddl, source_query_sha256=digest(query), ddl_sha256=digest(ddl)))
    return dict(format=1, source_schema=schema, routines=routines, views=views,
                adaptations=['explicit source namespace in routine declarations',
                             'explicit source view column labels'],
                dependency_order_qualified=False, target_ddl_qualified=False,
                routine_execution_qualified=False, view_values_qualified=False,
                ownership_mapping_qualified=False, application_qualified=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        parser.error('report already exists; choose a new path to preserve prior evidence')
    content = (args.snapshot / 'manifest.json').read_bytes()
    report = code_plan(json.loads(content))
    report['schema_manifest_sha256'] = hashlib.sha256(content).hexdigest()
    save_report(args.report, report)
    print(f"Planned {len(report['routines'])} routines and {len(report['views'])} views; no SQL executed")


if __name__ == '__main__':
    main()
