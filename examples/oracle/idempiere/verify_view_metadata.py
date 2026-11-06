"""Check source view labels and broad protocol type families without executing rows.

This detects unresolved text projections for source numeric/date columns. It is
not a precision, scale, width, nullability, value, privilege, or execution proof.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

from import_bicdb import quote, save_report
from schema_code import code_plan


# Exact numeric OIDs only: floating point is not an equivalent representation of
# arbitrary Oracle NUMBER. CHAR/VARCHAR2 width and padding remain separate gates.
_FAMILIES = {'NUMBER': {20, 21, 23, 1700}, 'CHAR': {25, 1042, 1043},
             'VARCHAR2': {25, 1042, 1043}, 'DATE': {1114}}


def projection_plan(manifest):
    plan = code_plan(manifest)
    columns = {(item['table'], item['name']): item for item in manifest['columns']}
    result = []
    for view in plan['views']:
        expected = []
        for name in view['columns']:
            column = columns[(view['name'], name)]
            kind = column['type']
            if kind not in _FAMILIES:
                raise ValueError('unqualified view metadata family: ' + kind)
            expected.append(dict(name=name, source_type=kind, allowed_oids=sorted(_FAMILIES[kind])))
        result.append(dict(name=view['name'], columns=expected))
    return result


def describe(connection, sql):
    # Use the unnamed statement slot to bound session state. Parse/Describe do
    # not send Execute, including when the view would raise on row evaluation.
    parsed = connection.pgconn.prepare(b'', sql.encode('utf-8'))
    if parsed.status != 1:
        raise ValueError(parsed.error_message.decode(errors='replace'))
    result = connection.pgconn.describe_prepared(b'')
    if result.status != 1:
        raise ValueError(result.error_message.decode(errors='replace'))
    return [dict(name=result.fname(i).decode('utf-8'), oid=result.ftype(i), typmod=result.fmod(i))
            for i in range(result.nfields)]


def compare(expected, actual):
    labels = [item['name'] for item in expected] == [item['name'] for item in actual]
    families = len(expected) == len(actual) and all(
        column['oid'] in source['allowed_oids'] for source, column in zip(expected, actual))
    return dict(labels_matched=labels, type_families_matched=families, matched=labels and families)


def verify(connection, manifest):
    views = projection_plan(manifest)
    report = dict(format=1, source_schema=manifest['source_schema'], checks=[], success=False,
                  protocol='PostgreSQL Parse and Describe; no Execute messages',
                  precision_scale_width_nullability_qualified=False, view_values_qualified=False,
                  view_execution_qualified=False, ownership_mapping_qualified=False,
                  application_qualified=False)
    for view in views:
        row = dict(name=view['name'], expected=view['columns'], matched=False)
        try:
            actual = describe(connection, 'SELECT * FROM ' + quote(manifest['source_schema'])
                              + '.' + quote(view['name']))
            row.update(actual=actual, **compare(view['columns'], actual))
        except Exception as error:
            row.update(error_type=type(error).__name__, detail=str(error))
        report['checks'].append(row)
    report['success'] = all(item['matched'] for item in report['checks'])
    report['verified_views'] = sum(item['matched'] for item in report['checks'])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--pg-dsn-env', default='BICDB_PG_DSN')
    parser.add_argument('--view-ddl-quiesced', action='store_true', required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists(): parser.error('preserve existing report; choose a new path')
    content = (args.snapshot/'manifest.json').read_bytes()
    manifest = json.loads(content)
    projection_plan(manifest)  # Reject unsupported source types before target access.
    import psycopg
    with psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True, connect_timeout=10,
                         options='-c bicdb.sql_profile=oracle-preview-v1') as connection:
        if connection.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
            raise ValueError('target did not confirm the Oracle preview profile')
        report = verify(connection, manifest)
    report.update(schema_manifest_sha256=hashlib.sha256(content).hexdigest(),
                  view_ddl_quiesced_confirmed=True)
    save_report(args.report, report)
    print('Projection labels and type families matched:', report['verified_views'], '/', len(report['checks']))
    return 0 if report['success'] else 1


if __name__ == '__main__': raise SystemExit(main())
