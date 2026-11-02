#!/usr/bin/env python3
"""Compare Oracle DECODE SQL values, errors, metadata and typed PG binds.

Use private connection configuration and dedicated databases. No DSNs or driver
messages are written to the report. This runner creates no persistent objects.
"""
import argparse
import datetime
import json
import os
from pathlib import Path

from compare import canonical, compare_cases, read_cases


def run(oracle, bicdb):
    fixture, digest = read_cases(Path(__file__).with_name('decode-cases.json'))
    report = dict(format=1, oracle_version=oracle.version,
                  bicdb_profile='oracle-preview-v1', fixture_sha256=digest,
                  application_qualified=False, cases=[], success=False,
                  scope=fixture['scope'])
    with oracle.cursor() as cursor:
        cursor.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_NUMERIC_CHARACTERS','NLS_DATE_FORMAT','NLS_DATE_LANGUAGE','NLS_TIMESTAMP_FORMAT')")
        report['nls'] = dict(cursor.fetchall())
    if report['nls'] != dict(NLS_NUMERIC_CHARACTERS='.,', NLS_DATE_FORMAT='DD-MON-RR',
                             NLS_DATE_LANGUAGE='AMERICAN', NLS_TIMESTAMP_FORMAT='DD-MON-RR HH.MI.SSXFF AM'):
        raise ValueError('DECODE fixture requires the recorded preview NLS defaults')
    report['cases'] = compare_cases(oracle, bicdb, fixture['cases'])
    for case, result in zip(fixture['cases'], report['cases']):
        if 'expected' in case:
            result['expected'] = case['expected']
            result['matched'] &= result['oracle'].get('rows') == case['expected']
            with oracle.cursor() as source, bicdb.cursor() as target:
                source.execute(case['sql'])
                target.execute(case['sql'], prepare=False)
                result['oracle_type'] = source.description[0].type_code.name
                result['bicdb_type_oid'] = target.description[0].type_code
            result['matched'] &= (result['oracle_type'] == case['oracle_type']
                                  and result['bicdb_type_oid'] == case['bicdb_type_oid'])
        else:
            result['expected_error'] = case['expected_error']
    bind_cases = [
        ('number_search', "DECODE('001',1,'match','fallback')", ('001', 1), 'match'),
        ('text_search', "DECODE(1,'001','match','fallback')", (1, '001'), 'fallback'),
        ('date_search', "DECODE(TIMESTAMP '2024-02-29 00:00:00.123456',DATE '2024-02-29','match','fallback')",
         (datetime.datetime(2024,2,29,0,0,0,123456), datetime.date(2024,2,29)), 'match'),
        ('timestamp_search', "DECODE(TIMESTAMP '2024-02-29 00:00:00.123456',TIMESTAMP '2024-02-29 00:00:00.123456','match','fallback')",
         (datetime.datetime(2024,2,29,0,0,0,123456), datetime.datetime(2024,2,29,0,0,0,123456)), 'match'),
    ]
    for name, expression, params, expected in bind_cases:
        row = dict(id='bind_' + name, source_sql='SELECT '+expression+' FROM DUAL',
                   target_sql="SELECT DECODE(%s,%s,'match','fallback') FROM DUAL", expected=[[expected]])
        for label, connection in [('oracle',oracle), ('bicdb',bicdb)]:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(row['source_sql']) if label == 'oracle' else cursor.execute(row['target_sql'], params, prepare=True)
                    row[label] = dict(rows=[[canonical(v) for v in values] for values in cursor.fetchall()])
                    row[label]['type'] = cursor.description[0].type_code.name if label == 'oracle' else cursor.description[0].type_code
            except Exception as error:
                detail=error.args[0] if error.args else None
                row[label]=dict(code=getattr(error,'sqlstate',None) or getattr(detail,'full_code',None))
        row['matched'] = (row['oracle'].get('rows') == row['bicdb'].get('rows') == [[expected]]
                          and row['oracle'].get('type') == 'DB_TYPE_VARCHAR' and row['bicdb'].get('type') == 25)
        report['cases'].append(row)
    report['matched'] = sum(row['matched'] for row in report['cases'])
    report['total'] = len(report['cases'])
    report['success'] = report['matched'] == report['total']
    return report


def main():
    import oracledb
    import psycopg
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args=parser.parse_args()
    config=json.loads(args.oracle_config.read_text())
    password=Path(config['password_file'])
    if not password.is_absolute():
        password=args.oracle_config.parent/password
    oracledb.defaults.fetch_decimals=True
    with oracledb.connect(user=config['user'], password=password.read_text().strip(),
                          host=config['host'], port=config['port'], service_name=config['service_name']) as oracle:
        oracle.call_timeout=10000
        with psycopg.connect(os.environ['BICDB_PG_DSN'], autocommit=True, options='-c bicdb.sql_profile=oracle-preview-v1') as target:
            if target.info.parameter_status('bicdb_sql_profile') != 'oracle-preview-v1':
                raise RuntimeError('target did not confirm the preview profile')
            report=run(oracle,target)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ('success','matched','total')}))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
