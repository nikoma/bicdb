"""Read-only source observations for LIKE escape values and diagnostics.

Observations are not implementation qualification. Each case retains its SQL,
binds and either the three-valued predicate result or Oracle error number.
"""
import argparse
import json
from pathlib import Path

PLANS = [
    ('default_backslash', r"'a_b' LIKE 'a\_b'", {}),
    ('explicit_backslash', r"'a_b' LIKE 'a\_b' ESCAPE '\'", {}),
    ('explicit_pipe', "'I_name' LIKE 'I|_%' ESCAPE '|'", {}),
    ('explicit_unicode', "'x%tail' LIKE 'x§%%' ESCAPE '§'", {}),
    ('literal_null_escape', "'abc' LIKE 'abc' ESCAPE NULL", {}),
    ('literal_empty_escape', "'abc' LIKE 'abc' ESCAPE ''", {}),
    ('varchar_null_escape', "'abc' LIKE 'abc' ESCAPE CAST(NULL AS VARCHAR2(1))", {}),
    ('char_null_escape', "'abc' LIKE 'abc' ESCAPE CAST(NULL AS CHAR(1))", {}),
    ('bind_null_escape', "'abc' LIKE 'abc' ESCAPE :p_escape", {'p_escape': None}),
    ('bind_empty_escape', "'abc' LIKE 'abc' ESCAPE :p_escape", {'p_escape': ''}),
    ('bind_pipe_escape', "'a_b' LIKE 'a|_b' ESCAPE :p_escape", {'p_escape': '|'}),
    ('null_source_null_escape', "CAST(NULL AS VARCHAR2(8)) LIKE 'abc' ESCAPE NULL", {}),
    ('null_source_long_escape', "CAST(NULL AS VARCHAR2(8)) LIKE 'abc' ESCAPE 'xx'", {}),
    ('null_pattern_long_escape', "'abc' LIKE CAST(NULL AS VARCHAR2(8)) ESCAPE 'xx'", {}),
    ('long_escape', "'abc' LIKE 'abc' ESCAPE 'xx'", {}),
    ('trailing_escape', "'abc' LIKE 'abc|' ESCAPE '|'", {}),
    ('invalid_escape_pair', "'abcx' LIKE 'abc|x' ESCAPE '|'", {}),
    ('percent_escape', "'a_b' LIKE 'a%_b' ESCAPE '%'", {}),
    ('underscore_escape', "'a%b' LIKE 'a_%b' ESCAPE '_'", {}),
]


def observe(factory):
    import oracledb
    report = dict(format=1, reference='oracle', original_application_objects_modified=False,
                  cases=[], observation_completed=False, implementation_qualified=False,
                  application_qualified=False)
    with factory() as connection:
        connection.autocommit = False
        connection.call_timeout = 15000
        report['source_version'] = connection.version
        try:
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION READ ONLY')
                for name, predicate, binds in PLANS:
                    sql = 'SELECT CASE WHEN (' + predicate + ') THEN 1 WHEN NOT (' + predicate + ') THEN 0 ELSE 2 END FROM DUAL'
                    item = dict(id=name, sql=sql, binds=binds)
                    try:
                        cursor.execute(sql, binds or None)
                        item['truth'] = int(cursor.fetchone()[0])
                    except oracledb.Error as error:
                        source = error.args[0]
                        code = getattr(source, 'code', None)
                        item.update(oracle_error_code=code, oracle_full_code=getattr(source, 'full_code', None))
                        if not isinstance(code, int) or code >= 12500:
                            report['cases'].append(item)
                            raise
                    report['cases'].append(item)
        finally:
            connection.rollback()
    report['observation_completed'] = len(report['cases']) == len(PLANS)
    report['connection_closed'] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    import oracledb
    config = json.loads(args.oracle_config.read_text())
    if config['user'].upper() in {'SYS', 'SYSTEM'}:
        parser.error('use a dedicated non-administrative reference schema')
    password = Path(config['password_file'])
    if not password.is_absolute():
        password = args.oracle_config.parent / password
    def factory():
        return oracledb.connect(user=config['user'], password=password.read_text().strip(),
                                host=config['host'], port=config['port'], service_name=config['service_name'])
    report = observe(factory)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(observation_completed=report['observation_completed'], cases=len(report['cases']))))
    return 0 if report['observation_completed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
