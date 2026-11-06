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
                cursor.execute("SELECT PARAMETER,VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET')")
                report['database_nls'] = dict(cursor.fetchall())
                cursor.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_COMP','NLS_SORT','NLS_LENGTH_SEMANTICS')")
                report['session_nls'] = dict(cursor.fetchall())
                # Compare a literal with the same bound Unicode value. Oracle
                # escape observations depend on the source/client encoding;
                # retain byte lengths and dumps rather than inferring them.
                cursor.execute("SELECT LENGTH('§'),LENGTHB('§'),DUMP('§',1016),LENGTH(:v),LENGTHB(:v),DUMP(:v,1016) FROM DUAL", {'v': '§'})
                report['unicode_encoding_control'] = dict(zip(
                    ['literal_length', 'literal_byte_length', 'literal_dump',
                     'bind_length', 'bind_byte_length', 'bind_dump'], cursor.fetchone()))
                report['reference_driver_version'] = oracledb.__version__
                report['reference_driver_thin_mode'] = oracledb.is_thin_mode()
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
                        if (not isinstance(code, int) or code <= 0 or code >= 12500
                                or not str(item['oracle_full_code']).startswith('ORA-')):
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
