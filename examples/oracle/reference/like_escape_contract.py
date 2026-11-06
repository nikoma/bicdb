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
    ('bound_unicode_escape', "'x%tail' LIKE :pattern ESCAPE :escape", {'pattern': 'x§%%', 'escape': '§'}),
    ('national_unicode_escape', "N'x%tail' LIKE N'x§%%' ESCAPE N'§'", {}),
    ('unistr_unicode_escape', r"'x%tail' LIKE 'x'||UNISTR('\00A7')||'%%' ESCAPE UNISTR('\00A7')", {}),
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


# Keep expression and filtering observations separate: constant folding or
# invalid-escape validation may differ before a row predicate is evaluated.
CONTEXT_PLANS = [
    ('where_literal_null', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL UNION ALL SELECT NULL FROM DUAL) WHERE v LIKE 'abc' ESCAPE NULL", {}),
    ('where_bound_null', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL UNION ALL SELECT NULL FROM DUAL) WHERE v LIKE 'abc' ESCAPE :e", {'e': None}),
    ('where_literal_empty', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL) WHERE v LIKE 'abc' ESCAPE ''", {}),
    ('where_null_source', "SELECT COUNT(*) FROM (SELECT CAST(NULL AS VARCHAR2(8)) AS v FROM DUAL) WHERE v LIKE 'abc' ESCAPE :e", {'e': None}),
    ('where_no_rows', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL WHERE 1=0) WHERE v LIKE 'abc' ESCAPE NULL", {}),
]


def national_plans():
    """Read-only character-family probes; expected values come from the source."""
    def literal(value, national):
        return ('N' if national else '') + "'" + value.replace("'", "''") + "'"
    plans = []
    for national in [False, True]:
        family = 'national' if national else 'ordinary'
        for label, value in [('bmp', 'é'), ('supplementary', '😀'), ('composite', 'e\u0301')]:
            for number, pattern in enumerate(['_', '__', '%', '_%', '%_', value]):
                plans.append((f'{family}_{label}_{number}',
                              literal(value, national)+' LIKE '+literal(pattern, national), {}))
        for pattern in ['_', '__']:
            plans.append((f'{family}_mixed_pattern_{len(pattern)}',
                          literal('😀', national)+' LIKE '+literal(pattern, not national), {}))
        for pattern_national in [False, True]:
            for escape_national in [False, True]:
                for label, escape in [('ascii', '|'), ('latin', '§'), ('han', '界'), ('supplementary', '😀')]:
                    plans.append((f'{family}_escape_{label}_p{int(pattern_national)}_e{int(escape_national)}',
                                  literal('x%tail', national)+' LIKE '
                                  +literal('x'+escape+'%%', pattern_national)+' ESCAPE '
                                  +literal(escape, escape_national), {}))
    plans.extend([
        ('national_null_escape', "N'abc' LIKE N'abc' ESCAPE NULL", {}),
        ('national_empty_escape', "N'abc' LIKE N'abc' ESCAPE N''", {}),
        ('national_null_source', "CAST(NULL AS NVARCHAR2(8)) LIKE N'abc' ESCAPE N'§'", {}),
        ('national_null_pattern', "N'abc' LIKE CAST(NULL AS NVARCHAR2(8))", {}),
        ('national_fixed_source_no_padding_pattern', "CAST(N'A' AS NCHAR(3)) LIKE N'A'", {}),
        ('national_fixed_source_wildcard_padding', "CAST(N'A' AS NCHAR(3)) LIKE N'A__'", {}),
        ('ordinary_fixed_source_national_pattern', "CAST('A' AS CHAR(3)) LIKE N'A__'", {}),
        ('national_fixed_pattern', "N'A' LIKE CAST(N'A' AS NCHAR(3))", {}),
        ('national_cast_single_escape', "N'x%tail' LIKE N'x§%%' ESCAPE CAST(N'§' AS NCHAR(1))", {}),
        ('national_cast_padded_escape', "N'x%tail' LIKE N'x§%%' ESCAPE CAST(N'§' AS NCHAR(2))", {}),
    ])
    return plans


NATIONAL_CONTEXT_PLANS = [
    ('national_null_input', "SELECT COUNT(*) FROM (SELECT CAST(NULL AS NVARCHAR2(8)) AS v FROM DUAL) WHERE v LIKE N'abc' ESCAPE N'§'", {}),
    ('national_empty_input', "SELECT COUNT(*) FROM (SELECT CAST('abc' AS NVARCHAR2(8)) AS v FROM DUAL WHERE 1=0) WHERE v LIKE N'abc' ESCAPE NULL", {}),
]


# CAST bounds and assignment errors are separate from pattern matching. Keep
# these read-only source controls distinct so LIKE coverage cannot imply DDL
# or overflow qualification.
NATIONAL_CAST_PLANS = [
    ('nvarchar_ascii_fit', "CAST('abc' AS NVARCHAR2(3)) = N'abc'", {}),
    ('nvarchar_ascii_overflow', "CAST('abc' AS NVARCHAR2(2)) IS NOT NULL", {}),
    ('nchar_ascii_fit', "CAST('abc' AS NCHAR(3)) = N'abc'", {}),
    ('nchar_ascii_overflow', "CAST('abc' AS NCHAR(2)) IS NOT NULL", {}),
    ('nvarchar_supplementary_fit', "CAST(N'😀' AS NVARCHAR2(2)) = N'😀'", {}),
    ('nvarchar_supplementary_overflow', "CAST(N'😀' AS NVARCHAR2(1)) IS NOT NULL", {}),
    ('nchar_supplementary_fit', "CAST(N'😀' AS NCHAR(2)) LIKE N'__'", {}),
    ('nchar_supplementary_overflow', "CAST(N'😀' AS NCHAR(1)) IS NOT NULL", {}),
    ('nvarchar_bmp_fit', "CAST(N'界' AS NVARCHAR2(1)) = N'界'", {}),
    ('nchar_bmp_padding', "CAST(N'界' AS NCHAR(3)) LIKE N'界__'", {}),
    ('nvarchar_numeric', "CAST(123 AS NVARCHAR2(8)) = N'123'", {}),
    ('nchar_numeric', "CAST(123 AS NCHAR(4)) LIKE N'123_'", {}),
    ('nvarchar_null', "CAST(NULL AS NVARCHAR2(2)) IS NULL", {}),
    ('nchar_null', "CAST(NULL AS NCHAR(2)) IS NULL", {}),
    ('nvarchar_empty', "CAST(N'' AS NVARCHAR2(2)) IS NULL", {}),
    ('nchar_empty', "CAST(N'' AS NCHAR(2)) IS NULL", {}),
    ('nvarchar_zero_width', "CAST(NULL AS NVARCHAR2(0)) IS NULL", {}),
    ('nchar_zero_width', "CAST(NULL AS NCHAR(0)) IS NULL", {}),
    ('nvarchar_standard_limit', "CAST(NULL AS NVARCHAR2(2000)) IS NULL", {}),
    ('nvarchar_over_standard', "CAST(NULL AS NVARCHAR2(2001)) IS NULL", {}),
    ('nchar_standard_limit', "CAST(NULL AS NCHAR(1000)) IS NULL", {}),
    ('nchar_over_standard', "CAST(NULL AS NCHAR(1001)) IS NULL", {}),
    ('nvarchar_trailing_spaces', "CAST(N'ab  ' AS NVARCHAR2(2)) = N'ab'", {}),
    ('nchar_trailing_spaces', "CAST(N'ab  ' AS NCHAR(2)) = N'ab'", {}),
]
NATIONAL_CAST_CONTEXT_PLANS = [
    ('nvarchar_ascii_value_overflow', "SELECT CAST('abc' AS NVARCHAR2(2)) FROM DUAL", {}),
    ('nchar_ascii_value_overflow', "SELECT CAST('abc' AS NCHAR(2)) FROM DUAL", {}),
    ('nvarchar_supplementary_value_overflow', "SELECT CAST(N'😀' AS NVARCHAR2(1)) FROM DUAL", {}),
    ('nchar_supplementary_value_overflow', "SELECT CAST(N'😀' AS NCHAR(1)) FROM DUAL", {}),
    ('nvarchar_trailing_space_value_overflow', "SELECT CAST(N'ab  ' AS NVARCHAR2(2)) FROM DUAL", {}),
    ('nchar_trailing_space_value_overflow', "SELECT CAST(N'ab  ' AS NCHAR(2)) FROM DUAL", {}),
    ('nvarchar_numeric_value_overflow', "SELECT CAST(123 AS NVARCHAR2(2)) FROM DUAL", {}),
    ('nchar_padding_value', "SELECT CAST(N'界' AS NCHAR(3)) FROM DUAL", {}),
    ('nvarchar_supplementary_value', "SELECT CAST(N'😀' AS NVARCHAR2(2)) FROM DUAL", {}),
    ('nvarchar_numeric_value', "SELECT CAST(123 AS NVARCHAR2(8)) FROM DUAL", {}),
    ('nvarchar_raw_value', "SELECT CAST(HEXTORAW('AB') AS NVARCHAR2(8)) FROM DUAL", {}),
    ('nvarchar_date_value', "SELECT CAST(DATE '2024-01-02' AS NVARCHAR2(40)) FROM DUAL", {}),
    ('nvarchar_timestamp_value', "SELECT CAST(TIMESTAMP '2024-01-02 03:04:05' AS NVARCHAR2(80)) FROM DUAL", {}),
    ('oversized_nvarchar_empty_input', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL WHERE 1=0) WHERE CAST(v AS NVARCHAR2(2001)) IS NULL", {}),
    ('oversized_nchar_empty_input', "SELECT COUNT(*) FROM (SELECT 'abc' AS v FROM DUAL WHERE 1=0) WHERE CAST(v AS NCHAR(1001)) IS NULL", {}),
]


def national_cast_unit_contexts():
    """Inspect raw national code units without asking a text driver to decode
    potentially unpaired UTF-16 after a bounded SQL CAST. Keep the direct value
    contract separate: a successful dump is not text transport qualification.
    """
    result = []
    for name, sql, binds in NATIONAL_CAST_CONTEXT_PLANS:
        if sql.startswith('SELECT CAST(') and sql.endswith(' FROM DUAL'):
            expression = sql[len('SELECT '):-len(' FROM DUAL')]
            sql = 'SELECT DUMP(' + expression + ',1016) FROM DUAL'
        result.append((name, sql, binds))
    return result


def semantic_error(code, full_code):
    # ORA-12704 (character set mismatch), for example, is a semantic
    # observation, not a transport failure merely because it exceeds 12500.
    return (isinstance(code, int) and code > 0 and str(full_code).startswith('ORA-')
            and code not in {28, 1013, 1017, 1033, 1034, 1089, 1090, 2396, 3113, 3114, 3135}
            and not 12100 <= code < 12300 and not 12500 <= code < 12700)


def observe(factory, *, contract='ordinary'):
    import oracledb
    if contract not in {'ordinary', 'national', 'national-casts', 'national-cast-units'}:
        raise ValueError('unknown LIKE observation contract')
    plans, contexts = {
        'ordinary': (PLANS, CONTEXT_PLANS),
        'national': (national_plans(), NATIONAL_CONTEXT_PLANS),
        'national-casts': (NATIONAL_CAST_PLANS, NATIONAL_CAST_CONTEXT_PLANS),
        'national-cast-units': (NATIONAL_CAST_PLANS, national_cast_unit_contexts()),
    }[contract]
    report = dict(format=1, reference='oracle', contract=contract, original_application_objects_modified=False,
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
                cursor.execute("SELECT PARAMETER,VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_COMP','NLS_SORT','NLS_LENGTH_SEMANTICS','NLS_DATE_FORMAT','NLS_TIMESTAMP_FORMAT','NLS_TIMESTAMP_TZ_FORMAT','NLS_NUMERIC_CHARACTERS','NLS_DATE_LANGUAGE')")
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
                for name, predicate, binds in plans:
                    sql = 'SELECT CASE WHEN (' + predicate + ') THEN 1 WHEN NOT (' + predicate + ') THEN 0 ELSE 2 END FROM DUAL'
                    item = dict(id=name, sql=sql, binds=binds)
                    try:
                        cursor.execute(sql, binds or None)
                        item['truth'] = int(cursor.fetchone()[0])
                    except oracledb.Error as error:
                        source = error.args[0]
                        code = getattr(source, 'code', None)
                        item.update(oracle_error_code=code, oracle_full_code=getattr(source, 'full_code', None))
                        if not semantic_error(code, item['oracle_full_code']):
                            report['cases'].append(item)
                            raise
                    report['cases'].append(item)
                report['predicate_contexts'] = []
                for name, sql, binds in contexts:
                    item = dict(id=name, sql=sql, binds=binds)
                    try:
                        cursor.execute(sql, binds or None)
                        item['rows'] = [list(row) for row in cursor.fetchall()]
                    except oracledb.Error as error:
                        source = error.args[0]
                        code = getattr(source, 'code', None)
                        item.update(oracle_error_code=code, oracle_full_code=getattr(source, 'full_code', None))
                        if not semantic_error(code, item['oracle_full_code']):
                            report['predicate_contexts'].append(item)
                            raise
                    report['predicate_contexts'].append(item)
        finally:
            connection.rollback()
    report['observation_completed'] = (len(report['cases']) == len(plans)
                                       and len(report['predicate_contexts']) == len(contexts))
    report['connection_closed'] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--contract', choices=['ordinary', 'national', 'national-casts', 'national-cast-units'], default='ordinary')
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
    report = observe(factory, contract=args.contract)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(observation_completed=report['observation_completed'], cases=len(report['cases']))))
    return 0 if report['observation_completed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
