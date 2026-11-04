"""Compare scalar aggregate values and PostgreSQL transport types.

Both text and binary result formats use prepared PostgreSQL parameters. Oracle
reference metadata is checked as NUMBER, separately from PostgreSQL NUMERIC OIDs.
"""
import uuid

from aggregate_conditionals import execute, main as comparison_main
from compare import canonical


def run(reference, target, native=False):
    token = uuid.uuid4().hex[:12].upper()
    prefix = 'public.' if native else ''
    headers, lines = [prefix + 'COMPAT_ST_' + name + '_' + token for name in ['H', 'L']]
    plans = [
        ('correlated_sum', f'(SELECT COALESCE(SUM(l.amount),0) FROM {lines} l WHERE h.k=l.header_k)', 1, [[{'number':'123.5'}]]),
        ('empty_sum', f'(SELECT COALESCE(SUM(l.amount),0) FROM {lines} l WHERE l.header_k=9)', 1, [[{'number':'0'}]]),
        ('null_sum', f'(SELECT SUM(l.amount) FROM {lines} l WHERE l.header_k=9)', 1, [[None]]),
        ('empty_outer', f'(SELECT SUM(l.amount) FROM {lines} l WHERE h.k=l.header_k)', 9, []),
        ('max_amount', f'(SELECT MAX(l.amount) FROM {lines} l WHERE h.k=l.header_k)', 1, [[{'number':'123.5'}]]),
        ('wrapped_sum', f'COALESCE((SELECT SUM(l.amount) FROM {lines} l WHERE h.k=l.header_k),0)', 1, [[{'number':'123.5'}]]),
    ]
    report = dict(format=1, reference='postgres' if native else 'oracle', application_qualified=False, target_protocol='prepared, text and binary results', cases=[], cleanup_errors=[], success=False)
    created = []
    try:
        for label, conn in [('source', reference), ('bicdb', target)]:
            for name, sql in [(headers,f'CREATE TABLE {headers}(k NUMERIC(20,0) PRIMARY KEY)'),(lines,f'CREATE TABLE {lines}(header_k NUMERIC(20,0),amount NUMERIC(20,2))')]:
                execute(conn, sql)
                created.append((label,conn,name))
            execute(conn, f'INSERT INTO {headers} VALUES(1)')
            execute(conn, f'INSERT INTO {lines} VALUES(1,123.50)')
        for name, expression, key, expected in plans:
            for binary in [False, True]:
                case = dict(id=name+('_binary' if binary else '_text'), sql=f'SELECT {expression} FROM {headers} h WHERE h.k={{P}}', key=key, expected=dict(rows=expected), expected_pg_oids=[1700])
                source_type_ok = False
                for label, conn in [('source', reference), ('bicdb', target)]:
                    oracle_source = label == 'source' and not native
                    sql = case['sql'].replace('{P}', ':1' if oracle_source else '%s')
                    result = {}
                    try:
                        cursor = conn.cursor() if oracle_source else conn.cursor(binary=binary)
                        with cursor:
                            if oracle_source:
                                import oracledb
                                cursor.execute(sql, [key])
                                result['oracle_types'] = [str(column.type_code) for column in cursor.description]
                                source_type_ok = all(column.type_code == oracledb.DB_TYPE_NUMBER for column in cursor.description)
                            else:
                                cursor.execute(sql, [key], prepare=True)
                                result['type_oids'] = [column.type_code for column in cursor.description]
                                if label == 'source':
                                    source_type_ok = result['type_oids'] == [1700]
                            result['rows'] = [[canonical(value) for value in row] for row in cursor.fetchall()]
                    except Exception as exc:
                        result.update(exception=type(exc).__name__,sqlstate=getattr(exc,'sqlstate',None),message=str(exc).splitlines()[0])
                    case[label] = result
                case['source_qualified'] = source_type_ok and case['source'].get('rows') == expected
                case['matched'] = case['source_qualified'] and case['bicdb'].get('rows') == expected and case['bicdb'].get('type_oids') == [1700] and 'exception' not in case['bicdb']
                report['cases'].append(case)
    finally:
        for label, conn, name in reversed(created):
            try:
                execute(conn, f'DROP TABLE {name}')
            except Exception as exc:
                report['cleanup_errors'].append(dict(backend=label,exception=type(exc).__name__))
    report['total'] = len(report['cases'])
    report['matched'] = sum(case['matched'] for case in report['cases'])
    report['source_qualified'] = len(report['cases']) == len(plans)*2 and all(case['source_qualified'] for case in report['cases'])
    report['success'] = report['source_qualified'] and report['matched'] == report['total'] and not report['cleanup_errors']
    return report


def main():
    return comparison_main(runner=run, description=__doc__)


if __name__ == '__main__':
    raise SystemExit(main())
