"""Qualify embedded SQL columns and procedural/SQL-function argument names.

Oracle PL/SQL and PostgreSQL SQL-language functions have separate reference
declarations. This does not qualify PostgreSQL PL/pgSQL variable-conflict modes.
"""
import uuid

from aggregate_conditionals import execute, main as comparison_main


def run(reference, target, native=False):
    token = uuid.uuid4().hex[:12].upper()
    schema = 'public'
    prefix = schema + '.' if native else ''
    values, keys = [prefix + 'COMPAT_SC_' + name + '_' + token for name in ['VALUES', 'KEYS']]
    plans = [
        ('column', values, 'label', ''),
        ('qualified', values + ' r', 'r.label', ''),
        ('expression', values, 'TRIM(label)', ''),
        ('filter', values, 'label', " AND label='column'"),
        ('variable_only', keys, 'label', ''),
    ]
    ddl = [
        ('TABLE', values, f'CREATE TABLE {values}(k INTEGER,label VARCHAR(20))'),
        ('TABLE', keys, f'CREATE TABLE {keys}(k INTEGER)'),
    ]
    routines = []
    for index, (name, relation, projection, condition) in enumerate(plans):
        routine = prefix + 'COMPAT_SC_F' + str(index) + '_' + token
        query = f'SELECT {projection} FROM {relation} WHERE k=p_key{condition}'
        if native:
            sql = f'CREATE FUNCTION {routine}(label TEXT,p_key INTEGER) RETURNS TEXT LANGUAGE sql AS $body${query}$body$'
        else:
            sql = f"CREATE FUNCTION {routine}(p_key NUMBER) RETURN VARCHAR2 IS label VARCHAR2(20):='local'; BEGIN SELECT {projection} INTO label FROM {relation} WHERE k=p_key{condition}; RETURN label; END;"
        ddl.append(('FUNCTION', routine, sql))
        routines.append((name, routine, [['local' if name == 'variable_only' else 'column']]))
    report = dict(format=1, reference='postgres' if native else 'oracle', application_qualified=False, target_protocol='prepared', native_language='sql' if native else None, cases=[], cleanup_errors=[], success=False)
    if not native:
        report['target_schema'] = execute(target, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL", rows=True)[0][0]
        if report['target_schema'].upper() == 'PUBLIC':
            raise ValueError('use a dedicated existing non-public Oracle-profile target schema')
    created = []
    try:
        for label, conn in [('source', reference), ('bicdb', target)]:
            for kind, name, sql in ddl:
                execute(conn, sql)
                created.append((label, conn, kind, name))
            execute(conn, f"INSERT INTO {values} VALUES(1,'column')")
            execute(conn, f'INSERT INTO {keys} VALUES(1)')
        for name, routine, expected in routines:
            case = dict(id=name, expected=dict(rows=expected))
            for label, conn in [('source', reference), ('bicdb', target)]:
                marker = ':1' if label == 'source' and not native else '%s'
                args = '%s,%s' if native else marker
                sql = f'SELECT {routine}({args})' + ('' if native else ' FROM DUAL')
                params = ['local', 1] if native else [1]
                try:
                    case[label] = dict(rows=execute(conn, sql, params, rows=True))
                except Exception as exc:
                    code = getattr(exc.args[0], 'code', None) if exc.args else None
                    case[label] = dict(oracle_code=code, sqlstate=getattr(exc, 'sqlstate', None), exception=type(exc).__name__, message=str(exc).splitlines()[0])
            case['source_qualified'] = case['source'] == case['expected']
            case['matched'] = case['source_qualified'] and case['bicdb'] == case['expected']
            report['cases'].append(case)
    finally:
        for label, conn, kind, name in reversed(created):
            try:
                signature = '(TEXT,INTEGER)' if native and kind == 'FUNCTION' else ''
                execute(conn, f'DROP {kind} {name}{signature}')
            except Exception as exc:
                report['cleanup_errors'].append(dict(backend=label, kind=kind, exception=type(exc).__name__))
    report['total'] = len(report['cases'])
    report['matched'] = sum(case['matched'] for case in report['cases'])
    report['source_qualified'] = len(report['cases']) == len(plans) and all(case['source_qualified'] for case in report['cases'])
    report['success'] = report['source_qualified'] and report['matched'] == report['total'] and not report['cleanup_errors']
    return report


def main():
    return comparison_main(runner=run, description=__doc__)


if __name__ == '__main__':
    raise SystemExit(main())
