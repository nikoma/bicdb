"""Independent expected orphan counts on disposable tables, without repair.

Uses the public inventory query builder, with explicit expected counts rather
than treating equal failures as agreement. Oracle empty strings and PostgreSQL
empty strings have different expected results. This is a bounded query fixture,
not a source application inventory or full referential-integrity qualification.
"""
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'idempiere'))
from foreign_key_orphans import count_orphans
from import_bicdb import quote


CASES = ['duplicate_orphans', 'composite_partial_null', 'crossed_key_order',
         'empty_parent', 'empty_child', 'nullable_parent', 'self_reference',
         'fixed_character_padding', 'empty_character_key']


def expected_count(case, oracle):
    return dict(duplicate_orphans=2, composite_partial_null=2, crossed_key_order=1,
                empty_parent=3, empty_child=0, nullable_parent=1, self_reference=2,
                fixed_character_padding=2, empty_character_key=2 if oracle else 3)[case]


def observe(factory, progress=None):
    report = dict(cases=[], cleanup_errors=[])
    for case in CASES:
        outcome = dict(case=case, success=False)
        with factory() as connection:
            native = type(connection).__module__.startswith('oracledb')
            oracle = native or connection.info.parameter_status('bicdb_sql_profile') == 'oracle-preview-v1'
            connection.autocommit = True
            report['server_version'] = connection.version if native else connection.info.server_version
            cursor = connection.cursor()
            cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual" if native else 'SELECT current_schema()')
            schema = cursor.fetchone()[0]
            stem = 'BIC_ORP_' + uuid.uuid4().hex[:10].upper()
            # Quoted dots and spaces are identifier content on both references.
            # Oracle rejects an embedded double quote, so those escapes are
            # exercised separately by the PostgreSQL/native inventory tests.
            parent, child = stem + '.P Q', stem + '.C Q'
            created = []
            if progress is not None:progress(dict(case=case, schema=schema, parent=parent, child=child))
            def relation(name):return quote(schema) + '.' + quote(name)
            def create(name, columns):
                cursor.execute('CREATE TABLE ' + relation(name) + '(' + columns + ')')
                created.append(name)
            def insert(name, rows):
                for row in rows:cursor.execute('INSERT INTO ' + relation(name) + ' VALUES(' + row + ')')
            try:
                if case in ('composite_partial_null', 'crossed_key_order'):
                    create(parent, '"A" NUMERIC(18,0),"B" NUMERIC(18,0)')
                    create(child, '"X" NUMERIC(18,0),"Y" NUMERIC(18,0)')
                    if case == 'composite_partial_null':
                        insert(parent, ['1,10', '1,20', '2,30'])
                        insert(child, ['1,10', '1,99', '1,99', 'NULL,99', '99,NULL', 'NULL,NULL', '2,30'])
                        parent_columns = ['A', 'B']
                    else:
                        insert(parent, ['7,8']);insert(child, ['8,7', '7,8', 'NULL,7'])
                        parent_columns = ['B', 'A']
                    columns = ['X', 'Y']
                elif case == 'self_reference':
                    create(child, '"ID" NUMERIC(18,0),"REF" NUMERIC(18,0)')
                    insert(child, ['1,NULL', '2,1', '3,9', '4,9'])
                    parent = child;columns = ['REF'];parent_columns = ['ID']
                else:
                    kind = 'CHAR(4)' if case == 'fixed_character_padding' else ('VARCHAR(8)' if case == 'empty_character_key' else 'NUMERIC(18,0)')
                    create(parent, '"ID" ' + kind);create(child, '"REF" ' + kind)
                    if case == 'fixed_character_padding':
                        parents, children = ["'A'"], ["'A'", "'A '", "'a'", "'B'", 'NULL']
                    elif case == 'empty_character_key':
                        parents, children = ["'A'"], ["'A'", "''", 'NULL', "'B'", "'B'"]
                    elif case == 'empty_parent':
                        parents, children = [], ['1', 'NULL', '2', '2']
                    elif case == 'empty_child':
                        parents, children = ['1'], []
                    elif case == 'nullable_parent':
                        parents, children = ['1', 'NULL'], ['1', 'NULL', '2']
                    else:
                        parents, children = ['1', '2'], ['1', '1', '3', '3', 'NULL']
                    insert(parent, parents);insert(child, children)
                    columns = ['REF'];parent_columns = ['ID']
                item = dict(table=child, parent_table=parent, columns=columns, parent_columns=parent_columns)
                outcome['orphan_rows'] = count_orphans(cursor, schema, item)
                outcome['expected_orphan_rows'] = expected_count(case, oracle)
                outcome['success'] = outcome['orphan_rows'] == outcome['expected_orphan_rows']
            except Exception as error:
                outcome['error_type'] = type(error).__name__
                outcome['sqlstate'] = getattr(error, 'sqlstate', None)
                value = error.args[0] if error.args else None
                outcome['oracle_code'] = getattr(value, 'code', None)
            finally:
                for name in reversed(created):
                    try:cursor.execute('DROP TABLE ' + relation(name))
                    except Exception as error:
                        report['cleanup_errors'].append(dict(case=case, table=name, error_type=type(error).__name__))
                cursor.close()
            report['cases'].append(outcome)
    report['success'] = len(report['cases']) == len(CASES) and all(item['success'] for item in report['cases']) and not report['cleanup_errors']
    return report


def run(reference_factory, candidate_factory, progress=None):
    reference = observe(reference_factory, progress)
    candidate = observe(candidate_factory, progress)
    matched = sum(left['case'] == right['case'] and left['success'] and right['success'] and
                  left['orphan_rows'] == right['orphan_rows']
                  for left, right in zip(reference['cases'], candidate['cases'], strict=True))
    return dict(reference=reference, candidate=candidate, matched=matched, total=len(CASES),
                success=reference['success'] and candidate['success'] and matched == len(CASES))
