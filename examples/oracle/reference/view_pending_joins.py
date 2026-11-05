"""View joins retain SQL visibility while transactions buffer unrelated writes."""
import uuid
from row_lock_waits import main as comparison_main


def observe(factory):
    ordinary = [[2, 'N']]
    expected = {
        'before': ordinary,
        'pending_update_comma': ordinary,
        'pending_update_explicit': ordinary,
        'pending_update_known_null': [],
        'pending_update_scalar_local': ordinary,
        'pending_update_scalar_correlated': ordinary,
        'pending_update_scalar_coalesced': ordinary,
        'pending_secondary_case': ordinary,
        'pending_secondary_range': ordinary,
        'pending_secondary_filter': ordinary,
        'rollback_update': ordinary,
        'pending_insert_comma': ordinary,
        'rollback_insert': ordinary,
        'pending_delete_comma': ordinary,
        'rollback_delete': ordinary,
        'pending_matched_row_excluded': [],
        'pending_matched_row_reenabled': [[2, 'Y']],
        'rollback_matched_row': ordinary,
        'committed_update': ordinary,
        'pending_view_source_comma': [[3, 'N']],
        'pending_view_source_explicit': [[3, 'N']],
        'pending_view_source_coalesce': [[3, 'N']],
        'rollback_view_source': ordinary,
    }
    cases = {key: dict(id=key, expected=value, qualified=False)
             for key, value in expected.items()}
    report = dict(cases=list(cases.values()), cleanup_errors=[])
    prefix = 'BICDB_VJ_' + uuid.uuid4().hex[:8].upper()
    items, headers, lines, view = [prefix + suffix for suffix in ['I', 'H', 'L', 'V']]
    made = []
    with factory() as conn:
        oracle = type(conn).__module__.startswith('oracledb')
        conn.autocommit = not oracle
        report['server_version'] = conn.version if oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                if oracle:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, prepare=True)
                return cursor.fetchall() if cursor.description else cursor.rowcount

        def check(name, sql):
            actual = [[int(row[0]), row[1]] for row in execute(sql)]
            cases[name].update(actual=actual, qualified=actual == expected[name])

        def begin():
            if not oracle:
                execute('BEGIN')

        try:
            for name, ddl in [
                (items, f'CREATE TABLE {items}(item_id NUMERIC(10,0) PRIMARY KEY,allow_zero CHAR(1),kind CHAR(1),verified CHAR(1))'),
                (headers, f'CREATE TABLE {headers}(header_id NUMERIC(10,0) PRIMARY KEY,item_id NUMERIC(10,0),active CHAR(1),mode_flag CHAR(1))'),
                (lines, f'CREATE TABLE {lines}(line_id NUMERIC(10,0) PRIMARY KEY,header_id NUMERIC(10,0),item_id NUMERIC(10,0),active CHAR(1),qty NUMERIC)'),
            ]:
                execute(ddl)
                made.append(('TABLE', name))
            execute(f'CREATE INDEX {prefix}X ON {headers}(item_id)')
            execute(f'CREATE INDEX {prefix}Y ON {lines}(header_id)')
            execute(f'CREATE INDEX {prefix}Z ON {items}(kind)')
            execute(f'CREATE INDEX {prefix}W ON {items}(kind,item_id,allow_zero)')
            execute(f"CREATE VIEW {view}(edge_id,active,parent_id,child_id,qty) AS SELECT l.line_id,l.active,v.item_id,l.item_id,l.qty FROM {headers} v JOIN {lines} l ON v.header_id=l.header_id WHERE v.active='Y' AND v.mode_flag='A'")
            made.append(('VIEW', view))
            execute(f"INSERT INTO {items} VALUES(1,'Y','Y','Y')")
            execute(f"INSERT INTO {items} VALUES(2,'Y','N','N')")
            execute(f"INSERT INTO {headers} VALUES(10,1,'Y','A')")
            execute(f"INSERT INTO {lines} VALUES(100,10,2,'Y',5)")
            conn.commit()
            where = "v.parent_id=1 AND v.child_id<>1 AND (i.kind='N' OR i.verified='Y') AND v.active='Y'"
            comma = f'SELECT v.child_id,i.kind FROM {view} v,{items} i WHERE v.child_id=i.item_id AND {where}'
            explicit = f'SELECT v.child_id,i.kind FROM {view} v JOIN {items} i ON v.child_id=i.item_id WHERE {where}'
            begin()
            check('before', comma)
            execute(f"UPDATE {items} SET allow_zero='N' WHERE item_id=1")
            check('pending_update_comma', comma)
            check('pending_update_explicit', explicit)
            check('pending_update_known_null', comma + ' AND i.item_id=CAST(NULL AS NUMERIC)')
            check('pending_update_scalar_local', comma.replace(
                'v.child_id=i.item_id',
                f'i.item_id=(SELECT MAX(s.item_id) FROM {lines} s)'))
            check('pending_update_scalar_correlated', comma.replace(
                'v.child_id=i.item_id',
                f'i.item_id=(SELECT MAX(s.item_id) FROM {lines} s WHERE s.item_id=v.child_id)'))
            check('pending_update_scalar_coalesced', comma.replace(
                'v.child_id=i.item_id',
                f'i.item_id=COALESCE((SELECT MAX(s.item_id) FROM {lines} s WHERE s.item_id=v.child_id),3)'))
            check('pending_secondary_case', comma.replace(
                'v.child_id=i.item_id',
                "i.kind=CASE WHEN v.child_id=2 THEN 'N' ELSE 'Y' END"))
            check('pending_secondary_range', comma.replace(
                'v.child_id=i.item_id',
                "i.kind<=CASE WHEN v.child_id=2 THEN 'N' ELSE 'A' END"))
            check('pending_secondary_filter', comma.replace(
                'v.child_id=i.item_id',
                "i.kind='N' AND i.item_id>0 AND i.allow_zero=CASE WHEN v.child_id=2 THEN 'Y' ELSE 'N' END"))
            conn.rollback()
            check('rollback_update', comma)
            conn.rollback()
            begin()
            execute(f"INSERT INTO {items} VALUES(3,'Y','N','N')")
            check('pending_insert_comma', comma)
            conn.rollback()
            check('rollback_insert', comma)
            conn.rollback()
            begin()
            execute(f'DELETE FROM {items} WHERE item_id=1')
            check('pending_delete_comma', comma)
            conn.rollback()
            check('rollback_delete', comma)
            conn.rollback()
            begin()
            execute(f"UPDATE {items} SET kind='Y' WHERE item_id=2")
            check('pending_matched_row_excluded', comma)
            execute(f"UPDATE {items} SET verified='Y' WHERE item_id=2")
            check('pending_matched_row_reenabled', comma)
            conn.rollback()
            check('rollback_matched_row', comma)
            conn.rollback()
            begin()
            execute(f"UPDATE {items} SET allow_zero='N' WHERE item_id=1")
            conn.commit()
            check('committed_update', comma)
            conn.rollback()
            begin()
            execute(f"INSERT INTO {items} VALUES(3,'Y','N','N')")
            execute(f'UPDATE {lines} SET item_id=3 WHERE line_id=100')
            check('pending_view_source_comma', comma)
            check('pending_view_source_explicit', explicit)
            check('pending_view_source_coalesce', comma.replace('v.child_id=i.item_id', 'COALESCE(v.child_id,2)=i.item_id'))
            conn.rollback()
            check('rollback_view_source', comma)
            conn.rollback()
        except Exception as error:
            report['execution_error'] = type(error).__name__ + ':' + str(error)[:1500]
        finally:
            try:
                conn.rollback()
                conn.autocommit = True
            except Exception as error:
                report['cleanup_errors'].append(str(error)[:300])
            for kind, name in reversed(made):
                try:
                    execute(f'DROP {kind} {name}' + (' PURGE' if oracle and kind == 'TABLE' else ''))
                except Exception as error:
                    report['cleanup_errors'].append(str(error)[:300])
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a['id'], source=a, bicdb=b, source_qualified=a['qualified'],
                  matched=a['qualified'] and b['qualified'])
             for a, b in zip(source['cases'], target['cases'], strict=True)]
    cleanup = source['cleanup_errors'] + target['cleanup_errors']
    return dict(format=1, application_qualified=False,
                reference_version=source['server_version'], cases=cases,
                total=len(cases), matched=sum(case['matched'] for case in cases),
                source_qualified=all(case['source_qualified'] for case in cases),
                cleanup_errors=cleanup,
                source_execution_error=source.get('execution_error'),
                target_execution_error=target.get('execution_error'),
                success=len(cases) == 23 and all(case['matched'] for case in cases) and not cleanup)


if __name__ == '__main__':
    raise SystemExit(comparison_main(runner=run, description=__doc__))
