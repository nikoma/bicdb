"""Source dependency ordering refuses stale code, cycles, and incomplete graphs."""
import copy
import unittest

from schema_dependencies import capture, ordered_views, view_order


def manifest():
    return dict(source_schema='S', routines=[dict(name='F', kind='FUNCTION',
        ddl='CREATE OR REPLACE FUNCTION F RETURN NUMBER AS BEGIN RETURN 1; END;\n')],
        views=[dict(name=name, query='SELECT 1 AS X FROM dual', read_only='N') for name in ['CHILD', 'PARENT']],
        columns=[dict(table=name, name='X', position=1) for name in ['CHILD', 'PARENT']])


def edge(name='CHILD', reference='PARENT', **changes):
    result = dict(name=name, kind='VIEW', referenced_owner='S', referenced_name=reference,
                  referenced_kind='VIEW', link=None, dependency_kind='HARD')
    result.update(changes)
    return result


class Dependencies(unittest.TestCase):
    def test_parents_precede_children_in_stable_source_order(self):
        m = manifest()
        self.assertEqual(view_order(m, [edge()]), ['PARENT', 'CHILD'])
        self.assertEqual(view_order(m, []), ['CHILD', 'PARENT'])
        self.assertEqual(view_order(m, [edge(), edge()]), ['PARENT', 'CHILD'])

    def test_cycles_missing_source_views_and_remote_edges_are_rejected(self):
        for edges in [[edge(), edge('PARENT', 'CHILD')], [edge(reference='ABSENT')],
                      [edge(link='REMOTE')], [edge(reference='CHILD')]]:
            with self.subTest(edges=edges), self.assertRaises(ValueError):
                view_order(manifest(), edges)

    def test_dictionary_does_not_invent_dynamic_or_routine_view_edges(self):
        self.assertEqual(view_order(manifest(), [edge(kind='FUNCTION'), edge(referenced_kind='TABLE')]),
                         ['CHILD', 'PARENT'])

    def cursor(self, source=None, views=None):
        m = manifest()
        class Cursor:
            def execute(self, sql):
                if 'FROM user_source' in sql:
                    self.rows = source if source is not None else [('F', 'FUNCTION', 1, m['routines'][0]['ddl'][18:])]
                elif 'FROM user_views' in sql:
                    self.rows = views if views is not None else [(v['name'], v['query'], v['read_only']) for v in m['views']]
                elif 'FROM user_dependencies' in sql:
                    self.rows = [tuple(edge().values())]
                else: raise AssertionError(sql)
            def __iter__(self): return iter(self.rows)
        return Cursor()

    def test_capture_checks_code_and_does_not_claim_target_compatibility(self):
        result = capture(self.cursor(), manifest())
        self.assertEqual(result['view_order'], ['PARENT', 'CHILD'])
        self.assertTrue(result['code_identities_verified'])
        self.assertFalse(result['target_view_types_qualified'])
        self.assertFalse(result['dynamic_dependencies_qualified'])

    def test_changed_missing_or_unordered_routines_and_views_are_rejected(self):
        for source in [[], [('F', 'FUNCTION', 1, 'FUNCTION F RETURN NUMBER;')],
                       [('F', 'FUNCTION', 2, 'text')], [('EXTRA', 'FUNCTION', 1, 'text')]]:
            with self.subTest(source=source), self.assertRaises(ValueError):
                capture(self.cursor(source=source), manifest())
        for views in [[], [('CHILD', 'changed', 'N')], [('CHILD', 'x', 'N')]*2]:
            with self.subTest(views=views), self.assertRaises(ValueError):
                capture(self.cursor(views=views), manifest())

    def test_report_is_bound_to_snapshot_and_recomputed_dependency_order(self):
        m = manifest()
        good = capture(self.cursor(), m)
        good['schema_manifest_sha256'] = 'snapshot'
        self.assertEqual(ordered_views(m, good, 'snapshot'), ['PARENT', 'CHILD'])
        for changes in [dict(schema_manifest_sha256='other'), dict(code_identities_verified=False),
                        dict(view_order=['CHILD', 'PARENT']), dict(source_schema='OTHER'),
                        dict(source_ddl_quiesced_confirmed=False)]:
            report = copy.deepcopy(good);report.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ordered_views(m, report, 'snapshot')


if __name__ == '__main__': unittest.main()
