"""Stored source declaration identity and projection preservation contracts."""
import copy
import unittest

from schema_code import code_plan, routine_ddl


class StoredCodePlan(unittest.TestCase):
    def manifest(self):
        return dict(source_schema='SOURCE', routines=[], views=[
            dict(name='V', query="SELECT t.*, ';' FROM t -- original text", read_only='Y')],
            columns=[dict(table='V', name='B', position='2'),
                     dict(table='V', name='A', position='1')])

    def test_body_is_preserved_even_with_dynamic_sql_and_matching_identifiers(self):
        body = "(x NUMBER) RETURN VARCHAR2 AS BEGIN RETURN 'CREATE FUNCTION F SOURCE.F'; END;"
        source = 'CREATE OR REPLACE FUNCTION F' + body
        result = routine_ddl('SOURCE', dict(name='F', kind='FUNCTION', ddl=source))
        self.assertEqual(result, 'CREATE OR REPLACE FUNCTION "SOURCE"."F"' + body)

    def test_quoted_names_and_source_qualified_declarations(self):
        routine = dict(name='a"b', kind='PROCEDURE',
                       ddl='create procedure "S pace" . "a""b" AS BEGIN NULL; END;')
        self.assertEqual(routine_ddl('S pace', routine),
                         'create procedure "S pace"."a""b" AS BEGIN NULL; END;')
        routine = dict(name='F', kind='FUNCTION', ddl='create function source.f RETURN NUMBER AS BEGIN RETURN 1; END;')
        self.assertIn('"SOURCE"."F" RETURN', routine_ddl('SOURCE', routine))

    def test_rejects_wrong_owner_kind_name_or_nonstandalone_source(self):
        for ddl, kind in [('CREATE FUNCTION OTHER.F RETURN NUMBER', 'FUNCTION'),
                          ('CREATE FUNCTION G RETURN NUMBER', 'FUNCTION'),
                          ('CREATE PROCEDURE F AS BEGIN NULL; END;', 'FUNCTION'),
                          ('CREATE PACKAGE F AS END;', 'PACKAGE'),
                          ('CREATE FUNCTION F$MORE RETURN NUMBER', 'FUNCTION')]:
            with self.subTest(ddl=ddl), self.assertRaises(ValueError):
                routine_ddl('SOURCE', dict(name='F', kind=kind, ddl=ddl))

    def test_view_labels_follow_positions_and_query_is_unchanged(self):
        manifest = self.manifest()
        original = copy.deepcopy(manifest)
        plan = code_plan(manifest)
        self.assertEqual(manifest, original)
        self.assertEqual(plan['views'][0]['columns'], ['A', 'B'])
        self.assertEqual(plan['views'][0]['ddl'],
                         'CREATE VIEW "SOURCE"."V"("A","B") AS '
                         + manifest['views'][0]['query'] + '\nWITH READ ONLY')
        self.assertFalse(plan['target_ddl_qualified'])
        self.assertFalse(plan['ownership_mapping_qualified'])

    def test_view_metadata_must_be_complete_and_unambiguous(self):
        for columns in [[], [dict(table='V', name='A', position=2)],
                        [dict(table='V', name='A', position=1), dict(table='V', name='B', position=1)],
                        [dict(table='V', name='A', position=1), dict(table='V', name='A', position=2)]]:
            manifest = self.manifest()
            manifest['columns'] = columns
            with self.subTest(columns=columns), self.assertRaises(ValueError):
                code_plan(manifest)

    def test_unknown_readonly_and_empty_query_are_rejected(self):
        for changes in [dict(read_only=None), dict(query='  ')]:
            manifest = self.manifest()
            manifest['views'][0].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                code_plan(manifest)

    def test_duplicate_objects_cannot_hide_an_overwrite(self):
        manifest = self.manifest()
        manifest['views'] *= 2
        with self.assertRaises(ValueError): code_plan(manifest)
        manifest = self.manifest()
        manifest['routines'] = [dict(name='V', kind='FUNCTION', ddl='CREATE FUNCTION V RETURN NUMBER')]
        with self.assertRaises(ValueError): code_plan(manifest)

    def test_sql_identifiers_are_escaped_and_nul_is_rejected(self):
        manifest = self.manifest()
        manifest['source_schema'] = 's"quoted'
        self.assertIn('"s""quoted".', code_plan(manifest)['views'][0]['ddl'])
        manifest['source_schema'] = 'bad\x00name'
        with self.assertRaises(ValueError): code_plan(manifest)


if __name__ == '__main__': unittest.main()
