"""Protect source units, value preservation, and completed journal declarations."""
import unittest

import character_types as target


def column(kind='VARCHAR2', unit='B', length=3, byte_length=3, **changes):
    result = dict(table='T',name='V',type=kind,character_unit=unit,
                  characters=length,bytes=byte_length,virtual='NO')
    result.update(changes)
    return result


class CharacterTypesTests(unittest.TestCase):
    def test_source_units_and_standard_byte_ceiling_are_retained(self):
        self.assertEqual(target.declaration(column()), ('VARCHAR2(3 BYTE)',1043,7))
        self.assertEqual(target.declaration(column(unit='C',byte_length=12)), ('VARCHAR2(3 CHAR)',1043,7))
        self.assertEqual(target.declaration(column(kind='CHAR',unit='C',length='2000',byte_length='2000')),
                         ('CHAR(2000 CHAR)',1042,2004))

    def test_missing_units_inconsistent_bytes_and_unqualified_types_fail(self):
        for changes in [dict(character_unit=None),dict(character_unit='X'),dict(bytes=12),
                        dict(characters=True),dict(characters=0),dict(characters=4001),
                        dict(characters='3.5'),dict(virtual='YES'),dict(type='NVARCHAR2')]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                target.declaration(column(**changes))

    def test_only_unambiguous_base_table_columns_are_reconciled(self):
        c = column()
        manifest = dict(tables=[dict(name='T',columns=['V'])],columns=[c,column(table='VIEW')])
        self.assertEqual(target.character_columns(manifest),[c])
        for changes in [dict(columns=[c,c]),dict(tables=[dict(name='T',columns=['OTHER'])]),
                        dict(tables=[dict(name='T',columns=['V'])]*2),
                        dict(columns=[column(type='NCHAR')])]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                target.character_columns(dict(manifest,**changes))

    def test_read_only_validation_rejects_multibyte_overflow_and_padding_changes(self):
        target.validate_saved_value(column(),'é')
        target.validate_saved_value(column(),'A\u0301')
        target.validate_saved_value(column(unit='C',byte_length=12),'😀')
        target.validate_saved_value(column(kind='CHAR'),'é ')
        target.validate_saved_value(column(kind='CHAR',unit='C',byte_length=12),'é  ')
        target.validate_saved_value(column(kind='CHAR',unit='C',length=2000,byte_length=2000),'é'+' '*1998)
        for c,value in [(column(),'😀'),(column(),'é\u0301'),(column(kind='CHAR'),'A'),
                        (column(kind='CHAR'),'é  '),(column(),''),(column(),1)]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                target.validate_saved_value(c,value)
        target.validate_saved_value(column(),None)

    def test_resume_never_accepts_missing_or_different_known_provenance(self):
        columns=[column()]
        target.validate_logical_types({'V':None},columns,False)
        target.validate_logical_types({'V':'VARCHAR2(3 BYTE)'},columns,True)
        with self.assertRaisesRegex(ValueError,'completed'):
            target.validate_logical_types({'V':None},columns,True)
        with self.assertRaisesRegex(ValueError,'different'):
            target.validate_logical_types({'V':'VARCHAR2(3 CHAR)'},columns,False)

    def test_fingerprint_validation_checks_each_quoted_source_column(self):
        class Description:
            name='V'
        class Cursor:
            description=[Description()]
            def execute(self,*args,**kwargs): pass
            def __iter__(self):return iter([('é',),(None,),('é',)])
        result=target.inspect_table(Cursor(),'A"B',dict(name='T',columns=['V']),[column()])
        self.assertEqual(result['rows'],3)
        with self.assertRaisesRegex(ValueError,'capacity'):
            target.inspect_table(Cursor(),'S',dict(name='T',columns=['V']),[column(length=1,byte_length=1)])


if __name__ == '__main__':
    unittest.main()
