import unittest
import tempfile
from pathlib import Path
import zipfile
import upstream_test_inventory as target


def class_dump(name='example.Tests',methods='',tail=''):
    return f'''Classfile jar:file:/private/test.jar!/{name.replace('.','/')}.class
public class {name}
  flags: (0x0021) ACC_PUBLIC, ACC_SUPER
  this_class: #1 // {name.replace('.','/')}
Constant pool:
  #2 = Utf8 org.junit.jupiter.api.Test
{{
{methods}
}}
{tail}
'''


class InventoryParserTests(unittest.TestCase):
    def test_method_annotations_are_distinct_from_constant_pool_and_display_text(self):
        text=class_dump(methods='''  public void actual();
    descriptor: ()V
    RuntimeVisibleAnnotations:
      0: #16()
        org.junit.jupiter.api.Test
  public void ordinary();
    descriptor: ()V
    Code:
      0: ldc #2 // String org.junit.jupiter.api.Test
''')
        classes=target.parse_disassembly(text)
        self.assertEqual([x['name'] for x in classes[0]['direct_test_methods']],['actual'])
        self.assertEqual(classes[0]['direct_test_methods'][0]['descriptor'],'()V')

    def test_class_disable_is_not_assigned_to_the_last_method(self):
        text=class_dump(methods='''  void one();
    descriptor: ()V
    RuntimeVisibleAnnotations:
      0: #2()
        org.junit.jupiter.api.Test
''',tail='''RuntimeVisibleAnnotations:
  0: #3()
    org.junit.jupiter.api.Disabled
''')
        item=target.parse_disassembly(text)[0]
        self.assertTrue(item['disabled_annotation'])
        self.assertFalse(item['direct_test_methods'][0]['disabled_annotation'])

    def test_parameterized_and_legacy_markers_are_retained_without_invocation_expansion(self):
        methods='''  public void repeated(java.lang.String);
    descriptor: (Ljava/lang/String;)V
    RuntimeVisibleAnnotations:
      0: #2()
        org.junit.jupiter.params.ParameterizedTest(
          name="test"
        )
        org.junit.jupiter.api.Disabled
  public void old();
    descriptor: ()V
    RuntimeVisibleAnnotations:
        org.junit.Test
'''
        item=target.parse_disassembly(class_dump(methods=methods))[0]
        self.assertEqual(len(item['direct_test_methods']),2)
        self.assertTrue(item['direct_test_methods'][0]['disabled_annotation'])

    def test_missing_descriptors_duplicate_classes_and_truncated_bodies_fail_closed(self):
        for text in [class_dump()+class_dump(),class_dump().replace('}\n',''),
                     class_dump(methods='''  public void broken();
    RuntimeVisibleAnnotations:
        org.junit.jupiter.api.Test
''')]:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):target.parse_disassembly(text)

    def test_hardlinked_and_symlinked_reports_cannot_replace_the_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);jar=root/'tests.jar';jar.write_bytes(b'original')
            for report in [jar,root/'hardlink',root/'symlink']:
                if report.name=='hardlink':report.hardlink_to(jar)
                if report.name=='symlink':report.symlink_to(jar)
                with self.assertRaises(ValueError):target.validate_report_path(jar,report)
                self.assertEqual(jar.read_bytes(),b'original')

    def test_checksum_mismatch_is_refused_before_invoking_javap(self):
        with tempfile.TemporaryDirectory() as directory:
            jar=Path(directory)/'tests.jar'
            with zipfile.ZipFile(jar,'w') as archive:archive.writestr('example/Test.class',b'fixture')
            with self.assertRaisesRegex(ValueError,'SHA-256'):
                target.inventory(jar,'example.',javap='/not-a-javap-program',expected_sha256='0'*64)


if __name__=='__main__':unittest.main()
