"""Compile and execute generated transport code using a synthetic provider fixture."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('provider_prepare', HERE / 'prepare_provider.py')
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)
JAVA_PATH = 'org.compiere.db.oracle.provider/src/org/compiere/db/DB_Oracle.java'
MANIFEST_PATH = 'org.compiere.db.oracle.provider/META-INF/MANIFEST.MF'

# This is a test-owned API stub, not copied upstream application code.
SOURCE = r'''package org.compiere.db;
import java.sql.Driver;
class OracleDriver extends org.postgresql.Driver {}
class CConnection {
    boolean isBequeath() { return false; }
    boolean isViaFirewall() { return false; }
    String getDbHost() { return "localhost"; }
    int getDbPort() { return 5441; }
    String getDbName() { return "bicdb"; }
    String getDbUid() { return "bicdb"; }
}
public class DB_Oracle {
    private static OracleDriver s_driver;
    public static String DRIVER = "oracle.jdbc.OracleDriver";
    private String m_connectionURL;
    public Driver getDriver() throws Exception {
        s_driver = new OracleDriver();
        return s_driver;
    }
    public String getName() { return "Oracle"; }
    public String getConnectionURL(CConnection connection) {
        System.setProperty("oracle.jdbc.v$session.program", "iDempiere");
        return "original";
    }
    public String getConnectionURL(String dbHost, int dbPort, String dbName, String userName) {
        m_connectionURL = "jdbc:oracle:thin:@" + dbHost;
        return m_connectionURL;
    }
    public static void main(String[] args) throws Exception {
        DB_Oracle provider = new DB_Oracle();
        System.out.println(provider.getConnectionURL("::1", 5441, "the db", args[0]));
        System.out.println(provider.getDriver().getClass().getName());
    }
}
'''.encode()
MANIFEST = b'''Manifest-Version: 1.0
Require-Bundle: org.example.base,
 wrapped.com.oracle.database.jdbc.ojdbc17;bundle-version="23.26.0.0"
'''


class ProviderPreparationTest(unittest.TestCase):
    def fixture(self, root):
        for name, data in [(JAVA_PATH, SOURCE), (MANIFEST_PATH, MANIFEST)]:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    def git(self, command, **kwargs):
        if command[-1] == 'HEAD':
            return (PREPARE.PIN + '\n')
        if command[-1] == 'HEAD:' + JAVA_PATH:
            return SOURCE
        if command[-1] == 'HEAD:' + MANIFEST_PATH:
            return MANIFEST
        raise AssertionError('Unexpected Git inspection')

    def generated_java(self, adapter):
        pg = Path(os.environ.get('POSTGRES_JDBC_JAR', Path.home() / '.m2/repository/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar'))
        sdk = Path(os.environ.get('BICDB_ORACLE_ADAPTER_JAR', HERE.parent / 'jdbc/target/oracle-jdbc-0.1.0-SNAPSHOT.jar'))
        self.assertTrue(pg.is_file(), 'Build the pinned PostgreSQL JDBC dependency first')
        self.assertTrue(sdk.is_file(), 'Build examples/oracle/jdbc first')
        with tempfile.TemporaryDirectory(prefix='bicdb-provider-generator-') as directory:
            root = Path(directory)
            self.fixture(root)
            report = root / 'report.json'
            with patch.object(PREPARE.subprocess, 'check_output', side_effect=self.git):
                PREPARE.prepare(root, report, jdbc_blocks=adapter)
            classes = root / 'classes'
            classes.mkdir()
            classpath = os.pathsep.join(map(str, [pg, sdk]))
            subprocess.run(['javac', '-J-Xmx256m', '--release', '17', '-cp', classpath,
                            '-d', str(classes), str(root / JAVA_PATH), str(HERE / 'TransportProbe.java')],
                           check=True, capture_output=True, text=True)
            for user, expected in [('garden', '"GARDEN"'), ('a"b', '"A""B"'), ('é', '"É"')]:
                result = subprocess.run(['java', '-Xmx128m', '-Dbicdb.pgwire=true', '-cp', str(classes) + os.pathsep + classpath,
                                         'org.compiere.db.DB_Oracle', user], check=True, capture_output=True, text=True)
                url, driver = result.stdout.splitlines()
                scheme = 'jdbc:bicdb:oracle://' if adapter else 'jdbc:postgresql://'
                self.assertTrue(url.startswith(scheme + '[::1]:5441/the+db?'))
                self.assertEqual(parse_qs(url.split('?', 1)[1])['currentSchema'], [expected])
                self.assertEqual(parse_qs(url.split('?', 1)[1])['options'], ['-c bicdb.sql_profile=oracle-preview-v1'])
                self.assertEqual(driver, 'org.bicdb.jdbc.oracle.OracleDriver' if adapter else 'org.postgresql.Driver')
            self.assertFalse(json.loads(report.read_text())['application_qualified'])
            self.assertEqual('org.bicdb.jdbc.oracle' in (root / MANIFEST_PATH).read_text(), adapter)

    def test_legacy_transport_generated_source_compiles_and_round_trips_schema(self):
        self.generated_java(False)

    def test_adapter_generated_source_compiles_and_round_trips_schema(self):
        self.generated_java(True)

    def test_dirty_provider_source_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix='bicdb-provider-dirty-') as directory:
            root = Path(directory)
            self.fixture(root)
            original = SOURCE + b'// unrelated change\n'
            (root / JAVA_PATH).write_bytes(original)
            with patch.object(PREPARE.subprocess, 'check_output', side_effect=self.git):
                with self.assertRaisesRegex(ValueError, 'local changes'):
                    PREPARE.prepare(root, root / 'report.json', jdbc_blocks=True)
            self.assertEqual((root / JAVA_PATH).read_bytes(), original)
            self.assertEqual((root / MANIFEST_PATH).read_bytes(), MANIFEST)
            self.assertFalse((root / 'report.json').exists())

    def test_unpinned_checkout_is_rejected_before_mutation(self):
        with tempfile.TemporaryDirectory(prefix='bicdb-provider-pin-') as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(PREPARE.subprocess, 'check_output', return_value='0' * 40 + '\n'):
                with self.assertRaisesRegex(ValueError, 'pinned source'):
                    PREPARE.prepare(root, root / 'report.json', jdbc_blocks=True)
            self.assertEqual((root / JAVA_PATH).read_bytes(), SOURCE)
            self.assertFalse((root / 'report.json').exists())


if __name__ == '__main__':
    unittest.main()
