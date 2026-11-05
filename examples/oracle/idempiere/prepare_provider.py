#!/usr/bin/env python3
"""Prepare an explicit PostgreSQL transport option in the pinned external provider.

The application remains GPL-licensed upstream source outside this repository.
This does not select its PostgreSQL SQL dialect: its Oracle provider stays active.
"""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

PIN = 'da0730aec42f3e9f5f671e81a70ff71e24d5e534'
ROOT = Path(__file__).resolve().parents[3]


def replace_once(source, before, after):
    if source.count(before) != 1: raise ValueError('pinned provider context does not match')
    return source.replace(before, after, 1)


def prepare(checkout, report, jdbc_blocks=False):
    checkout = checkout.resolve()
    if checkout.is_relative_to(ROOT): raise ValueError('use an external application checkout')
    commit = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip()
    if commit != PIN: raise ValueError('application checkout does not match the pinned source')
    relative = 'org.compiere.db.oracle.provider/src/org/compiere/db/DB_Oracle.java'
    path = checkout / relative
    before = path.read_bytes()
    original = subprocess.check_output(['git', '-C', str(checkout), 'show', 'HEAD:' + relative])
    if before != original: raise ValueError('provider has local changes; preserve them and use a fresh checkout')
    source = before.decode().replace('\r\n', '\n')
    source = replace_once(source, 'private static OracleDriver', 'private static Driver')
    source = replace_once(source, 'DRIVER = "oracle.jdbc.OracleDriver";',
        'DRIVER = Boolean.getBoolean("bicdb.pgwire") ? "org.postgresql.Driver" : "oracle.jdbc.OracleDriver";')
    source = replace_once(source, 's_driver = new OracleDriver();',
        's_driver = Boolean.getBoolean("bicdb.pgwire") ? new org.postgresql.Driver() : new OracleDriver();')
    anchor = '        System.setProperty("oracle.jdbc.v$session.program", "iDempiere");'
    source = replace_once(source, anchor, """        if (Boolean.getBoolean("bicdb.pgwire")) {
            if (connection.isBequeath() || connection.isViaFirewall())
                throw new IllegalArgumentException("BicDB PostgreSQL transport requires a direct TCP connection");
            return getConnectionURL(connection.getDbHost(), connection.getDbPort(), connection.getDbName(), connection.getDbUid());
        }
""" + anchor)
    anchor = '        m_connectionURL = "jdbc:oracle:thin:@"'
    source = replace_once(source, anchor, r"""        if (Boolean.getBoolean("bicdb.pgwire")) {
            String host = dbHost.contains(":") && !dbHost.startsWith("[") ? "[" + dbHost + "]" : dbHost;
            String database = java.net.URLEncoder.encode(dbName, java.nio.charset.StandardCharsets.UTF_8);
            String schema = java.net.URLEncoder.encode("\"" + userName.toUpperCase(java.util.Locale.ROOT).replace("\"", "\"\"") + "\"", java.nio.charset.StandardCharsets.UTF_8);
            m_connectionURL = "jdbc:postgresql://" + host + ":" + dbPort + "/" + database
                + "?ApplicationName=iDempiere&stringtype=unspecified&tcpKeepAlive=true"
                + "&options=-c%20bicdb.sql_profile%3Doracle-preview-v1&currentSchema=" + schema;
            return m_connectionURL;
        }
""" + anchor)
    manifest = checkout / 'org.compiere.db.oracle.provider/META-INF/MANIFEST.MF'
    manifest_before = manifest.read_bytes()
    manifest_original = subprocess.check_output(['git', '-C', str(checkout), 'show', 'HEAD:org.compiere.db.oracle.provider/META-INF/MANIFEST.MF'])
    if manifest_before != manifest_original: raise ValueError('provider manifest has local changes')
    updated = replace_once(manifest_before.decode(),
        ' wrapped.com.oracle.database.jdbc.ojdbc17;bundle-version="23.26.0.0"',
        ' wrapped.com.oracle.database.jdbc.ojdbc17;bundle-version="23.26.0.0",\n org.postgresql.jdbc;bundle-version="42.7.8"')
    if jdbc_blocks:
        source = replace_once(source, '"org.postgresql.Driver"', '"org.bicdb.jdbc.oracle.OracleDriver"')
        source = replace_once(source, 'new org.postgresql.Driver()', 'new org.bicdb.jdbc.oracle.OracleDriver()')
        source = replace_once(source, '"jdbc:postgresql://"', '"jdbc:bicdb:oracle://"')
        updated = replace_once(updated, ' org.postgresql.jdbc;bundle-version="42.7.8"',
            ' org.postgresql.jdbc;bundle-version="42.7.8",\n org.bicdb.jdbc.oracle;bundle-version="[0.1.0,0.1.1)"')
    report_data = dict(source_commit=commit, mode_property='-Dbicdb.pgwire=true',
        sql_profile='oracle-preview-v1', application_qualified=False,
        adaptations=['Oracle provider retained; PostgreSQL JDBC transport selected explicitly'],
        files=[])
    if jdbc_blocks:
        report_data['adaptations'].append('General JDBC adapter preserves anonymous block boundaries and input parameters')
        report_data['jdbc_adapter_bundle'] = 'org.bicdb.jdbc.oracle;bundle-version="[0.1.0,0.1.1)"'
    for destination, old, new in [(path, before, source.encode()), (manifest, manifest_before, updated.encode())]:
        report_data['files'].append(dict(path=str(destination.relative_to(checkout)),
            before_sha256=hashlib.sha256(old).hexdigest(), after_sha256=hashlib.sha256(new).hexdigest()))
        destination.write_bytes(new)
    report.write_text(json.dumps(report_data, indent=2) + '\n')
    return report_data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--jdbc-blocks', action='store_true', help='require the separate Oracle preview JDBC adapter bundle')
    args = parser.parse_args()
    print(json.dumps(prepare(args.checkout, args.report, args.jdbc_blocks), indent=2))


if __name__ == '__main__': main()
