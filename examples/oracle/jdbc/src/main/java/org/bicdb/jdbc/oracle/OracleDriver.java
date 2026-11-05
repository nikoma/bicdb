// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/** Explicit Oracle preview opt-in; PostgreSQL remains the underlying driver. */
public final class OracleDriver implements java.sql.Driver {
    private static final String PREFIX = "jdbc:bicdb:oracle:";
    private final org.postgresql.Driver delegate = new org.postgresql.Driver();

    static {
        try { DriverManager.registerDriver(new OracleDriver()); }
        catch (SQLException error) { throw new ExceptionInInitializerError(error); }
    }

    @Override public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) return null;
        Connection connection = delegate.connect(postgresUrl(url), properties);
        if (connection == null) throw new SQLException("Invalid PostgreSQL transport URL", "08001");
        try {
            return OracleConnections.wrap(connection);
        } catch (SQLException | RuntimeException error) {
            try { connection.close(); } catch (SQLException close) { error.addSuppressed(close); }
            throw error;
        }
    }

    @Override public boolean acceptsURL(String url) { return url != null && url.startsWith(PREFIX); }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        return delegate.getPropertyInfo(acceptsURL(url) ? postgresUrl(url) : url, properties);
    }
    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("org.bicdb.jdbc.oracle"); }
    private static String postgresUrl(String url) { return "jdbc:postgresql:" + url.substring(PREFIX.length()); }
}
