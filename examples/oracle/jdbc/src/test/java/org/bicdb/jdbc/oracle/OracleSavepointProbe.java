// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/** Real JDBC reuse after an expired savepoint, on a disposable nonce table. */
final class OracleSavepointProbe {
    private static void check(String label, boolean valid) {
        if (!valid) throw new AssertionError(label);
        System.out.println("passed=" + label);
    }

    static int run(Connection connection) throws Exception {
        String table = "SP_JDBC_" + UUID.randomUUID().toString().replace("-", "");
        boolean created = false;
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY)");
                created = true;
            }
            connection.setAutoCommit(false);
            Savepoint expired = connection.setSavepoint();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO " + table + " VALUES(1)");
            }
            connection.rollback();
            try {
                connection.rollback(expired);
                throw new AssertionError("expired JDBC savepoint succeeded");
            } catch (SQLException error) {
                check("expired_jdbc_savepoint_diagnostic", "3B001".equals(error.getSQLState())
                        && error.getMessage().contains("ORA-01086"));
            }
            Savepoint retained = connection.setSavepoint("mixed \" name");
            try (Statement statement = connection.createStatement()) {
                check("jdbc_save_after_expired_savepoint", statement.executeUpdate("INSERT INTO " + table + " VALUES(2)") == 1);
            }
            connection.rollback(retained);
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                check("jdbc_quoted_savepoint_rollback", rows.next() && rows.getInt(1) == 0);
            }
            connection.releaseSavepoint(retained);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO " + table + " VALUES(3)");
            }
            connection.commit();
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT id FROM " + table)) {
                check("jdbc_recovered_transaction_commit", rows.next() && rows.getInt(1) == 3 && !rows.next());
            }
            return 4;
        } finally {
            if (!connection.getAutoCommit()) connection.rollback();
            connection.setAutoCommit(true);
            if (created) try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + table);
            }
        }
    }
}
