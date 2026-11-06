// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/** Executable integration checks. Run only against a disposable development DB. */
public final class OracleAdapterProbe {
    private static int checks;
    private OracleAdapterProbe() {}

    private static void check(String label, boolean valid) {
        if (!valid) throw new AssertionError(label);
        checks++;
        System.out.println("passed=" + label);
    }

    private static List<Integer> keys(Connection connection, String table) throws SQLException {
        List<Integer> result = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT key_value FROM " + table + " ORDER BY key_value")) {
            while (rows.next()) result.add(rows.getInt(1));
        }
        return result;
    }

    private static void state(String label, String expected, SqlAction action) throws Exception {
        try { action.run(); }
        catch (SQLException error) { check(label, expected.equals(error.getSQLState())); return; }
        throw new AssertionError(label + " unexpectedly succeeded");
    }

    @FunctionalInterface private interface SqlAction { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Supply one disposable JDBC URL");
        Class.forName(OracleDriver.class.getName());
        Properties properties = new Properties();
        properties.setProperty("user", System.getenv().getOrDefault("BICDB_JDBC_USER", "bicdb"));
        String password = System.getenv("BICDB_JDBC_PASSWORD");
        if (password != null) properties.setProperty("password", password);
        if (System.getenv("BICDB_JDBC_EXPECT_PROFILE_REJECTION") != null) {
            state("postgres_profile_not_wrapped", "22023", () -> {
                try (Connection ignored = DriverManager.getConnection(args[0], properties)) {
                    throw new AssertionError("PostgreSQL profile was accepted");
                }
            });
            System.out.println("checks=" + checks);
            return;
        }

        String table = "BICDB_JDBC_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = DriverManager.getConnection(args[0], properties)) {
            checks += OracleBlobProbe.run(connection);
            checks += OracleSavepointProbe.run(connection);
            check("connection_wrapper_identity", connection.unwrap(Connection.class) == connection
                    && connection.isWrapperFor(org.postgresql.PGConnection.class));
            check("metadata_connection_identity", connection.getMetaData().getConnection() == connection);
            check("metadata_unwrap_connection_identity", connection.getMetaData().unwrap(java.sql.DatabaseMetaData.class).getConnection() == connection);
            boolean created = false;
            try {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE " + table + "(key_value INTEGER PRIMARY KEY,payload VARCHAR(400))");
                    created = true;
                }
                String payload = "quote ' Привет; ? $bicdb_oracle_0$";
                org.postgresql.core.BaseConnection base = connection.unwrap(org.postgresql.core.BaseConnection.class);
                org.postgresql.jdbc.PreferQueryMode mode = base.getQueryExecutor().getPreferQueryMode();
                try (PreparedStatement statement = connection.prepareStatement(
                        "DECLARE item INTEGER := ?; BEGIN INSERT INTO " + table + " VALUES(item,?); END;")) {
                    check("prepared_connection_identity", statement.getConnection() == connection);
                    check("parameter_count", statement.getParameterMetaData().getParameterCount() == 2);
                    check("parameter_native_types", statement.getParameterMetaData().getParameterType(1) == java.sql.Types.INTEGER
                            && statement.getParameterMetaData().getParameterType(2) == java.sql.Types.VARCHAR);
                    check("metadata_restores_query_mode", base.getQueryExecutor().getPreferQueryMode() == mode);
                    check("anonymous_metadata_has_no_result_columns", statement.getMetaData() == null);
                    check("prepare_has_no_effects", keys(connection, table).isEmpty());
                    statement.unwrap(org.postgresql.PGStatement.class).setPrepareThreshold(1);
                    for (int key : new int[] {1, 2}) {
                        statement.setInt(1, key);
                        statement.setString(2, payload);
                        statement.execute();
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement("SELECT payload FROM " + table + " WHERE key_value=?")) {
                    statement.setInt(1, 2);
                    try (ResultSet row = statement.executeQuery()) {
                        check("result_statement_identity", row.getStatement() == statement
                                && row.getStatement().getConnection() == connection);
                        org.postgresql.PGResultSetMetaData origin = row.getMetaData().unwrap(org.postgresql.PGResultSetMetaData.class);
                        check("catalog_column_origin_schema", !origin.getBaseSchemaName(1).isEmpty());
                        check("standard_schema_name_keeps_driver_behavior", row.getMetaData().getSchemaName(1).isEmpty());
                        check("catalog_column_origin_table", row.getMetaData().getTableName(1).equalsIgnoreCase(table));
                        check("catalog_column_origin_name", row.getMetaData().getColumnName(1).equalsIgnoreCase("payload"));
                        check("repeat_unicode_and_delimiter_payload", row.next() && payload.equals(row.getString(1)) && !row.next());
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "BEGIN INSERT INTO " + table + " VALUES(?,q'[isn't ?; END;]'); END;")) {
                    check("alternative_quote_input_count", statement.getParameterMetaData().getParameterCount() == 1);
                    statement.setInt(1, 3);
                    statement.execute();
                }
                try (Statement statement = connection.createStatement()) {
                    check("statement_connection_identity", statement.getConnection() == connection);
                    check("raw_multistatement_block", !statement.execute("DECLARE item INTEGER := 4; BEGIN INSERT INTO " + table
                            + " VALUES(item,'raw'); INSERT INTO " + table + " VALUES(5,NQ'!Ж''? END;!'); END;"));
                    statement.executeUpdate("BEGIN INSERT INTO " + table + " VALUES(6,'update'); END;");
                    statement.executeLargeUpdate("BEGIN INSERT INTO " + table + " VALUES(7,'large update'); END; -- trailing comment");
                    try (ResultSet rows = statement.executeQuery("SELECT payload FROM " + table + " WHERE key_value=3")) {
                        check("alternative_quote_value", rows.next() && "isn't ?; END;".equals(rows.getString(1)));
                    }
                    try (ResultSet rows = statement.executeQuery("SELECT payload FROM " + table + " WHERE key_value=5")) {
                        check("national_alternative_quote_value", rows.next() && "Ж''? END;".equals(rows.getString(1)));
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "BEGIN INSERT INTO " + table + " VALUES(?,'batch'); END;")) {
                    for (int key : new int[] {8, 9}) {
                        statement.setInt(1, key); statement.addBatch();
                    }
                    check("prepared_batch_results", statement.executeBatch().length == 2);
                }
                List<Integer> baseline = Arrays.asList(1,2,3,4,5,6,7,8,9);
                check("all_standalone_writes", keys(connection, table).equals(baseline));
                connection.setAutoCommit(false);
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("INSERT INTO " + table + " VALUES(10,'prior')");
                }
                try (PreparedStatement statement = connection.prepareStatement("BEGIN INSERT INTO " + table
                        + " VALUES(?,'temporary'); INSERT INTO " + table + " VALUES(1,'duplicate'); END;")) {
                    statement.setInt(1, 11);
                    state("block_error_identity", "23505", statement::execute);
                }
                check("prior_write_survives_block_failure", keys(connection, table).equals(Arrays.asList(1,2,3,4,5,6,7,8,9,10)));
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("INSERT INTO " + table + " VALUES(12,'after error')");
                }
                connection.rollback();
                check("caller_rollback_restores_baseline", keys(connection, table).equals(baseline));
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("INSERT INTO " + table + " VALUES(10,'prior commit')");
                }
                try (PreparedStatement statement = connection.prepareStatement("BEGIN INSERT INTO " + table
                        + " VALUES(?,'commit'); COMMIT; INSERT INTO " + table + " VALUES(12,'temporary'); INSERT INTO "
                        + table + " VALUES(1,'duplicate'); END;")) {
                    statement.setInt(1, 11);
                    state("post_commit_error_identity", "23505", statement::execute);
                }
                connection.rollback();
                check("explicit_commit_survives_caller_rollback", keys(connection, table).equals(Arrays.asList(1,2,3,4,5,6,7,8,9,10,11)));
                connection.setAutoCommit(true);
                try (PreparedStatement statement = OracleBlocks.prepare(base,
                        "BEGIN INSERT INTO " + table + " VALUES(?,'helper'); END;")) {
                    check("explicit_helper_input_metadata", statement.getParameterMetaData().getParameterCount() == 1);
                    check("explicit_helper_connection_identity", statement.getConnection() == base);
                    statement.setInt(1, 13); statement.execute();
                }
                try (PreparedStatement statement = OracleBlocks.prepare(connection,
                        "BEGIN INSERT INTO " + table + " VALUES(?,'wrapped helper'); END;")) {
                    check("wrapped_helper_connection_identity", statement.getConnection() == connection);
                    statement.setInt(1, 14); statement.execute();
                }
                check("both_helper_writes_preserved", keys(connection, table).containsAll(Arrays.asList(13, 14)));
                boolean metadataFailed = false;
                try (PreparedStatement statement = connection.prepareStatement("BEGIN INSERT ; item := ?; END;")) {
                    statement.getParameterMetaData();
                } catch (SQLException expected) { metadataFailed = true; }
                check("invalid_metadata_is_rejected", metadataFailed);
                check("failed_metadata_restores_query_mode", base.getQueryExecutor().getPreferQueryMode() == mode);
                String unsupported = "BEGIN INSERT INTO " + table + " VALUES(99,'unsupported'); END;";
                state("generated_keys_rejected_before_execution", "0A000", () -> connection.prepareStatement(unsupported, Statement.RETURN_GENERATED_KEYS));
                state("callable_blocks_rejected_before_execution", "0A000", () -> connection.prepareCall(unsupported));
                try (Statement statement = connection.createStatement()) {
                    state("raw_block_batch_rejected_before_execution", "0A000", () -> statement.addBatch(unsupported));
                }
                Properties noCache = new Properties(); noCache.putAll(properties);
                noCache.setProperty("preparedStatementCacheQueries", "0");
                try (Connection uncached = DriverManager.getConnection(args[0], noCache)) {
                    state("disabled_cache_fails_before_execution", "0A000", () -> uncached.prepareStatement(unsupported));
                    state("disabled_cache_with_alternative_quotes_fails_before_execution", "0A000", () -> uncached.prepareStatement(
                            "BEGIN INSERT INTO " + table + " VALUES(99,q'[isn't ?; END;]'); END;"));
                    try (Statement statement = uncached.createStatement()) {
                        statement.execute("BEGIN NULL; END;");
                    }
                    check("raw_block_does_not_require_query_cache", true);
                }
                Properties noCacheBytes = new Properties(); noCacheBytes.putAll(properties);
                noCacheBytes.setProperty("preparedStatementCacheSizeMiB", "0");
                try (Connection uncached = DriverManager.getConnection(args[0], noCacheBytes)) {
                    state("zero_byte_cache_fails_before_execution", "0A000", () -> uncached.prepareStatement(unsupported));
                }
                check("unsupported_calls_have_no_write_effects", !keys(connection, table).contains(99));
            } finally {
                if (!connection.getAutoCommit()) connection.rollback();
                connection.setAutoCommit(true);
                if (created) {
                    try (Statement statement = connection.createStatement()) { statement.execute("DROP TABLE " + table); }
                    System.out.println("cleanup=true");
                }
            }
        }
        System.out.println("checks=" + checks);
    }
}
