// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.NativeQuery;
import org.postgresql.core.Parser;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SqlCommand;

/** Explicit input-only anonymous block preparation over stock PostgreSQL JDBC. */
public final class OracleBlocks {
    private OracleBlocks() {}

    public static PreparedStatement prepare(Connection connection, String sql) throws SQLException {
        synchronized (connection) {
            BaseConnection base = base(connection);
            String normalized = OracleSql.anonymous(sql);
            if (normalized == null) throw new SQLException("Expected an Oracle anonymous block", "42601");
            if (OracleConnections.wrapped(connection)) return connection.prepareStatement(sql);
            seed(base, sql, normalized);
            return (PreparedStatement) OracleConnections.statement(connection.prepareStatement(sql), connection, base, true);
        }
    }

    static BaseConnection base(Connection connection) throws SQLException {
        if (connection.isClosed()) throw new SQLException("Connection is closed", "08003");
        BaseConnection base = connection.unwrap(BaseConnection.class);
        if (!"oracle-preview-v1".equals(base.getParameterStatus("bicdb_sql_profile")))
            throw new SQLException("Select oracle-preview-v1 using the PostgreSQL startup options", "22023");
        return base;
    }

    static NativeQuery parse(String sql, boolean parameters, boolean escapes) throws SQLException {
        // Oracle character literals treat backslashes literally. Alternative
        // quoting is normalized before applying the PostgreSQL JDBC lexer.
        if (escapes) sql = Parser.replaceProcessing(sql, true, true);
        // Non-splitting Parser mode still removes top-level semicolons. A
        // temporary lexical parenthesis protects every internal separator.
        // It is removed before transmission; it is not a server SQL wrapper.
        String envelope = "(" + sql + "\n)";
        List<NativeQuery> queries = Parser.parseJdbcSql(envelope, true, parameters, false, false, false);
        if (queries.size() != 1) throw new SQLException("Atomic block parsing returned multiple queries", "42601");
        NativeQuery parsed = queries.get(0);
        if (!parsed.nativeSql.startsWith("(") || !parsed.nativeSql.endsWith("\n)"))
            throw new SQLException("Atomic block parser did not preserve the submission boundary", "42601");
        String text = parsed.nativeSql.substring(1, parsed.nativeSql.length() - 2);
        int[] positions = Arrays.stream(parsed.bindPositions).map(position -> position - 1).toArray();
        // PL/SQL is not an INSERT batch even if its last SQL command is INSERT.
        return new NativeQuery(text, positions, false, SqlCommand.BLANK);
    }

    static void seed(BaseConnection connection, String original, String normalized) throws SQLException {
        if (connection.isClosed()) throw new SQLException("Connection is closed", "08003");
        QueryExecutor executor = connection.getQueryExecutor();
        CachedQuery query = new CachedQuery(original, executor.wrap(List.of(parse(normalized, true, true))), false);
        executor.releaseQuery(query);
        CachedQuery admitted;
        try { admitted = executor.borrowQuery(original); }
        catch (SQLException error) {
            throw new SQLFeatureNotSupportedException("Atomic block was not admitted to the driver query cache", "0A000", error);
        }
        boolean matched = admitted == query;
        executor.releaseQuery(admitted);
        if (!matched)
            throw new SQLFeatureNotSupportedException(
                    "Anonymous block preparation requires a query cache large enough to retain the block", "0A000");
    }
}
