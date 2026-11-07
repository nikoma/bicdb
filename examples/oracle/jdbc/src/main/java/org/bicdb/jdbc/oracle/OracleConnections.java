// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.QueryExecutor;
import org.postgresql.jdbc.PreferQueryMode;

/** JDBC interface wrappers; no private driver members or driver fork. */
public final class OracleConnections {
    private OracleConnections() {}

    public static Connection wrap(Connection connection) throws SQLException {
        if (wrapped(connection)) return connection;
        BaseConnection base = OracleBlocks.base(connection);
        return (Connection) Proxy.newProxyInstance(OracleConnections.class.getClassLoader(),
                new Class<?>[] {Connection.class, PGConnection.class}, new ConnectionHandler(connection, base));
    }

    static boolean wrapped(Connection connection) {
        return Proxy.isProxyClass(connection.getClass())
                && Proxy.getInvocationHandler(connection) instanceof ConnectionHandler;
    }

    private static final class ConnectionHandler implements InvocationHandler {
        final Connection delegate;
        final BaseConnection base;
        ConnectionHandler(Connection delegate, BaseConnection base) { this.delegate = delegate; this.base = base; }

        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (objectMethod(proxy, method, args)) return objectResult(proxy, method, args);
            if (method.getName().equals("unwrap") && ((Class<?>) args[0]).isInstance(proxy)) return proxy;
            if (method.getName().equals("isWrapperFor") && ((Class<?>) args[0]).isInstance(proxy)) return true;
            if (method.getName().equals("createBlob")) {
                if (delegate.isClosed()) throw new SQLException("Connection is closed", "08003");
                return new OracleCreatedBlob();
            }
            Object result;
            boolean anonymous = false;
            if (method.getName().equals("prepareStatement")) {
                String sql = (String) args[0];
                String normalized = OracleSql.anonymous(sql);
                if (normalized != null) {
                    anonymous = true;
                    rejectReturningKeys(method, args);
                    synchronized (delegate) {
                        OracleBlocks.seed(base, sql, normalized);
                        result = call(delegate, method, args);
                    }
                } else result = call(delegate, method, args);
            } else if (method.getName().equals("prepareCall") && OracleSql.anonymous((String) args[0]) != null) {
                throw unsupported("Anonymous callable OUT parameters are not implemented");
            } else result = call(method.getDeclaringClass().isInstance(delegate) ? delegate : base, method, args);
            if (result instanceof Statement) return statement((Statement) result, (Connection) proxy, base, anonymous);
            if (result instanceof DatabaseMetaData) {
                Object metadata = result;
                return Proxy.newProxyInstance(OracleConnections.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class},
                        (p, m, a) -> {
                            if (objectMethod(p, m, a)) return objectResult(p, m, a);
                            if (m.getName().equals("unwrap") && ((Class<?>) a[0]).isInstance(p)) return p;
                            if (m.getName().equals("isWrapperFor") && ((Class<?>) a[0]).isInstance(p)) return true;
                            Boolean folding = identifierFolding(m.getName());
                            if (folding != null) return folding;
                            return m.getName().equals("getConnection") ? proxy : call(metadata, m, a);
                        });
            }
            return result;
        }
    }

    /**
     * This profile folds unquoted identifiers to upper case, while PgJDBC reports PostgreSQL's
     * lower-case folding. Callers that fold a name before a metadata lookup need the profile's rule.
     */
    static Boolean identifierFolding(String method) {
        switch (method) {
            case "storesUpperCaseIdentifiers": return Boolean.TRUE;
            case "storesLowerCaseIdentifiers":
            case "storesMixedCaseIdentifiers":
            case "supportsMixedCaseIdentifiers": return Boolean.FALSE;
            default: return null;
        }
    }

    static Statement statement(Statement delegate, Connection owner, BaseConnection base, boolean anonymous) {
        if (Proxy.isProxyClass(delegate.getClass()) && Proxy.getInvocationHandler(delegate) instanceof StatementHandler)
            return delegate;
        Class<?> primary = delegate instanceof java.sql.CallableStatement ? java.sql.CallableStatement.class
                : delegate instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
        return (Statement) Proxy.newProxyInstance(OracleConnections.class.getClassLoader(),
                new Class<?>[] {primary, PGStatement.class}, new StatementHandler(delegate, owner, base, anonymous));
    }

    private static final class StatementHandler implements InvocationHandler {
        final Statement delegate;
        final Connection owner;
        final BaseConnection base;
        final boolean anonymous;
        boolean escapes = true;
        ResultSet lastRows;
        ResultSet lastView;
        StatementHandler(Statement delegate, Connection owner, BaseConnection base, boolean anonymous) {
            this.delegate = delegate; this.owner = owner; this.base = base; this.anonymous = anonymous;
        }
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (objectMethod(proxy, method, args)) return objectResult(proxy, method, args);
            if (name.equals("getConnection")) return owner;
            if (name.equals("unwrap") && ((Class<?>) args[0]).isInstance(proxy)) return proxy;
            if (name.equals("isWrapperFor") && ((Class<?>) args[0]).isInstance(proxy)) return true;
            if (delegate instanceof PreparedStatement
                    && OracleBlobs.bind((PreparedStatement) delegate, method, args)) return null;
            if (anonymous && (name.equals("getParameterMetaData") || name.equals("getMetaData"))) {
                // Describe has no simple-query equivalent. Never send an
                // unbound block as a simple query while obtaining metadata.
                synchronized (base) {
                    QueryExecutor executor = base.getQueryExecutor();
                    PreferQueryMode previous = executor.getPreferQueryMode();
                    if (previous == PreferQueryMode.SIMPLE) executor.setPreferQueryMode(PreferQueryMode.EXTENDED);
                    try { return call(delegate, method, args); }
                    finally { executor.setPreferQueryMode(previous); }
                }
            }
            if (args != null && args.length > 0 && args[0] instanceof String
                    && (name.equals("execute") || name.equals("executeQuery") || name.equals("executeUpdate")
                        || name.equals("executeLargeUpdate") || name.equals("addBatch"))) {
                String normalized = OracleSql.anonymous((String) args[0]);
                if (normalized != null) {
                    if (delegate instanceof PreparedStatement)
                        throw unsupported("Use the prepared statement's parameterless execute methods");
                    if (name.equals("addBatch"))
                        throw unsupported("Use PreparedStatement batches for anonymous blocks");
                    rejectReturningKeys(method, args);
                    synchronized (owner) {
                        CachedQuery query = new CachedQuery(args[0], base.getQueryExecutor().wrap(
                                java.util.List.of(OracleBlocks.parse(normalized, false, escapes))), false);
                        try {
                            int flags = name.equals("executeUpdate") || name.equals("executeLargeUpdate")
                                    ? QueryExecutor.QUERY_NO_RESULTS : 0;
                            boolean rows = delegate.unwrap(BaseStatement.class).executeWithFlags(query, flags);
                            if (name.equals("execute")) return rows;
                            if (name.equals("executeQuery")) {
                                if (!rows) throw new SQLException("Anonymous block returned no rows", "02000");
                                return delegate.getResultSet();
                            }
                            if (name.equals("executeLargeUpdate")) return delegate.getLargeUpdateCount();
                            return delegate.getUpdateCount();
                        } finally { query.query.close(); }
                    }
                }
            }
            Object target = method.getDeclaringClass().isInstance(delegate) ? delegate : delegate.unwrap(PGStatement.class);
            Object result = call(target, method, args);
            if (name.equals("setEscapeProcessing")) escapes = (boolean) args[0];
            if (result instanceof ResultSet) {
                ResultSet rows = (ResultSet) result;
                if (lastRows != rows) {
                    lastRows = rows;
                    lastView = (ResultSet) Proxy.newProxyInstance(OracleConnections.class.getClassLoader(),
                            new Class<?>[] {ResultSet.class}, (p, m, a) -> {
                                if (objectMethod(p, m, a)) return objectResult(p, m, a);
                                if (m.getName().equals("unwrap") && ((Class<?>) a[0]).isInstance(p)) return p;
                                if (m.getName().equals("isWrapperFor") && ((Class<?>) a[0]).isInstance(p)) return true;
                                if (a != null && (m.getName().equals("getBlob")
                                        || (m.getName().equals("getObject") && a.length == 2 && a[1] == java.sql.Blob.class))
                                        && OracleBlobs.binaryColumn(rows, a[0])) return OracleBlobs.read(rows, a[0]);
                                Object value = call(rows, m, a);
                                return m.getName().equals("getStatement") && value != null ? proxy : value;
                            });
                }
                return lastView;
            }
            return result;
        }
    }

    private static void rejectReturningKeys(Method method, Object[] args) throws SQLException {
        // Result-set options are supported by the ordinary three/four-argument
        // factories. Two arguments instead select a generated-key overload.
        if (args.length != 2) return;
        Object option = args[1];
        if (option instanceof Integer && (int) option == Statement.NO_GENERATED_KEYS) return;
        if (option instanceof int[] && ((int[]) option).length == 0) return;
        if (option instanceof String[] && ((String[]) option).length == 0) return;
        throw unsupported("Generated keys for anonymous blocks are not implemented");
    }

    private static boolean objectMethod(Object proxy, Method method, Object[] args) {
        return method.getDeclaringClass() == Object.class;
    }
    private static Object objectResult(Object proxy, Method method, Object[] args) {
        switch (method.getName()) {
            case "equals": return proxy == args[0];
            case "hashCode": return System.identityHashCode(proxy);
            case "toString": return "BicDB Oracle preview JDBC wrapper";
            default: throw new IllegalStateException("Unexpected Object method");
        }
    }
    private static Object call(Object delegate, Method method, Object[] args) throws Throwable {
        try { return method.invoke(delegate, args); }
        catch (InvocationTargetException error) { throw error.getCause(); }
    }
    private static SQLFeatureNotSupportedException unsupported(String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }
}
