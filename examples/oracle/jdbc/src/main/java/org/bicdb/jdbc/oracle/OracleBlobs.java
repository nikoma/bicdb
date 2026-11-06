// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import javax.sql.rowset.serial.SerialBlob;

/** Binary-value bridging, without PostgreSQL OID large objects or writable locators. */
final class OracleBlobs {
    private static final long MAX_BUFFERED_BLOB = 64L * 1024 * 1024;
    private OracleBlobs() {}

    static boolean bind(PreparedStatement statement, Method method, Object[] args) throws SQLException {
        String name = method.getName();
        if (args == null || args.length < 2) return false;
        int parameter;
        if (name.equals("setNull") && args[1].equals(Types.BLOB)) {
            statement.setNull((Integer) args[0], Types.BINARY);
            return true;
        }
        boolean blobObject = name.equals("setObject")
                && (args.length == 2 ? args[1] instanceof Blob
                    : Integer.valueOf(Types.BLOB).equals(args[2]) || args[2] == JDBCType.BLOB);
        if (!name.equals("setBlob") && !blobObject) return false;
        parameter = (Integer) args[0];
        if (name.equals("setBlob") && method.getParameterTypes()[1] == InputStream.class) {
            InputStream stream = (InputStream) args[1];
            if (args.length == 3) {
                long length = (Long) args[2];
                if (length < 0) throw new SQLException("BLOB stream length must not be negative", "22023");
                statement.setBinaryStream(parameter, stream, length);
            } else statement.setBinaryStream(parameter, stream);
        } else if (args[1] == null) {
            statement.setNull(parameter, Types.BINARY);
        } else {
            if (!(args[1] instanceof Blob))
                throw new SQLException("BLOB object binding requires java.sql.Blob", "22023");
            Blob blob = (Blob) args[1];
            long length = blob.length();
            if (length < 0) throw new SQLException("BLOB length must not be negative", "22023");
            if (length > MAX_BUFFERED_BLOB)
                throw new SQLFeatureNotSupportedException("Buffered BLOB object binding is limited to 64 MiB; use an InputStream binding", "0A000");
            byte[] bytes = length == 0 ? new byte[0] : blob.getBytes(1, (int) length);
            if (bytes == null || bytes.length != length)
                throw new SQLException("BLOB object returned an incomplete value", "22000");
            statement.setBytes(parameter, bytes);
        }
        return true;
    }

    static boolean binaryColumn(ResultSet rows, Object column) throws SQLException {
        int index = column instanceof Integer ? (Integer) column : rows.findColumn((String) column);
        return "bytea".equalsIgnoreCase(rows.getMetaData().getColumnTypeName(index));
    }

    static Blob read(ResultSet rows, Object column) throws SQLException {
        byte[] bytes = column instanceof Integer ? rows.getBytes((Integer) column) : rows.getBytes((String) column);
        return bytes == null ? null : new Snapshot(bytes);
    }

    /** A materialized read snapshot. Mutations must never silently lose writes. */
    private static final class Snapshot implements Blob {
        private final SerialBlob value;
        Snapshot(byte[] bytes) throws SQLException { value = new SerialBlob(bytes); }
        @Override public long length() throws SQLException { return value.length(); }
        @Override public byte[] getBytes(long pos, int length) throws SQLException { return value.getBytes(pos, length); }
        @Override public InputStream getBinaryStream() throws SQLException { return value.getBinaryStream(); }
        @Override public InputStream getBinaryStream(long pos, long length) throws SQLException { return value.getBinaryStream(pos, length); }
        @Override public long position(byte[] pattern, long start) throws SQLException { return value.position(pattern, start); }
        @Override public long position(Blob pattern, long start) throws SQLException { return value.position(pattern, start); }
        @Override public void free() throws SQLException { value.free(); }
        private SQLException readOnly() {
            return new SQLFeatureNotSupportedException("Writable JDBC BLOB locators are not implemented; bind a replacement binary value", "0A000");
        }
        @Override public int setBytes(long pos, byte[] bytes) throws SQLException { throw readOnly(); }
        @Override public int setBytes(long pos, byte[] bytes, int offset, int length) throws SQLException { throw readOnly(); }
        @Override public OutputStream setBinaryStream(long pos) throws SQLException { throw readOnly(); }
        @Override public void truncate(long length) throws SQLException { throw readOnly(); }
    }
}
