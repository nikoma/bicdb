// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

/** A bounded client value created explicitly for later parameter binding. */
final class OracleCreatedBlob implements Blob {
    static final int MAX_BYTES = 64 * 1024 * 1024;
    private byte[] bytes = new byte[0];
    private int size;

    private void live() throws SQLException {
        if (bytes == null) throw new SQLException("BLOB has been freed", "55000");
    }
    private int offset(long pos) throws SQLException {
        live();
        if (pos < 1 || pos > (long) size + 1)
            throw new SQLException("BLOB position is outside the value", "22023");
        return (int) (pos - 1);
    }
    @Override public synchronized long length() throws SQLException { live(); return size; }
    @Override public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        int start = offset(pos);
        if (length < 0) throw new SQLException("BLOB length must not be negative", "22023");
        return Arrays.copyOfRange(bytes, start, start + Math.min(length, size - start));
    }
    @Override public synchronized InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(getBytes(1, size));
    }
    @Override public synchronized InputStream getBinaryStream(long pos, long length) throws SQLException {
        int start = offset(pos);
        if (length < 0 || length > size - start)
            throw new SQLException("BLOB stream range is outside the value", "22023");
        return new ByteArrayInputStream(getBytes(pos, (int) length));
    }
    @Override public synchronized long position(byte[] pattern, long start) throws SQLException {
        live();
        if (pattern == null || start < 1)
            throw new SQLException("Invalid BLOB search arguments", "22023");
        if (start > (long) size + 1) return -1;
        // KMP bounds search time even for repeated bytes and long patterns.
        if (pattern.length > size - (start - 1)) return -1;
        if (pattern.length == 0) return start;
        int[] prefix = new int[pattern.length];
        for (int i = 1, j = 0; i < pattern.length; i++) {
            while (j > 0 && pattern[i] != pattern[j]) j = prefix[j - 1];
            if (pattern[i] == pattern[j]) j++;
            prefix[i] = j;
        }
        for (int i = (int) start - 1, j = 0; i < size; i++) {
            while (j > 0 && bytes[i] != pattern[j]) j = prefix[j - 1];
            if (bytes[i] == pattern[j]) j++;
            if (j == pattern.length) return (long) i - pattern.length + 2;
        }
        return -1;
    }
    @Override public long position(Blob pattern, long start) throws SQLException {
        synchronized (this) { live(); }
        if (pattern == null || start < 1) throw new SQLException("Invalid BLOB search arguments", "22023");
        long length = pattern.length();
        if (length < 0) throw new SQLException("Negative BLOB pattern length", "22023");
        if (length > MAX_BYTES) return -1;
        return position(length == 0 ? new byte[0] : pattern.getBytes(1, (int) length), start);
    }
    @Override public int setBytes(long pos, byte[] value) throws SQLException {
        if (value == null) throw new SQLException("Null BLOB bytes", "22023");
        return setBytes(pos, value, 0, value.length);
    }
    @Override public synchronized int setBytes(long pos, byte[] value, int offset, int length) throws SQLException {
        int start = offset(pos);
        if (value == null || offset < 0 || length < 0 || offset > value.length - length)
            throw new SQLException("Invalid BLOB byte range", "22023");
        if (length > MAX_BYTES - start)
            throw new SQLException("Created BLOB is limited to 64 MiB", "54000");
        int end = start + length;
        if (end > bytes.length) {
            int capacity = (int) Math.min(MAX_BYTES, Math.max(end, Math.max(32L, bytes.length * 2L)));
            bytes = Arrays.copyOf(bytes, capacity);
        }
        System.arraycopy(value, offset, bytes, start, length);
        size = Math.max(size, end);
        return length;
    }
    @Override public synchronized OutputStream setBinaryStream(long pos) throws SQLException {
        offset(pos);
        return new OutputStream() {
            private long next = pos;
            private boolean closed;
            @Override public void write(int value) throws IOException { write(new byte[] {(byte) value}, 0, 1); }
            @Override public void write(byte[] value, int offset, int length) throws IOException {
                if (closed) throw new IOException("BLOB output stream is closed");
                try { next += setBytes(next, value, offset, length); }
                catch (SQLException failure) { throw new IOException(failure.getMessage(), failure); }
            }
            @Override public void close() { closed = true; }
        };
    }
    @Override public synchronized void truncate(long length) throws SQLException {
        live();
        if (length < 0 || length > size)
            throw new SQLException("Invalid BLOB truncation length", "22023");
        size = (int) length;
        bytes = Arrays.copyOf(bytes, size);
    }
    @Override public synchronized void free() { bytes = null; size = 0; }
}
