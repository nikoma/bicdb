// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import javax.sql.rowset.serial.SerialBlob;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class OracleBlobsTest {
    static final class Bindings {
        final List<String> names = new ArrayList<>();
        Object[] values;
        final PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {PreparedStatement.class}, (p, m, a) -> {
                    assertNotEquals("setBlob", m.getName(), "must not enter the OID large-object path");
                    names.add(m.getName()); values = a; return null;
                });
        boolean bind(String name, Class<?>[] signature, Object... values) throws Exception {
            return OracleBlobs.bind(statement, PreparedStatement.class.getMethod(name, signature), values);
        }
    }

    @Test void streamOverloadsKeepTheirLengthsAndCallerOwnedStreams() throws Exception {
        Bindings b = new Bindings();
        InputStream stream = new ByteArrayInputStream(new byte[] {0, 1, -1});
        assertTrue(b.bind("setBlob", new Class<?>[] {int.class, InputStream.class}, 1, stream));
        assertEquals(List.of("setBinaryStream"), b.names);
        assertSame(stream, b.values[1]);
        assertEquals(2, b.values.length);
        assertTrue(b.bind("setBlob", new Class<?>[] {int.class, InputStream.class, long.class}, 2, stream, 3L));
        assertEquals(3L, b.values[2]);
        assertEquals(0, stream.read(), "the adapter must neither consume nor close a caller stream");
    }

    @Test void negativeStreamLengthsDoNotReplaceExistingParameters() throws Exception {
        Bindings b = new Bindings();
        SQLException error = assertThrows(SQLException.class, () -> b.bind("setBlob",
                new Class<?>[] {int.class, InputStream.class, long.class}, 1, null, -1L));
        assertEquals("22023", error.getSQLState());
        assertTrue(b.names.isEmpty());
    }

    @Test void blobObjectsAndTypedObjectOverloadsBindBinaryValues() throws Exception {
        Bindings b = new Bindings();
        Blob blob = new SerialBlob(new byte[] {0, 12, -1});
        assertTrue(b.bind("setBlob", new Class<?>[] {int.class, Blob.class}, 1, blob));
        assertEquals("setBytes", b.names.get(0));
        assertArrayEquals(new byte[] {0, 12, -1}, (byte[]) b.values[1]);
        assertTrue(b.bind("setObject", new Class<?>[] {int.class, Object.class}, 2, blob));
        assertTrue(b.bind("setObject", new Class<?>[] {int.class, Object.class, int.class}, 2, blob, Types.BLOB));
        assertTrue(b.bind("setObject", new Class<?>[] {int.class, Object.class, java.sql.SQLType.class}, 2, blob, JDBCType.BLOB));
        assertArrayEquals(new byte[] {0, 12, -1}, (byte[]) b.values[1]);
    }

    @Test void nullAndEmptyBlobBindingsRemainDifferent() throws Exception {
        Bindings b = new Bindings();
        assertTrue(b.bind("setBlob", new Class<?>[] {int.class, Blob.class}, 1, null));
        assertEquals(Types.BINARY, b.values[1]);
        assertTrue(b.bind("setNull", new Class<?>[] {int.class, int.class}, 1, Types.BLOB));
        assertEquals(Types.BINARY, b.values[1]);
        assertTrue(b.bind("setObject", new Class<?>[] {int.class, Object.class, int.class}, 1, null, Types.BLOB));
        assertEquals(Types.BINARY, b.values[1]);
        assertTrue(b.bind("setBlob", new Class<?>[] {int.class, Blob.class}, 1, new SerialBlob(new byte[0])));
        assertArrayEquals(new byte[0], (byte[]) b.values[1]);
    }

    @Test void unrelatedSettersStayOnTheOrdinaryDriverPath() throws Exception {
        Bindings b = new Bindings();
        assertFalse(b.bind("setBytes", new Class<?>[] {int.class, byte[].class}, 1, new byte[0]));
        assertFalse(b.bind("setObject", new Class<?>[] {int.class, Object.class, int.class}, 1, "text", Types.VARCHAR));
        assertFalse(b.bind("setNull", new Class<?>[] {int.class, int.class}, 1, Types.INTEGER));
        assertTrue(b.names.isEmpty());
    }

    @Test void namedCallableBindingsKeepTheirExistingDriverBoundary() throws Exception {
        Bindings b = new Bindings();
        Method method = CallableStatement.class.getMethod("setBlob", String.class, Blob.class);
        assertFalse(OracleBlobs.bind(b.statement, method, new Object[] {"p_blob", new SerialBlob(new byte[] {1})}));
        assertTrue(b.names.isEmpty());
    }

    @Test void oversizedBlobObjectsFailBeforeReadingOrBinding() throws Exception {
        Bindings b = new Bindings();
        Blob blob = (Blob) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Blob.class},
                (p, m, a) -> { assertEquals("length", m.getName()); return Long.MAX_VALUE; });
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> b.bind("setBlob", new Class<?>[] {int.class, Blob.class}, 1, blob));
        assertTrue(b.names.isEmpty());
    }

    private ResultSet rows(byte[] value, String type) {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ResultSetMetaData.class}, (p, m, a) -> type);
        return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {ResultSet.class},
                (p, m, a) -> {
                    if (m.getName().equals("getMetaData")) return metadata;
                    if (m.getName().equals("findColumn")) return 1;
                    if (m.getName().equals("getBytes")) return value;
                    throw new AssertionError("unexpected result-set method " + m.getName());
                });
    }

    @Test void binarySnapshotsPreserveValuesAndRejectUnpersistedWrites() throws Exception {
        ResultSet rows = rows(new byte[] {0, 1, 2, -1}, "bytea");
        assertTrue(OracleBlobs.binaryColumn(rows, "payload"));
        Blob blob = OracleBlobs.read(rows, 1);
        assertEquals(4, blob.length());
        assertArrayEquals(new byte[] {0, 1, 2, -1}, blob.getBinaryStream().readAllBytes());
        assertArrayEquals(new byte[] {1, 2}, blob.getBinaryStream(2, 2).readAllBytes());
        assertEquals(2, blob.position(new byte[] {1, 2}, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> blob.setBytes(1, new byte[] {9}));
        assertThrows(SQLFeatureNotSupportedException.class, () -> blob.setBinaryStream(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> blob.truncate(0));
        assertArrayEquals(new byte[] {0, 1, 2, -1}, blob.getBytes(1, 4));
        blob.free();
        assertThrows(SQLException.class, blob::length);
    }

    @Test void nullEmptyAndOtherColumnTypesAreNotConfused() throws Exception {
        assertNull(OracleBlobs.read(rows(null, "bytea"), 1));
        Blob empty = OracleBlobs.read(rows(new byte[0], "bytea"), "payload");
        assertEquals(0, empty.length());
        assertEquals(-1, empty.getBinaryStream().read());
        assertFalse(OracleBlobs.binaryColumn(rows(new byte[] {1}, "oid"), 1));
    }
}
