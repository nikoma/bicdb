// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import static org.junit.jupiter.api.Assertions.*;

class OracleCreatedBlobTest {
    @Test void createsIndependentEmptyValuesAndRejectsClosedConnection() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        Connection delegate = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {BaseConnection.class}, (p, m, a) -> {
                    if (m.getName().equals("isClosed")) return closed.get();
                    if (m.getName().equals("unwrap")) return ((Class<?>) a[0]).cast(p);
                    if (m.getName().equals("getParameterStatus")) return "oracle-preview-v1";
                    throw new AssertionError("unexpected delegation: " + m.getName());
                });
        Connection connection = OracleConnections.wrap(delegate);
        Blob first = connection.createBlob(), second = connection.createBlob();
        assertEquals(0, first.length());
        first.setBytes(1, new byte[] {0, -1});
        assertEquals(0, second.length());
        closed.set(true);
        assertEquals("08003", assertThrows(SQLException.class, connection::createBlob).getSQLState());
    }

    @Test void appendsOverwritesSlicesAndReturnsCopies() throws Exception {
        Blob blob = new OracleCreatedBlob();
        byte[] source = {1, 2, 3};
        assertEquals(3, blob.setBytes(1, source));
        source[0] = 9;
        assertEquals(2, blob.setBytes(2, new byte[] {7, 8, 9, 10}, 1, 2));
        blob.setBytes(4, new byte[] {0, -128, -1});
        assertArrayEquals(new byte[] {1, 8, 9, 0, -128, -1}, blob.getBytes(1, 100));
        byte[] copy = blob.getBytes(1, 1); copy[0] = 33;
        assertArrayEquals(new byte[] {1}, blob.getBytes(1, 1));
        assertArrayEquals(new byte[] {9, 0}, blob.getBinaryStream(3, 2).readAllBytes());
        assertEquals(6, blob.getBinaryStream().readAllBytes().length);
        blob.truncate(2);
        assertArrayEquals(new byte[] {1, 8}, blob.getBytes(1, 10));
        blob.truncate(0);
        assertArrayEquals(new byte[0], blob.getBinaryStream().readAllBytes());
    }

    @Test void outputWritesGrowAndClosedOrFreedStreamsFail() throws Exception {
        Blob blob = new OracleCreatedBlob();
        OutputStream stream = blob.setBinaryStream(1);
        stream.write(0); stream.write(new byte[] {2, 3, 4}, 1, 2);
        assertArrayEquals(new byte[] {0, 3, 4}, blob.getBytes(1, 3));
        stream.close();
        assertThrows(IOException.class, () -> stream.write(9));
        OutputStream stale = blob.setBinaryStream(2);
        blob.free(); blob.free();
        assertThrows(SQLException.class, blob::length);
        assertThrows(SQLException.class, blob::getBinaryStream);
        assertInstanceOf(SQLException.class, assertThrows(IOException.class, () -> stale.write(1)).getCause());
    }

    @Test void searchesBinaryPatternsWithOverlappingPrefixes() throws Exception {
        Blob blob = new OracleCreatedBlob();
        blob.setBytes(1, new byte[] {1, 1, 1, 2, 0, -1});
        assertEquals(2, blob.position(new byte[] {1, 1, 2}, 1));
        assertEquals(5, blob.position(new byte[] {0, -1}, 2));
        assertEquals(-1, blob.position(new byte[] {1, 1}, 3));
        assertEquals(-1, blob.position(new byte[] {1}, Long.MAX_VALUE));
        Blob pattern = new OracleCreatedBlob(); pattern.setBytes(1, new byte[] {0, -1});
        assertEquals(5, blob.position(pattern, 1));
    }

    @Test void invalidRangesAndOverflowFailWithoutMutation() throws Exception {
        Blob blob = new OracleCreatedBlob(); blob.setBytes(1, new byte[] {1, 2});
        assertThrows(SQLException.class, () -> blob.setBytes(Long.MAX_VALUE, new byte[] {8}));
        assertThrows(SQLException.class, () -> blob.setBytes(1, new byte[] {8}, Integer.MAX_VALUE, 1));
        assertThrows(SQLException.class, () -> blob.setBytes(4, new byte[] {8}));
        assertThrows(SQLException.class, () -> blob.getBytes(0, 1));
        assertThrows(SQLException.class, () -> blob.getBytes(1, -1));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(2, Long.MAX_VALUE));
        assertThrows(SQLException.class, () -> blob.truncate(3));
        assertArrayEquals(new byte[] {1, 2}, blob.getBytes(1, 2));
    }

    @Test void capacityLimitRejectsBeforeChangingExistingValue() throws Exception {
        Blob blob = new OracleCreatedBlob();
        blob.setBytes(1, new byte[OracleCreatedBlob.MAX_BYTES]);
        assertEquals("54000", assertThrows(SQLException.class,
                () -> blob.setBytes(OracleCreatedBlob.MAX_BYTES, new byte[] {1, 2})).getSQLState());
        assertEquals(OracleCreatedBlob.MAX_BYTES, blob.length());
        assertArrayEquals(new byte[] {0}, blob.getBytes(OracleCreatedBlob.MAX_BYTES, 1));
        blob.free();
    }
}
