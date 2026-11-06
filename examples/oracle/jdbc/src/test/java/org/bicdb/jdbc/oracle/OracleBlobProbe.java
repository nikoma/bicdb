// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.io.ByteArrayInputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.UUID;
import javax.sql.rowset.serial.SerialBlob;

/** Real binary-value round trips; never creates PostgreSQL OID large objects. */
final class OracleBlobProbe {
    private static void check(String label, boolean value) {
        if (!value) throw new AssertionError(label);
        System.out.println("passed=" + label);
    }

    static int run(Connection connection) throws Exception {
        String table = "BLOB_JDBC_" + UUID.randomUUID().toString().replace("-", "");
        byte[] bytes = {0, 1, 2, 127, -128, -1};
        int checks = 0;
        boolean created = false;
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + table + "(key_value INTEGER PRIMARY KEY,payload BLOB)");
                created = true;
            }
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table + " VALUES(?,?)")) {
                for (int variant = 0; variant < 8; variant++) {
                    insert.setInt(1, variant);
                    switch (variant) {
                        case 0: insert.setBlob(2, new ByteArrayInputStream(bytes)); break;
                        case 1: insert.setBlob(2, new ByteArrayInputStream(bytes), (long) bytes.length); break;
                        case 2: insert.setBlob(2, new SerialBlob(bytes)); break;
                        case 3: insert.setObject(2, new SerialBlob(bytes)); break;
                        case 4: insert.setObject(2, new SerialBlob(bytes), Types.BLOB); break;
                        case 5: insert.setObject(2, new SerialBlob(bytes), JDBCType.BLOB); break;
                        case 6: insert.setBlob(2, new SerialBlob(new byte[0])); break;
                        default: insert.setNull(2, Types.BLOB);
                    }
                    check("blob_bind_variant_" + variant, insert.executeUpdate() == 1); checks++;
                }
            }
            try (PreparedStatement read = connection.prepareStatement("SELECT payload FROM " + table + " WHERE key_value=?")) {
                for (int variant = 0; variant < 8; variant++) {
                    read.setInt(1, variant);
                    try (ResultSet rows = read.executeQuery()) {
                        if (!rows.next()) throw new AssertionError("missing BLOB row " + variant);
                        Blob value = variant % 2 == 0 ? rows.getBlob(1) : rows.getBlob("PAYLOAD");
                        if (variant == 7) {
                            check("blob_null", value == null && rows.wasNull()); checks++;
                        } else {
                            byte[] expected = variant == 6 ? new byte[0] : bytes;
                            check("blob_value_variant_" + variant,
                                    !rows.wasNull() && value.length() == expected.length
                                    && Arrays.equals(expected, value.getBinaryStream().readAllBytes())); checks++;
                            Blob converted = rows.getObject(1, Blob.class);
                            check("blob_object_variant_" + variant,
                                    Arrays.equals(expected, converted.getBinaryStream().readAllBytes())); checks++;
                            try {
                                value.setBytes(1, new byte[] {9});
                                throw new AssertionError("snapshot mutation silently accepted");
                            } catch (SQLFeatureNotSupportedException error) {
                                check("blob_snapshot_rejects_write_" + variant, "0A000".equals(error.getSQLState())); checks++;
                            }
                            converted.free(); value.free();
                        }
                    }
                }
            }
            return checks;
        } finally {
            if (created) try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + table);
            }
        }
    }
}
