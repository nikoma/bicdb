// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.postgresql.core.NativeQuery;
import static org.junit.jupiter.api.Assertions.*;

final class OracleSqlTest {
    @Test void ordinaryStatementsKeepTheStockDriverPath() throws Exception {
        for (String sql : new String[] {"SELECT 1", "BEGIN", "BEGIN WORK", "BEGIN TRANSACTION",
                "DO $$BEGIN RAISE NOTICE 'x'; END$$", "CREATE FUNCTION f() RETURNS text AS $$BEGIN END$$",
                "DECLARE c CURSOR FOR SELECT 'BEGIN END'", "/* BEGIN END */ SELECT q'[text]'"})
            assertNull(OracleSql.anonymous(sql), sql);
    }

    @Test void allInternalSemicolonsAndInputPositionsSurvive() throws Exception {
        String sql = "DECLARE x INTEGER; BEGIN x:=?; INSERT INTO example_rows VALUES(x,?); END;";
        NativeQuery query = OracleBlocks.parse(OracleSql.anonymous(sql), true, true);
        assertEquals("DECLARE x INTEGER; BEGIN x:=$1; INSERT INTO example_rows VALUES(x,$2); END;", query.nativeSql);
        assertEquals(2, query.bindPositions.length);
        for (int i = 0; i < query.bindPositions.length; i++)
            assertTrue(query.nativeSql.substring(query.bindPositions[i]).startsWith("$" + (i + 1)));
        assertFalse(query.multiStatement);
        assertFalse(query.command.isBatchedReWriteCompatible());
    }

    @Test void quoteKindsProtectQuestionMarksAndEndKeywords() throws Exception {
        String sql = "BEGIN INSERT INTO example_rows VALUES(?, q'[isn't ?; END;]'); END;";
        String normalized = OracleSql.anonymous(sql);
        assertEquals("BEGIN INSERT INTO example_rows VALUES(?, 'isn''t ?; END;'); END;", normalized);
        assertEquals(1, OracleBlocks.parse(normalized, true, true).bindPositions.length);
        assertEquals("BEGIN SELECT N'Ж''? END;' INTO x; END;",
                OracleSql.anonymous("BEGIN SELECT NQ'!Ж'? END;!' INTO x; END;"));
        assertEquals("BEGIN SELECT 'a''?END;' INTO x; END;",
                OracleSql.anonymous("BEGIN SELECT q'😀a'?END;😀' INTO x; END;"));
        assertEquals("BEGIN SELECT '' INTO x; END;", OracleSql.anonymous("BEGIN SELECT q'''' INTO x; END;"));
    }

    @Test void commentsAndIdentifiersDoNotCreateInputsOrBlockEnd() throws Exception {
        String sql = "-- ? DECLARE\nBEGIN INSERT INTO \"q?END\" VALUES(?, 'it''s ?'); /* ? */ END;";
        NativeQuery parsed = OracleBlocks.parse(OracleSql.anonymous(sql), true, true);
        assertEquals(1, parsed.bindPositions.length);
        assertTrue(parsed.nativeSql.contains("\"q?END\""));
        assertNull(OracleSql.anonymous("BEGIN SELECT 'END' INTO x; -- END\n"));
    }

    @Test void trailingLineCommentsDoNotConsumeTheTemporaryEnvelope() throws Exception {
        String sql = "BEGIN NULL; END; -- final comment";
        assertEquals(sql, OracleBlocks.parse(OracleSql.anonymous(sql), true, true).nativeSql);
    }

    @Test void unparameterizedSubmissionsKeepQuestionMarks() throws Exception {
        NativeQuery parsed = OracleBlocks.parse("BEGIN INSERT INTO example_rows VALUES(1,'?'); END;", false, true);
        assertEquals(0, parsed.bindPositions.length);
        assertTrue(parsed.nativeSql.contains("'?'"));
    }

    @Test void oracleBackslashesDoNotHideFollowingParameters() throws Exception {
        NativeQuery parsed = OracleBlocks.parse(OracleSql.anonymous(
                "BEGIN INSERT INTO example_rows VALUES('\\',?); END;"), true, true);
        assertEquals(1, parsed.bindPositions.length);
        assertTrue(parsed.nativeSql.contains("'\\',"));
    }

    @Test void jdbcEscapesAreProcessedWithinTheSingleSubmission() throws Exception {
        NativeQuery parsed = OracleBlocks.parse("BEGIN INSERT INTO example_rows VALUES({d '2026-01-02'},?); END;", true, true);
        assertEquals(1, parsed.bindPositions.length);
        assertFalse(parsed.nativeSql.contains("{d"));
        assertTrue(parsed.nativeSql.contains("2026-01-02"));
    }

    @Test void malformedCandidateLiteralsFailBeforeSubmission() {
        for (String sql : new String[] {"BEGIN SELECT q'[unclosed' INTO x; END;", "BEGIN 'unclosed", "BEGIN /* unclosed"}) {
            SQLException error = assertThrows(SQLException.class, () -> OracleSql.anonymous(sql));
            assertEquals("42601", error.getSQLState());
        }
    }

    @Test void explicitDriverDoesNotClaimPostgresqlUrls() {
        OracleDriver driver = new OracleDriver();
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/bicdb"));
        assertFalse(driver.acceptsURL(null));
        assertTrue(driver.acceptsURL("jdbc:bicdb:oracle://localhost/bicdb"));
    }
}
