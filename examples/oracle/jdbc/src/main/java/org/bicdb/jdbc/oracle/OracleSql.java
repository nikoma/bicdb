// SPDX-License-Identifier: Apache-2.0
package org.bicdb.jdbc.oracle;

import java.sql.SQLException;

/** Lexical boundaries only; the server remains responsible for Oracle grammar. */
final class OracleSql {
    private OracleSql() {}

    /** Return a normalized anonymous submission, or null for ordinary SQL. */
    static String anonymous(String sql) throws SQLException {
        if (sql == null) return null;
        StringBuilder out = new StringBuilder(sql.length());
        boolean candidate = false;
        boolean first = true;
        boolean begin = false;
        boolean end = false;
        for (int i = 0; i < sql.length();) {
            int at = i;
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                out.append(c);
                i++;
            } else if (sql.startsWith("--", i)) {
                i = sql.indexOf('\n', i + 2);
                if (i < 0) i = sql.length();
                out.append(sql, at, i);
            } else if (sql.startsWith("/*", i)) {
                int depth = 1;
                i += 2;
                while (i < sql.length() && depth != 0) {
                    if (sql.startsWith("/*", i)) { depth++; i += 2; }
                    else if (sql.startsWith("*/", i)) { depth--; i += 2; }
                    else i++;
                }
                if (depth != 0) throw syntax("Unterminated SQL comment");
                out.append(sql, at, i);
            } else if (Character.isLetter(c) || c == '_') {
                i++;
                while (i < sql.length() && identifier(sql.charAt(i))) i++;
                String word = sql.substring(at, i);
                if (first) {
                    first = false;
                    candidate = word.equalsIgnoreCase("BEGIN") || word.equalsIgnoreCase("DECLARE");
                    if (!candidate) return null;
                }
                if ((word.equalsIgnoreCase("Q") || word.equalsIgnoreCase("NQ"))
                        && i < sql.length() && sql.charAt(i) == '\'') {
                    int delimiterAt = i + 1;
                    if (delimiterAt >= sql.length()) throw syntax("Missing alternative quote delimiter");
                    int delimiter = sql.codePointAt(delimiterAt);
                    if (Character.isWhitespace(delimiter))
                        throw syntax("Invalid alternative quote delimiter");
                    int close = delimiter == '[' ? ']' : delimiter == '(' ? ')'
                            : delimiter == '{' ? '}' : delimiter == '<' ? '>' : delimiter;
                    String terminator = new String(Character.toChars(close)) + "'";
                    int contentAt = delimiterAt + Character.charCount(delimiter);
                    int closeAt = sql.indexOf(terminator, contentAt);
                    if (closeAt < 0) throw syntax("Unterminated alternative quoted literal");
                    if (word.length() == 2) out.append('N');
                    out.append('\'').append(sql.substring(contentAt, closeAt).replace("'", "''")).append('\'');
                    i = closeAt + terminator.length();
                } else {
                    begin |= word.equalsIgnoreCase("BEGIN");
                    end |= word.equalsIgnoreCase("END");
                    out.append(word);
                }
            } else if (c == '\'' || c == '"') {
                if (first) return null;
                i = quoted(sql, i, c);
                out.append(sql, at, i);
            } else if (c == '$') {
                if (first) return null;
                int tagEnd = i + 1;
                while (tagEnd < sql.length()
                        && (Character.isLetterOrDigit(sql.charAt(tagEnd)) || sql.charAt(tagEnd) == '_')) tagEnd++;
                if (tagEnd < sql.length() && sql.charAt(tagEnd) == '$'
                        && (tagEnd == i + 1 || !Character.isDigit(sql.charAt(i + 1)))) {
                    String tag = sql.substring(i, tagEnd + 1);
                    int closeAt = sql.indexOf(tag, tagEnd + 1);
                    if (closeAt < 0) throw syntax("Unterminated dollar quoted literal");
                    i = closeAt + tag.length();
                } else i++;
                out.append(sql, at, i);
            } else {
                if (first) return null;
                out.append(c);
                i++;
            }
        }
        return candidate && begin && end ? out.toString() : null;
    }

    private static boolean identifier(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }

    private static int quoted(String sql, int at, char quote) throws SQLException {
        int i = at + 1;
        while (i < sql.length()) {
            if (sql.charAt(i++) != quote) continue;
            if (i < sql.length() && sql.charAt(i) == quote) { i++; continue; }
            return i;
        }
        throw syntax("Unterminated SQL literal or identifier");
    }

    private static SQLException syntax(String message) { return new SQLException(message, "42601"); }
}
