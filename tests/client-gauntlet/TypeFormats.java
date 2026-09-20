import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class TypeFormats {
    private static final String QUERY = """
        SELECT TRUE::bool, (-12345)::int2, 123456789::int4,
               9007199254740993::int8, 1.5::float4, (-2.25)::float8,
               12345678901234567890.125::numeric, 'client-text'::text,
               '\\x00ff10'::bytea
        """;

    private static void expect(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void run(String baseUrl, boolean binary) throws Exception {
        String separator = baseUrl.contains("?") ? "&" : "?";
        String url = baseUrl + separator + "binaryTransfer=" + binary
            + "&prepareThreshold=" + (binary ? "-1" : "0");
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery(QUERY)) {
            expect(row.next(), "JDBC query returned no row");
            expect(row.getBoolean(1), "JDBC bool mismatch");
            expect(row.getShort(2) == -12345, "JDBC int2 mismatch");
            expect(row.getInt(3) == 123456789, "JDBC int4 mismatch");
            expect(row.getLong(4) == 9007199254740993L, "JDBC int8 mismatch");
            expect(Float.compare(row.getFloat(5), 1.5f) == 0, "JDBC float4 mismatch");
            expect(Double.compare(row.getDouble(6), -2.25d) == 0, "JDBC float8 mismatch");
            expect(row.getBigDecimal(7).equals(new BigDecimal("12345678901234567890.125")),
                "JDBC numeric mismatch");
            expect(row.getString(8).equals("client-text"), "JDBC text mismatch");
            expect(Arrays.equals(row.getBytes(9), new byte[] {0, (byte) 0xff, 0x10}),
                "JDBC bytea mismatch");
            expect(!row.next(), "JDBC query returned extra rows");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("DATABASE_URL TARGET required");
        URI uri = URI.create(args[0]);
        String userInfo = uri.getRawUserInfo();
        String user = userInfo == null ? "" : userInfo.split(":", 2)[0];
        String password = userInfo != null && userInfo.contains(":")
            ? userInfo.substring(userInfo.indexOf(':') + 1) : "";
        String jdbcUrl = "jdbc:postgresql://" + uri.getHost() + ":" + uri.getPort()
            + uri.getPath() + "?user=" + URLDecoder.decode(user, StandardCharsets.UTF_8)
            + "&password=" + URLDecoder.decode(password, StandardCharsets.UTF_8);
        run(jdbcUrl, false);
        run(jdbcUrl, true);
        System.out.println("JDBC text/binary gauntlet passed for " + args[1]);
    }
}
