// Local transport acceptance against a new, isolated development database.
import java.sql.*;
import java.math.BigDecimal;
import org.compiere.db.DB_Oracle;
public class TransportProbe {
  public static void main(String[] args) throws Exception {
    DB_Oracle provider = new DB_Oracle();
    boolean blocks = Boolean.getBoolean("bicdb.jdbc.blocks");
    String url = provider.getConnectionURL("127.0.0.1", Integer.parseInt(args[0]), "bicdb", "bicdb");
    if (!provider.getName().equals("Oracle")) throw new AssertionError("SQL provider changed");
    String expectedDriver = blocks ? "org.bicdb.jdbc.oracle.OracleDriver" : "org.postgresql.Driver";
    if (!provider.getDriver().getClass().getName().equals(expectedDriver)) throw new AssertionError("unexpected transport driver");
    try (Connection c = provider.getDriver().connect(url, new java.util.Properties() {{ setProperty("user", "bicdb"); }})) {
      try (Statement s = c.createStatement()) {
        s.executeUpdate("CREATE SCHEMA BICDB");
        s.executeUpdate("CREATE TABLE JDBC_PROBE(AD_CLIENT_ID NUMBER PRIMARY KEY, NAME VARCHAR2(60))");
      }
      c.setAutoCommit(false);
      try (PreparedStatement s = c.prepareStatement("INSERT INTO JDBC_PROBE VALUES(?,?)")) {
        s.setBigDecimal(1, new BigDecimal("12345678901234567890"));s.setString(2,"value");s.executeUpdate();
      }
      if (blocks) {
        try (PreparedStatement s = c.prepareStatement("BEGIN INSERT INTO JDBC_PROBE VALUES(?,?); END;")) {
          s.setBigDecimal(1, new BigDecimal("12345678901234567891"));
          s.setString(2, "bound block");
          s.execute();
        }
      }
      c.commit();
      try (PreparedStatement s = c.prepareStatement("SELECT AD_CLIENT_ID,NAME FROM JDBC_PROBE WHERE AD_CLIENT_ID=?")) {
        s.setBigDecimal(1, new BigDecimal("12345678901234567890"));
        try (ResultSet r = s.executeQuery()) {
          if (!r.next() || !r.getBigDecimal(1).equals(new BigDecimal("12345678901234567890")) || !r.getString(2).equals("value") || r.next()) throw new AssertionError("JDBC round-trip differs");
        }
      }
      if (blocks) {
        try (PreparedStatement s = c.prepareStatement("SELECT AD_CLIENT_ID,NAME FROM JDBC_PROBE WHERE AD_CLIENT_ID=?")) {
          s.setBigDecimal(1,new BigDecimal("12345678901234567891"));
          try (ResultSet r = s.executeQuery()) {
            if (!r.next() || !r.getBigDecimal(1).equals(new BigDecimal("12345678901234567891"))
                || !r.getString(2).equals("bound block") || r.next()) throw new AssertionError("anonymous numeric round-trip differs");
          }
        }
      }
    }
    System.out.println("Oracle provider / PostgreSQL JDBC transport: passed exact numeric prepared insert, commit and lookup");
    if (blocks) System.out.println("Oracle anonymous input block: passed exact numeric prepared insert");
  }
}
