import java.sql.*;
import java.util.*;

/** Read-only reference control. Emit Java UTF-16 units, never lossy Unicode. */
public final class NationalCastJdbcReference {
  static String required(String key) {
    String value=System.getenv(key);
    if(value==null || value.isEmpty()) throw new IllegalStateException("Missing reference setting: "+key);
    return value;
  }
  static String quote(String value) {
    if(value==null) return "null";
    StringBuilder out=new StringBuilder("\"");
    for(char unit:value.toCharArray()) {
      if(unit=='"' || unit=='\\') out.append('\\').append(unit);
      else if(unit<32 || unit>126) out.append(String.format(Locale.ROOT,"\\u%04x",(int)unit));
      else out.append(unit);
    }
    return out.append('"').toString();
  }
  static String units(String value) {
    if(value==null)return "null";
    StringJoiner out=new StringJoiner(",","[","]");
    for(char unit:value.toCharArray())out.add(quote(String.format(Locale.ROOT,"%04X",(int)unit)));
    return out.toString();
  }
  static String getter(ResultSet rows,boolean national)throws SQLException {
    try{return "{\"units\":"+units(national?rows.getNString(2):rows.getString(2))+"}";}
    catch(SQLFeatureNotSupportedException error){return "{\"error_code\":"+error.getErrorCode()+",\"sqlstate\":"+quote(error.getSQLState())+"}";}
  }
  public static void main(String[] args)throws Exception {
    String[][] cases={
      {"nvarchar_ascii_truncation","CAST('abc' AS NVARCHAR2(2))"},
      {"nchar_ascii_truncation","CAST('abc' AS NCHAR(2))"},
      {"nvarchar_lone_high_surrogate","CAST(N'😀' AS NVARCHAR2(1))"},
      {"nchar_lone_high_surrogate","CAST(N'😀' AS NCHAR(1))"},
      {"nvarchar_complete_pair","CAST(N'😀' AS NVARCHAR2(2))"},
      {"nchar_complete_pair","CAST(N'😀' AS NCHAR(2))"},
      {"nchar_padding","CAST(N'界' AS NCHAR(3))"},
      {"nvarchar_numeric_overflow","CAST(123 AS NVARCHAR2(2))"}
    };
    Properties properties=new Properties();properties.setProperty("user",required("BICDB_ORACLE_REFERENCE_USER"));properties.setProperty("password",required("BICDB_ORACLE_REFERENCE_PASSWORD"));
    StringJoiner observations=new StringJoiner(",","[","]");String version,driver;
    DriverManager.setLoginTimeout(30);
    try(Connection connection=DriverManager.getConnection(required("BICDB_ORACLE_REFERENCE_JDBC_URL"),properties)) {
      connection.setAutoCommit(false);
      try(Statement statement=connection.createStatement()){statement.setQueryTimeout(30);statement.execute("SET TRANSACTION READ ONLY");}
      try {
        version=connection.getMetaData().getDatabaseProductVersion();driver=connection.getMetaData().getDriverVersion();
        for(String[] item:cases) {
          String prefix="{\"id\":"+quote(item[0])+",\"expression\":"+quote(item[1]);
          try(Statement statement=connection.createStatement();ResultSet rows=statement.executeQuery("SELECT DUMP("+item[1]+",1016),"+item[1]+" FROM DUAL")) {
            if(!rows.next())throw new IllegalStateException("Reference case returned no row");
            String dump=rows.getString(1);
            observations.add(prefix+",\"dump\":"+quote(dump)+",\"jdbc_type\":"+rows.getMetaData().getColumnType(2)+",\"type_name\":"+quote(rows.getMetaData().getColumnTypeName(2))+",\"getString\":"+getter(rows,false)+",\"getNString\":"+getter(rows,true)+"}");
            if(rows.next())throw new IllegalStateException("Reference case returned extra rows");
          }catch(SQLException error) {
            if(error.getErrorCode()!=25137)throw error;
            observations.add(prefix+",\"error_code\":"+error.getErrorCode()+",\"sqlstate\":"+quote(error.getSQLState())+"}");
          }
        }
      }finally{connection.rollback();}
    }
    System.out.println("{\"format\":1,\"reference\":\"oracle-jdbc\",\"observation_completed\":true,\"source_version\":"+quote(version)+",\"driver_version\":"+quote(driver)+",\"original_application_objects_modified\":false,\"implementation_qualified\":false,\"application_qualified\":false,\"cases\":"+observations+"}");
  }
}
