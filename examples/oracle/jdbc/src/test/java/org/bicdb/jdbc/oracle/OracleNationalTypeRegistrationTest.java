package org.bicdb.jdbc.oracle;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.postgresql.core.*;
import org.postgresql.jdbc.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real PGJDBC metadata objects with a simulated catalog transport. The fixture
 * OIDs are not a server ABI reservation or a live PostgreSQL-protocol test. */
final class OracleNationalTypeRegistrationTest {
    static final int VARYING=900010, VARYING_ARRAY=900011, FIXED=900012, FIXED_ARRAY=900013, SPOOF=900014;
    static final String NAME="bicdb_fixture_nvarchar_v1";
    static final class VaryingArrayMarker { }
    static final class FixedArrayMarker { }

    static final class Fixture {
        TypeInfoCache info;
        final org.postgresql.util.LruCache<Object,FieldMetadata> fieldCache =
                new org.postgresql.util.LruCache<>(4,4096L,false);
        final BaseConnection connection=(BaseConnection)Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{BaseConnection.class},(p,m,a)-> {
                    if(m.getName().equals("getTypeInfo"))return info;
                    if(m.getName().equals("getFieldMetadataCache"))return fieldCache;
                    if(m.getName().equals("prepareStatement"))return statement((String)a[0]);
                    throw new AssertionError("Unexpected connection operation: "+m.getName());
                });
        Fixture(){info=new TypeInfoCache(connection,65535);}
        void register(){
            // Distinct marker classes avoid overwriting String[]/PGobject[]
            // mappings. The future adapter must normalize national class names.
            info.addCoreType(NAME,VARYING,Types.NVARCHAR,VaryingArrayMarker.class.getName(),VARYING_ARRAY);
            info.addCoreType("bicdb_fixture_nchar_v1",FIXED,Types.NCHAR,FixedArrayMarker.class.getName(),FIXED_ARRAY);
        }
        PgResultSetMetaData metadata(int...oids){
            Field[] fields=new Field[oids.length];
            for(int i=0;i<oids.length;i++){
                fields[i]=new Field(NAME,oids[i]);
                // Computed result fields have no table-column catalog lookup.
                fields[i].setMetadata(new FieldMetadata(NAME));
            }
            return new PgResultSetMetaData(connection,fields);
        }
        PreparedStatement statement(String sql){
            long[] selected={SPOOF};
            return (PreparedStatement)Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{PreparedStatement.class,BaseStatement.class},(p,m,a)-> {
                        switch(m.getName()){
                            case "setLong": case "setInt":selected[0]=((Number)a[1]).longValue();return null;
                            case "executeWithFlags":return true;
                            case "getResultSet":return rows(sql,selected[0]);
                            case "close":return null;
                            default:throw new AssertionError("Unexpected catalog statement operation: "+m.getName());
                        }
                    });
        }
        ResultSet rows(String sql,long selected){
            boolean names=sql.startsWith("SELECT n.nspname = ANY(");
            long[] oids=sql.contains("?")?new long[]{selected}:new long[]{VARYING,FIXED,SPOOF};
            int[] index={-1};
            return (ResultSet)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{ResultSet.class},(p,m,a)-> {
                switch(m.getName()){
                    case "next":return ++index[0]<oids.length;
                    case "close":return null;
                    case "getLong":return oids[index[0]];
                    case "getBoolean":return names;
                    case "getString":
                        if(names)return ((Number)a[0]).intValue()==2?"public":NAME;
                        if(a[0].equals("typtype"))return oids[index[0]]==SPOOF?"e":"b";
                        if(a[0].equals("typname"))return oids[index[0]]==FIXED?"bicdb_fixture_nchar_v1":NAME;
                        throw new AssertionError("Unexpected catalog column");
                    default:throw new AssertionError("Unexpected catalog result operation: "+m.getName());
                }
            });
        }
    }

    @Test void registeredOidsIdentifyResultAndParameterFamilies()throws Exception{
        Fixture f=new Fixture();f.register();
        ResultSetMetaData rows=f.metadata(VARYING,FIXED);
        assertEquals(Types.NVARCHAR,rows.getColumnType(1));
        assertEquals(Types.NCHAR,rows.getColumnType(2));
        ParameterMetaData parameters=new PgParameterMetaData(f.connection,new int[]{VARYING,FIXED});
        assertEquals(Types.NVARCHAR,parameters.getParameterType(1));
        assertEquals(Types.NCHAR,parameters.getParameterType(2));
    }
    @Test void ordinaryTextDoesNotBecomeNationalFromItsColumnLabel()throws Exception{
        Fixture f=new Fixture();f.register();
        assertEquals(Types.VARCHAR,f.metadata(Oid.TEXT).getColumnType(1));
    }
    @Test void sameNamedUserEnumCannotAcquireTheRegisteredNationalType()throws Exception{
        Fixture f=new Fixture();f.register();PgResultSetMetaData rows=f.metadata(SPOOF);
        assertEquals(NAME,rows.getColumnTypeName(1));
        assertEquals(Types.VARCHAR,rows.getColumnType(1));
        assertNotEquals(Types.NVARCHAR,rows.getColumnType(1));
        assertEquals(Types.NVARCHAR,f.metadata(VARYING).getColumnType(1));
    }
    @Test void normalJavaArrayMappingsArePreserved()throws Exception{
        Fixture f=new Fixture();int strings=f.info.getJavaArrayType(String.class.getName());
        int objects=f.info.getJavaArrayType(org.postgresql.util.PGobject.class.getName());f.register();
        assertEquals(strings,f.info.getJavaArrayType(String.class.getName()));
        assertEquals(objects,f.info.getJavaArrayType(org.postgresql.util.PGobject.class.getName()));
        assertEquals(VARYING_ARRAY,f.info.getJavaArrayType(VaryingArrayMarker.class.getName()));
        assertEquals(FIXED_ARRAY,f.info.getJavaArrayType(FixedArrayMarker.class.getName()));
    }
    @Test void bulkCatalogCachingCannotOverwriteRegisteredOidFamilies()throws Exception{
        Fixture f=new Fixture();f.register();f.info.cacheSQLTypes();
        assertEquals(Types.NVARCHAR,f.metadata(VARYING).getColumnType(1));
        assertEquals(Types.NCHAR,f.metadata(FIXED).getColumnType(1));
        assertEquals(Types.VARCHAR,f.metadata(SPOOF).getColumnType(1));
        assertEquals(Types.VARCHAR,f.metadata(Oid.TEXT).getColumnType(1));
    }
}
