# Oracle preview JDBC adapter

This reference adapter preserves Oracle anonymous submissions over stock
PostgreSQL JDBC 42.7.8. It uses the driver's public interfaces, retains its
parameter codecs and ordinary SQL execution, and requires an explicit
[`oracle-preview-v1`](../../../docs/oracle-preview-profile.md) server connection.
It is a separate client component; unchanged Oracle JDBC drivers still require
Oracle's protocol and are not supported by this adapter.

Build with Java 11 or later and Maven:

```sh
mvn -f examples/oracle/jdbc/pom.xml package
```

The local artifact is `target/oracle-jdbc-0.1.0-SNAPSHOT.jar` under this directory.
Include it and PostgreSQL JDBC 42.7.8 on the classpath. No published Maven release
or installed application integration is implied by this build.

```java
Class.forName("org.bicdb.jdbc.oracle.OracleDriver");
String url = "jdbc:bicdb:oracle://127.0.0.1:5441/bicdb"
    + "?options=-c%20bicdb.sql_profile%3Doracle-preview-v1";
try (Connection connection = DriverManager.getConnection(url, user, password);
     PreparedStatement statement = connection.prepareStatement(
         "DECLARE item INTEGER := ?; BEGIN INSERT INTO entries VALUES(item,?); END;")) {
    statement.setInt(1, 7);
    statement.setString(2, "a value; with punctuation");
    statement.execute();
}
```

The URL scheme selects this adapter. The startup option selects the server SQL
profile; both are explicit. The adapter rejects other profiles and leaves ordinary
`jdbc:postgresql:` URLs to the PostgreSQL driver. Existing PostgreSQL connections
to the preview profile can also use `OracleConnections.wrap(connection)` or
`OracleBlocks.prepare(connection, sql)`.

Anonymous `BEGIN ... END` and `DECLARE ... BEGIN ... END` submissions retain their
internal semicolons, including bound declarations and assignments. Literal and
comment question marks remain literal. Oracle alternative/national quoting is
normalized before JDBC parameter parsing, preserving embedded apostrophes and
Unicode delimiters. Prepared blocks retain the standard setters, input metadata,
repeated execution and prepared batches. Ordinary transaction commands and SQL
use the underlying driver. Direct `Statement.execute`, `executeUpdate` and
`executeLargeUpdate` also preserve an anonymous block as one submission.

Prepared anonymous blocks require an enabled driver query cache large enough
to retain the SQL. Cache admission is checked before preparation; a disabled or
undersized cache fails with SQLSTATE `0A000` before execution. Direct statement
blocks do not require the query cache. The adapter marks procedural submissions
as procedural, preventing the insert-batch rewrite from rewriting them as INSERTs.

Anonymous callable OUT parameters, generated keys for anonymous blocks and
anonymous blocks passed to `Statement.addBatch(String)` are explicitly rejected
before execution. Use prepared batches for input-only block batches. This does
not qualify complete Oracle JDBC APIs, LOB locator semantics, connection recovery
or full application compatibility. Driver-specific unwrapping exposes the native
driver and can bypass the adapter. As with the underlying connection, callers
must serialize use of a connection; these wrappers do not add concurrent JDBC
connection semantics.

### Binary BLOB values

The adapter binds `setBlob(InputStream)` and its explicit-length overload through
the PostgreSQL driver's BYTEA stream setters. These calls retain the driver's
stream handling and do not allocate PostgreSQL OID large objects. Caller-owned
streams remain caller-owned. `setBlob(Blob)` and Blob-valued `setObject` overloads
copy binary values, with a 64 MiB buffering limit; use an InputStream for larger
values, subject to the underlying driver and server limits. Null BLOB bindings
remain different from non-null empty values.

`getBlob` and typed `getObject(..., Blob.class)` on BYTEA results return
materialized read-only snapshots, with one-based JDBC read positions and
`free()` support. They are not writable Oracle locators. Mutation methods fail
explicitly with `0A000`; they never accept a detached write that would be lost.
Read/write locator lifecycle, `Connection.createBlob()`, complete Oracle BLOB
metadata and streaming result reads require further implementation. Ordinary
PostgreSQL connections retain their existing driver behavior.

### Validation

The unit tests exercise lexical boundaries and the driver's public parser. The
executable `OracleAdapterProbe` under `src/test/java` additionally requires a
disposable database. It creates/removes a nonce table and checks actual input
metadata, repeated execution, Unicode, alternative quoting, direct statements,
prepared batches, rollback, explicit commits and fail-before-execution paths.
Do not run that probe against an application database.

The Linux runner (Python 3.11 or later and JDK 17) instead creates its own disposable databases:

```sh
python3 examples/oracle/jdbc/run_probe.py --build-server \
  --postgres-jar "$HOME/.m2/repository/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar" \
  --output /tmp/oracle-jdbc-results.json
```

It builds the adapter, runs the unit and generated-provider compilation tests, then exercises all three query modes
and the PostgreSQL-profile rejection control. Rust builds use one job with debug
symbols and incremental compilation disabled. Available memory is monitored
throughout owned build/probe commands; `--minimum-free-mib` sets the host headroom
floor. `--skip-build` reuses an existing adapter build, and `--server` accepts a
prebuilt `oracle_server` example. All disposable servers must stop cleanly.
Anonymous metadata uses extended Describe even in simple-query mode; the selected
mode is restored after both successful and failed metadata requests.

The JAR includes an OSGi manifest requiring the pinned PostgreSQL JDBC bundle.
Application-bundle installation and runtime qualification are separate from the
classpath checks. The adapter inherits the Apache-2.0 license of `examples/`;
its JAR carries that license, and the PostgreSQL dependency retains its own license.

The executable probe also checks field-origin schema, table and column lookup
through stock PostgreSQL JDBC catalog queries. It uses PostgreSQL's
`getBaseSchemaName` API for the origin schema and preserves the driver's standard
`getSchemaName` behavior. Server-side catalog handling remains a separately
versioned capability; passing the adapter's lexical unit tests alone does not
establish this interoperability.
