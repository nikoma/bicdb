# Oracle JDBC national-character code-unit source control

Eight read-only cases on Oracle 23.26.3 with Oracle JDBC 23.26.0.0.0 compare
DUMP(...,1016), JDBC type metadata, getString and getNString. The probe records
Java UTF-16 units as hexadecimal arrays so lone surrogates survive JSON output.
It uses the original Oracle provider's defaultNChar and V8Compatible properties.
No original application objects were modified. The reference container was
restored to stopped after observation.

Both getters preserve the lone high-surrogate D83D produced by casting U+1F600
to NVARCHAR2(1) or NCHAR(1). Width two preserves D83D DE00. This establishes a
source JDBC behavior distinct from the earlier Python thin-driver decoding
failure; replacing the lone unit with U+FFFD would lose observed information.

The remaining cases record ASCII truncation to 'ab', NCHAR space padding, and
numeric overflow ORA-25137 with JDBC SQLState 99999. Metadata identifies
NVARCHAR2 as JDBC type -9 and NCHAR as -15. These are observations of this
server/driver/property combination, not every Oracle client or configuration.

The [raw report](observations.json) and [hashes and scope](summary.json) are
retained. Eight local serialization controls independently check paired units,
unpaired high/low units, padding and JSON escaping without using a database.

This does not qualify BicDB's national-character runtime or the selected
PostgreSQL-driver codec. Those still need a lossless typed value/storage
representation, versioned metadata, operator/conversion rules, actual JDBC
round trips and independent default-PostgreSQL regressions. Original application
acceptance and the complete Oracle programme remain separate gates.

To reproduce using an existing authorized Oracle reference endpoint, provide
BICDB_ORACLE_REFERENCE_JDBC_URL, BICDB_ORACLE_REFERENCE_USER and
BICDB_ORACLE_REFERENCE_PASSWORD through your credential environment. Supply your
own Oracle JDBC JAR; the repository does not redistribute that dependency.

```sh
mkdir -p /tmp/bicdb-national-jdbc-reference
javac -encoding UTF-8 -d /tmp/bicdb-national-jdbc-reference \
  examples/oracle/reference/NationalCastJdbcReference.java \
  examples/oracle/reference/NationalCastJdbcReferenceSerializationTest.java
java -cp /tmp/bicdb-national-jdbc-reference \
  NationalCastJdbcReferenceSerializationTest
java -Xmx256m -Doracle.jdbc.defaultNChar=true -Doracle.jdbc.V8Compatible=true \
  -cp /tmp/bicdb-national-jdbc-reference:/path/to/ojdbc17.jar \
  NationalCastJdbcReference > /private/national-jdbc-observations.json
```

The observer explicitly starts a read-only transaction, rolls it back, closes the
connection and distinguishes the expected numeric error from other SQL errors.
A failed invocation must not be reported as a completed eight-case observation.
