# PostgreSQL JDBC type-identity dependency contract

Five controls use the actual PGJDBC 42.7.8 TypeInfoCache,
PgResultSetMetaData and PgParameterMetaData classes with a simulated catalog
transport. They establish how a future national-character adapter can recognize
registered type OIDs without inspecting ordinary text contents or relying on a
column label/type-name match.

The controls verify result and parameter JDBC families, an ordinary text column
with a national-looking label, a same-named user enum, preservation of ordinary
String[]/PGobject[] mappings, and preservation of registered OID families when
catalog metadata is cached. Fixture OIDs are test symbols, not reservations for
a production server ABI. Distinct registration marker classes avoid replacing
existing Java-array mappings.

This checkpoint includes 25 existing adapter unit checks and the five new
metadata controls. These 30 checks passed in a fresh 40-check development run;
ten additional tests for an unmerged encoding primitive are excluded from this
checkpoint. The adopted published SDK/test sources and POM match the executed
snapshot byte for byte. The ten existing production classes also match the
staged application SDK. The provenance identifies exact sources, dependency and
report hashes. No failed run or stale report is counted as passing evidence.

The earlier attempts exposed missing metadata-cache support in the test fixture
and an insufficient 128 MiB test heap for the unchanged 64 MiB BLOB boundary
check. Their logs/reports were preserved. The completed run used a fresh report
directory and a bounded 320 MiB test heap; no production limit or assertion was
weakened.

No production adapter code or live application staging changes here. The tests
do not create server types, register application connections or qualify live
PostgreSQL-protocol behavior. Remaining work includes the runtime/storage value,
server type contract, capability verification, qualified parameter binding,
result decoding and actual driver round trips. Full Oracle compatibility and
original application acceptance remain separate gates.

Reproduce the five focused controls:

```sh
cd examples/oracle/jdbc
mvn -Dtest=OracleNationalTypeRegistrationTest test
```

Run all 30 checkpoint unit checks with `mvn -DargLine=-Xmx320m test`.
See [the provenance](summary.json) for the precise scope of the adopted checks.
