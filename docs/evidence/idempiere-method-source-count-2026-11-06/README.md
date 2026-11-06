# Original static parameter provider count

The pinned release-13 JAR contains 1,441 directly annotated test methods across
155 classes. One is `EmailFormatValidatorTest.test`, a parameterized test with
one explicit `@MethodSource("data")`. The original static provider returned a
collection of **24 argument arrays**, each matching the test method's arity.
The helper checked that the loaded class originated from the expected JAR;
its digest matches the earlier [compiled inventory](../idempiere-upstream-test-inventory-2026-11-05/README.md).

This expands the current plan to **1,464 expected invocations**, including
1,311 beyond the selected 89 foundation and 64 business tests. These remain
planned counts. Invoking the provider did not run the test assertions, discover
JUnit's execution tree, evaluate conditions or qualify application behavior.
Original assertions and bytecode were unchanged. See [summary.json](summary.json).

The [helper](../../../examples/oracle/idempiere/OriginalMethodSourceCount.java)
accepts a class, method and expected JAR. Run it only for trusted application code:
it initializes the selected class and invokes its provider. Its supported case
is one explicit static no-argument method source returning a nonempty collection
of argument arrays. Other provider forms need separate qualification.

```sh
java -Xms16m -Xmx96m --class-path "$TEST_PROVIDER_CLASSPATH" \
  examples/oracle/idempiere/OriginalMethodSourceCount.java \
  org.idempiere.test.base.EmailFormatValidatorTest test "$TEST_JAR"
```

The classpath must include the original test JAR, JUnit Jupiter API/params 5.9.1,
JUnit Platform Commons 1.9.1, OpenTest4J 1.2.0 and API Guardian 1.1.2. The original
provider returned 24; a deliberately incorrect expected JAR was rejected.
No Oracle or PostgreSQL server was used, and this is not database qualification.
