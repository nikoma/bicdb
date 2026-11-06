# Pinned upstream application test inventory

The [summary](summary.json) inventories **342 compiled classes**, including
**155 classes with 1,441 directly annotated JUnit test methods**, from the pinned
release-13 application test JAR. The [class list](classes.json) retains per-class
hashes, direct method counts and annotation kinds. The original test JAR was
checked against its expected digest before inspection; application code was not
modified or executed.

The five foundation classes have exactly **89 direct methods**: invoice 2,
customer invoice 9, shipment 5, persistent-object 53 and insert-SQL 20. This
cross-check agrees with the selected foundation batch's expected counts. Six
tool tests passed, including annotation/string separation, class-versus-method
disable state, parameterized/legacy annotations, malformed/duplicate metadata,
input alias protection and checksum rejection before tool invocation.

These are inventory counts, not 1,441 executed tests or a percentage of product
completion. Framework discovery, inherited/composed annotations, conditions,
parameter expansion and external dependencies require separate evidence. The
full application also needs business workflows and operational acceptance beyond
the foundation classes. See the [tool guide](../../oracle-application-test-inventory.md).
