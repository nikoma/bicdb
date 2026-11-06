# Original attachment tests with JDBC binary-value bindings

The unchanged original iDempiere `AttachmentProviderTest` reports **three
passes, zero failures, zero errors and zero skips** on the original Oracle
schema clone over PostgreSQL JDBC. This includes `testDBLargeObjectProvider`,
which previously failed first at heap-row locking and then at JDBC OID large
object creation. The general engine locking and binary-value adapter changes
resolve those observed failures without changing the original test assertions,
source, class bytes or schema declarations.

The [provenance record](summary.json) identifies backend source `a97545f` and
adapter source `3db8c5d`, their exact artifact hashes, original source/class/JAR
hashes, XML hash and four verified runtime class origins. JVM and database both
exited zero; their processes were confirmed stopped. The closed passed test
database was removed, reclaiming 575,684,608 allocated bytes. Logs, original XML,
provenance, the protected pristine baseline and earlier failed databases remain.

These three invocations are a retest and are not added to the 402 distinct
original invocations already reported across different backend revisions.
Earlier failed results remain historical evidence. This result establishes the
selected attachment tests, not arbitrary attachment editing or writable JDBC
persistent locators. The subsequently qualified `createBlob()` adapter was not
used in this application run. The vendor-invoice ordering failure, remaining
application tests, paged deployment, UI workflows and the full Oracle programme
remain open. There is no full application or production qualification.
