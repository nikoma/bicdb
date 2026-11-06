# Original attachment retest after heap-row locking

The unchanged original `AttachmentProviderTest` class reports **two passes and
one error** on backend source `a97545fc25234c89a4361e0dbfe8c98a02cb3e94` with the
earlier JDBC adapter whose hash is recorded in [summary.json](summary.json).
This retest is not added to distinct application-invocation totals.

The former declared-primary-key rejection is no longer observed. The BLOB save
now reaches JDBC `setBlob(InputStream)`, where the earlier adapter delegates to
PostgreSQL's OID large-object API. Its lookup fails with `lo_creat is unknown`.
The test ultimately reports `DeleteError` during cleanup. This identifies the
next general adapter integration gap; it is not a passing attachment workflow.

Original test source/class hashes and four runtime class origins were checked.
The JVM exits 255, the database exits zero, and both processes stop. The failed
database and all original evidence are preserved. A new binary-value adapter
needs its own native and application qualification; it was not used in this run.
