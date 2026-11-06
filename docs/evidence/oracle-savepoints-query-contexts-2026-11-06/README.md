# Oracle savepoint boundaries and statement query contexts

The Oracle preview now reports ORA-01086 / SQLSTATE `3B001` for an invalid
transport savepoint, including the idle boundary immediately after full rollback.
Oracle SAVEPOINT can establish an idle transaction. Missing-savepoint lookup
preserves existing writes and marks; it does not clear an unrelated unrecovered
failure. PostgreSQL keeps its explicit transaction-block requirement and abort
behavior. Parsed names preserve quoted case, spaces and escaped quotes, including
PostgreSQL's optional WORK/TRANSACTION rollback-to forms.

Oracle-preview LIKE now uses a profile-specific runtime helper so bound escape
values receive the same checks as literals. On the recorded AL32UTF8/BINARY
source, NULL/empty and multibyte ordinary escapes report ORA-01425; missing or
illegal escaped pattern characters report ORA-01424. There is no implicit escape.
PostgreSQL independently retains UNKNOWN for a NULL escape and accepts a single
Unicode escape character. The [source observations](../oracle-like-source-observations-2026-11-06/README.md)
include 22 expression cases, five filter/empty-input contexts and NLS/encoding
provenance. National patterns, LIKEC/LIKE2/LIKE4, arbitrary collations and general
expression-valued escapes remain separate implementation contracts.

DELETE candidate filtering now retains pure uncorrelated subqueries within the
statement, before writes or triggers. Tuple membership borrows cached rows.
Results never carry into a subsequent statement. Volatile calls, view bodies,
locking reads and modifying CTEs are declined by the memo safety check. Correlated
queries still execute with fresh outer values; their column lookup layout can be
reused, including at the top level. These rules do not depend on application or
table names. In the reproducible 64-row duplicate-cleanup shape, full scans fall
from 129 to 3 in both profiles while correct rows and pending/rollback visibility
are preserved. This is a plan control, not a production latency benchmark.

The [provenance record](summary.json) identifies backend source `20bfbc5`, exact
artifact hashes and each check's execution revision. Checks over unchanged SQL
code were reused after a recorded dependency diff; changed pgwire/native/JDBC
paths were rerun. Results: 739 SQL tests pass with four existing ignores; 261
native tools tests report 257 passes and four external-reference skips; all 183
protocol, 48 Oracle-profile and 44 authorization/JSON checks pass. The JDBC
adapter passes 25 unit tests and 250 real-driver checks across three Oracle
execution modes and default-profile rejection. The staged adapter's ten
production class files and driver services match the probed artifact.

Four nonce-table Oracle savepoint observations match and clean up successfully.
Their original controller subsequently failed a separate, incorrect NULL-LIKE
assumption. That interruption remains evidence; the independent LIKE follow-up
establishes the observed rules used here. Earlier native failures and resource
interruptions are retained rather than relabelled as passing runs.

The original application's JSON and client-setup retests are still required on
the merged backend and qualified paged baseline. The unchanged original test
assertions and bytecode have not been altered. This checkpoint is not complete
application acceptance, Oracle conformance, vendor certification, a published
release, a verified deployment or production qualification.
