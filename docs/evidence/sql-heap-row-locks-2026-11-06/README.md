# Physical row locking without declared primary keys

Locking reads now carry each heap row's stored record identity through
materialization, filtering, joins and ordering. They use that identity for the
ordinary transaction lock instead of reconstructing a key from visible values.
Identical visible rows remain independently lockable. The execution-local
identity stays out of wildcard output and catalogs. Indexed join shortcuts that
do not preserve this identity use the ordinary materialized path instead.

Eight native regressions, independently executed for PostgreSQL and Oracle
preview, cover duplicate tuples, NOWAIT/SKIP LOCKED, pending inserts/updates,
savepoint lock release, restart, join targets and outer-join restrictions.
They also check SELECT/UPDATE privilege requirements, UPDATE row policies,
revocation and denial of policy changes by non-owners. The Oracle OF cases name
columns; PostgreSQL OF cases name relations. Existing row-limit syntax selects
one duplicate tuple in the contention fixture; this does not qualify additional
Oracle row-limiting grammar.

Oracle preview also preserves GRANT/REVOKE command grammar without reserving
those words in unrelated expressions. Native policy creation and row-security
ALTER TABLE operations can use the existing administrative boundary when the
target relation is explicitly quoted. Their executor ownership checks remain
authoritative. This is native operational metadata, not Oracle VPD syntax or a
complete Oracle privileges/role-naming implementation.

All checks below executed source `a97545fc25234c89a4361e0dbfe8c98a02cb3e94`:

| Validation | Result |
| --- | --- |
| Native heap-lock contracts | 8 passed |
| SQL library | 734 passed, four existing ignores |
| Protocol | 183 passed |
| Oracle wire suite | 48 passed |
| Migration/native tools | 223 passed, four external references skipped |
| Authorization/JSON integration | 44 passed |
| CLI build and storage-copy command help | Passed |

[summary.json](summary.json) records exact counts, binary hashes and resource
observations. The native heap fixtures use embedded-memory storage. A paged
application baseline, source-Oracle comparison, full application contention,
deadlock/fairness parity and production behavior remain separate gates.

The qualified source export contains 15 tables without declared primary keys
out of 915 tables, including attachment, image and archive BLOB storage tables.
This implementation is general SQL behavior; it contains no application-table
special cases. The original attachment test class still needs a fresh run on
the delivered binary. Earlier failed application evidence, the vendor-invoice
failure, remaining original classes and the full Oracle programme remain open.
