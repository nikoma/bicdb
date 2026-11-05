# BLOB migration resume after a clean server restart

The public importer regression fixture now closes its connection, stops the
owned server, reopens the same native database on the same endpoint, and resumes
using the existing journal. Before resume can rewrite a column or rebuild owner
metadata, a locked persistent APPEND succeeds against the reopened BLOB. The
fixture reads the extended bytes, rolls the transaction back, and verifies the
original bytes. Resume then preserves the original complete table fingerprint.

The complete Python importer suite passes 41 tests with zero skips using
psycopg's C implementation and the recorded built server. The fixture also keeps
its NULL, empty binary, unrelated BYTEA, nullable/non-null timestamptz, quoted
schema, source-journal mismatch and changed-target refusal checks. Both server
instances must exit successfully; class cleanup removes the owned database,
including on failures.

This is clean shutdown/reopen evidence. The example server fixture does not
establish power-loss durability, interrupted native commit recovery, distributed
writer fencing, production qualification or full application acceptance. The
[migration guide](../../oracle-blob-migration.md) and
[programme](../../oracle-compatibility-programme.md) retain those separate gates.
