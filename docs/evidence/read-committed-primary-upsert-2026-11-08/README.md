# READ COMMITTED primary-key upserts

An explicit READ COMMITTED transaction can start its statement while a rival
INSERT owns the primary-key row. Before this fix, it waited, then chose INSERT
using its old snapshot: `RETURNING val` gave10 instead of updating20+10 to30.
The failing two-storage-mode protocol reproduction is source13266010, remote
`/root/bicdb-durability-s3/read-committed-upsert-repro.log`.

The accompanying PostgreSQL16.13 reference observed the waiter blocked on
`Lock/transactionid`, then returning30 after the rival committed. The repair
uses the existing READ COMMITTED row reservation and latest-version check before
choosing the primary-key conflict action. It pins that version, retains other
relations' original statement snapshot, applies the same native read policy and
SQL row policies/triggers/constraints, and writes with the checked row version.

At5799e211 the complete concurrent_upsert target passed8 tests; RLS index
candidates18, RLS write paths5 and type durability7 also passed. The new fixture
covers winner commit and rollback in both storage modes, preserving an earlier
write and savepoint in the waiting transaction. It checks the returned value,
commits, and verifies both rows. Remote log: `read-committed-upsert-all.log`.
The earlier simpler primary-key fixture passed2/2 at93f6c2fa.

This is deliberately scoped to primary-key arbiters whose SQL row identity is
already exact. Secondary-unique and typed-identity arbiters retain their previous
behavior; the existing secondary-unique after-snapshot regression still expects
a retryable40001 for explicit DO UPDATE. Pending secondary winners need
transaction-lifetime unique-key reservations across all relevant writers, a
separate semantic/performance change. This evidence does not close that gap or
claim all PostgreSQL ON CONFLICT concurrency semantics. Fixed-snapshot isolation
keeps its existing path. No application acceptance result is claimed here.
