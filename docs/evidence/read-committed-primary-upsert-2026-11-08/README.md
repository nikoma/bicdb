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

This is deliberately scoped to unsigned primary-key arbiters whose SQL row
identity is already exact. Signed mutation-grant sessions retain their previous
action-specific authorization and explicit retry requirement; their protected
write helper does not yet carry the refreshed row version. Secondary-unique and
typed-identity arbiters also retain their previous behavior; the existing secondary-unique after-snapshot regression still expects
a retryable40001 for explicit DO UPDATE. Pending secondary winners need
transaction-lifetime unique-key reservations across all relevant writers, a
separate semantic/performance change. This evidence does not close that gap or
claim all PostgreSQL ON CONFLICT concurrency semantics. Fixed-snapshot isolation
keeps its existing path. No application acceptance result is claimed here.

Independent review found the initial helper validated UPDATE authority for a
possible fresh INSERT. Source2f485893 reproduces denial of an INSERT-only signed
grant (`cannot lock for Update`); the final scope guard excludes signed sessions
from this optimization rather than changing their native authorization contract.
New tests cover fresh/conflicting DO UPDATE and DO NOTHING under INSERT-only
authority in both modes, plus an exact signed snapshot-invisible winner schedule
that must retain its retry requirement and never overwrite the committed winner.
See [the remaining work ledger](../../read-committed-upsert-work.md).
