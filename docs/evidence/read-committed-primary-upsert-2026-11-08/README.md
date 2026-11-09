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

At22dc7099 the final unsigned-only guard passed both signed authority tests
(two modes, all four action cases and the protected invisible-winner schedule),
and all8 concurrent protocol tests. Signed DO UPDATE on a fresh row accepts its
INSERT-only grant; an actual update still rejects it. Signed DO NOTHING accepts
both fresh and conflicting cases. The protected race returns10, rejects COMMIT
with TransactionConflict, and leaves winner20 intact, explicitly retaining that
open repair. Log: `read-committed-grant-scoped-fixed.log`. The initial grant
review failure is `read-committed-grant-review-repro.log` at2f485893.

Composite constraint review found that reversed conflict target columns bypassed
ordered arbiter inference. At03e7d654 the committed asymmetric control already
passed, but both racing modes returned(1,2,10) instead of(1,2,30). A value-order-only
patch at7c66e649 remained red; the full repair confines unordered column-set
inference to ON CONFLICT and keeps the chosen arbiter's canonical encoding
order. Both primary row-ID shortcuts derive their values from authoritative PK
column order. At427099dc the static composite test1, signed authority tests2,
and complete concurrent protocol target10 all passed,0fail. Log:
`read-committed-composite-complete.log`; preserved red logs
`read-committed-composite-review-repro.log` and `read-committed-composite-scoped.log`.
The separate PostgreSQL asymmetric reference is stored alongside the original.
Final broad and CI results must be scoped after this grouped correction.
