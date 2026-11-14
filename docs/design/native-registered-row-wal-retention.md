# Registered row WAL retention until protected progress

This source-only batch starts from `10d17149886e629c535b6b5523a89f87ae83ba81`.
No local Cargo build or hosted test has run on this batch. Test-only precursor
`21047396aab8068c3f7b624bdec39f75785ef7d0` adds the required assertions; it is a
source-predicted regression, not an independently executed red result.

The native Transaction path auto-pins its logical WAL before commit. The private
Registered row commit consumes that Transaction; successful finalization clears
its pin before the caller publishes the protected RowsHeader. A canceled caller
can therefore retain `AppliedKnown` and the native intent while owning no WAL
lease. The existing RowPending also preserves Unknown or durable-pending-apply
outcomes, but previously owned no independent retention lease.

RowPending now owns one actual NativeWalRetentionPin acquired before its first
native row-intent write. The same actual pending Transaction receives a clone;
clones share one bounded lease. Commit finalization or Transaction consumption
cannot release the pending owner's lease. The original pending step retains it
through errors, Unknown, durable-pending-apply and post-applied cancellation.
It releases only after exact committed RowsHeader readback and all existing
fresh context postchecks permit removing that pending step. No field or status
flag grants mutation, progress, read readiness or Serving.

The existing post-applied caller-cancellation regression now checks one actual
active lease, performs an explicit ServerPaged checkpoint after the original
scope drops, and requires byte-identical nonempty logical WAL. A fresh exact-step
retry must complete protected progress without another logical transaction or
changed B. The completed step must release its lease; another explicit checkpoint
can then cut that WAL. Pre-WAL cancellation and inner durable/apply failure also
require the original retained lease and unchanged transaction/intent behavior.
All original caller budgets and assertions remain. These controls are unexecuted
at this freeze; they must pass on the composed source before any validation claim.

The host's deferred `auto_checkpoint_if_due` call is not the ServerPaged trigger:
that function runs only for EmbeddedMemory. The relevant native paths are explicit
`checkpoint_for_resume`, close, other maintenance and startup recovery. Native
page transaction status remains valid below the frozen watermark except recorded
abort exceptions; disappearance from the resident `.transactions` table is not a
GC defect and does not equate a page XID to logical TxId or commit sequence B.

This fixes only SAME-OPEN retention. RAM pins vanish after a crash. Generic
non-replication paged open currently replays logical WAL, republishes outboxes and
cuts the recovered log before a Registered-image row-intent reconciliation pass.
A durable intent alone does not retain that evidence or recover a live grant.
Cold support still needs a bounded durable outstanding-intent retention/admission
barrier before every cut, especially open, plus an actual exact TxId / incarnation /
mutation-hash / sequence resolver or committed protected surrogate. Existing
CommitResolution requires a known incarnation and sequence, not merely TxId/hash;
RowIntent stores no logical sequence. Ambiguity must preserve CLOSED state and
original evidence; no new transaction, visible row or maximum B can resolve it.
No cold reconciliation, logical-WAL replay producer, native final Image, Ready,
Serving, physical fault/rebalance or production qualification is claimed here.
