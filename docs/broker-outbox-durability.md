# Transactional broker retention and publication

Transactional messages use the transaction log's durable commit decision. The
broker event segment is an outbox projection: publication may be completed by
recovery, but a rollback must never publish and recovery must never resurrect a
message that retention already removed.

## Publication and checkpoints

The transaction log is synced before row materialization. After rows become
visible, record-audit and broker outboxes are published before the commit releases
its admission guard. A checkpoint therefore cannot discard their redo while a
caller pauses between the core commit and its finalizer calls. Those public
finalizer calls remain idempotent; they do no extra work after normal completion.

A failed broker publication fences the database at `broker_outbox.publish` while
the durable business transaction remains committed. Checkpoints, later broker
publishes and broker consumption refuse the failed handle. Verified reopen must
finish every retained committed broker publication before returning a usable
handle. An interrupted reopen is safe to repeat.

This moves existing work inside admission; it adds no commit-path fsync and does
not enable experimental commit modes. SQL and broker operations check handle
health. EventStream snapshots and subscription delivery also report a recovery
fence through checked results; see [the Rust API migration](event-read-fencing.md).

## Retention receipts

A trim atomically rewrites its queue's retained messages and control events. Its
existing durable `BrokerQueueTrimmed` marker now also holds UUID/original-sequence
receipts for transactional publications removed by that trim. Recovery recognizes
those receipts before appending any message, even while a lagging standby keeps
the original committed decision in the transaction log.

A transaction may deduplicate through an idempotency key onto another message's
UUID. The broker tracks that alias until the original message is trimmed, then
persists the alias receipt in the same trim marker. Before trimming, recovery can
reconstruct the alias from the retained original message and transaction log.
This also works when a transactional publication deduplicates onto a message
published directly outside a transaction.

Receipts identify transaction-specific UUIDs, rather than keeping idempotency keys
forever. A new transaction can legitimately reuse a key after retention. If it
dies before publication, recovery publishes that new UUID rather than confusing
it with an earlier transaction's receipt. A commit-sequence watermark alone is
insufficient because concurrent commits can publish in a different order.

There are at most 65,536 durable receipts across all queues, and a separate bound
of 65,536 recovery-only legacy identities/idempotency aliases. Trimming refuses
before discarding identity evidence when a bound is reached. It leaves retained
messages intact. Ordinary nontransactional messages do not consume receipt space.
A durable full transaction-log cut clears obsolete evidence; verified reopen can
also prune receipts absent from its complete recovered transaction log. Partial
log cuts conservatively retain receipts until verified reopen. Receipt pruning
may rewrite the event segment at checkpoint/open, outside the commit hot path.
If optional cleanup fails, either the old or newly pruned complete segment is
safe: removed receipts were proved no longer replayable before rewriting.
Housekeeping can retry without licensing republication.

## Older stores

Retained messages from older binaries are identified by complete transaction-log
replay before trimming is exposed. Their existing event metadata is preserved,
including any provenance; recovery-only identity protection survives subsequent
queue rewrites.

An old trim marker contains no proof about the UUIDs it erased. If a retained
committed transaction refers to a missing publication behind such a marker, the
store refuses ambiguous recovery. It does not guess whether to republish an event
that a consumer may already have processed. Independent publication evidence or
a verified restore is required to resolve that old state. Unambiguous legacy
markers can be upgraded during verified reopen.

The new fields use the existing event/frame representation. Older binaries do
not implement the new receipt invariant and must not be used to recover or trim
stores relying on this guarantee. No production deployment or format-wide
migration is implied by merging the fix.

## Validation

`tests/broker_outbox_retention.rs` reproduces two defects on the previous main:
trimmed committed messages returning after reopen in both storage modes, and a
checkpoint between durable core commit and caller finalization retaining the row
while losing both its broker and record-audit events.

The regressions cover both modes, repeated reopens, idempotency aliases and later
key reuse, write/short-write/fsync failures with fencing, and a child-process crash
matrix. The matrix checks an independently fsynced acknowledgment ledger, every
acknowledged row, pending message identity, retention state, interrupted
republication and two successive verified reopens. It uses process aborts plus
page-WAL volatile-tail emulation; it is not a hard-VM-reset qualification.

Unit tests preserve the original recovery assertions of two existing crash-window
fixtures, using an explicit `cfg(test)` crash-emulation boundary now that a normal
core commit completes publication before returning. Separate unit regressions
cover ambiguous legacy recovery, legacy identity protection across another
queue's rewrite, receipt-cap refusal and full-log-cut reclamation.
