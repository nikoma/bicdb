# Transaction outcome and write receipts

Native execution layers can retain observations of the transaction that produced
an execution value. `Transaction::observe_outcome()` returns a cloneable
`TransactionOutcomeObserver`. It contains no database pointer and grants no read
or write permission. Ordinary transactions allocate no observer state unless it
is requested; a post-decision failure also preserves its recovery-required state
for observers requested after the error.

An observer reports `Pending`, `Committing { sequence }`,
`Committed { sequence }`, `Aborted`, or `RecoveryRequired { sequence }`.
Committed means record application and the configured WAL durability boundary
completed. It is not a durability certificate independent of the database's sync
configuration. Errors after a decision may have persisted must not be treated as
aborts. A recovery-required observer keeps that outcome after its transaction is
dropped; it cannot resolve a decision by inspecting a subsequently reopened
database. Hosts must recover and reacquire execution state through their ordinary
recovery procedure.

`Transaction::observe_record_write(collection, record_id)` observes the most recent
buffered write for that record. Registration performs no mutation and acquires no
row lock. It fails when the transaction is not pending or the record has no
buffered write. Its receipt number is unique within that transaction and is never
reused after rollback. Use `same_transaction` on outcome observers to establish
identity: transaction numbers alone can coincide across databases.

A write receipt exposes its buffered prefix length and its first rollback
boundary. The boundary records both the retained write count and the highest
receipt number allocated before the rollback. A later write which reuses the
same log position therefore cannot resurrect an earlier receipt. Registration
of an earlier write after a savepoint does not make that write part of the
savepoint's discarded suffix. Direct write truncation, savepoint rollback and
procedural exception rollback use the same receipt invalidation path.

Inspect both the rollback boundary and transaction outcome. A missing rollback
boundary does not prove commitment. A rolled-back write can belong to a
transaction which later commits another branch; conversely, a receipt without a
savepoint boundary can belong to an aborted transaction. Superseding writes are
not savepoint rollback, and a receipt does not promise that its row remains the
latest committed value.

Receipt state follows the transaction when hosts move it between short-lived SQL
sessions. Shared state has no user callbacks. Observations may outlive transaction
and database objects without retaining those objects. Expired write handles are
pruned from transaction-local bookkeeping; no global receipt registry is used.

The tests in `crates/bicdb-core/tests/transaction_receipts.rs` cover nested rollback,
reused slots, late registration, abandonment, direct and buffered commit,
pre-decision uniqueness rejection/retry, reopen and observation across threads.
`commit_recovery_atomicity.rs` additionally checks post-decision failures and full
WAL recovery in embedded-memory and paged modes, including observers requested
after a buffered commit error.

These APIs support future execution-value provenance, including persistent LOB
snapshots. They do not themselves implement LOB identities, reads or mutation,
and do not change PostgreSQL transaction semantics or qualify an application.

On Linux, an isolated child also exercises a real kernel-enforced short WAL
append. The observer remains recovery-required through transaction/database
drop, and the durable WAL watermark does not advance past the failed append.

## Transaction-local write annotations

`annotate_record_write(collection, id, namespace, key, value)` associates an
opaque integer tag with an actual buffered record write. `record_write_tag`
returns the newest surviving tag in the pending transaction. Namespace/key are
128-bit integers and the value is a 64-bit integer; there are no caller callbacks,
application payloads or database pointers in this state. Tags grant no mutation
authority and are not WAL data or durable commit evidence.

Nested rollback follows the observed write prefix; late registration for an
older retained write survives. Repeated annotation of the same key/write replaces
the prior tag. Invalidated versions are pruned and cannot reappear when write
slots are reused. Terminal outcomes clear the tag map, and separate transactions
never share it. BLOB mutation uses this to distinguish pending deletion and
replacement; current owner, SQL authorization and row-lock checks remain separate.
