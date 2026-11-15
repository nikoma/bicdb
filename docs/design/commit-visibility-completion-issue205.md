# Commit visibility completion: issue 205 source proposal

This unversioned proposal starts at `1fcfdc3adf7e38422173e072e6fec4ad56e48188`.
It is **not compiled, executed, merged, or qualified**. The baseline A/B schedule
in [issue 205](https://github.com/nikoma/bicdb/issues/205) is a source-derived
counterexample to the old completion contract, not an executed loss report.
The accompanying JSON preserves the earlier design and consumer-map distinctions.

## Ordinary completion and ownership

`db/commit_snapshot.rs::wait_until_visible` retains its original position and
five-second bound. It now returns whether the actual contiguous applied prefix
covered the selected sequence. A healthy expiration does not fence the database.
The original result remains sticky: later convergence does not retroactively
acknowledge a call that already exhausted its window.

`db/spatial_paged.rs::commit_transaction_inner` still performs the original WAL,
apply, row-lock release, change-capture, audit and broker publication work. It
records actual local completion without claiming successful ordinary visibility
when the wait expires. The outer ordinary admission finalizer requires both a
non-exhausted original wait and the actual contiguous watermark at or above the
selected sequence. Otherwise it retains the actual transaction/sequence/open in
`CommitOutcomeUnknown` and preserves required memory-job obligations. No original
business transaction is replayed and no permanent WAL-pin map is introduced.

`Transaction::commit_with_sequence_in_place(&mut self)` retains the original
owner through the exact consuming API's durability, admission, broker, memory-job
and checkpoint order. The consuming API delegates to it. `TxState::Committed`
is inspected on that same owner before its original drop boundary; neither an
Unknown error nor an assigned sequence proves a commit or abort. In particular,
Pending after a durable error is not an abort proof.

`SqlSession::commit` uses this retained owner. For actual Committed plus an error,
it detaches the committed DDL entries from rollback state, clears savepoints and
commits GUC state **before** fallible committed catalog/purge cleanup. The original
typed error is returned; a secondary cleanup error remains an attached notice.
It advances no successful independent/session floor. The statement wrappers in
`session.rs::with_statement_transaction` and the routine savepoint wrapper cannot
recover the detached entries as ordinary undo. A genuine SQL control uses a valid
Sync model whose output has a real dimension conflict only after the source
transaction commits; it checks DDL, full selected values, GUC state, the original
error and a later rollback request. That is generic Committed-plus-error branch
coverage, not a timed visibility-Unknown SQL control. No SQL-dependency test-hook
feature is added; timed-Unknown SQL and PG wire cleanup integration remain
unexecuted/missing.

## Public buffered and other SQL callers

| Caller | Completion boundary |
| --- | --- |
| Core consuming and retained-owner APIs | Original inline durable delivery, ordinary finalizer and remaining obligations. |
| `db/access_mutation.rs::commit_buffered_transaction` | Returns an incomplete sequence handoff; its public documentation requires WAL delivery and the ordinary admission finalizer before ACK. |
| PGwire `lib.rs` autocommit and `sql_parsing.rs` explicit COMMIT | Both call the mandatory finalizers. Successful session floors advance only when durability, admission and memory finalization succeed. |
| SQL `session.rs::commit` | Same-owner actual-state cleanup described above. |
| SQL `recycle_bin_journal.rs` consuming calls (baseline lines 105, 352), `rename_journal.rs` (213, 239), and `txn_update.rs` independent commit | Existing consuming/error propagation remains; no general new uncertain-DDL reconciliation protocol is claimed. |

Other Core buffered call sites are existing test/helper paths; a raw returned
sequence is not upgraded to a public success receipt. Public callers cannot
construct the private native completion profile.

PGwire extracts its committed DDL log and clears transaction state before
propagating finalizer errors. The extracted vector has no undo-on-Drop behavior.
For actual Committed plus successful WAL delivery and an admission/memory error,
explicit COMMIT first attempts the unchanged committed catalog-scope cleanup on a
nonblocking recursive shared DB borrow. It uses the actual durable decision and
existing header-absence/context/token/child checks, not the error tag or a new
owner grant. Only the outer DB-borrow attempt is nonblocking: the unchanged
catalog helper retains its existing internal locks and storage I/O, so complete
cleanup under stalled storage is not proven bounded. It acquires no new exclusive
DB lease or wait window. A secondary cleanup failure is a notice attached to the
exact original error. The successful
path keeps its original transaction-drop/exclusive purge order. Purge requiring
mutable DB access remains deferred on an error. If the outer borrow attempt is busy or
fails strict checks, a context-bearing logical reservation can remain until
existing cleanup/reopen; no retained mutex, successful cleanup, or new cold DDL
reconciliation is claimed for that case.

## Native LOCAL completion is a separate typed boundary

Native Strong point reads intentionally use a genuine opaque target-local
materialization floor. Ordinary SQL's global-prefix rule must not turn an
unrelated range's gap into a universal native apply dependency.

The private `native_wal_apply::NativeWalApplyPreparation` is produced before
effects only by three certified/origin/historical apply paths and the two private
Registered/Joined row-import entrypoints. It borrows the original pending command
plus actual mutation authority, or shares the **same allocation** of an original
sealed RowGrant. There is no public constructor, serde path, arbitrary numeric
floor, generic tag, imported-projection flag, or bypass boolean that selects it.
Sharing a RowGrant does not keep its weak lexical permission alive.

Pre-WAL checks still bind the same DB/transaction, open, mutation hash, original
armed scope allocation/lease and protected pending/native state, or the unchanged
full RowGrant validator. At completion the actual original AppliedKnown state or
same-Tx synchronized WAL receipt is mandatory. The existing native receipt minter
and validator retain their local contract. Later independent range lineage,
certificate/local-B, quorum and command-completion checks remain mandatory.
This local receipt is not RangeQQ, Serving, Strong permission or a native APP ACK.

Even on an actual typed native transaction, the **public ordinary finalizer**
still checks the global prefix. A default false wait-exhausted flag cannot escape
that guard. The native component probes this wrong consumer only after actual
typed apply validation, then continues through the original private consumer.

The ordinary native-admitted lane can return visibility uncertainty before its
`local_applied` callback. Its transaction pin drops normally. Existing consensus
CQ entries retain the full original command and checksum until Applied/Aborted;
the recovery release clears volatile admission, not that durable command. Fresh
historical recovery validates the protected prefix/pending command, obtains real
current authority and replays the **same certified command**, with imported
projection semantics, before minting a new actual local receipt. This can require
fresh quorum, runtime and freeze admission and remain unavailable. It is not an
old receipt reconstruction, automatic cold repair, or a new remote DECISION
attempt. No unbounded pin-until-close repair is asserted.

## Memory-job readiness, concurrency and uncertainty

New source jobs carry `source_commit_seq` DATA from their actual committed source
transaction. Existing legacy jobs and genuinely visible scan producers retain
None. Both worker paths check the actual contiguous prefix before model or record
reads; a deferred job is neither Failed nor Indexed nor processed.
Existing same-body/source-hash dedup and `get`/latest-snapshot behavior are not
redesigned. An older eligible job can observe a later physically applied record;
this proposal does not infer an incorrect successful ACK from that baseline
source concern. Its derived write still requires ordinary global completion.

Same-open workers claim actual job IDs with bounded lexical ownership (at most
4096 concurrent claims). The brief claim mutex is released before embedding,
transactions and status I/O. Other IDs remain independently eligible. After a
claim, the worker rereads the same current job; an older captured Pending cohort
cannot execute a job already retained as OutcomeUnknown. Claims live through the
real write result and status publication, and release on return or unwind.
Reports expose selected `deferred` and `claim_deferred` counts, including the cap.
Asynchronous deferred work remains Pending. A Sync caller requires every selected
obligation actually Indexed; in-flight/cap deferral cannot become source ACK.
Only its selected actual derived uncertainty is returned internally; the source
transaction finalizer retains its own original identity externally.

A derived write returning real CommitOutcomeUnknown is retained as strict
OutcomeUnknown with its observed transaction/sequence/open/error. Normal workers
do not retry it, infer success from equal payloads, or emit a fresh business audit
as reconciliation. Concurrent ordinary updates cannot erase the marker. A status
publication failure is attributed to the current update's actual attempt, never
an unrelated older unresolved job elsewhere in the store. All observed outcomes
remain retained even when a multi-collection publication failure returns one
representative current attempt in its typed API error; that error does not
enumerate the complete retained set.

Before publishing new Unknown sidecar semantics, every jobs writer holds the same
jobs snapshot through an unconditional durable root-format reader barrier and
status publication. The sticky `memory_job_outcome_unknown_v1` bit is node-local,
not a portable image capability, and forces durable later format replacements.
Older normal opens must refuse at the early format gate before page/log recovery;
late JSON status rejection alone would be insufficient. The actual atomic barrier
has the named `memory_jobs.outcome_unknown.format` fault site. Real barrier/status
I/O failure fences through the existing durability mechanism and retains the RAM
outcome. The healthy five-second timeout itself never causes that fence.

**Open cold gap:** derived WAL precedes Unknown metadata. A crash or pre-rename
barrier failure can leave the old on-disk Pending job, despite an observed RAM
Unknown. Record WAL does not reconstruct that job attempt on reopen. The new
reader bit protects published new status semantics; it does not implement atomic
WAL/job handoff, cold no-replay, exactly-once jobs, or same-original derived outcome
reconciliation. A before-rename failure control must not claim such recovery.

## Validation scope

The source controls use genuine ServerPaged/fsync transactions, original WAL and
apply pause points, full row/index checks and unrelated fresh readers. They cover
long and short A/B gaps, Pending Sync work, retained actual owner state, real
derived-write uncertainty and ordinary reopen of a successfully persisted Unknown.
The INITIAL RF3 component uses actual coordinator/transport/native follower apply
and tests local completion while another range's global gap remains. It does not
establish current MQ, an enrolled SDK host, TLS, Strong or physical HA qualification.
The reader check simulates a current validator lacking the new capability; it is
not execution of an older binary. Additional real atomic-barrier failure and stale-worker-cohort controls are
source definitions, not results. The per-job claim-cap/claimed-Sync-specific
negative is not executed or defined by these controls; the strict selected-job
completion predicate is source-reviewed only. No local Rust execution has occurred.

All earlier design notes and frozen PR203/204 evidence remain unchanged. This
proposal assigns no source version, built binary, release, deployment or complete
operator/controller/cold-recovery status.
