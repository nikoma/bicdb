# Registered remote history and row WAL

This batch is source only. It has not been compiled or run. It builds on the
separate protected proposal and catalogue Installing producers at `7349adaaa`
and `fd4d0595d`; those revisions' CI results do not cover this batch.

The actual original Registered receiver retains a distinct image installation
owner for the selected range before history or row WAL. It keeps the original
host attempt, actual transferred A lease, source MF owner, original DB/open,
protected metadata owner, schema generation and namespace pins. Every bounded
step requires a fresh original target role/OWN and an authenticated source
History or Rows frame from that same MF. No remote fact becomes local
`SourceRangeImageLineage`, an installed writer, or a Serving grant.

`NativeRegisteredRemoteMutationScope` holds the actual original hook/admission
and native journal, validates the original Registered birth and A freeze, and
permits its own Controlled writer. It accepts actual copied-row visibility B
above zero. The original `NativeRegisteredRemoteAttemptScope` and NoDomain
checks retain their B0 contract. Neither missing native history nor B becomes
native range R=0. The immutable native base/prefix/pending preimage stays
mandatory; source-image invalidation generations may change during row apply.

The context independently validates the protected native proposal CURRENT and
archive, real committed page xid, actual UID/open/birth and the original
opaque catalogue Installing fence. The latter checks the actual in-memory
and durable catalogue candidate. The shared proposal reader takes no second
native-journal lock. These constraints are necessary, and none is a DTO-based
row authority.

The retained history owner chooses one generation before its first put.
Bounded authenticated steps verify every entry from zero through source R,
the exact H0 and H, command commitments, source receipt, wire chain, session
and cursor. Entries and progress are written in one native transaction. The
original attempted header bytes and xid remain retained on failure. A separate
bounded pass reads the actual stored spine before an opaque history proof is
created. Partial, reordered, forked, missing or unaudited history cannot admit
row WAL. This first Registered lane starts from actual empty target native/raw
obligations; it does not substitute for a populated Assigned target's history
compatibility proof.

For each row step, the original host retains the actual pending Transaction,
source step and proposed progress before native intent WAL. The native intent
binds A, MF, history generation/header, source cursor/hash, actual logical TxId
and exact mutation hash. Only a committed, exactly read-back intent can precede
logical row WAL. A private transaction grant binds those facts to the actual
Controlled claim, current original role/OWN/member/frame and a weak lexical
scope lifetime. Only the synchronous scope owns the strong lifetime reference;
it expires before its actual lock guards drop. The grant cannot extend them.

Commit validates that exact private grant before reusing the scope's admission
read guard. Ordinary commits retain their existing admission locks. This avoids
a recursive read waiting behind a queued writer. The private Registered commit
entry uses the actual inner `tx_log.write_durable` boundary to retain
`WalDurablePendingApply(sequence)` before crash cuts and physical/index apply.
The Backup race that proves the same sequence already durable follows this
boundary too. After actual materialization/visibility and committed-state
assignment, it retains `AppliedKnown(sequence)` before outbox finalizers.
Only the latter can complete row progress, subject to all fresh guards and
healthy recovery state. It defers automatic checkpointing until the
actual lexical hook/admission/journal and engine guards have dropped, avoiding
an admission-write deadlock. The normal public commit path is unchanged.

Physical row projection disables `enqueue_memory_jobs_on_commit` before row
buffering, and the private grant requires it to remain disabled. This matches
the existing imported-projection path in `commit_snapshot.rs`. Source
`97c54218d` did not suppress these jobs: a later non-fencing model/sidecar error
could leave `AppliedKnown` while retry skipped the unfinished job. This
follow-up closes that source-level gap; it has not been compiled or run.
Generated business-record audits, source change-capture operations and new
application broker publishes are suppressed on this private physical-copy
path. Local spatial-index maintenance audit events may still occur. Memory
index assets and their rebuilding/recovery remain separately unqualified;
disabling job generation does not prove those assets have moved or rebuilt.

Caller cancellation before row WAL leaves the same pending Transaction and
durable native intent for a fresh bounded request. Cancellation after known
durable row WAL leaves that exact sequence known; fresh same-step progress
completion must not execute another logical transaction. An unknown logical-WAL
outcome remains explicitly unavailable with its original TxId and payload
retained. This batch does not infer success from visible rows, retry with a
different TxId, or implement cold reconstruction or automatic resolution of
unknown logical-WAL outcomes.

The sticky local `native_registered_remote_image_v1` fence is excluded from
portable reader/schema flags. Its durable replacement is repeated before
protected writes, including visible-flag retries. Actual older-binary refusal
and injected failures at this new image fence have not been tested here.

Five genuine fifth-node source selectors under
`native_registered_fifth_remote_image_actual_` cover intended stored-history
and row import beyond B0, missing-audit refusal, pre-WAL caller cancellation
with the same Transaction, and post-applied-row cancellation with same-Tx
progress reconciliation. They use actual source frames while export progresses,
preserve original identities, raw catalogue and the other range's native point,
and compare the imported acknowledged record and content commitment. They have
not run. The fifth selector uses the existing process-global
`BICDB_TEST_FAIL_COMMIT_APPLY` seam in an isolated child process after actual
logical-WAL durability. It requires retained `WalDurablePendingApply`, a fenced
target, no applied-progress claim and no replacement transaction on fresh
retry. The other fault cuts cancel actual callers. These are not fsync-failure
or hardware power-loss experiments.

There is no finished target Image, independent full target scan/verification,
Ready, writer policy, strong-read readiness, routing activation, first-range
Serving/ACK, populated SAME-State second-range install, cold crash recovery,
automatic controller integration, physical rebalance or capacity qualification
in this batch. Those remain acceptance gates. Bounded batch sizes and finite
request deadlines do not establish a physical RTO or a bound on disk I/O.
The actual service mutex and native journal remain held across fsync here;
these tests do not establish independent progress for another range when I/O
stalls. That operational isolation gate remains open.

## Separate source-fixture correction after the failed concurrent cohort

The exact `7ac4f2672` five-selector run `43478288480` failed overall:
1 passed and 4 failed. Each failed control stopped while counting a local OLD
closure after that collector's finite metadata observation expired. Those four
row-fault controls were not tested in that cohort. Its original source, logs
and artifacts remain unchanged; switching a later fixture does not make that
run pass or demonstrate cluster availability.

The five later `TargetImageRows*` fixtures now use the existing production
remote OLD path. Fresh mutation requests close every actual OLD member, their
original deadlines expire, and a real retained donor independently requests
read-only authenticated observations from every OLD member before producing
its remote cut certificate. The donor must be an actual OLD member; this
fixture additionally selects a nonleader that is not the retiring source.
Every observed native floor, committed/pending command and ancestry constraint
remains mandatory in the production collector. Unknown closure refuses the
certificate. Before/after native records are retained for every OLD member;
read-only collection must preserve every projected field, including protected
checksums and native transaction identities. These JSON comparisons do not
independently hash the encoded page-value bytes. The original source acknowledgment ledger and target assertions
are unchanged.

This is a caller correction to exercise the genuine production remote lane.
It neither renews a local OLD observation nor changes the local OLD API's
issuance contract. Existing local OLD tests retain their original path.
No request, role or network timeout is increased. A requested five-second MQ
budget can be shortened by the actual listener's finite network deadline;
the shared TLS fixture currently configures 1,500 ms per exchange. The changed
fixtures require fresh runtime validation; source inspection is not a passing
fault test or physical qualification result.


## Fresh authority at the production remote collector's final boundary

The production donor now fetches all original authenticated bounded Observe
frames before acquiring the final collector's fresh OWN. The observation-phase
OWN is not renewed or used as final authority after expiry. Each original frame
retains its original request token; record and both finish boundaries require
that token to remain live. Expired/canceled frames cannot be reconstructed from
their reply DATA. The actual donor/source process and native closure are checked
before and after the final round.

The final fresh OWN must satisfy the existing full selected-plan/native-member
contract: exact selected range/configuration, session/nonce, old/new identities
and births, target, source schema and restore activation. Selected relocation
phase must equal the observation-phase phase, and term/index cannot regress.
Unrelated committed metadata changes are allowed by the existing plan validator;
there is no new equality check on the entire topology, topology generation or
read index. A phase/configuration change refuses the cut, without another Close
or native transaction.

Five new actual TLS fixture controls are source-only at this revision. They
intend to expire the observation-phase MQ while keeping original Observe callers
live, commit an unrelated range QPS change, commit a selected relocation phase
change, revoke the original donor's actual writer hook, and cancel the original
collector caller. They run the same real post-fetch collector path. Success
controls require a distinct fresh final context and all actual OLD frames;
negative controls require no certificate. All compare every actual native OLD
record before and after. They stop before source image publication or target
installation. None has been compiled or executed for this changed source yet.
