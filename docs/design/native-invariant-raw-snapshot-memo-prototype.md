# Native invariant raw snapshot memo: source prototype

Base source505: `e25b3ba194c6f3ad73e2f393b68b175fbf1edd92`.
No build, runtime test, profile, performance result, release or deployment.

The prototype shares only full raw relation materializations plus the original
immutable transaction upsert/delete overlay. `Arc<[Record]>` avoids repeated
full scans and nested record-body clones; nested binding maps borrow records.
Predicates, tenant/role/publication policies, principal checks, owner resolution,
root subjects, callback/SQL behavior and expression ordering remain evaluated.
All subject rows and all original dependency rows remain present. Cache keys
use exact relation spelling, preserving the original overlay comparison.

One production validator entry exists in Core: `commit_transaction_inner` calls
`validate_native_commit_validators` after its admission guards. All eight
`NativeCommitValidator` variants are fixed native checks, not arbitrary mutation
callbacks. The Transaction is borrowed immutably during validation. Each of the
sixteen expression invocations receives the same lexical pass object. It dies
on success/error before commit application and is never retained on a DB/tx.

**Important proof limitation found during review:** the invariant-only global
mutex excludes other invariant transactions, but ordinary transactions can use
the concurrent path. It is not by itself proof of a globally stable snapshot.
The successor shares rows only while borrowing the same database's actual
exclusive Serializable guard. Read-committed transactions, including the
forced-global mode, retain fresh scans. A transaction isolation enum alone is
not proof of held admission. Application isolation is unchanged.

Source review also found an actual raw-writer escape:
`BicDb::paged_records_handle(&self)` returns a clonable `Arc<PagedRecords>`.
It can outlive the outer database read guard and mutate native rows directly,
without the Serializable gate. The successor marks raw capability issuance
before returning any Arc and never resets that fact on the same paged open.
It takes only nonblocking shared admission: `None` now means either no paged
store or busy exclusive admission. Thus a reentrant commit hook cannot block
on its own exclusive gate, and a raw writer cannot first escape during an
eligible memo pass. Once any raw capability escaped, all subsequent passes use
the original fresh scans, even after the original handle was dropped.

This is an explicit behavior change to the hidden, unstable raw-handle API.
It is source-only until peer review and execution, with no build authorization
or performance claim. Normal application code in this repository does not call
the raw getter; its eight current references are the definition and tests.

Counterexamples required before any production claim:

- A concurrent non-invariant writer must not be hidden by a stale pass cache;
  default concurrent mode deliberately does not share materializations.
- Rollback/new transactions and different upsert/delete overlays need fresh raw
  materializations; no cache may cross pass or commit boundaries.
- Untouched historical root rows, nested anti-existence/sums, cross-owner facts
  and same-ID tenant records must still be checked. No affected-row filtering.
- Repeat references to a relation must preserve authorization failures and
  policy resolution, including a revoked publication; only raw rows are cached.
- Inspect competing native-import/replication paths and actual serializable
  admission coverage before treating that deployment as a stable snapshot.
- Memo retention uses the smaller of16MiB and the inherited positive memory
  limit. It estimates bodies with the same helper as `retain_record`, counts
  each Arc body once, and falls back to the full uncached scan when a relation
  or combined memo exceeds that cap. It never truncates rows or raises limits.
  Before a miss it discards cache-only bodies; active recursive/subject Arcs
  continue to own exactly the working set the uncached evaluator needed. The
  original query deadline and memory failure are checked on hits and loops.
  Cooperative accounting is not hard RSS reservation; temporary scans retain
  their existing accounting and require resource validation.

The first five source tests cover all historical/tenant root rows and the
unchanged tx overlay, small-cap and combined-live-body uncached fallback, cache-only eviction, a real large relation with no
active budget and with a sufficient active budget, and expired/already-failed
scopes on a memo hit. They have not been compiled or run. They do not replace
application acceptance, race/isolation testing or profiling.

Five additional controls exercise actual raw native mutation followed by fresh
full scans after early issuance, concurrent late issuance while a real memo
holds the exclusive guard, refusal of missing/foreign guard and RC caching,
an escaped `Arc<PagedStore>` preventing a fresh open after `BicDb` and
`PagedRecords` have dropped, and a real reentrant `CommitAdmission` callback.
The latter runs in an owned child with a five-second failure bound, so a
blocking-lock regression cannot hang the parent test suite. These are ten
source cases, all uncompiled/unexecuted at this checkpoint.

## Actual writer and lifetime review

| Entry/capability at source505 | Exclusion or limitation |
| --- | --- |
| Normal `Transaction::commit` | `commit_transaction_inner` holds shared/exclusive Serializable admission across validators, decision and apply; image-row and PITR transactions use this path too. |
| Replication import, bulk load, offline chain repair and DDL | Actual entrypoints require `&mut BicDb`; a validator's borrowed same database excludes them. `BicDb` has no `Clone` implementation. This argument alone never excludes a previously escaped raw Arc. |
| `in_paged_transaction` and direct native receipts | The page writer gate does not acquire Serializable admission. Inspected history/receipt/proposal/audit callers use reserved native keys, rather than application record rows. Index/FTS maintenance changes derived keys. Ordinary row apply uses normal commit; open-time recovery runs before the database is published. |
| `paged_records_handle` | Only public arbitrary raw writer extracted from this database. It now records sticky issuance under nonblocking shared admission before escaping; unavailable under exclusive admission. `PagedRecords::store` permits retaining the lower `Arc<PagedStore>` and is covered by that same issuance. |
| Read-ahead, vacuum, checkpoint, storage-ops handles and unique-index cursors | Own private paged Arcs but expose scoped maintenance/read methods, not raw row/store getters or `Deref` to a writer. Their operation results are telemetry/reports/read rows, not an arbitrary writer. Physical maintenance preserving row values is distinct from logical mutation. |
| Independent `PagedRecords::open` / `PagedStore::open` | They acquire the actual paged-directory `DirectoryLock`; they cannot create a second writer against the same live directory. |

`PagedRecords` owns `Arc<PagedStore>`; `PagedStore` owns a nonoptional
`DirectoryLock` (`bicdb-page/src/paged.rs`). Even an escaped lower Store Arc
retains that lock after the outer database drops. Core's storage-mode fence
prevents reopening a paged database in another mode, and the next normal paged
open must acquire that same lock. The per-open sticky flag therefore cannot be
reset by dropping/reopening `BicDb` while any escaped old writer survives.
No unlock API or safe constructor alias creates a new records wrapper around
the old writer. The negative reopen control must still verify this source
argument on the actual build; existing comments are not an executed proof.

The raw getter has no production in-repository call from an exclusive scope.
External admission hooks can call it, which is why a blocking read lock would
be wrong. The new actual callback control requires `None` and a completed
commit. None of this changes startup catalogue admission: its RC transaction
continues using fresh scans; a separately specified Serializable catalogue
audit and read-only admission semantics remain independent work.

Existing commit trace attributes native validators to CONFLICT, not VALIDATE,
and emits only every50000 commits. The observed growing ERP mutation latency
is not profiled evidence that these scans are its dominant cost. No speedup is
predicted or measured by this source prototype.
