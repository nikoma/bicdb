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
Therefore this prototype shares rows only when the existing serializable
exclusive admission or forced global writer gate is already active; otherwise
it retains the original fresh-scan behavior. It changes neither isolation nor
admission. Runtime attaches all catalog invariant bundles to app invocations,
but that alone is not proof that every possible competing relation writer uses
the invariant mutex. The actual ERP default-mode cache-hit rate is unknown.

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
- Cached full relations remain alive to pass end. Existing per-scan query memory
  accounting does not establish a bound on their combined residency; this
  memory tradeoff requires separate validation before production use.

Existing commit trace attributes native validators to CONFLICT, not VALIDATE,
and emits only every50000 commits. The observed growing ERP mutation latency
is not profiled evidence that these scans are its dominant cost. No speedup is
predicted or measured by this source prototype.
