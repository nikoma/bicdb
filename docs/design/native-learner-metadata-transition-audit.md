# Same-service learner metadata transition: source audit

Scope: source `bb725beb11d698f9f4a4a6955196b875335cd89d` plus the frozen finite
enrollment checkpoint `1d88047c9f1ca53588612b6c831090a25a9ea80a`. This is a
next-step design, not implemented transition or executed acceptance. Current
remote enrollment checks are still pending. No Cargo was run for this audit.

## Existing seams and concrete missing wiring

| Source / symbol | Available guarantee | Remaining seam |
| --- | --- | --- |
| `distribution_native_host/learner_birth.rs`: `NativeUnassignedLearnerHost` | Owns actual factory, same service/range-store and provisional listener; `stop_and_drain` retains stopping handle on error | No retained drained proof or pending owned-MStore/metadata-listener phase |
| `distribution_transport.rs`: `DrainedClusterDataListener` | Binds actual service Arc and exact RangeWriteStore-open UUID; counts accepted workers until drained | Must be retained by transition owner, not inferred from shutdown/join |
| `distribution_transport.rs`: listener capture / `learner_birth_provider.is_some()` | Captured provisional presence stays immutable, even when Weak expires; only birth/Ping | Attaching a metadata store to that same live listener never opens Append/Vote |
| `distribution_data.rs`: `transition_initial_preparation_listener` | Real drained same-service transition plus protected initial-all-C exposure | Wrong purpose for an existing-cluster learner; cannot synthesize an all-C permit/marker |
| `distribution_consensus/native_metadata_store.rs`: `initialize_native_owned` | Real native owner, protected distinct StoreUUID, fsynced state and Registered receipt before success | Host must first validate own fresh learner HostMQ and actual enrolled birth; raw C parameter is not an authorization surface |
| Same file: `reopen_native_owned` | Requires Registered protected record plus SAME UUID state, no missing-file fallback; retains actual-open owner | No current learner-host cold route selects this instead of the pre-init factory loader |
| Same file: `prepare_initial_native_owned` / `NativeMetadataPreVoteFact` | Exact initial-C never-exposed continuation | Requires own VOTER and is initial-purpose; not a learner Prepared-resume API |
| `distribution_native_host/learner_factory.rs`: `reopen_uninitialized` / `enrollment_proposal` | Refuses ANY protected metadata initialization, including Prepared | Cannot be used after BEGIN or to reset a partially initialized store |
| `distribution_transport.rs`: `start_cluster_data_server_with_recovery_runtime` | Same-open metadata/provider attachment checks; immutable listener-mode capture | Need purpose-specific metadata-only attachment after drain, with no learner-birth surface or ordinary data endpoints |

## Minimum live API / ownership contract

Use a mutable owner retaining its actual factory and SAME service/range-store
through every failure. Suggested operations are `prepare_enrolled_metadata`
then `start_enrolled_metadata_listener`; names are proposals, not current APIs.
Their private phase state must retain stopping/drained listener custody, actual
owned MStore (once made), provider/transport and any newly bound stopping handle.
Only the final successful handoff may move these owners to a live metadata-host
wrapper. An error must leave the original owner inspectable/retryable.

The input authority is a NEW actual **learner's own** HostMQ under the original
operation deadline/cancellation. The enrollment controller's returned receipt
is a committed identity FACT, not initialization authority. Check the current
local member is Active Learner, has no replicas, has exactly this actual native
birth/incarnation/pin/bootstrap/restore/schema, and the old voter cohort is
native. Bind/revalidate actual DB-open lifetime, factory schema/local namespace
identity and from-birth metadata-init capability. Do not infer any of these
from the audit result or serialized manifest alone.

1. Acquire/revalidate current own HostMQ outside all latches. Stop/drain the real
   provisional handle; preserve it and its active counter if the original token
   expires. Match the drained service Arc and range-store-open UUID.
2. Take only short actual DB reads for birth/schema/record inspection, then
   release them. Initialize or strictly reopen the owned MStore OUTSIDE a
   service/metadata mutex, retaining the resulting owner before further fallible
   work. Initialization internally acquires DB reads before exposing a mutex;
   persistence later uses actual-open/health checks without MetaMutex→DB locks.
3. Validate the exact owned MStore against the actual service open. Under an
   actual drained-service transition, clear the learner-birth attachment only
   for the NEXT listener capture and select a permanent metadata-only mode.
   The OLD listener's captured Some remains allowlisted forever. Do not create
   another ClusterDataNodeService/RangeWriteStore on the same DB.
4. Build the client/provider around that SAME MStore. Bind a NEW metadata-only
   listener with ordinary data/control stores/recovery endpoints unavailable.
   Ping, Append, metadata probes and role-refused metadata vote/acquisition are
   sufficient; initial-all-C preparation is not a learner grant. Recheck the
   original proof/token after bind. On failure request shutdown and RETAIN the
   stopping handle until genuine drain; `join()` alone is not worker drain.
5. Real leader Append/snapshot catch-up must use current committed binding and
   protocol-27 destination checks. The owned ACK producer then supplies the
   current actual birth/StoreUUID proof. Only those actual responses may update
   leader progress. Promotion remains a separate current-leader action after
   genuine catch-up, never this host's caller-provided installed position.

Once an endpoint could receive Append, it may have learned a term/log before a
host method returns. All Registered-state retries must preserve that state;
never re-enter term-zero mode because startup returned an error. An initialized
learner still cannot vote/start election, has no range floor or writer policy,
and its immutable metadata listener must refuse ordinary application traffic.

## Failure cuts and minimum supported policy

| Cut | Durable/live state | Safe next action |
| --- | --- | --- |
| Before drain / canceled active worker | Original listener still captured provisional; no store created | Retain stopping owner; fresh cleanup budget can complete real drain, without granting authority |
| After drain, before metadata prepare | Same native DB and range-store, no protected metadata record | Independently fresh current own learner MQ can retry initialization |
| After protected Prepared, before state file | Actual native record has StoreUUID/C hash, `registered=false` | Current generic initializer/reopen safely quarantine. Need a learner-purpose exact continuation; never allocate another UUID/reset |
| After fsynced state, before Registered | Same Prepared record plus actual term-zero state | Purpose-specific continuation must verify SAME UUID/C hash and exact never-exposed learner state before finishing registration |
| Registered before bind, or bind fails | Protected Registered record plus same state; native owner may already be held | Retain/reuse that owner, or strict reopen after owner drop. Preserve term/vote/log even if still zero |
| Listener bound then original scope expires | Countable Append may already have happened | Shutdown/drain while retaining owners; strict Registered reopen only, never pre-vote reset |
| Cold Registered root, state missing/wrong UUID | Protected ever-initialized fact survives | Refuse; no generic open or fresh factory fallback |

The protected metadata record stores the original C **hash**, not the whole C.
After Prepared-before-state-file, an exact-C continuation can use a fresh MQ
only if it supplies that SAME C and stable local identity. If current C has
advanced and the original full C is unavailable, existing APIs cannot safely
invent its contents. Minimum implementation must explicitly quarantine that
case, or separately persist a protected/bound initial configuration candidate
before supporting broader Prepared continuation. Initial-all-C pre-vote logic
must not be relabeled to bypass the learner-role condition.

`reopen_uninitialized` deliberately refuses even Prepared records. A separate
cold owned-metadata path must load and validate the actual birth/manifest/schema
then call the protected metadata loader. It must not call factory creation or
pretend a Registered root is uninitialized. The existing owner/StoreUUID contract
also does NOT protect same-UUID rollback of an old term/vote envelope; that needs
additional anchors and is not claimed by this transition.

## Smallest actual TLS control

Extend the existing four protected voter listeners plus enrolled fifth factory.
Use fresh fifth-owned HostMQ, real drain, exact same service pointer/range-store
UUID, actual protected StoreUUID initialization and new metadata-only listener.
Have the real leader deliver its authoritative committed snapshot/Append to the
fifth; record its genuine native owned ACK and verify the leader's actual match
frontier reaches commit. Before promotion, check receiver vote/election refusal
and no ranges/rows/native floor/writer. Separately promote through old/new metadata
quorum, then show a real current vote; ordinary application traffic must STILL
refuse on the metadata-only listener.

Retain actual roots and cut receipts for Prepared-before-state, state-before-
Registered, post-Registered/bind failure and post-bind cancellation. Strict cold
copies must either resume the explicitly supported exact case or report
quarantine, with the SAME StoreUUID/term/log/local UID. Keep expired-original
proofs expired even after independently fresh MQ; never fabricate authority or
manually clear a protected record. This is the next acceptance target, not a
claim that the fifth operational loop or HA is complete.
