# Native learner cold metadata continuation: source checkpoint

Base: `44ed8a4bb02f54dd78b569ba23914704bbb5fff8`. This is an unexecuted
implementation of the bounded part of design `a133f84ff`. Formatting/static
checks are not compilation, CI, physical crash, operator or HA evidence.
No local Cargo, version bump, main push, binary, deployment or Azure action is
part of this checkpoint. Earlier hot same-service results remain separate.

## Persisted facts and strict branches

For a NEW native learner metadata initializer, the actual DB and metadata-root
owner claims precede a bounded (4 MiB complete envelope) original-full-C sidecar.
Its atomic file and directory fsync, including the metadata directory's parent
link, precede the first StoreUUID allocation and protected native Prepare.
An existing protected record or metadata state cannot be overwritten by this
initializer. A sidecar-only cut has no metadata identity or permission.

The existing native receipt's plain SHA256 of bounded original C authenticates
that historical configuration. The sidecar's own checksum detects damage; it
never creates current authority. The actual native record additionally protects
an optional, actual sorted `initial_namespace_ids` list for NEW learner
initialization. Its ordinary checksum covers the list. The new cold unassigned
learner path verifies current local namespaces against it; Prepared without
that protection remains quarantined. This is an initial physical fact, not a
new serving, range image, schema transition or metadata promotion grant.

Actual own fresh HostMQ, current Active Learner/no-replica assignment, actual
birth/incarnation/pins, schema/restore, immutable initialization capability,
writer/abort host contract and actual DB/root ownership are separate inputs.
The cold loader opens an existing manifest/root only. Nonzero business sequence
is an additional refusal, never an emptiness/readiness proof from a counter.

| Actual protected phase | Behavior |
| --- | --- |
| No record and no state, including sidecar-only cut | Independently fresh own MQ may initialize the first actual UUID; re-fsync C before Prepare. A retained ambiguous pre-protection owner must be dropped/reopened rather than silently reidentified. |
| Prepared | Require protected namespace binding, original sidecar matching protected C SHA, exact SAME StoreUUID and original C, T0/no vote/no leader/empty log/base/commit. Missing state is created only as that exact never-exposed state. Existing nonzero or conflicting state is never reset. Fsync/readback precedes finishing the SAME protected registration. |
| Registered | Existing strict native reopen preserves UUID, term, vote, full log, base/commit, C and restore. Missing/corrupt/foreign state refuses, regardless of sidecar presence. Only volatile Follower/leader labels are cleared by ordinary reopen. |

Restrictive state persistence remains possible before Registered, but countable
actor/listener checks require the actual protected Registered identity. Initial
all-C Prepared continuation preserves its prior actual-voter check without
minting a pre-vote fact before Registered.

## Cold host ownership

`NativeColdLearnerMetadataHost::reopen` opens the strictly existing root only;
it creates no listener or backend. `start` requires a separately fresh own MQ.
It retains the actual MStore before persistence errors and creates its new
service using a plain exclusive store borrow BEFORE exposing a MetaMutex.
There is no MetaMutex-to-DB-lock inversion and no fabricated dead-listener drain.
The service/MStore use the SAME actual DB open and protected StoreUUID.

A NEW listener is permanently metadata-only. No old captured provisional mode
is broadened. Bind failures retain backend/service ownership. The new listener
is retained before independent original-MQ and caller cancellation checks;
a post-bind cancellation requests shutdown and retains its stopping/drain
custody. Fresh retry drains that actual listener and reuses the same identity.
No retry renews an old observation or clears a durable metadata identity.

## Reader compatibility control

Pre-field `NativeMetadataStoreRecord` uses `deny_unknown_fields`. A present
namespace binding therefore makes that strict older record decoder fail before
validation; hypothetical stripping/ignoring also changes the authenticated
checksum. This is the actual protected-record read path, not an assumption that
an optional field is harmless. None omission retains actual old voter bytes and
checksum exactly. The source includes an old-codec test that consumes
bytes fetched directly from actual native pages, plus the stripped-field
checksum negative and real old-voter byte parity control. These tests have NOT
run yet. There is no older-binary, whole-profile rolling-upgrade qualification.

Existing Registered roots without the new field retain their prior strict
identity/state contract; they do not gain new namespace-parity evidence.
New cold Prepared cannot silently retrofit them. A promoted data/metadata
voter's initial namespace fact is not an unconditional ban on every future DDL;
current UID validation is scoped to Prepared and the unassigned learner path.

## Test source and remaining gates

Four `native_learner_cold_` tests use four genuine live TLS metadata voters and a
fifth actual enrolled factory. They explicitly drop ALL target owners, fsync a
COPY, then strictly reopen that copy. They cover sidecar-only and both Prepared
cuts, actual bound TLS Append ACK, unchanged UID/birth/UUID, no vote or data
service, another all-owner Registered reopen with full durable-state comparison,
missing/changed sidecar, nonzero term/vote, wrong UUID, Registered state loss,
real occupied-port refusal, independent original-MQ post-bind cancellation and
same-owner retry. The old-codec test consumes actual native-page bytes; no
serialized value becomes authority. Original and copied roots remain retained.
These are orderly-close/software file-loss controls, not SIGKILL or power loss.

The current leader must remain a voter in the retained original/committed C.
The normal Append leader-voter guard stays intact. No current snapshot is
labelled with HostMQ getters' read index/term: this batch does not install such
a snapshot, so it needs no inferred exact-snapshot seal. A newly elected leader
absent from old C remains explicitly unsupported. Generic strict reopen may
also reject larger membership changes; this batch does not weaken it.

A distinct exact committed-snapshot producer, changed-membership/new-leader
catch-up, promotion/assigned-data cold startup, certificate/schema transition,
whole-cohort outage, same-UUID old-envelope rollback/term anchors, unsupported
ambiguous disk cuts, automatic controller/CLI scheduling, user ACK/Strong read
and final physical fault-safe qualification remain OPEN. The four live voters
in these tests are not a claim that a whole five-process outage self-heals.
