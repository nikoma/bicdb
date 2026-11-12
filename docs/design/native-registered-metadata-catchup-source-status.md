# Registered metadata catch-up and promoted cold reopen

Source successor based on `d194594b138fced00b611bef2b00e70f950e1045`.
This implementation and its new controls have static checks only. No local
Cargo, new execution, binary, deployment or fault-safe operator acceptance is
claimed. Earlier cold/supervisor results remain their separate exact cohorts.

## Two distinct authority levels

`NativeOwnedMetadataMemberObservation` stays unchanged. Ready/control requires
actual local committed C, term and commit index to match separately fresh OWN
Host MQ, plus its existing protected identity/DB checks. A global configured
Voter role or an advertised index does not satisfy that producer.

The new non-Clone/non-Serde `NativeRegisteredMetadataCatchupObservation` is a
lower, purpose-specific actual-owner observation. It BORROWS the original
sealed OWN Host MQ, retains its independent cancellation and deadline, and
binds actual protected Registered StoreUUID, same DB-open lifetime, actual local
FSM checksum, local/global identity and native birth. Local committed C can
still name this same instance as Learner while global C has promoted it.

It requires unchanged Active local actor/incarnation/pins/birth, stable restore
and actual schema, original protected namespace UUIDs, no local/global data
assignment and no business-sequence history. Global C/term/read index cannot
regress, and same-generation conflicting C refuses. The actual current leader
must be an eligible voter in retained local C. Unknown/new leaders outside
that C require the separate exact-snapshot admission producer and refuse here.
No snapshot is reconstructed from Host MQ getters. Both C copies are bounded.

The lower observation authorizes only ownership of a metadata-only listener and
normal actual Append catch-up. It cannot initialize a store, allocate a UUID,
change a term/vote/log/commit, authorize promotion/control, arm Serving or mint
READY. The existing Raft actor predicates continue to use actual persisted
local C: a still-local Learner cannot vote until real Append installs promotion.
A diagnostic `local_installed_current` condition is necessary only; READY still
consumes the stronger unchanged producer before its ready/control branch.

## Initialization and reopen boundaries

All original blank/create/Prepared learner predicates remain Learner-only.
The separate `reopen_registered_metadata` factory loader requires an existing
actual protected Registered root and original namespace binding. It opens the
actual birth/schema/manifest; it performs no schema installation or genesis.
Missing/Prepared/old-unbound protected records refuse this new path. A numeric
zero business sequence is refusal context only, never proof of initialization.

Registered metadata construction uses strict same-UUID reopen and retains the
actual owner on constructor/persist/cancel errors. Same-owner retry re-fsyncs
unchanged retained state; it does not reset durable fields or overwrite the
original-C sidecar. Missing/wrong metadata state remains quarantined. Older
Registered roots without protected namespace IDs retain their prior contracts;
they do not acquire this new promoted continuation by retrofit.

Fresh canceled/stopping after-bind owners can retry under fresh OWN MQ after
actual global promotion. A new dedicated true-drain service transition consumes
the lower observation; captured old provisional allowlists never widen.
Cold Registered Voters use a separate metadata-only service constructor with
the actual same-open owner. The same service/store remain retained on all errors.

The public learner facade emits `metadata_catchup` when this lower scope is
valid but local C/term/index lags. It does not label that state
`owned_metadata_voter`. Actual READY still needs the unchanged strict context.
Driver's separate metadata-control branch must preserve this distinction when
composing control-listener ownership and automatic maintenance.

## Genuine test source

The new Core control uses four actual TLS metadata voters and a fifth actual
born-enforced enrolled factory. It obtains a genuine protected catch-up ACK,
actual promotion-readiness proof and independently fresh post-ACK leader MQ.
It commits promotion through actual old/new voter majorities while withholding
committed-promotion delivery to the fifth, cancels the fifth after bind, then
checks strict READY refusal versus lower catch-up acceptance on the SAME owner.
An independently canceled original MQ remains canceled even with an uncanceled
consumer parent. Real Append subsequently installs promotion and supplies the
same protected StoreUUID ACK; only then does strict READY pass.

It ends ALL target DB/store/service/provider/listener owners, copies stopped
bytes, strictly reopens the promoted metadata-only Voter, and compares original
birth, every namespace UUID, protected StoreUUID and FULL durable term/vote/
base/log/commit/config/restore state (excluding only volatile role/leader labels).
Missing-state and wrong-UUID copies refuse without creating a state/listener;
ordinary data RPC remains refused. The original-C sidecar remains byte-identical.
These are test SOURCE, not executed results or a process/power-cut claim.

The actual CLI kill/reopen successor after default automatic promotion is the
next integration control, to compose only after Driver's automatic/control CLI
is stable. The earlier executed fifth-kill CLI cohort covered a Learner only.

## Open gates

Unknown-new-leader exact snapshot admission, changed schema/restore/pins/birth,
assigned-data roots, old Prepared provenance, whole-cohort outage, same-UUID
old-envelope rollback/term anchors, native data ACK/read failover and physical
HA remain separate unsupported or unqualified paths. This metadata-only
continuation is not complete Milestone 1 or automatic multi-host recovery.
