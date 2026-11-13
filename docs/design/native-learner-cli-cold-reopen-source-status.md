# Explicit CLI cold learner restart: source successor

Date: 2026-11-12. Base: `0e43b3e480da67bae5dcc1647b9ee3f68148eea1`.
This successor has only formatting and static checks at authoring time. No local
Cargo, executable, release, deployment or new application acceptance is claimed.

## Earlier executed Core cohort

The four `native_learner_cold_` Core controls passed at exact Root
`80a2ff0b62867c8feeac490ce6534c8786be2bae`, remote run `43198801014`, job
`131116521452`: 4 passed, 0 failed, 0 ignored, 872 filtered, 8.60 seconds.
All-target compilation and the isolated consumer check passed there. Raw log
SHA-256: `f3ef97856f2e9900d067c6a2816caf2df045fa72213a847cc708e70dcf361c21`.
Those controls dropped all target owners and opened copies after orderly close.
They did not execute the new CLI/process-kill successor below. Original failed
attempts and historical source assessments remain separate.

## Explicit persisted-root owner

`NativeMetadataLearnerHost::reopen_metadata_only` obtains the caller's own
fresh Host MQ over actual TLS, verifies the explicitly supplied current local
endpoint/incarnation, and consumes `NativeColdLearnerMetadataHost::reopen`.
The bootstrap snapshot is routing DATA. No fetched schema, declared birth,
read-index getter or serialized status becomes authority. Reopen installs no
schema and initializes no genesis. The actual persisted factory, birth, all
namespace UUIDs, protected metadata StoreUUID, original-C sidecar and retained
metadata state determine which already-supported cold path is eligible.

The new `cluster native-metadata serve-learner-reopen` command requires an
existing root and new private operator files outside its canonical native root.
`serve-learner-fresh` still requires a nonexistent root. Reopen initially holds
only the actual existing DB owner. A separate `start-learner-metadata` action
uses another independently fresh own MQ to recover the protected backend and
start a new immutable metadata-only listener. Busy/canceled/bind/constructor
errors retain the typed cold phase owner inside the command loop. No old
provisional listener is widened, and no backend/status label grants a vote,
range floor, data Serving or writer admission.

## New executable test source, not yet executed

The existing actual five-process CLI test now kills the fifth process only
AFTER its real TLS bound Append ACK. It waits for termination, preserves the
old command/status/log files and exact metadata state at that cut, and restarts
that same root through the new CLI command. The four existing metadata voters
remain live. It requires:

- fresh-over-existing rejection without changing the stored state;
- unchanged actual birth/hash/schema and every local namespace UUID;
- no backend/listener from the initial reopen constructor;
- same protected StoreUUID after explicit cold start;
- exact durable term, vote, base, complete log, commit, topology and restore
  state across reopen (only volatile role/leader labels are excluded);
- a new real TLS bound Append ACK from that same backend;
- continued learner membership and ordinary data RPC refusal on every host.

The test uses no private factory calls to replace process startup. It retains
all process roots/logs on failure. No new TTL, synthetic reply, counter seed,
metadata reset or schema retrofit was introduced.

## Boundaries still open

The current leader must remain an eligible voter in the retained original or
committed C. Changed membership/new-leader-not-in-old-C snapshot recovery,
promoted or data-assigned learner roots, schema/certificate/identity changes,
whole-cohort outage, physical power loss and same-UUID old-envelope rollback
remain unsupported or separately unqualified. Old Prepared roots missing the
protected namespace binding remain quarantined. Existing Registered roots
retain their original strict contract; this path does not retrofit provenance.
Automatic join/promotion is a separate implementation track. This metadata-only
checkpoint does not complete Milestone 1 or establish user ACK/Strong-read HA.
