# Native owned metadata supervisor: source checkpoint

Date: 2026-11-12. Parent source: `2cc5debdb9cc180ad8a68013fac7e794608f875a`.
This batch has formatting/static checks only. No local Cargo, remote execution,
operator acceptance, built artifact or deployment is claimed for these changes.

## Ownership and authority boundaries

`NativeMetadataSupervisor` retains the actual owned metadata store and its
captured actual service-open guard, protected Registered StoreUUID and original
DB-open lifetime. Construction requires actual mutual-TLS configuration and
uses a dedicated transport over the SAME published routing cache. It never
opens generic metadata, installs a declared schema, seeds a recovered epoch,
or treats a status/JSON field as authority. A per-actual-Arc weak registry
permits one live OR stopping worker; it is a scheduling guard, not a quorum.

The caller supplies the process lifetime explicitly. Every operation is a
bounded child of that original cancellation/deadline. Bootstrap/action 5s or
30s children must not be supplied as a supervisor lifetime. Stop cancels the
worker and waits only within the cleanup budget, retaining the handle on
failure. The actual worker retains store/DB owners until it really exits even
if its outer handle is dropped while RPC is in flight.

One worker alternates bounded timer/election/replication work. No service, DB,
metadata or routing latch is held over network I/O. Per-peer children share the
remaining ORIGINAL round budget; replication rotates the first peer so an
unreachable peer is not permanently first. A busy local store or failed RPC is
not a successful phase; retry rechecks the real owner/current role/C.

## Real elections and current-term anchors

Monotonic local elapsed time measures silence. A SHA-256-derived per-actor,
per-term delay avoids adjacent node IDs campaigning at the same instant.
Actual authenticated, structurally valid and successfully persisted leader
Append/read-probe contact renews the volatile timer. Stale terms, malformed
requests and persistence failure do not renew it. A legitimate previous-log
conflict may renew leader contact after persistence; its rejection never
advances accepted replication progress or counts as an Append success.

A candidate first persists its real self-vote, collects actual request-bound
TLS vote responses, and rechecks the same candidacy before becoming leader.
Both actual old and pending-new voter majorities are required. Votes were
requested using committed-C receiver identities; an old incarnation/pin/birth
cannot count toward a different pending-new actor just because the node IDs
are equal. Legacy public election APIs remain unchanged.

`become_leader` writes the real current-term barrier. An elected leader with
an uncommitted barrier keeps replicating that same barrier on later ticks
instead of starting more terms. The diagnostic distinction is configured
membership / elected role / committed current-term anchor. None is a lease:
Host MQ still needs its existing fresh actual old/new probe quorum, and data
writer/Serving/recovery/read admission retain their independent proof gates.
Learners, retired actors and replaced incarnations cannot campaign. An actual
healthy owner may publish restrictive durable topology even after retirement;
it cannot use that publication as voting eligibility.

Replication uses the existing bounded accepted-frontier builder and actual
native-bound Append response validation. Backend committed C is republished
on every valid tick, covering a cache publication missed after persistence.
No rejection hint or unrelated tail becomes a match-index ACK.

## Source controls and integration boundary

Three new `native_owned_metadata_supervisor_` Core test sources use genuine
born-enforced stores/protected IDs and actual pinned TLS:

- isolated real candidate/no fresh MQ, then majority election and current-term
  barrier, stationary heartbeat stability, leader endpoint/worker outage,
  surviving-majority election and healed old-leader refusal;
- stale/malformed traffic does not renew contact, valid real Host-MQ read-probe
  does, and a second live worker for the same actual store is refused;
- actual metadata-store owner close/strict reopen preserves protected UUID,
  term, vote, base, complete log, commit/config/restore bytes, then resumes TLS.

These controls have NOT run. Core outages stop/drain real listeners/workers;
they are NOT process SIGKILL, all-DB-owner reopen or physical power loss. The
strict-reopen test ends all metadata-store owners while retaining the same
actual DB owner; the separate earlier cold-learner cohort covers all target DB
owner loss. Do not conflate those scopes.

The initial/control facade exposes `start_automatic_metadata(process_parent)`
only after the real owned listener exists, and stops the supervisor before
draining the listener. This checkpoint does NOT yet invoke it from the CLI.
Driver's separate automatic-join/member-context branch must wire initial CLI
maintenance and promoted learner ownership using the full process parent.
Manual election/heartbeat commands remain diagnostics. Cold promoted-voter
startup is still unsupported until a distinct actual Registered-member
producer is implemented; no uninitialized learner predicate is weakened here.

Actual process leader-kill/restart, five-process automatic election/join,
changed-identity configuration progress, large-membership/slow-network timer
availability, membership churn, whole-cohort outage and physical fault-safe
qualification remain OPEN until their complete integrated tests execute.

## Separately executed earlier cold CLI result

Root source `2cc5debdb9cc180ad8a68013fac7e794608f875a`, remote run
`43201140398`, job `131124577734`: the real five-process cold CLI test passed
1/0/0 ignored in 3.62s; all-target/isolated-consumer checks passed. Raw log SHA
`6acfb3b9ad230d848de638ed85523125879be31d58c1d78a74908c12392ca646`.
It killed the fifth only after its actual bound ACK, captured durable metadata
state AFTER kill+wait, reopened the SAME persisted root/StoreUUID/birth/schema/
all namespace UUIDs/state and required a new real bound ACK. All five data
endpoints refused and the fifth stayed a metadata learner. This earlier result
is not validation of this new supervisor source, nor physical HA/data-RPO proof.
