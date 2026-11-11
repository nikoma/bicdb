# Event routing network-deadline repair — 2026-11-11

This independent fix addresses event-client retry availability. It does not
implement strong reads, native image handoff, distributed SQL or production
qualification. The original Azure scale-out result remains **FAILED**.

## Reproduction and result

The unchanged runtime at main **1.0.468-beta**,
`60154f1f9a0b7793a74aae7e8ebc32845be01e42`, was compiled with only the new
regression fixtures added. Both new cases failed: a silent event endpoint and
a silent metadata-discovery endpoint each exhausted the logical request before
routing recovered. Their combined test window was 5.11 seconds for clients
configured with a two-second logical budget and a five-second RPC timeout.
The retained patch modifies only the final test module.

Candidate `4a5f0c503b616c8d0ed35938f5ceb32b7f89548d` was separately compiled
from its own worktree. All five event routing tests passed in 1.57 seconds:
the two new silent-socket cases plus existing transient discovery, committed
follower snapshot and legacy-controller discovery controls. The successful
payload is checked. The mocks speak the actual TCP protocol but return synthetic
responses; no database, transaction quorum, strong-read guarantee, rebalance,
RPO/RTO, throughput or physical qualification is exercised.

## Behavior

`ClusterEventClient` propagates one original cancellation deadline through event
RPCs and metadata discovery. Each event attempt uses half the remaining budget,
leaving time for refresh after a silent old route. Each discovery member has at
most one second within that original budget; snapshot and legacy fallback share
that slice. Socket progress and retries do not renew the parent budget. Existing
transport methods retain their original public interfaces; the client uses
cancellable internal variants.

A timeout still has an uncertain write outcome. Identical-key retries preserve
the existing idempotency contract. Event `Get` remains eventual. Blocking
hostname resolution and large synchronous local processing are not converted
into cancellable work by this patch; numeric member addresses avoid the resolver
limitation. No hard arbitrary-hostname end-to-end deadline is qualified.

| Finding | Classification | Evidence / next smallest test |
|---|---|---|
| Current-main silent event route recovery | FAILED | New TCP test fails on unchanged runtime; retained before output. |
| Current-main silent discovery retry | FAILED | Independent new TCP test fails on unchanged runtime. |
| Candidate network budget and retry controls | PASSED | Fresh compile, five exact tests and checked payloads. |
| Original Azure false absence / rebalance cause | INCONCLUSIVE | These TCP fixtures do not identify either historical cause. |
| Strong reads, image recovery, converged five-node throughput | NOT TESTED | Require native recovery/installation integration and a new isolated run. |
| Initial candidate cache result | INCONCLUSIVE / excluded | Cargo reused the baseline executable across same-version worktrees; it is not a test of candidate code. |

## Reproduction and evidence integrity

Raw outputs, resource samples, exact commands, source revisions and the test-only
baseline patch are in [the evidence bundle](evidence/event-routing-deadline-2026-11-11/).
The baseline compile took 74.020 seconds, with 21,478,256,640 bytes minimum
available memory. The valid candidate compile/run took 62.017 seconds, with
18,984,898,560 bytes minimum available. Both used jobs=1, incremental compilation
disabled, core opt-level=0/debug=0 and release dependencies; neither was paused
or interrupted. These are test-run resource observations, not database latency.

An initial candidate invocation reported Cargo FRESH and reused the prior
worktree's test binary (including the baseline source line numbers). Its complete
output is preserved under `after-invalid-cache-reuse` and excluded. Touching the
candidate's own `src/lib.rs` changed no source bytes but forced a fresh compile;
stderr records `Compiling bicdb-core` at the correct worktree. When reproducing
with a shared target, force a fresh compilation from each intended worktree and
check the path; do not delete another agent's shared build cache.

## Final versioned routing control

Exact private **1.0.470-beta** head
`b09ad4f1bc37ab441ed9a62d9abfa695f9184315` added the next adversarial case:
a former leader remains permanently silent while a different healthy member
returns a newer synthetic committed snapshot with a changed range epoch/leader.
The client discovers that member, sends the event request to the new leader at
the new epoch, and checks the returned payload within its original three-second
network budget. No application shard management occurs in this client fixture.

All **six** event routing tests passed after a fresh compile at that exact
versioned source; the module took 2.89 seconds, including the other silent-socket
cases. Compile/run time was 68.017 seconds, minimum observed available memory
24,374,517,760 bytes, with no pauses or interruptions. Raw evidence is under
`after-final-470`. The runtime patch is still independent of native image code.
This synthetic two-endpoint case is not actual consensus failover, a linearizable
read, ordinary PostgreSQL gateway qualification or a physical scale-out result.
The larger native recovery campaign remains mandatory.
