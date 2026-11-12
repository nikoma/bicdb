# Native recovery fence quorum: component evidence

Status: passed local component tests; integrated recovery and physical qualification remain open.

| Invocation | Exact source | Result | Minimum available RAM |
| --- | --- | --- | --- |
| v1 | `218d9073309b4c5205ef07ebb940b0926e289c85` | 5 passed, 78.023 seconds | 22,894,997,504 bytes |
| v2 | `5cc568374f0ef7257ca4cdaf1ecbfc6e95857e02` | 5 passed, 70.018 seconds | 22,070,571,008 bytes |

Both sources are private composition checkpoints with a historical package version
of 1.0.466-beta, not published releases or the current main version. The later
source retains original-parent cancellation and opaque shared fence observations,
refuses read-only role escalation, and requires active recovery rather than
completed epoch-authority observations.

Tests use actual new native database births, actual server_paged databases with
fsync enabled, actual receiver drains, and durable range catalogues. The metadata
fixtures run actual metadata quorum begin/probe/record/finish over a simulated
registered-identity transport. They do not use network TLS or independent machines.
The test databases intentionally have no range genesis. A successful fence majority
must preserve unknown native floors, never convert them to verified empty genesis.

Scenarios: RF1 persisted fencing with unknown physical floor; RF3 distinct-voter
majority without requiring the data leader's acknowledgement; duplicate/insufficient
votes; a real but differently scoped session; and original parent cancellation.

No test here establishes native-ready replica quorum, historical command recovery,
replica activation, strong-read readiness, ordinary PostgreSQL routing, scale-out,
power-loss durability, RPO/RTO, or production admission. Five tests on two revisions
remain five scenarios, not ten independent guarantees. Re-run against the final
integrated version before publication or a physical campaign.

`run.py` records the exact source, patch hash, command, exit result, available RAM,
swap, memory pressure and disk headroom, and forces the intended worktree's core
source to recompile rather than trusting cross-worktree Cargo freshness. Test core
optimization/debug settings are both zero; dependencies use the release profile.
There were no memory pauses or interruptions. The resulting test executable was
not independently archived or hashed for these invocations. Compressed stdout,
stderr and resource samples are retained alongside the uncompressed results.
