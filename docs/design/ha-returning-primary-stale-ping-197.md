# Returning-primary stale Ping: issue 197

[Issue197](https://github.com/nikoma/bicdb/issues/197) is a real required-CI
regression at frozen source `2c0193c37852546c51b99e2918c24ea17bb44e2b`.
PR194's [PGwire job132489784128](https://github.com/nikoma/bicdb/actions/runs/43609026644/job/132489784128)
completed with failure. Its `embedded_ha` target recorded 11 passed and one
failed in 33.30s. The complete lossless raw log is 789,941 bytes, SHA256
`e9718a8d4d69a8e1f1e45e693d4fa32ee5ecb238f5357559bb960d59af302645`.
Other useful jobs continued; no retry, cancellation, deadline widening or change
to the original test was used to obtain this evidence.

The failed caller is
`embedded_quorum_survives_primary_loss_and_fences_a_returning_writer`,
`crates/bicdb-pgwire/tests/embedded_ha.rs:393`: the wait at lines392–406 for the
original n0 to reopen as a standby and show value8. It is not the initial
primary-election wait or an injected fault.

The retained log and unchanged caller establish this order:

1. The initial `with_write` INSERT(value7) returned successfully and both
   standbys reached the existing read condition. n0 was then shut down.
2. n1 won term1 with two of three votes, reopened Primary(epoch1,data_epoch1),
   and n2 followed it. The existing condition required a live writer lease and
   the follower's matching primary ID/qualified epoch.
3. A new-primary `with_write` UPDATE(value8) returned successfully through the
   existing idempotent retry condition. Exact failed/retried callback counts
   were not journaled; these are at least two successful API returns, not an
   external attempt/ACK ledger or an RPO result.
4. The original n0 reopened Primary(epoch0,data_epoch0), with no current peer
   lease. Its repeated Ping(epoch0) requests failed with
   `primary epoch is behind this node's durable election term`.
5. The returning-n0 wait timed out. UPDATE(value9) and the later old-writer
   refusal assertion were not reached. The wait short-circuits on the expected
   Standby role, so the log does not prove a successful stale/absent data read.

Libtest emitted this captured stdout/stderr when the case failed. Those log
printing timestamps are not original per-stage clocks. No CPU, disk, network
latency or physical data-loss cause is inferred from them.

## Confirmed source cause and correction

`high_availability.rs::serve_peer` returned an error from the stale Ping's
`require_replication_primary_epoch(epoch)?` before sending Pong. The existing
`primary_supervisor` discovers a higher actual primary epoch from that Pong;
without it n0 cannot invoke `observe_replication_epoch`, fence, request restart
and rejoin. The stricter stale-contact guard unintentionally removed the
necessary information path.

The correction gates only `heard_now`: the same current-epoch/role predicates
and durable-term check must pass before contact is renewed. A configured,
authenticated election peer still receives the actual informational Pong with
the receiver's recorded epoch and primary ID even when its Ping is stale.
That response does not record a commit ACK, renew the sender's lease, grant a
vote, declare a candidate primary or advance data epoch. Source/peer admission,
stream-before-apply, final ACK qualification and all timeout/backoff settings
remain unchanged. The original full embedded acceptance test is byte-identical.

The new private unit control uses the real TCP handler and its SAME original
HaRuntime, created through the production constructor. Real file-backed native
candidacy/votes/promotion produce the known newer epoch; no vote/history DTO or
fake clock is installed. The stale request receives actual Pong data without
changing that runtime's contact Instant. The positive exchange then comes from the actual quorum-elected n1 leader,
using its own recorded epoch, and exercises current-term contact renewal. An
informational reply to a follower does not make that follower a primary.
This is development TCP, not TLS or multiprocess qualification. The control and
correction have not been compiled/executed in this successor.

`capabilities/catalog.json` now names actual handshake v5 and states that quorum
settings alone do not establish preservation. Conditional/unsupported statuses
and admission behavior are not promoted. Snapshot data-head/ACK-copy issue193,
trusted all-legacy upgrades, old-binary missing-root rollback, bounded recovery,
read consistency and physical qualification remain open. The original source,
failed run and raw evidence retain their exact identities.
