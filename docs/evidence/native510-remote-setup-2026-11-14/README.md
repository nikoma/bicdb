# Native 510 remote-phase setup refusals

The complete original source `d8115fedf13b0bd7107cfddd2664f2c6ea2f261f`
run `43572675143`, native job `132380250977`, **failed: 273 passed, 47 failed,
1 ignored**. This leaf addresses fixture setup and a refusal-message assertion
for 14 of those failures. The other 33 failures remain separately investigated.
The original run remains failed. This correction has not been compiled or run.

[Original observations](original-observations.json) retains every affected Rust
name, actual error and panic location, plus the complete log identity. The
interleaved error preceding the postcommit-cancellation DECISION failure was
checked against the raw log; its actual refusal is the same acquisition error.
No production database contents, fixture keys or complete RPC frames are included.

## Thirteen NEW / DECISION cases

All 13 failed at the shared `remote_new_tests.rs:171` setup acquisition, before
participant materialization and the intended NEW / DECISION fault controls.
`validate_acquire_requester` in `distribution_consensus.rs` requires the actual
requester to be the selected range's current leader, with matching epoch,
incarnation, active lifecycle and voter status. The actual error is a combined
requester/range-authority refusal; it is not evidence of a timeout or data loss.

The inner fixture unconditionally used `clients[0]`, conflating the metadata
leader destination with the required range-leader requester. Its outer S2
fixture already selected the requester from the pending range's leader. This
leaf does the same for the inner acquisition, using the original transport's
actual participant identity and comparing pending range epoch/leader to the
original source manifest. It still requests a genuine fresh observation at
metadata node 0 with the same five-second parent and acquisition budget.
No DTO becomes a Role, OWN, OLD quorum or participant capability.

A new failure-only report retains the actual selected range, source range,
requester, metadata destination, error, original ACKs and source-frame journal
before the original unwrap. It does not retry. All subsequent participant,
pin, native-record, uncertainty, fault and application assertions are unchanged.
Selecting the correct actor is a source-level fixture correction; later runtime
results must establish whether each previously blocked control succeeds.

## Discovery wrong-actor case

The original call was refused with `source discovery target identity differs`.
The production `DiscoveryCaller::Target` branch checks authenticated peer node
identity before checking donor/birth identity. The old test expected the later
`actual donor/target identity differs` error, so it failed on a valid earlier
refusal. This leaf matches the exact target-identity refusal and records it
before asserting. The actual wrong-peer RPC, no-candidate assertion, subsequent
genuine target discovery, and final unchanged OLD-record assertions remain.
Production admission checks are unchanged.

## Validation boundary

Only standalone formatting, diff and source-preservation checks ran for this
leaf. No Rust build/test, CI, deadline change, automatic retry, production
semantic change, version bump, deployment or physical experiment was performed.
The production gate remains zero lost acknowledged commits, correct reads and
routing, bounded fault-safe rebalance, and sustained gain after convergence.
