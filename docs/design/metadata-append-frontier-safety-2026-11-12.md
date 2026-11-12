# Metadata Append frontier safety — 2026-11-12 UTC

## Finding and scope

**Failed:** a genuine three-store metadata-consensus execution can acknowledge an
untouched divergent follower suffix as if it matched the leader's bounded Append
request. The leader can then mark a topology proposal committed even though that
proposal exists on only one store. Subsequent genuine majority recovery discards
that proposal. This is a metadata-consensus safety defect, not a measurement of
business-row loss, physical HA, TLS operation or Azure fault behavior. No causal
link to the original Azure invisibility or relocation failures is established.

The source baseline is public main **1.0.481-beta**,
`3cce710c689ba8ec7a820538113e389481e04669`. Main 1.0.482-beta
`f43583da4c34209e810c5e6a8f5925831b2949a1` changes Oracle backup dispatch
and version metadata; it does not change this consensus implementation.

The code path is
`crates/bicdb-core/src/distribution_consensus.rs::handle_append_request`,
followed by `record_append_response`. The former returns `last_log_index()`
after processing a bounded request. The untouched suffix need not match the
sender. The latter counts that returned index when advancing commit.

## Smallest deterministic reproduction

The regression is
`distribution_consensus::append_frontier_tests::metadata_append_actual_bounded_prefix_ack_cannot_commit_divergent_tail`.
It uses three actual fsynced `MetadataConsensusStore` instances, their real
votes, generated requests and returned acknowledgments. The harness drops
message delivery; it does not invent terms, log entries, response indices or
vote results.

1. B wins term 1 with C. Deliver its barrier at index 1 to C and drop the real
   reply. Both retain that uncommitted entry.
2. B wins terms 2 and 3 with A. Drop replication of its later barriers, leaving
   them only on B. A genuine mismatching heartbeat advances C's term to 3.
3. C wins term 4 with A, adds its new barrier at index 2 and proposes a distinct
   topology at index 3. B's entries 2 and 3 remain different old barriers.
4. Send C's actual one-entry Append batch covering only the shared index 1 to B.
   The unfixed receiver returns index 3. Recording that real reply lets C claim
   its own distinct topology at index 3 committed without replicating it.
5. Make C unavailable. Elect B with A in term 5, deliver real one-entry batches
   and their actual replies, and commit B's barrier at index 4. Heal C with B's
   real committed snapshot. Strictly reopen all three stores. C's earlier
   topology proposal is gone.

The regression performs recovery and reopen verification **before** the final
ACK/frontier and no-premature-commit assertions, preserving failure evidence.

```sh
cargo test -p bicdb-core --lib --locked \
  metadata_append_actual_bounded_prefix_ack_cannot_commit_divergent_tail \
  -- --nocapture --test-threads=1
```

Set `BICDB_TEST_EVIDENCE_ROOT` to an empty owned directory to retain the original
state files, exact pre-recovery copies, request/reply trace and result JSONs
under that directory. The focused manual CI job uploads that opted-in directory
along with the tested source identity and complete test output. The workflow's
normal pull-request and main-push paths still run the complete suite matrix.

## Revision-scoped evidence

| Revision and check | Classification | Result |
| --- | --- | --- |
| Tests-only `e5abb9f9b842081385d0f93c5ab7abd75b6819df`, [run 43171290447](https://github.com/nikoma/bicdb/actions/runs/43171290447) | Failed | One executed regression failed: returned ACK 3, matched frontier 1. All recovery/reopen assertions preceding that final assertion completed. Compilation and isolated consumer checks passed. The first run retained log evidence but did not upload the temporary state directory; do not claim that run archived raw state files. |
| Retained-state baseline `c295916d0030f3fbd37962150cfd1f65dc70e752`, [run 43173136830](https://github.com/nikoma/bicdb/actions/runs/43173136830) | Failed | Zero passed, one failed, 538 filtered out, 0.08 s. Archived actual states and trace confirm ACK 3 for matched index 1, premature C commit index 3, subsequent replacement by genuine majority recovery, and strict reopen of all three stores. |
| Fixed candidate `e0bb370fb1bad2cddc783b1b8f98775735281c05`, [run 43173148741](https://github.com/nikoma/bicdb/actions/runs/43173148741) | Passed | One passed, zero failed, 538 filtered out, 0.07 s. Archived actual states confirm ACK 1 and no premature topology commit (C commit index 0). Genuine majority recovery and strict reopen of all three stores pass. This is the focused test result, not a full-suite or physical qualification. |
| Additional controls `6458bf6d0c121e647ae0a1f57fa85a06d16b4163`, [run 43174340023](https://github.com/nikoma/bicdb/actions/runs/43174340023) | Passed | Two passed, zero failed, 538 filtered out, 0.08 s. The original divergent-tail regression and genuine snapshot-plus-entry, duplicate partial-batch, empty-heartbeat, commit propagation and strict-reopen control pass. All-target and isolated consumer compilation checks pass. Full matrix is skipped in this focused manual run. |

The [retained artifact comparison](../evidence/metadata-append-frontier-2026-11-12/README.md)
includes both original CI ZIPs, hashes for every member and an offline custody
verifier. All twelve state-envelope checksums match. This preserves the evidence
beyond GitHub's temporary artifact retention. Additional genuine snapshot-plus-entry,
bounded duplicate-batch and empty-heartbeat controls execute in the separate
two-test run above. The original one-test artifact pair remains unchanged.

## Repair and remaining gates

The separate fix tracks the prefix actually established by the matching
previous entry, an installed snapshot and contiguous accepted request entries.
It acknowledges that frontier and caps follower commit at the same frontier.
It does not truncate an unrelated uncommitted suffix merely to make its
reported tail equal the sender's prefix. The native owned-ACK implementation
must bind this corrected frontier **before** sealing the reply digest.

This is a receiver-side repair. An older receiver can still return the
demonstrated over-ACK; the unchanged wire format does not establish safe rolling
mixed-version operation. Qualification must identify the exact revisions of all
counted metadata replicas.

Mixed-version operation, arbitrary fault schedules, full operational failover,
physical scale-out, acknowledged application-commit preservation and production
qualification are **not tested** by this reproduction. The original failed
Azure qualification and its immutable evidence remain unchanged. Broader
metadata/consumer regressions and the complete versioned candidate checks must
pass before merging the repair. A source merge is separate from a built binary,
a published release and a verified deployment.
