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
| Retained-state baseline `c295916d0030f3fbd37962150cfd1f65dc70e752`, [run 43173136830](https://github.com/nikoma/bicdb/actions/runs/43173136830) | Not tested at this documentation checkpoint | Test custody improvement only; runtime remains unfixed. Inspect its terminal result and archived state before updating this classification. |
| Fixed candidate `e0bb370fb1bad2cddc783b1b8f98775735281c05`, [run 43173148741](https://github.com/nikoma/bicdb/actions/runs/43173148741) | Not tested at this documentation checkpoint | Source fix is prepared; execution was still in progress. A passing result would cover this deterministic metadata schedule, not production HA. |

## Repair and remaining gates

The separate fix tracks the prefix actually established by the matching
previous entry, an installed snapshot and contiguous accepted request entries.
It acknowledges that frontier and caps follower commit at the same frontier.
It does not truncate an unrelated uncommitted suffix merely to make its
reported tail equal the sender's prefix. The native owned-ACK implementation
must bind this corrected frontier **before** sealing the reply digest.

Mixed-version operation, arbitrary fault schedules, full operational failover,
physical scale-out, acknowledged application-commit preservation and production
qualification are **not tested** by this reproduction. The original failed
Azure qualification and its immutable evidence remain unchanged. Broader
metadata/consumer regressions and the complete versioned candidate checks must
pass before merging the repair. A source merge is separate from a built binary,
a published release and a verified deployment.
