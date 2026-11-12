# Native abort-history implementation checkpoint

This is an uncompiled source checkpoint, not experiment evidence. The implementation through `08cc1b567` in `feat/native-no-data-abort-history` is based on `039af8f12`. No Cargo check, test execution, merge, binary, deployment or Azure qualification is attached to this document. The earlier design and failed-prepare evidence retain their original source identities.

The candidate-first source now connects the actual enforced Core admission path to receiver-owned abort recovery:

1. An actual from-birth writer-start is protected in native page WAL before Prepare. A protected, monotone ever-Commit record precedes Commit. The JSON catalogue is a checked mirror, not the only record of a vote.
2. A typed prepare-only rejection retains the original command and mutation lease through the actual Core commit-stack unwind. Core releases real locks and effect buffers before recording its protected no-effect intent. `Aborted` is published to the local transaction observer only after that intent is durable; uncertainty uses `RecoveryRequiredBeforeLocalCommit` without sequence 0.
3. A distinct typed uncertain-admission handle keeps the actual normal mutation lease until Core releases its real row locks and effect buffers, even when a buffered failed transaction remains alive. It emits neither a no-effect intent nor `Aborted`; possible Commit decisions remain recovery-required before any local sequence.
4. Each permanent promise recipient independently fetches the actual origin intent and obtains fresh same-voter metadata authority. Read-only candidate queries use the existing read-only fenced-observer witness; they cannot expose a mutating role. Promise and resolve requests contain no claimed voter quorum.
5. Distinct authenticated permanent promises for the exact original request seal an abort quorum. The promise applies to the slot. Its original command, candidate and no-effect audits remain immutable even if another uncommitted variant is proposed for the same slot. A new Commit vote remains forbidden.
6. After separately fresh post-quorum metadata, one actual native metadata-WAL transaction appends canonical `Aborted` history and the protected decision/prefix. The typed local data bound comes from the actual predecessor and remains unchanged. No empty application transaction or materialized application-WAL receipt is fabricated.
7. Only the resulting actual native receipt may mark the raw entry resolved. Recovery freeze, permanent promises and normal admission refusal remain until the existing actual Ready, Complete and activation protocol finishes.

The new optional metadata birth field is `abort_protocol_version`. `None` is omitted and preserves earlier serialized bindings; `Some(1)` comes from actual native admission. Eligibility requires enforced birth2, writer1, metadata-initialization capability1, the sticky abort reader fence, and a native-WAL protected range-catalogue StoreID registration. An old prototype cannot acquire eligibility from a flag or a recreated empty catalogue. M1 initial birth offers/configuration require abort1; ordinary older-format open/read behavior remains a separate contract.

Catalogue15 and the existing private protocol23 are implementation fences, not authority. Actual joint14 markers must be preserved on composition; a fresh abort catalogue has no joint marker or joint reader flag. After-joint no-data publication remains unsupported until its actual joint-domain producer is composed and qualified.

## Regression sources, not passing results

| Source filter | Intended actual boundary | Execution status |
| --- | --- | --- |
| `native_abort_actual_enforced_prepare_failure_canonical_metadata_history_then_next_ack` | Fresh enforced RF3 policies before the first attempt; real failed peer Prepare after protected starts; actual Core cleanup; mTLS promises; unchanged per-node B; actual Ready/Complete/local activation; next user ACK | Not compiled or run |
| `native_abort_actual_enforced_prior_leader_no_effect_fresh_e2_abort_then_new_leader_ack` | Four genuine other-range ACKs and selected ACK1; failed attempt2; actual committed E2/new leader and fresh host policies; NoData2 keeps B>R; actual recovery/activation; ACK3 and original ACK1 payload survive | Not compiled or run |
| `native_abort_actual_enforced_post_durable_commit_errors_cannot_become_no_effect_or_abort` | Real peer Commit persistence followed by returned errors; no successful user ACK; observer unknown without local sequence; protected ever-Commit retained; buffered transaction cleanup releases actual row locks while preserving Unknown; bare Abort refused | Not compiled or run |
| `native_abort_actual_canonical_publication_before_raw_mark_cold_reuses_exact_decision` | Actual durable native Abort before raw marking; all old DB/service/runtime owners gone; strict native and owned metadata reopen; fresh policies/authority; identical native decision/H/B; next ACK | Not compiled or run |
| `native_abort_birth_registration_refuses_missing_or_foreign_range_catalogue_at_r0` | Actual registered R0 birth rejects missing/foreign catalogue; exact original restoration remains distinguishable from reset | Not compiled or run |
| `native_abort_birth_capability_preserves_old_bytes_and_requires_writer_domain` | Metadata data-shape/serialization negative only; no DTO supplies factory authority | Not compiled or run |

The lifecycle source also checks the formerly hidden case of a peer with rawR0/nativeR0 but a protected writer-start at1: it must refuse a healthy empty read observation. Read/Ready construction and revalidation compare actual protected controls with the same Store mirror and immutable native history. Native read catalogue commitments include the range's permanent abort controls.

The cold regression is an orderly full-owner cut with a fault after genuine native publication, not SIGKILL, media-loss or physical host testing. Its helper retains failed DB and private TLS/metadata roots; successful cases clean them. No archive or runtime result exists yet. Existing legacy and earlier Commit-uncertainty regressions are independent and have not been rerun on this source.

## Remaining required work

- Run the first coherent compilation and actual regressions after shared disk/build coordination; retain every first failure and source identity.
- Wire automatic native host/coordinator recovery scheduling. The existing `recover_local_leadership` raw-tail path does not invoke this new Runtime route. Current lifecycle regressions explicitly orchestrate actual protocol APIs; their activation is the trusted local API, not an incoming authenticated activation wire endpoint. Automatic/cross-process self healing is not established.
- Complete minority permanent-promise A followed by independently proven committed B adoption. Preserve A's promise/no-effect audit, persist B's genuine decision before changing the mirror, and never turn materialization into a new Commit vote.
- Provide an origin-unavailable producer. An unavailable origin must not be replaced by a phantom Core cleanup receipt. A separate closed-next-slot protocol needs actual from-birth never-Commit evidence and current same-voter authority; its canonical absence must not synthesize a business command.
- Implement and exercise lagging-replica canonical-abort decision adoption/gap catch-up, including original budget/cancellation and crash retry.
- Qualify publication ambiguity, mixed-candidate promises, delayed Commit, reopen and compaction interactions on actual services. Fixed permanent-record capacity exhaustion remains explicit refusal, with no unsafe eviction.
- Compose current main, metadata preparation, actual-open/writer guards and joint format changes without losing their mode markers or protections; run relevant broad checks before any main merge/version bump.

The origin-unavailable and already-decided adoption cases remain bounded-self-healing blockers. This checkpoint does not complete the abort protocol or the overall native-host/Azure milestone.

## Integrated source review: sustained-operation blockers

The following findings are **source inspection**, confirmed in the integrated
batch at `1b191d613aa8884fcda852795c36b6cfb9fd5f55`. They are not a measured
throughput result or an explanation of the original Azure slowdown. First
integrated test compilation stopped on a moved test variable; source
`be1345d3cfeed7b416a9e60af6547633b9b30114` corrects that error, and its
[independent CI run](https://github.com/nikoma/bicdb/actions/runs/43162977062)
was still running at this observation. No abort regression pass is claimed.

| Finding | Classification | Exact source contract and smallest next control |
| --- | --- | --- |
| Permanent successful-write capacity | Failed against sustained-operation requirement; dynamically not tested | `distribution_range_consensus.rs` calls `begin_native_abort_attempt` before Prepare for every abort1 command. Both the mirror and native journal cap permanent slots at 4,096 per range; normal raw-log compaction and epoch changes do not reclaim them. Run 4,096 genuine ACKs and attempt 4,097 on an enforced fixture, preserve the complete ledger and verify exact refusal/history after strict reopen. Do not seed counters. |
| Database-wide retained byte capacity | Failed against sustained-operation requirement; dynamically not tested | `db/range_image_native/abort_history/slot_journal.rs` limits the sum of encoded full slot records across all ranges to 64 MiB, each slot to 3 MiB and the inventory record separately to 8 MiB. Other no-effect/promise/decision records and WAL are outside that byte sum. This source cannot ingest an 8 GiB workload through ordinary enforced writes. Increasing constants is not a durable long-run design. |
| Restrictive closure headroom | Inconclusive dynamically; confirmed missing reservation in source | Admission charges the current encoded slot size. Replacing `null` with a 64-character no-effect or promise hash adds about 62 bytes per field, so an admitted start near the aggregate limit can lack space for later restrictive closure. Use actual large acknowledged commands, then genuine failed Prepare/Core cleanup near the bound; retain the earliest refusal and check actual no-effect/promise publication. |
| Point-read/Ready verification cost | Not tested for performance; growing work confirmed in source | `require_actual_abort_controls_covered` loads and validates full commands for all ranges before selecting the requested range; construction and revalidation repeat this. Characterize actual read work and memory on an unchanged range while genuine ACK history grows elsewhere. No capacity-gain claim follows from the source complexity. |

Required design work remains unimplemented: publish a protected monotone
closed-slot checkpoint before pruning, permanently reject new votes below it,
retain immutable outcomes and audit commitments, preserve genuine committed
decision adoption and lagging-replica catch-up, and reconcile older JSON with
the protected checkpoint across interrupted checkpoint/truncation/reopen.
Read validation needs an actual per-range commitment bound to the current
open/generation. New starts must reserve terminal restrictive-control space.
Unsafe eviction or a larger hard limit does not satisfy these obligations.

Keep the six existing functional regressions and these capacity/closure
controls separate. Until both are resolved, the candidate is not suitable for
the proposed sustained scale-out experiment. Do not shrink the workload to
conceal these limits or label a bounded demonstration production qualification.
