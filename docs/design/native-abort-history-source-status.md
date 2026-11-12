# Native abort-history implementation checkpoint

This is an uncompiled source checkpoint, not experiment evidence. The implementation through `33096b4b9` in `feat/native-no-data-abort-history` is based on `039af8f12`. No Cargo check, test execution, merge, binary, deployment or Azure qualification is attached to this document. The earlier design and failed-prepare evidence retain their original source identities.

The candidate-first source now connects the actual enforced Core admission path to receiver-owned abort recovery:

1. An actual from-birth writer-start is protected in native page WAL before Prepare. A protected, monotone ever-Commit record precedes Commit. The JSON catalogue is a checked mirror, not the only record of a vote.
2. A typed prepare-only rejection retains the original command and mutation lease through the actual Core commit-stack unwind. Core releases real locks and effect buffers before recording its protected no-effect intent. `Aborted` is published to the local transaction observer only after that intent is durable; uncertainty uses `RecoveryRequiredBeforeLocalCommit` without sequence 0.
3. Each permanent promise recipient independently fetches the actual origin intent and obtains fresh same-voter metadata authority. Read-only candidate queries use the existing read-only fenced-observer witness; they cannot expose a mutating role. Promise and resolve requests contain no claimed voter quorum.
4. Distinct authenticated permanent promises seal an abort quorum. The promise applies to the slot. Its original command, candidate and no-effect audits remain immutable even if another uncommitted variant is proposed for the same slot. A new Commit vote remains forbidden.
5. After separately fresh post-quorum metadata, one actual native metadata-WAL transaction appends canonical `Aborted` history and the protected decision/prefix. The typed local data bound comes from the actual predecessor and remains unchanged. No empty application transaction or materialized application-WAL receipt is fabricated.
6. Only the resulting actual native receipt may mark the raw entry resolved. Recovery freeze, permanent promises and normal admission refusal remain until the existing actual Ready, Complete and activation protocol finishes.

The new optional metadata birth field is `abort_protocol_version`. `None` is omitted and preserves earlier serialized bindings; `Some(1)` comes from actual native admission. Eligibility requires enforced birth2, writer1, metadata-initialization capability1, the sticky abort reader fence, and a native-WAL protected range-catalogue StoreID registration. An old prototype cannot acquire eligibility from a flag or a recreated empty catalogue. M1 initial birth offers/configuration require abort1; ordinary older-format open/read behavior remains a separate contract.

Catalogue15 and the existing private protocol23 are implementation fences, not authority. Actual joint14 markers must be preserved on composition; a fresh abort catalogue has no joint marker or joint reader flag. After-joint no-data publication remains unsupported until its actual joint-domain producer is composed and qualified.

## Regression sources, not passing results

| Source filter | Intended actual boundary | Execution status |
| --- | --- | --- |
| `native_abort_actual_enforced_prepare_failure_canonical_metadata_history_then_next_ack` | Fresh enforced RF3 policies before the first attempt; real failed peer Prepare after protected starts; actual Core cleanup; mTLS promises; unchanged per-node B; actual Ready/Complete/local activation; next user ACK | Not compiled or run |
| `native_abort_actual_enforced_post_durable_commit_errors_cannot_become_no_effect_or_abort` | Real peer Commit persistence followed by returned errors; no successful user ACK; observer unknown without local sequence; protected ever-Commit retained; bare Abort refused | Not compiled or run |
| `native_abort_actual_canonical_publication_before_raw_mark_cold_reuses_exact_decision` | Actual durable native Abort before raw marking; all old DB/service/runtime owners gone; strict native and owned metadata reopen; fresh policies/authority; identical native decision/H/B; next ACK | Not compiled or run |
| `native_abort_birth_registration_refuses_missing_or_foreign_range_catalogue_at_r0` | Actual registered R0 birth rejects missing/foreign catalogue; exact original restoration remains distinguishable from reset | Not compiled or run |
| `native_abort_birth_capability_preserves_old_bytes_and_requires_writer_domain` | Metadata data-shape/serialization negative only; no DTO supplies factory authority | Not compiled or run |

The lifecycle source also checks the formerly hidden case of a peer with rawR0/nativeR0 but a protected writer-start at1: it must refuse a healthy empty read observation. Read/Ready construction and revalidation compare actual protected controls with the same Store mirror and immutable native history. Native read catalogue commitments include the range's permanent abort controls.

The cold regression is an orderly full-owner cut with a fault after genuine native publication, not SIGKILL, media-loss or physical host testing. Its helper retains failed DB and private TLS/metadata roots; successful cases clean them. No archive or runtime result exists yet. Existing legacy and earlier Commit-uncertainty regressions are independent and have not been rerun on this source.

## Remaining required work

- Run the first coherent compilation and actual regressions after shared disk/build coordination; retain every first failure and source identity.
- Complete minority permanent-promise A followed by independently proven committed B adoption. Preserve A's promise/no-effect audit, persist B's genuine decision before changing the mirror, and never turn materialization into a new Commit vote.
- Provide an origin-unavailable producer. An unavailable origin must not be replaced by a phantom Core cleanup receipt. A separate closed-next-slot protocol needs actual from-birth never-Commit evidence and current same-voter authority; its canonical absence must not synthesize a business command.
- Implement and exercise lagging-replica canonical-abort decision adoption/gap catch-up, including original budget/cancellation and crash retry.
- Qualify publication ambiguity, mixed-candidate promises, delayed Commit, reopen and compaction interactions on actual services. Fixed permanent-record capacity exhaustion remains explicit refusal, with no unsafe eviction.
- Compose current main, metadata preparation, actual-open/writer guards and joint format changes without losing their mode markers or protections; run relevant broad checks before any main merge/version bump.

The origin-unavailable and already-decided adoption cases remain bounded-self-healing blockers. This checkpoint does not complete the abort protocol or the overall native-host/Azure milestone.
