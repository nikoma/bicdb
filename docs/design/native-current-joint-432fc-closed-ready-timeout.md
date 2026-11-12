# 432fc: third Complete failed before activation

Exact source `432fc9bc5e9f681bb40e3bfe6b07fd043bcdf061`, candidate `1.0.489-beta`, has two terminal failed Core cohorts. No rerun, request/role budget increase, collector deadline change or fixture retry was used. Both all-target and isolated no-default consumer compilation jobs passed.

| Run / Core job | Actual selected scope | Terminal Core result |
|---|---|---|
| 43294492826 / 131449108302 | unchanged full post-joint recovery positive | **0 passed, 1 failed, 939 filtered, 101.66s** |
| 43294496599 / 131449126336 | `native_activation`: seven existing controls plus new heavy Runtime-idempotence test | **7 passed, 1 failed, 932 filtered, 89.94s** |

Both heavy tests failed at the unchanged `post_joint_no_data_tests.rs:742`, the third Complete RPC to n3. The positive reports `closed current native-ready majority unavailable: n2: ... query timeout`; the new idempotence case reports the same failure with n1. The new case stopped inside its unchanged full fresh-path prerequisite, before any CurrentJoint activation or idempotence assertion. Seven existing activation controls passed, including the NormalRecovery native-decision original-audit reuse control. That does not cover CurrentJoint's existing-decision producer branch. No CurrentJoint activation-fix runtime acceptance, recovery ACK4 or Strong R4 is established.

## Direct aggregate phase evidence

Durations are milliseconds from actual phase clocks. Their intervals overlap; do not add nested totals. Request4500ms / original parent5s / effective receiver approximately4s remain unchanged.

| Cohort | Receiver | Complete elapsed | Original receiver budget left | Outcome |
|---|---|---:|---:|---|
| Positive | n1 | 2894.045 | 1104ms | Reply returned |
| Positive | n2 | 3949.512 | 48ms | Reply returned |
| Positive | n3 | 2941.396 | 1056ms | Closed Ready quorum failed; no reply |
| Controls heavy case | n1 | 2819.406 | 1179ms | Reply returned |
| Controls heavy case | n2 | 3589.635 | 408ms | Reply returned |
| Controls heavy case | n3 | 2930.528 | 1067ms | Closed Ready quorum failed; no reply |

The positive's n3 closed-Ready phase began with2859ms left and failed after1803.024ms. The controls heavy case began with2861ms left and failed after1793.808ms. These are **aggregate collector durations**, not measured fixed peer TTLs. There are no exact peer queue/send/receipt, local-observation, receiver-role, receiver-latch or receiver-validation clocks in this source's logs. The final error names only the last retained peer error. Earlier and swallowed local failures cannot be reconstructed from these logs. Remaining parent time does not prove adequate time for the still-required persistence work.

By observed-stable/live-owner custody capture, both fixtures have n1/n2 NativeCompleted and n3 NativePrepared. Both reached the real previous joint release, ACK2/Strong R2, CurrentJoint fences/promises, canonical Abort3/native R3 and three Prepare replies; only the first two Complete replies returned. The 883 cohort remains distinct: it actually returned all three Complete replies, then failed at activation reentry. Neither source's passing gates can be relabelled as an end-to-end result for the other.

## What the source proves, and what remains unknown

`runtime/native_completion.rs::complete_native_recovery` gives protocol selection at most half the original remaining budget. On a Constraint it waits for the **sender-owned** operation workers to drain, then calls `acquire_closed_native_ready_with_operation`. That collector obtains its own fresh role and first performs a synchronous local full observation. It next dispatches at most four remote workers; each actual child slice is `remaining / (waves + 2)` with at most two attempts per peer. Those exact per-attempt amounts were not logged here and cannot be derived from the aggregate1.8s phase alone.

Each readonly remote handler independently obtains an actual observer witness, acquires its service latch and checked DB read, then performs the full actual Prepared/current-domain/materialization/binding validation. NativeCompleted has additional native-serving/current-lineage readback that NativePrepared lacks. These full paths deliberately remain full; no success/absence/Ready may be inferred from the stored phase alone. Source inspection identifies possible repeated work but does not measure its individual cost in this cohort.

A sender worker returning after a child timeout does not prove the remote receiver handler has exited. The sender's5ms operation-drain span is not receiver quiescence. A previous completion-status request could overlap a later Ready status on the same service, but **no local guard contention or receiver overlap has been demonstrated**. Nor does the last query-timeout error distinguish queueing, TLS/network, metadata witness, service/DB/journal contention or deep validation. Cause remains unknown pending correlated phase telemetry. There is no Azure or physical data-loss attribution.

## Retained exact evidence

All files are private under `/tmp/bicdb-native-foundations-ci`; no engine open, checkpoint or repair was performed.

| Payload | SHA-256 |
|---|---|
| Positive raw `432fc-activation-positive-job131449108302.log.gz` (decompressed) | `56fe5679f76e14c2ae90233f16fdca4eda2d5f20c2846153d693e7ab9b381184` |
| Controls raw `432fc-activation-controls-job131449126336.log.gz` | `ced64391a271a9457741b1ea9619a23244fdc81831cc714dafbe5c346a8ccfaa` |
| Positive compile job131449052524 raw | `725dc6f3595cca1772d6e290d693b3122e3ab44a5e0009b4bd89b7b12e47c9dc` |
| Controls compile job131449067989 raw | `dab34de96c0493441b8aa205fa120d3cce673f6cc4c1f02bdc28d929b9b1cdb7` |
| Positive artifact13978373656 ZIP | `73c56cc3ab690ba3958361cadccb0d110671b9ec9d5c8e83d1639b7339287742` |
| Positive manifest | `9a3c7496e4e2d246b6ad17abe0192b79b5a34aae7f310c1ee48fa3dec93aae18` |
| Controls artifact13977774837 ZIP | `5a0144b768520898ceffc6d575382e8194a07eebe12a00156b61a9cb9cb65ca1` |
| Controls manifest | `05fe114b0d4529e1ce367e12e028aa3b1fdbd6d9b104592fba091f215fff3f87` |

The positive manifest verified82/82 files,6,100,203 bytes; controls verified82/82 files,6,100,336 bytes. Copied files were observed stable with live engine owners; no atomic power-loss snapshot or independent replay was verified. Each ZIP has its own receipt. `432fc-activation-final-summary.json` retains exact source/jobs, selected test names, inclusive completion events, raw hashes and these limitations. Raw databases/keys/TLS are not published by this document.

## Small deterministic deadline test, separate from cost diagnosis

The existing actual mTLS `NetworkFixture` / `ObservedTlsTransport` in `rf3_fixture/native_historical_tls.rs` supplies real NativeReady observations and a return/delivery barrier pattern. A small separate control can use real local R1 plus a real clean-R0 minority, leaving one real matching-R1 peer necessary for the quorum. For BOTH bounded attempts, obtain that peer's genuine TLS observation, hold only its return until the **actual existing child token** expires, then return the original token's timeout. Do not manufacture a reply, proof, voter set, new deadline or receiver counter. RAII cleanup must release a barrier after error/panic.

Capture the actual request challenge/scope, attempt, queue/send/return times, child deadline and still-live parent at refusal. The round must refuse an unavailable matching majority without counting the expired response or changing any actual raw/native/UID/row/freeze fact. Check actual service/DB availability while the held response is outside their latches, so the schedule isolates deliberate delivery delay. This establishes deadline behavior only; it cannot explain432fc cost and must not replace the unchanged full positive. A later independently authorized recovery round may be tested separately, with old observations still expired and no old-TTL renewal.

The immediate diagnostic batch must instrument the original path: collector fresh role and synchronous local result (including swallowed errors), each actual peer queue/start/send/return/receipt/record/attempt and actual child/parent remaining time, plus receiver operation entry/exit, fresh readonly role, service/DB acquisition, native-domain/materialization/binding validation and final projection. Completion-status receiver entry/exit is also needed to establish or refute overlap after sender drain. Preserve production logic and all original deadlines; diagnose before proposing a change to allocation, worker count or physical validation.
