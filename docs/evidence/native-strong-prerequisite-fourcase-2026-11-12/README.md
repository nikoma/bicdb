# StrongR2 prerequisite: unchanged four-case diagnostic failure

Exact source **`7b55059fd610b40189afd239cf8e9ea7b2986ad4`**, package **1.0.490-beta**, DATA protocol **28**. One manual dispatch, [run 43330054124](https://github.com/nikoma/bicdb/actions/runs/43330054124), used the unchanged `native_current_joint_activation_scope_actual_postcat_` filter, four test threads and original deadlines. **Core job 131572255168: 0 passed / 4 failed / 0 ignored / 955 filtered, 78.27s.** Job 131572220029 passed the workspace all-target check and isolated workflow consumer without Core defaults. No local build or rerun was performed.

All four failures occur in the actual **StrongR2 prerequisite**, after the fixture's protected joint handoff/all-three release and selected-range ACK2, before the subsequent CurrentJointRecovery/activation-cut continuation. The requested cuts were **not exercised**. This does not invalidate or relabel the separate earlier eedf 2+1 positive tests. It also does not waive the original eedf four-case 0/4 gate.

| Test suffix | Actual returned failure | Requested postCAT cut |
| --- | --- | --- |
| `writer_replacement_refused` | DataQuorum: matching current-voter majority unavailable; n1/n2 query timeouts | Not reached |
| `caller_cancel_and_exact_fresh_recovery` | Same DataQuorum refusal | Not reached |
| `foreign_db_and_store_refused` | Same DataQuorum refusal | Not reached |
| `missing_decision_archive_refused` | Caller Canceled: query timeout | Not reached |

All panic at `native_strong_point_tests.rs:475`, the original `observed()` acceptance. `result.json` retains full test IDs and exact errors. The actual fixture performed image transfer/handoff through local opaque owners and real mTLS calls; these are not independent processes or an operational relocation controller. Its explicit writer refreshes remain fixture preconditions.

## What the new traces establish

The raw job contains **802 `NATIVE_STRONG_TRACE` events / 4 business IDs** and **zero `NATIVE_PROBE_TRACE` events**. The latter recovery diagnostic contexts were never entered. Copied business/challenge/node/deadline data correlate the actual Strong path without manufacturing a recovery scope. Clock values are process-local, inclusive span durations; a span end is neither success nor Operation drain. These measurements are from this fixture/run, not production or physical performance results.

There are 19 actual probe attempts: four local, fifteen remote. Five returned observations (four local plus one n2 reply), thirteen returned query timeout, and one remote attempt was canceled after the collector had a majority. All recorded observed points were actual **R2**. The target's local **B2** and the successful survivor's local **B4** are distinct facts, not a conversion from R. All four original caller parents were uncancelable in this fixture; the consuming read retained the finite minimum of request budget (5s) and original first-MQ deadline (about 4s).

Actual first remote child allowances were about **1.31–1.32s**; retries about **0.87–0.88s**, inside the unchanged original proof deadline. Three data-quorum failures returned with approximately **1.01s, 1.37s and 1.37s** remaining on that original proof. They exhausted the existing two attempts without a counted matching remote reply; that unused time is not proof that a longer slice would solve the complete protocol. The sender already caps the wire request against the child token; no missing wire-budget bug is claimed.

| Observed stage | Actual end count | Minimum / median / maximum |
| --- | ---: | --- |
| DB acquisition | 19 | 13 / 14 / 21 µs |
| Service acquisition, including retries | 19 | 1 / 3 / 382,228 µs |
| Current native lineage | 19 | 226,450 / 460,906 / 706,420 µs |
| Protected abort controls | 19 | 227,484 / 474,029 / 684,862 µs |
| Native physical observation | 19 | 2 / 218,473 / 668,970 µs |

The physical range includes immediate cancellation/refusal boundaries; its minimum is not a successful full validation cost. First remote service waits were 2–4 µs. Some retry service waits overlapped still-running earlier request validation; the receiver retains its actual service guard during that validation. This is evidence of measured request work and some retry waiting, not attribution to test concurrency, network, storage hardware or a particular inner check. The new spans do not separate every nested lineage/history/abort validation.

One business, `01a1d224-4cf0-7d32-a8c1-66c693e7fc40`, obtained actual n2 + local matching R2 observations. Its ancestry/final round validation took **1,737,100 µs**; the whole data-quorum stage took **3,107,072 µs**. The snapshot then took **702,462 µs**, final MQ **30,570 µs**, and final-currentness stage **173,543 µs**. The caller timed out; its worker returned FinalCurrentness timeout. No value/absence was published. These stages are nested where indicated and must not be summed indiscriminately. Three other businesses failed DataQuorum; the trace does not supply a reliable individual mapping from those three business IDs to the three identically failing test names, so none is invented.

## Retention and limits

`core.log.gz` and `compile.log.gz` are complete raw GitHub job logs. Their uncompressed SHA-256 hashes are respectively:

- `fcd10953ffa9e5459fc5825e657e6d96dc03a7f6c18acbb1c2626d1abae18c0c`
- `c164fee0e8003aebb3f68edd6f0819cfbb6270d33d534b1917c08f05c2945c59`

The private native-failure artifact is **13995525792**, ZIP **1,065,617 bytes**, SHA-256 **`87ac6582d5e2483fc6aca73fe712d5d1ed0c20ee28035693c6c595d227c4e912`**. Four exact-source manifests verify **300/300 files, 18,711,064 bytes**, including hidden lock files. Each selected-range ledger contains two records. All sixteen retained raw catalogue envelopes pass their checksums; their JSON observations are retained separately. These are observed-stable copies with engine owners alive, not atomic power-loss snapshots, native-page decoding, engine reopen, replay or postfailure ACK reconciliation. Native mesh signing files may be present in the restricted ZIP; the ZIP and database trees are deliberately absent from this public packet. TLS-private root exclusion is the manifest's explicit capture boundary, not a claim that all copied DB files are public.

Private custody path: `/tmp/bicdb-native-foundations-ci/7b5-strong-prerequisite-fourcuts-artifact13995525792.zip`. Public metadata, raw logs, source/rustc identity, selected-test list and deterministic trace summaries are included here. EOF/BrokenPipe diagnostics remain in the complete log; no causal interpretation or zero-error claim is made.

At receipt capture, Core and compile jobs were terminal; the overall workflow remained **in progress** because Formatting job 131572220036 was still in `actions/checkout@v4`, before rustfmt. `workflow-status.json` records that boundary. Do not describe that pending check as failed or passed.

Run `python3 verify.py --repo /root/bicdb --self-test` from this folder. Optional `--private-zip PATH` verifies the retained ZIP and every manifest payload using bytes only, without opening an engine. The verifier establishes packet integrity and these bounded result assertions, not full-source reconstruction or qualification. No production code, deadlines, test concurrency, assertions or retry policy changed for this diagnostic run. Native read availability, the complete four-case gate, operator/autonomous recovery and the full 8 GiB/4→5 physical goal remain **unqualified**.
