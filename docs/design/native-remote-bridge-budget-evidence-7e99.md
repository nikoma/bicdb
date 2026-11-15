# Remote bridge request budgets: unresolved source7e99 failures

The exact baseline is **1.0.520-beta / `7e99c9c96970b78a44402c975627c64e001adffa`**, tested tree `8022e9f6e6d51f9eb4691ca877c328229b29c3b1`, native job `132883862065` / [run `43737698638`](https://github.com/nikoma/bicdb/actions/runs/43737698638). Its native result was **failed: 396 passed, 55 failed, 1 ignored**. See the [complete regression evidence](native-520-regression-findings.md). This document does not resolve those failures or the original failed Azure experiment.

## Original results

The FIRST projection retains ten original bridge RPC deadline errors at these raw-log lines:

| Original call stage | Raw lines |
| --- | --- |
| `actual_ALL_new_native_bridge_for_Open` | 21547, 23849, 24151 |
| `actual_ALL_native_bridge_before_release` | 24872, 25175, 25478, 25782 |
| `actual_target_recipient_ALL_prepared_native_bridge` | 28264 |
| `actual_bridge_original_native_call_with_genuine_cut` | 28577, 28889 |

Several later Open/release cases failed at their shared bridge prerequisite, not their named final fault. Their elapsed calls were approximately 1.5 seconds under the original 1500 ms network deadline. Counts must come from the actual projection, not from test names or an assumed cause.

For `native_first_remote_bridge_commit_sync_unknown_remains_blocked`, raw line 28577 retains the original `io error: cluster RPC deadline expired` after 1,516,493 microseconds. The original post-call diagnostic at line 28581 has `bridge.actual_native_attempt: null`, no bridge current/archive records and no retained completed bridge owner. Its preparation record is an actual earlier committed XID `23`; it is not evidence that the intended bridge commit-sync fault ran. The assertion requiring an actual original bridge XID failed. The fault-cut guarantee is **not tested by this execution**.

For `native_first_remote_bridge_postcommit_server_cancel_resolves_same_native_xid`, raw line 28889 retains the original RPC deadline error. A later forensic operation at line 28892 separately reports `preparation forensic actual service busy`. That secondary diagnostic must not replace the original error or be treated as a proof that the bridge did or did not enter native IO.

## Source boundary and added diagnostics

`remote_serving_owner.rs::collect_actual_remote_preparations` sequentially obtains a fresh genuine metadata observation and performs an authenticated `ObservePrepared` for every final member, including the local member through its existing direct readonly listener path. It records each actual frame into the original round. The local member is not a fabricated vote or an application callback. After ALL, the bridge obtains a distinct local preparation observation, admits the original bridge operation, publishes native bridge state and performs post-native observation and reply checks.

This call graph is implemented source. The failed baseline does not contain phase timings sufficient to prove which step consumed the request budget. It does not establish a self-queue deadlock, slow storage, a particular peer's delay or a missing quorum. No collection is parallelized, cached, skipped or reauthorized in this diagnostic unit.

New **test-only** `NativeJointPhaseTrace` spans expose the original remaining request budget and elapsed time for:

- The actual bridge owner step and ALL-prepared collection.
- Each final member's fresh metadata acquisition and actual authenticated prepared read.
- Post-ALL local observation, native publication and post-native local observation.

The spans surround existing calls outside service, DB and consensus guards. Early errors leave an incomplete span; successful boundaries mark it complete. They do not renew the original Role/OWN/actor window or turn an identifier into authority. The shared collection spans also apply when the existing bridge readonly consumer calls that function.

The finite coordinator additionally emits the already-retained bounded original error, resolution error and up to eight owner-poll errors after the actual RPC returns and engine guards have unwound. The direct diagnostic and the shared test-only phase trace use fallible locked-stderr writes; an output error is ignored rather than panicking over the original RPC result. The original error is returned unchanged. Production code, mutation ordering, all original assertions, IO-fault counts, per-call deadlines and SDK/Strong tails remain unchanged.

## Classification and next checks

| Finding | Classification | Required evidence |
| --- | --- | --- |
| Original bridge prerequisite calls | Failed | Exact original RPC results above |
| Intended Unknown bridge WAL cut in the selected case | Not tested | No original native bridge attempt was retained; prerequisite did not reach the asserted cut |
| Original postcommit-cancel bridge outcome | Inconclusive | Original RPC error and separate forensic error do not establish entry, commit or cancellation cut |
| Exact phase causing deadline exhaustion | Inconclusive | Execute the new passive spans at the same source selectors and original windows |
| New diagnostics | Not tested | Formatting/source preservation only; no Rust compilation or runtime execution |
| Complete bridge recovery, automatic rebalance or physical HA | Not tested by this unit | Original full native acceptance and independent physical experiment remain required |

The smallest next check is the unchanged native `native_first_remote_bridge_commit_sync_unknown_remains_blocked` selector with these diagnostics, followed by the corresponding actual postcommit-cancel and full bridge/Open/release selectors. A full exact-head native pass remains necessary. Fixing service contention elsewhere does not establish that these bridge prerequisites now fit their original deadline.
