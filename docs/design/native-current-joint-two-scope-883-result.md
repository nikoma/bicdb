# Executed two-scope completion at 883; full recovery still failed

Exact candidate source `883f5f6e1ec30d96b14cb55ab346cf769c9ea693` (`1.0.489-beta`) has three separate Core results. No rerun or deadline extension was used:

| Run / Core job | Selected scope | Actual terminal Core result |
|---|---|---|
| 43288464580 / 131428110439 | `native_produced_` | 9 passed, 0 failed, 929 filtered, 0.61s |
| 43288468790 / 131428118476 | `current_joint_authority_tests` | 1 passed, 0 failed, 937 filtered, 72.11s |
| 43288472362 / 131428140992 | unchanged full post-joint NoData recovery positive | **0 passed, 1 failed, 937 filtered, 65.45s** |

The authority module selector includes only its one real authority/original-writer test. It does not include the two catalogue codecs or the metadata-role test selected by earlier broader `current_joint_` cohorts. Earlier four-test results are not coverage of 883. At this first receipt capture the positive's all-target compile step passed and its isolated consumer was still running; the other compilation jobs were still running. Those statuses must be reported separately from terminal Core results.

All nine producer controls passed, including same-source real role/Store rebinding refusal, real other-range busy refusal followed by guarded observer progress, original QQ cancellation, caller cancellation after fsync and persistence ambiguity/refsync. The other-range case is an RF1 native component control, not stalled-range isolation or concurrent application ACK qualification. CurrentJoint original writer identity is tested; the existing NormalRecovery born-v2 original-hook qualification gap remains separate.

## Direct completion phase evidence

The full test genuinely completed the previous joint release, target ACK2/Strong R2, three fresh CurrentJoint fences, canonical Abort3/native R3, and all three Prepare replies. It then **returned all three Complete RPCs** under the original request/role budgets. Durations are milliseconds; phase measurements are nested and must not be summed as disjoint costs.

| Receiver | Native producer including scope entry/exit | Full refresh including independent scope | Complete total | Original budget remaining on return |
|---|---:|---:|---:|---:|
| n1 | 330.330 | 288.174 | 1571.750 | 2427ms |
| n2 | 351.215 | 288.732 | 2116.626 | 1881ms |
| n3 | 259.507 | 230.827 | 1386.843 | 2611ms |

The native producer scope entries were 125.779/125.155/99.626ms. Their exits were 57.936/58.699/45.232ms. Independent post-CAT scope entries were 137.004/138.464/109.685ms and exits 58.841/57.445/45.776ms. All retain the same 4500ms request and five-second parent, with the existing approximately four-second effective receiver cap. This establishes actual progress through Complete for this run, not a general capacity/RTO guarantee or a passing full scenario.

The test next failed on the FIRST `Runtime.activate_native_recovery` at `post_joint_no_data_tests.rs:761`, with `native range image: native image journal busy`. No returned activation, recovery ACK4 or Strong R4 was reached. All three catalogue phases were NativeCompleted by custody capture. N1 WAL contains two identical R3 CurrentJoint activation-decision JSON fragments (current/archive payload), native xid49, SHA `9b4c237fae7105abfe7c5e2b550bb55f29042446e093ff6c99ab4f04b61de2ad`; both record self-checksums match. N2/n3 have no such fragments. This is read-only WAL fragment inspection, not independent native-root/commit/replay verification, cold reopen or a power-loss snapshot.

## Exact retained evidence

All paths below are private local custody under `/tmp/bicdb-native-foundations-ci`; no engine was opened:

- `883-two-scope-positive-job131428140992.log.gz`, decompressed raw SHA `fe97ce57a2d32102d8d36f98c806cd93ccc7f118b85bdb9f3e3467128bf58ab0`.
- `883-two-scope-controls-job131428110439.log.gz`, raw SHA `a20193bdf3c88208b2af8c5161f39bacf3709cea23e2e49afd67637d52386305`.
- `883-two-scope-authority-job131428118476.log.gz`, raw SHA `9aa0d557ea987b30b411ab1868d24eddd198e2f2fbc77e78c22ebe1133a4ec91`.
- Positive artifact13976687595 ZIP SHA `ec59d7a95782ac0759708b2cefa21e47d184b9de6ff1a47dd52c4dac2287ec51`; manifest SHA `c95d2104e0669ca5ab429f3c6eab1ee820d3206b6ebc0e7a4c1c7370b106623b`. All 82 files / 6,182,672 bytes verified, with the existing observed-stable/live-owner limitation.
- Controls artifact13975663045 ZIP SHA `3a1b76e5f1d31401e0bcda530f1efe0ed04e19cea85beaa69a135b74053efb00`; authority artifact13975283672 SHA `5307725d9fd19a9a0999214d173827c477c7d2f986b1d47878e8570e932894ac`. Both contain logs/source/compiler identity, no DB custody.
- `883-two-scope-positive-phase-profile.json`, SHA `a98f1e62499486bd576656f0fa9c2e8d4b93a5c3289e2c5d343bcc1de42f68ed`, retains inclusive phase data and explicitly qualified fragment inspection.

Earlier fd85 remains its exact failed source with only n1 Complete returned and n2 response uncertainty. Its [portable evidence](../evidence/native-current-joint-refresh-fd85-2026-11-12/README.md) and the older 99/d633 failures are unchanged.

## Source-backed first activation blocker

`db/range_image_native/serving_configuration/activation_decision.rs::persist_native_activation_decision` takes its actual journal guard before reading/writing the protected decision. Both its same-session existing-decision path and fresh post-WAL path then call `prepared.require_current_under_role` while still holding that guard. For CurrentJoint, the full validator invokes the ordinary current-domain factory, whose `current_read_scope` attempts to acquire the same nonreentrant journal again. The try-lock fails busy. NormalRecovery does not take that CurrentJoint factory branch. This is an inherited purpose-specific reentry now reached by genuine recovery; it is not a reason to weaken the ordinary validator.

The smallest proposed correction preserves actual decision WAL and current/archive validation under the real guard, explicitly drops that guard before the unchanged full Prepared/physical/CurrentJoint validation, then revalidates the actual receipt under its own new guard and checks original owner/QQ/caller at return. Both fresh and same-session branches need the same ordering. No timeout, retry, storage ordering, observer validation or fixture acceptance assertion should change. The next source must be independently reviewed and executed; this failed receipt grants no production/physical/operator HA qualification.
