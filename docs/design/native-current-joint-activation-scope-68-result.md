# CurrentJoint activation scopes: exact68 terminal results

Exact source `68be040ebb00a4c606d519697f913e310c496812`, source version `1.0.489-beta`. The original full acceptance child remains unchanged from source883. All three workflows are terminal **failure**; every all-target check and isolated consumer without Core defaults passed. The focused tests total **10 passed and 3 failed**. This is not a broad Core pass or completed recovery acceptance.

| Selection | Run / Core job | Actual result | Boundary |
| --- | --- | --- | --- |
| `native_current_joint_activation_scope_actual_` | 43315433251 / 131522161919 | 3 passed, 1 failed; 952 filtered; 90.87 s | Three actual fault controls pass; missing-archive control fails its Complete prerequisite before injecting its fault |
| Original full positive | 43315439379 / 131522178395 | 0 passed, 1 failed; 955 filtered; 83.58 s | All3 Complete and native activation/arm-release succeed; n3 final result observation refuses its domain binding |
| `native_activation` | 43315446340 / 131522192672 | 7 passed, 1 failed; 948 filtered; 66.97 s | Legacy7 pass; heavy prerequisite reaches the same n3 result-observation refusal before ACK4/idempotence |

The passing adversarial controls exercise actual post-catalogue foreign DB/Store refusal, actual writer replacement, and original-caller cancellation followed by a distinct fresh exact-audit recovery. The fourth control, `native_current_joint_activation_scope_actual_postcat_missing_decision_archive_refused`, fails at `activation_scope_cut_tests.rs:105`: Complete(n3) returns an error reply reporting closed current native-ready majority unavailable, with n2 timing out. Its archive deletion and intended assertion are **not reached**. The actual closed-Ready collector lasts 1777.016 ms and returns with 1068 ms of its parent left; this is an earlier peer-budget failure, not evidence that the missing archive was accepted.

## Actual heavy-flow progress

The original request caps, caller cancellation, worker logic, and all acceptance assertions are unchanged. Each cell is an actual phase duration / effective-token time remaining in milliseconds. Nested spans are inclusive and include tracing overhead; they are not additive independent costs.

| Actual phase | Original positive n1 | n2 | n3 |
| --- | ---: | ---: | ---: |
| Complete returned | 1951.004 / 2047 | 2242.396 / 1755 | 1454.626 / 2543 |
| Activation catalogue fsync | 3.802 / 3448 | 4.056 / 3383 | 3.371 / 3317 |
| Arm and serving release succeeded | 347.999 / 2750 | 348.812 / 2685 | 283.138 / 2751 |
| Result observation | 514.356 / 2236, success | 529.800 / 2155, success | 156.696 / 2594, **refusal** |
| Runtime activation total | 2263.402 / 2236, success | 2343.335 / 2156, success | 1904.129 / 2595, **error** |

| Actual phase | Activation-heavy n1 | n2 | n3 |
| --- | ---: | ---: | ---: |
| Complete returned | 1443.405 / 2555 | 1726.619 / 2271 | 1098.222 / 2900 |
| Activation catalogue fsync | 3.368 / 3706 | 3.388 / 3669 | 3.007 / 3494 |
| Arm and serving release succeeded | 257.598 / 3191 | 254.996 / 3156 | 218.219 / 3033 |
| Result observation | 379.215 / 2812, success | 389.045 / 2767, success | 119.021 / 2914, **refusal** |
| Runtime activation total | 1687.641 / 2812, success | 1718.581 / 2781, success | 1584.033 / 2915, **error** |

Both heavy cases fail at `post_joint_no_data_tests.rs:761` with `activated completion anchor differs from actual current same-voter native domain`. This is a semantic error after actual arm/release, with time left, not a timeout. The guard in `db/range_image_native/serving_configuration.rs` combines serving/scope/base/index/history/schema/restore/range checks and observer credentials against physical base members. The existing trace does not identify the exact failed predicate. A physical-versus-protected-effective membership mismatch is a source-grounded investigation, not yet a measured field-difference conclusion or permission to weaken identity checks.

Both retained heavy catalogues contain NativeActivated for n1/n2/n3, consistent with the direct successful fsync/arm traces. Nevertheless, the n3 Runtime call returns an error. Neither case reaches ACK4, StrongR4, or post-ACK Runtime idempotence. The seven passing legacy controls do not qualify those missing gates.

## Comparison with preserved f58 failures

[The separate f58 receipt](native-current-joint-observer-f58-result.md) remains unchanged. Its original positive expired during the first n1 native-decision preflight, before native decision WAL. Its activation-heavy case persisted n1 native decision and catalogue, then exhausted its token in full receipt-current validation and refused arm/release. Exact68 reaches all three native decision writes, catalogue fsyncs, and arm/releases before a different semantic refusal. These are distinct source-specific outcomes; the timings do not establish capacity, an RTO, or a cross-run speed guarantee.

## Retained evidence

Private files are under `/tmp/bicdb-native-foundations-ci`. Raw hashes are SHA-256 of the full decompressed GitHub job logs; each has a corresponding `-index.json`.

| Evidence | SHA-256 |
| --- | --- |
| `68-activation-scope-cuts-job131522161919.log.gz` | `4bac02ec4c15055200c18d4525f176e762091373f5a34fc767e284198ade1bd9` |
| `68-activation-scope-positive-job131522178395.log.gz` | `ba35ec993c9c7b7193ac0b98a03259c26e015a292dacbb897cee16f281c7361e` |
| `68-activation-scope-legacy-job131522192672.log.gz` | `db89e6c4ea8ad00143c661b1ea235ea271723231356a0598e67d4ace17d80738` |
| Cuts all-target/consumer raw job131522112990 | `ce794f53a8f17b4f6ca06387aaba1cbb10914c710e1a60dc1e0724e6c22c206e` |
| Positive all-target/consumer raw job131522131678 | `99d091399a0fdb7a65e64a8fedd2ebbf56a44e814bba62f2e88031e1cae7b39e` |
| Legacy all-target/consumer raw job131522154650 | `265a03e8e25e803ad4ff145fcd374a2390e09866a658d19d1a34098859315873` |
| Cuts artifact13987897504 ZIP | `175a3c980472396b865a12da2944d2f0eb6cc0323ae5e7caf771010f0b63ceb4` |
| Cuts custody manifest | `3d4cd3ed81912a8e40456870b57e202c480e80ce80414cbe7340ed42e7affb8f` |
| Positive artifact13987753163 ZIP | `a4fc58794dee7735dd090642557d52a8e8000fbfe58641b75194a09f280dfe03` |
| Positive custody manifest | `5b11944e8305321aa4e75688c7694881d9b1ab816ad0a9296ab420af66ac0cde` |
| Legacy artifact13988626543 ZIP | `c02c3ba4267140aabbed7ecb6eee79922fd8cc629b4d4109e89a07f80916a7eb` |
| Legacy custody manifest | `2fdf0120f5e0b55c81aea46ba52e9ea7fa7508928732890c1baaf7a6a3d7dbde` |
| `68-activation-scope-final-summary.json` | `fe971d432ac0db237418790d3526816b1b9d6ff58490608c1cbed335ee057cde` |

Each ZIP's source revision and reported digest were verified, then every custody file was checked against its manifest without opening an engine. Cuts custody verifies 78/78 files, 5,048,305 bytes; n1/n2 are NativeCompleted and n3 NativePrepared. Positive verifies 82/82 files, 6,282,525 bytes; legacy verifies 82/82 files, 6,282,658 bytes; both have all3 NativeActivated. These are observed-stable copies with live engine owners, not atomic power-loss snapshots or independent native WAL replay. No timeout change, rerun, new production implementation, binary/release/deployment, automatic operator HA, data RPO, or Azure causality is claimed.
