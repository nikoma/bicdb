# CurrentJoint observer batch f58: four Core results

Exact source `f58d14fc8836e5cb14ba5251d4415d5ac47fd5b0`, version `1.0.489-beta`. The original post-joint positive is unchanged from source883. All four Core selections are terminal; **full recovery still fails at first activation**.

| Selection | Run / Core job | Actual result | Scope |
| --- | --- | --- | --- |
| `current_joint_` | 43309668194 / 131502198055 | 5 passed, 0 failed; 75.06 s; 946 filtered | Codec2, actual metadata-role1, original authority/writer1, new real observer-scope cuts1 |
| `native_produced_` | 43309675405 / 131502195020 | 9 passed, 0 failed; 0.79 s; 942 filtered | Existing produced-completion boundary controls |
| Original full positive | 43309683056 / 131502214629 | 0 passed, 1 failed; 102.51 s; 950 filtered | All3 Complete return; first local Runtime activation expires before native decision write |
| `native_activation` | 43309691294 / 131502250718 | 7 passed, 1 failed; 62.07 s; 943 filtered | Legacy7 pass; heavy case persists native decision/catalogue but expires before arm/release |

These are 23 distinct selected tests: 21 pass and 2 fail. They are focused component selections, not a passing broad Core suite. The new observer cut control requires real all-member handoff, target ACK2/StrongR2, genuine CurrentJoint preparation and Completed R2 before actual foreign DB/Store, original cancellation and real hook-replacement checks. It passed those assertions; it does not substitute for the two failing recovery acceptances.

The original-positive all-target/isolated no-default consumer job131502175291 passed. At this receipt snapshot, authority job131502122560's two compile steps passed (job cleanup still live); activation job131502201947's all-target step passed and its consumer was live; producer job131502151515's all-target step was live and its consumer pending. **Unfinished checks are not passed checks**. Exact later terminal receipts may supplement this snapshot without relabeling source or Core results.

## Both heavy cases completed all three Complete calls

The original 4500 ms request/role caps and fixture parent remain unchanged. Values below are the actual Complete total and effective-token time left at return, not renewed budgets.

| Cohort | n1 total / left ms | n2 total / left ms | n3 total / left ms |
| --- | ---: | ---: | ---: |
| Original positive | 2562.274 / 1436 | 3025.721 / 972 | 1899.449 / 2098 |
| Activation heavy prerequisite | 1337.975 / 2660 | 1521.753 / 2476 | 977.889 / 3020 |

The observer path now executes successfully in both original flows, but the different costs must not be extrapolated between runners or described as measured capacity. Nested phase spans are inclusive and tracing overhead remains included.

## Distinct first-activation failures

Both first activations call the **actual local Runtime API**, which obtains fresh actual completion QQ and post-QQ metadata; neither uses a prewritten decision helper.

| n1 activation phase | Original positive ms | Activation-heavy ms |
| --- | ---: | ---: |
| Actual completion quorum | 834.742 | 432.777 |
| Fresh post-QQ metadata role | 11.557 | 10.524 |
| Store preflight | 497.882 | 244.348 |
| Owned completed materialization | 1554.250 | 776.145 |
| Native decision preflight | 1624.065; expires | 1409.227 |
| Native decision WAL | Not reached | 1.699; 1508 ms left |
| Actual current/archive readback under journal | Not reached | 118.672 |
| Full Prepared currentness after dropping journal | Not reached | 521.185 |
| Decision return recheck | Not reached | 118.497 |
| Catalogue fsync | Not reached | 2.578; 746 ms left |
| Full catalogue receipt currentness | Not reached | 986.139; token exhausted |
| Arm/release | Not reached | 0.032; refuses expired token |

The original positive errors at `post_joint_no_data_tests.rs:761` with `recovery-role witness scope changed or expired`, after its native-preflight span exhausts the actual remaining token. It never reaches the new native decision WAL or catalogue activation.

The activation-heavy case reaches the genuine **fresh native decision producer**, returns its receipt with 750 ms left, and fsyncs catalogue NativeActivated with 746 ms left. Its full receipt-current check consumes the remaining token; Runtime arm/release refuses. This exercises the prior fresh producer's explicit journal-drop/currentness path, but not a successful Runtime activation, ACK4, StrongR4, post-ACK Runtime idempotence, or the native producer's existing-same-session retry branch. A persisted NativeActivated phase is not an open/serving grant.

## Retained exact evidence

All files are private under `/tmp/bicdb-native-foundations-ci`. Raw hashes below are hashes of decompressed full GitHub job logs; no engine was opened or replayed.

| Evidence | SHA-256 |
| --- | --- |
| Authority raw `f58-observer-authority-job131502198055.log.gz` | `a300435da8dfaf5a89ca3cd5762969f9a6133ab41d8c64ad6f9680fddaacc209` |
| Producer raw `f58-observer-producer-job131502195020.log.gz` | `b356dd1d65d35aa702a8c54ff8e05854bbff28082cd621d524a1dae439821d99` |
| Original positive raw `f58-observer-positive-job131502214629.log.gz` | `86a30df076d0761893cfcf3a048ed540a45f4454d83357e76bfe75267e3dbcaa` |
| Activation raw `f58-observer-activation-job131502250718.log.gz` | `3fb9169cc0cc345c705282a8facb92f4483ada7294bffe4511b9e0a9ce81b1ea` |
| Positive all-target/consumer raw job131502175291 | `3488bfba0c7068558139b54a7faf68b76eb7c3371662b34eed6f92cb8e32c629` |
| Four-Core summary `f58-observer-four-core-summary.json` | `4afd6a1194e398886b8432999f10783f6425ce1e49a2f49c3c0473044a4d0a84` |
| Authority artifact13985233246 ZIP | `0ed929fdb3df24db1217c24c5e573d14c738be5b4a6821fb038cc285d47a581c` |
| Producer artifact13984584516 ZIP | `594ad013c8f1e23d679824b1d75a9722b32c96fce3104223ba86eca7fe7a5369` |
| Positive artifact13985228435 ZIP | `05bfe3cd60f67090a5d936af1b430e941f6dd24c0468d090175a7f5a1fde88fc` |
| Positive custody manifest | `063fcf152459fb6609c317eaf54b9284dbb835772ba0e968a493fff6095e66ab` |
| Activation artifact13985956572 ZIP | `d3a4900e6f89e039be580c579dc70e8442328986c015d05c33e16c9291b74df3` |
| Activation custody manifest | `6cf8f010dd915937fb82bc45e68ec4e780b97e557cb69269396a8f3964cfff8b` |

Every ZIP digest/source revision was checked. Passing authority/producer artifacts contain logs/source/compiler facts only; no DB custody is claimed for them. The positive manifest verifies 82/82 files, 6,133,320 bytes; all3 current recovery catalogue phases are NativeCompleted. The activation manifest verifies 82/82 files, 6,183,188 bytes; n1 is NativeActivated, n2/n3 NativeCompleted. These are observed-stable copies with live engine owners, not atomic power-loss snapshots or independent native replay. The direct fsync stage timings above are separate evidence from phase presence by later custody capture.

The prior ded3/be85/432fc/883 failures remain distinct and preserved. No rerun, deadline/worker change, production optimization, physical/operator HA, RTO/RPO, Azure cause, binary, release or deployment is claimed by this receipt. Any next activation-scope change must retain actual pre/post-WAL and independently fresh post-catalogue checks, original QQ/role/caller/writer, current/archive bindings and Frozen/pin/slot lifetimes; its result requires new exact-source validation.
