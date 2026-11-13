# Isolated eedf observer profile

Executed source `eedf23d58e236cf62d61366349ad4de3bd52373b`, version `1.0.489-beta`; run43322376572, Core131546115196. Exact selector `native_current_joint_activation_scope_actual_postcat_missing_decision_archive_refused`: **1 passed, 0 failed, 0 ignored, 956 filtered; 71.83 s**. All-target and isolated no-default consumer checks passed (131546072586). The actual post-catalogue missing-decision-archive cut was reached and refused with its existing frozen/data assertions intact.

This is a **new isolated-selection experiment**: one selected test, original `--test-threads=4`, unchanged source/environment/deadlines. The [earlier four-selected cohort](native-ready-observer-inner-result-eedf.md) remains **0 passed / 4 failed at StrongR2 before CurrentJoint observation**. No number of simultaneously active fixtures, concurrency cause, fix of those Strong failures, broad candidate qualification or capacity is established by this pass.

The raw log contains **1847 probe events, 12 actual observer contexts and 652 paired spans**. [Compact per-logical-RPC data](native-ready-observer-isolated-eedf-result.json) retains all tag counts/totals keyed by actual phase, node, challenge and session. Every context has 13 instrumented identity calls: **6 inside entry, 6 inside exit, and 1 between entry/exit in the callback**. Each includes a full-writer, owned-native-pin and native-fields subspan. These are measured tagged calls, not a count of every possible nested admission/writer operation.

## Representative actual RPC

Receiver n2, completion-status challenge `01a1d1f4-2b2c-79f0-a827-ceb6fe55ac5f`, session `01a1d1f3-eaef-7822-8fa8-c0e7b44406ff`. Its validation spans complete; the sender later reports cancellation after the collector obtains another peer, so it is not labeled a counted observation here.

| Boundary, ms | Inclusive | Disjoint immediate children only | Unattributed residual |
| --- | ---: | ---: | ---: |
| Entry | 232.663 | 232.419 | 0.244 |
| Exit | 119.471 | 119.311 | 0.160 |

Residual subtracts only immediate, temporally disjoint child spans; it includes untagged checks, clock and logging overhead. It is not pure CPU cost. Nested grandchildren are never added again.

Within entry, six tagged identity writer checks total 11.188 ms and six pin checks 7.730 ms. Within exit, six writer checks total 11.346 ms and six pin checks 7.682 ms. Constructor writer validation separately takes 2.345 ms. These tagged writer checks account for **24.879 / 352.134 ms, about 7.1%** of entry+exit, not the dominant measured cost. The callback's thirteenth writer/pin/native-fields checks take 1.868 / 1.280 / 0.133 ms separately and are not charged to either boundary.

The largest separately timed constructor operation is actual lineage construction, **106.697 ms**. Constructor full physical validation takes 44.480 ms; independent exit actual-lineage validation takes 50.968 ms. Joint-fact construction 45.452 ms, constructor prepared binding 29.946 ms, exit joint/saved binding 31.446 ms, and exit prepared binding 30.111 ms are **inclusive parents** containing identity/readback work. The two serving-current reads total 42.434 ms; their archive reads/validation total 0.573 ms. These totals must not be summed with their enclosing prepared/joint spans. Untagged nested work remains inside its measured parent.

Other completed contexts remain distinct: entry spans range 146.160–242.067 ms and exits 64.575–128.865 ms, with the same 6/6/1 identity count. The original Complete calls return on n1/n2/n3 in 1967.072 / 2367.020 / 1489.388 ms with 2031 / 1631 / 2508 ms left respectively. Some unnecessary peer senders are canceled after another observation suffices; successful receiver spans are not automatically counted votes. No production check was removed, no original authority renewed, and no timeout/worker policy changed.

## Evidence

Private root `/tmp/bicdb-native-foundations-ci`; complete hashes and per-RPC data are in the linked JSON. Core raw decompressed SHA-256 `cbc81a64dad74b4416613b39250083911c52b06aa0c00f67eadfdcaf81660e8b`; compile raw `5d31ba95c97d7861759c644a6af300818794b2e2a58ae3a28038b473307b6e02`. Full profile SHA `afc5c48683dbac323f1f4b46cf7627bb76007711de71b9bed134515e68729a14`; disjoint-boundary analysis v2 `67b18c7ad0a0e2c96b3ed5fada5c9705cab21df208b512edbee5a4617cd95bc0`; final receipt `6f23953a6fd847256ca75c6d8bf45e0db54a95c50c625c2e9a49697161590ca6`.

Artifact13991501860 ZIP/source verified, SHA `1c39d93e4032d25dd65d0ef9c022a4b56da0b8e59a375348f04decba2a862182`. It contains only four log/source/compiler files, **no DB custody**. No engine was opened or replayed. This measurement does not authorize redundant-check elimination, explain the earlier StrongR2 failures, or qualify automatic physical/operator HA.
