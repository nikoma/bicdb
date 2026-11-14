# Native range movement: remaining operational gates

This inspection covers candidate **1.0.510-beta** at
`d8115fedf13b0bd7107cfddd2664f2c6ea2f261f`. It describes source integration,
not an executed expansion or a qualified deployment. The original failed Azure
experiment remains unchanged. Generic balanced distribution and native protected
range movement have distinct admission contracts; success in one cannot prove
the other.

The [archived reports and source pins](../evidence/native-operational-source-audit-2026-11-14/manifest.json)
record the exact paths, blobs, observations and acceptance proposals. The
manifest records a remaining wording correction without rewriting either
original report.

| Boundary | Current source finding | Classification / next proof |
| --- | --- | --- |
| Native node enrollment | `distribution_native_host/operator.rs` and `operator_learner.rs` implement metadata enrollment and promotion. Their maintenance path does not attach a native range-placement worker. | **Incomplete integration.** Supported ordinary startup must create and drive a committed native relocation without fixture-only calls. |
| Initial source lifetime | Registration retains the actual process and starts `NativeInitialDataHost::start_hot_process`, which binds the original source process. Authenticated source discovery and frame protocols exist. | **Implemented source path; operational qualification not tested here.** An earlier claim that production source binding was absent was incorrect and withdrawn. The separate explicit bind helper having fixture-only callers did not prove that claim. |
| A joined node receives another range | `registration/joined_data/receive` has actual history, rows, final-image, base and selected-catalogue machinery. The inspected entry points lack the normal-process callers needed to complete the second move. | **Incomplete integration.** The same joined host must keep its first range serving while receiving the second, using original authority and authenticated frames. |
| Joined quorum and range-membership decision | Joined observation and exclusion types exist. The observation/remote participant dispatch lacks the Joined consumer and concrete quorum-bound native decision publisher and decision-catalogue adoption. Physical image/base catalogue-floor adoption already exists. | **Incomplete integration.** Retain the original attempt, transaction ID, exact candidate bytes and unknown outcome through failure and retry. A materialized image is insufficient decision evidence. |
| Activation and serving | Same-writer range-domain continuation exists. Joined activation, target-specific release and complete remote phase integration remain necessary. Existing generic image release explicitly refuses Joined provenance. | **Incomplete integration.** All required final members and post-quorums must complete before ordinary serving opens. Every strong read still requires its own valid authority/barrier. |
| A joined node later becomes a donor | Joined host construction lacks the initial host's source registry and OLD/source authority. Existing ingress rejects that role. Export also rechecks ordinary Serving after a freeze has invalidated that generation. | **Incomplete integration.** Supply genuine Joined source authority and a bounded historical export scope; preserve the ordinary Serving check for ordinary requests. |
| Joined data continuation after controller/process loss | Retained in-memory owners and pins do not establish reconstruction of Joined range attempts after process loss. This is distinct from implemented metadata persistence and recovery. Source export retirement releases pins; it does not itself safely delete an obsolete physical replica. | **Not qualified.** Prove persisted Joined data/attempt continuation and authorized cleanup separately from warm retry and image durability. |

The source paths above are rooted at `crates/bicdb-core/src`. Line numbers and
full paths are retained in the source reports. These findings are not evidence
of lost acknowledged data, nor an attribution of the original Azure slowdown.

## Smallest operational acceptance before another capacity run

Start four native nodes and join a fifth through the supported operator path.
Require the ordinary controller to schedule a real move and retain bounded
progress without manually installing ownership objects. Complete a first range
on node five, then move a second populated range onto that same live host.
Throughout both moves, use the same original client and unchanged timeout and
consistency settings. Record every attempt, acknowledgement, retry, failure and
uncertain outcome with its payload hash.

Require strong reads of the first range and acknowledged application writes
while the second moves. Inject lost replies and actual controller/process
restarts at the existing phase boundaries; reconcile the exact original
attempt instead of creating a replacement after an unknown result. Then make
the joined node a genuine donor in a separate movement test. These are proposed
gates, not passing tests.

Only after convergence and complete external-ledger reconciliation should a
physical experiment compare stable four-node and five-node throughput at equal
latency/error budgets. Report relocation throughput separately. Passing these
storage-engine gates would still not establish transparent cross-range SQL,
physical-host power-loss tolerance or production admission.
