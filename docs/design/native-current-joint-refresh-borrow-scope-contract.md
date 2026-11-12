# CurrentJoint completion: measured cost and a proposed borrowed scope

Status on 2026-11-12: **design and source audit only; no borrowed producer has been implemented or validated**. Exact profile source is `fd85a24392abf8a273479f8637e040523c053109`, candidate `1.0.489-beta`. The [portable evidence](../evidence/native-current-joint-refresh-fd85-2026-11-12/README.md) retains raw logs, a source reconstruction and the failed outcome. No phase budget, timeout, quorum, validation requirement or persistence boundary is proposed for removal.

## Observed execution

Run 43278537440 / Core job 131393411593 compiled all targets and the isolated consumer, then failed its one full recovery test (0 passed, 1 failed, 932 filtered, 83.78s). All three CurrentJoint fences, canonical Abort3/native R3 and Prepare responses completed after actual joint release, target ACK2 and Strong R2. N1 returned Complete. N2 persisted completion but its response expired; N3 Complete and recovery activation/ACK4/Strong R4 were not reached.

The original request caps remain 4500ms and 5 seconds. Effective receiver budgets start near 4 seconds. Durations below are milliseconds; remaining time comes from the actual original-role deadline, never a renewed grant.

| Actual Complete stage | N1 elapsed | N1 remaining | N2 elapsed | N2 remaining |
|---|---:|---:|---:|---:|
| Select decision | See raw phase trace | — | 1132.028 | 2866 |
| Drain | See raw phase trace | — | 5.062 | 2861 |
| Closed Ready QQ | See raw phase trace | — | 795.261 | 2066 |
| Post-QQ metadata role | 9.012 | 3086 | 18.152 | 2047 |
| Outer necessary checks | 2.555 | 3083 | 2.624 | 2043 |
| Native preflight | 762.864 | 2320 | 761.681 | 1282 |
| Native WAL publication | 172.387 | 2147 | 172.807 | 1109 |
| Full native readback | 615.702 | 1532 | 620.897 | 488 |
| Full pre-catalogue validation | 443.524 | 1088 | 443.440 | 44 |
| Catalogue fsync | 3.572 | 1085 | 5.929 | 39 |
| Full binding refresh | 853.740 | 231 | 394.153, expired in first fact check | Expired |
| Receipt current boundary | 2.624 | 228 | Not reached | — |
| Produced response | 2.410 | 226 | Not reached | — |
| Total | 3772.539, returned | 226 | 4352.933, failed | Expired |

Service/DB acquisition was microseconds in this run. N1 native serving SHA is `c2742c8e94b59f6a4d66ef66449aa1095020285ee2161da3282a4c9e5a044553`; N2 is `3347958945623f96d6429710d2264b7561046aeef51c00e1289ddb971ff96bce`. Both are R3/B4; logical H is `cf3adf5061c827d673733423fd30ee1262d880975e10c91f3fb2f28f3abb603b`. Direct trace ordering proves both returned from WAL and catalogue fsync before their original deadlines. N1 actually returned Complete; N2's response remains uncertain.

N1's disjoint refresh children were CurrentJoint fact/original archives 394.472ms, owned physical proof 53.118ms, current role/scope/point 0.291ms and exact serving readback 401.168ms. Within the last child, repeated physical validation cost 53.392ms, source-lineage construction 172.869ms, admission mint 1.002ms and receipt currentness 173.877ms. These are selected direct children, not an instruction to add all nested spans.

The fact factory first spent 191.218ms in its native-journal scope, then 193.939ms validating the returned fact again. Exact saved writer/archive-floor checks cost 0.192ms. Across the entire refresh, 246 admission validations cost 243.863ms inclusive, including 492 genesis validations (two initial ranges per mint), while four source-lineage constructions ran both existing full contract checks. Actual namespace UID/schema/reader checks totalled 0.331ms and catalogue/generation hashing 2.821ms. No scoped abort-inventory span executed. This profile does not justify blaming a row scan, namespace hashing or abort inventory. Nested costs overlap and tracing adds overhead.

## Source-backed duplication

| Path / symbol | Same-call work currently repeated | Restriction on reuse |
|---|---|---|
| `distribution_range_consensus/native_ready_prepare.rs::refresh_completed_native_binding` | Full CurrentJoint fact, full physical proof, then serving observer repeats the physical proof | Only this uninterrupted actual producer may reuse evidence; standalone refresh remains full |
| `db/range_image_native/serving_configuration.rs::observe_native_recovered_serving_for_physical` | Full physical, current lineage, admission, then receipt currentness reconstructs lineage | Private borrowed actual-physical/Store/role input; no caller supplied passed flag |
| `db/range_image_native/certified_prefix.rs::current_native_range_source_lineage` | Constructor validates full contract, then outer validation repeats contract plus distinct Pending checks | Preserve Pending and all distinct native checks; change only scoped construction after an invalidation audit |
| `db/range_image_native/serving_configuration/joint_handoff/bridge/activation/recovery.rs` | Factory reads actual bridge/current/archive under journal, drops it, then fact currentness reads again | Purpose discovery lacks Prepared pin; do not weaken the general factory |
| `db/range_image_native/serving_configuration.rs::persist_native_serving_from_decision` | Separate full preflight, actual native WAL, full readback, later full pre-CAT and refresh | Distinct before/after-WAL/CAT checks remain; only unchanged subfacts may be shared |

N2 had 39ms after catalogue fsync. Post-CAT-only deduplication cannot make its nontrivial full check fit. The next implementation must examine a **single journal-held actual execution** across the existing native publication/readback and catalogue publication/readback boundaries. It must not assume that pre-WAL serving pointer equals post-WAL state.

## Mutation and invalidation table

| Evidence / mutation path | What the actual ownership excludes | Necessary checks that remain |
|---|---|---|
| Actual DB reference borrowed under outer read guard | Replacing or dropping that inner BicDb; normal mutable DDL/schema/namespace installation | Same actual-open witness, health and real birth; shared atomic invalidations are not excluded |
| Exclusive service-owned RangeWriteStore | Another call mutating this actual raw catalogue/phase/current nonce/maps through that owner | Exact Store open UUID/root and durable health; no second handle or reopened Store substitution |
| Prepared's real Frozen permit and SourceCaptureReadPin | Ordinary range mutation and controlled image/prefix mutation; `try_controlled_mutation` refuses an active capture pin | Exact gate Arc, range, owner, lease/generation, no active/controlled mutation and no poison at entry/return |
| Native journal held by the borrowed execution | Concurrent protected native policy/pointer/archive/image mutations that obey this journal | Acquire the real guard once; every nested helper must borrow it instead of re-locking |
| Native writer installer | **Not excluded by service/DB/Frozen ownership alone**: it may validate the service before our lock, then acquire the journal and deactivate/publish policy | Original actual writer Arc, registration, coordinator, policy pointer, Store/open and schema bindings after journal acquisition and at return |
| `BicDb::install_commit_admission` / `clear_commit_admission` | These shared-reference hook changes do **not** acquire the native journal | Final actual hook/registration/pointer liveness check remains mandatory even while journal is held; a retained Arc alone is insufficient |
| Schema compatibility authority / native generation invalidation | Normal mutations require appropriate ownership; shared invalidation/health poison can remain possible | Compare actual schema generation/hash, gate/native generations, health and reader flags at final boundary |
| Original role/QQ/caller cancellation | No ownership blocks time passing or cancellation | Preserve all original tokens/deadlines and exact purpose/scope/current metadata requirements; never derive fresh authority from an old fact |
| Native WAL transaction | Changes serving pointer/archive, xid and durability state | Actual post-WAL readback must validate the **new** pointer and archive, exact unchanged R/H/B/UID/domain, and actual outcome |
| Catalogue persist | Changes phase/checksum/serving binding and uncertainty | Actual post-CAT readback validates expected new phase/binding and unchanged obligations; no pre-CAT checksum relabel |

Writer ordering matters. `install_enrolled_native_coordinator` takes the admission gate, preflights the coordinator/service, then takes the native journal, deactivates the prior hook and publishes current/previous native policy. A preflighted installer can therefore run during gaps between the current short journal acquisitions even while the completion service lock is held. Keeping the journal across the scope excludes that native publication, but **does not** exclude direct hook clearing, health poison, schema invalidation or cancellation. Necessary return checks must remain actual reads.

## Proposed private API boundary

The implementation should provide a DB-internal non-Clone/non-Serde borrowed execution, invoked only by the fixed actual completion producer. Its constructor receives the real physical proof/Prepared, exclusive actual Store, borrowed actual DB, sealed decision, fresh role and original cancellation. It acquires the actual journal nonwaiting and validates full original fact, birth/open, lineage, pin/freeze and writer bindings. No raw DTO, checksum, boolean or externally retained checked token can construct it.

The borrowed execution may share actual immutable admission/anchor/history subfacts while the above exclusions remain true. It must retain distinct full logical checks before WAL, after actual new native pointer/archive, before catalogue publication, and after actual new catalogue binding. Native fields changed by each write must be read again, not copied from prestate. It must continue to compare original and fresh metadata/caller purpose, point, obligations and original writer at the appropriate boundaries. It cannot escape the service/DB/Frozen/pin/journal borrow or become an alternative current grant.

`NativeJointServingBridgeReceipt::current_read_scope` currently acquires its own native journal. Calling it while the new guard is held would fail the try-lock. A new borrowed-guard implementation must accept the actual guard ownership internally, with the current standalone method remaining a full wrapper. Do not use a reentrant mutex, caller boolean, skipped check switch or scoped token that outlives the actual borrow.

The pure uncertain observer, later receipt currentness, purpose discovery and externally called standalone refresh keep their full validation. Persist errors, ambiguous outcomes, expired original caller and changed original writer still return refusal; exact refsync/recovery remains a distinct fresh protocol operation rather than a TTL renewal. No partial success may be reported as a returned completion.

Before runtime acceptance, required focused controls include actual writer/hook replacement, original caller cancellation after persistence, wrong actual Store/open/scope, unknown persistence and exact immutable obligation/history mismatch. The existing full positive must pass unchanged under the original budgets; then actual ABA/original-caller controls and broad relevant regressions are still required. A cost reduction is a hypothesis until measured. This document does not close cold, higher-epoch/new-leader, committed-tail, minority, unavailable-origin or operator fault-safe gates.
