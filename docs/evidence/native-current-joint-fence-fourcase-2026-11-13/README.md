# CurrentJoint fence trace: four-case Core pass, workflow formatting failure

Exact source **`5812d5750409e8b4f21d959fbaf367c8d4362f39`**, run **43343984328**:

| Gate | Actual result |
|---|---|
| Core131618814672, unchanged full four-case cohort | **4 passed / 0 failed / 0 ignored / 976 filtered, 67.70s** |
| all-target + isolated no-default consumer131618780247 | **PASS** |
| formatting131618780186 | **FAIL**, one extra blank line before `FENCE_TRACE_BEGIN` in distribution_range_recovery.rs |
| whole workflow | **FAILED** |

The later whitespace-only Root fix `1a3ee02ea` passed raw changed-file rustfmt; it is not the source executed here. No new dispatch or build was performed for this receipt. The original Source5cec four-case **3/1 FAILED** result remains preserved (run43339165885/Core131602906573,88.59s); its missing-archive cut was not reached. The new source adds passive cfg(test) diagnostics, not a production timing correction, so one passing whole cohort does **not** resolve the earlier availability/timing instability or demonstrate a cause.

All four explicit `ACTUAL_ACTIVATION_SCOPE_POSTCAT_CUT reached=true nativeR=2 no-extra-businessACK=true failed-cut-retained-actualFrozen=true` markers are present. Source assertions passed for actual foreign DB/Store, original-caller cancellation with exact fresh recovery, actual writer replacement, and missing protected decision archive after real durable postCAT activation. All retain the two original business ACKs, R2/H/local B/UID/rows/raw slots/promises and identical closed Frozen owners. This cut cohort performs CurrentJoint Ready/Complete at R2; it is **not** the separate canonical AbortR3/ACK4/StrongR4 full-positive test, a genuine fifth DATA image transfer, an operator/physical test or whole-candidate qualification.

## Source-correlated timing DATA

The retained log contains **1024 NATIVE_FENCE_TRACE events in32 buffers**, all `dropped=0`: four collector-begin buffers, twelve collector-record buffers, twelve CurrentJoint receiver buffers and four **earlier CommittedHandoff target-image** receiver buffers. CurrentJoint stage statistics below exclude those earlier four target-image buffers. This purpose attribution comes from the unchanged actual fixture producer: `complete_joint_tests.rs::install_actual_target` calls `native_joint_old_closure_tests.rs::role`, which explicitly requests `CommittedHandoff`. The trace itself has no purpose field; absence of a CurrentJoint stage alone would not establish NormalRecovery. It also contains564 Strong and7706 Ready/completion-status probe lines; those are separate contexts.

Each CurrentJoint test's original scope context is joined to its named captured libtest stdout via the actual `ACTUAL_POST_JOINT_FENCE` marker. Receiver node/nonce/context binds its three recipient calls. Collector-record `node=n3` denotes the initiating role's local coordinator, **not** the counted recipient; three successful record buffers are associated with n1/n2/n3 by the actual same-context marker order and unchanged serial fixture loop. Acquired recipient-role contexts are independently fresh and deliberately differ from the original collector role. No decoded timing field grants authority.

| Case | Original context | Actual n1/n2/n3 RPC ms | Role allowance at last third-record marker |
|---|---|---|---:|
| Writer replacement | 01a1d282-8033-7ef0-a97b-1091fa96a342 | 939 /962 /882 | 1194.109ms |
| Foreign DB/Store | 01a1d282-803d-70b0-9c9c-e1436d36d6a0 | 955 /978 /815 | 1228.726ms |
| Missing decision archive | 01a1d282-8067-7450-b443-49c11ceea69b | 953 /984 /813 | 1219.777ms |
| Original caller + fresh recovery | 01a1d282-8010-73b3-90a3-1e9b1b29ae26 | 928 /965 /833 | 1247.136ms |

At collector begin, business allowance was **4743.026–4764.486ms**, while role/effective-round allowance was **3972.793–3980.758ms**. The ORIGINAL role deadline was **770.233–784.632ms earlier** than its business parent. Every actual record check succeeded; the worst measured last-record role slack was **1194.109ms**, while the corresponding business marker reported1968ms. `round.finish` also succeeded in each case. That last-record timestamp precedes deferred diagnostic flush and return: it is not an exact returned-proof/finish deadline measurement. The older failed5cec run logged business512ms but no original role deadline or rejecting-record-stage trace; this new run supports the narrower-bound source contract, not retroactive measurement of the old failure.

| CurrentJoint stage (inclusive) | Samples | Min / max ms |
|---|---:|---:|
| Receiver through last pre-projection marker | 12 | 799.716 /968.742 |
| Actual fresh recipient witness | 12 | 8.336 /28.099 |
| Service preflight | 12 | 351.894 /430.039 |
| First actual joint-domain construction | 12 | 232.279 /291.136 |
| Actual Store fact currentness, both preflights | 24 | 110.006 /149.844 |
| Post-freeze actual joint-domain construction | 12 | 222.921 /272.355 |
| Second joint-fence preflight (contains its fact check) | 12 | 110.369 /150.198 |
| Durable catalogue write | 12 | 2.220 /4.628 |
| Response native install-safety inventory | 12 | 17.305 /24.758 |
| Full service fence producer | 12 | 422.226 /537.164 |

Actual reserve was3–6µs, service acquisition0–2µs, DB acquisition7–22µs and freeze/drain34–66µs in these12 CurrentJoint calls. Collector-record work through its last marker was144–171µs; nested scope validation130–159µs. These measurements identify substantial domain/currentness work in this bounded run, **not** writer dominance, hardware/parallel-test causation or an8GiB performance contract. Stage maxima can belong to different calls. Inclusive spans overlap: the fact check is inside its preflight, and preflight/domain/inventory are inside service/receiver spans. Do not sum the table.

The scalar buffer enters before runtime reservation and guard acquisition; marks under latches only copy timestamps/scalars into preallocated storage. Formatting/IO occurs on outer Drop after owned local guards drop and consumes the unchanged original allowance. Raw log wall timestamps can be flush times; use recorded monotonic event times. The final receiver marker is `local-observation-projection-begin`: receiver durations exclude that tail, deferred flush, network return and remote drain. Several tail-expression `-begin` markers intentionally lack matching `-ok`; their enclosing call success and actual final test assertions prove those paths returned, not an invented trace success bit. Zero overflow does not establish remote drain or completeness after process kill.

## Retention and verification

Full Core/compile/formatting raw logs are included. Core raw SHA-256 is **8f27bf0c6f2a9e2af190161b944a63dffd5445a82df85934bce0fbe12ab22dc8** (9,411,103 bytes). Artifact14000941836, ZIP SHA **51b521e32c132ece9b2caa99a3b8ae04976167ea61851474d85549bc41be96ab**,362562 bytes, contains **four workflow files only**. The ZIP is retained privately; no database custody was captured by these successful tests, and no engine was opened or replayed for this receipt. The older5cec79-file failure capture remains separate.

Run `python3 verify.py` offline. It checks bytes, exact source/job/result distinctions and independently rederives every buffered event/span/test correlation from the full raw log. It neither runs an engine nor contacts GitHub. The previously retained nine-case serving-scope pass atca838 and six-case CLI pass at5cec remain their own exact source/run scopes and are not relabelled as results at5812.

`python3 semantic_tamper.py` runs six rehashed semantic negative controls: workflow greenwashing, altered Core count, invented slack, an actual removed record-counted event with fully rederived profile, declaring the old cause fixed, and invented native custody. Every affected packet hash is repaired before the expected semantic rejection. These are not byte-corruption controls.
