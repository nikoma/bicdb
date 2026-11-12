# CurrentJoint Ready peer profile at ded3 (2026-11-12)

The unchanged positive **failed at the third Complete**, before CurrentJoint activation. This is diagnostic evidence of a bounded availability failure, not recovery success. No deadline, worker allocation, guard or acceptance assertion changed.

Exact source: `ded3c412cf25c12f3c6fe524d22aac02310a1c0b`, source version `1.0.489-beta`.

| Cohort | Run / Core job | Actual result | Reached boundary |
| --- | --- | --- | --- |
| Unchanged positive | 43303084124 / 131479178648 | 0 passed, 1 failed; 947 filtered; 84.36 s | n3 Complete returned a typed closed-Ready-quorum error; no completed observation |
| `native_activation` | 43303090440 / 131479193636 | 7 passed, 1 failed; 940 filtered; 85.11 s | Heavy case failed earlier in first n1 Prepare after catalogue persistence; no CurrentJoint Complete or activation |

The seven legacy activation controls passed. The heavy idempotence case did not reach its CurrentJoint activation prerequisite or idempotent continuation. Both all-target checks and both isolated no-default consumers passed. Both workflows nevertheless finished with failure because of their actual Core runtime failures.

The earlier `be85db58c1c4f54b13372a9a966794d146d31722` pair is a separate failed cohort: both Core listings and both all-target checks failed on the three inherited `db.scan` test calls. Both consumer checks were skipped. No selected tests or timing probes ran there. The test-only correction is in ded3; be85 is not runtime evidence.

## Positive operation and collector boundaries

All three new-domain fences, actual NoData promises, canonical Abort3/native-R3 checks and all three Prepare calls succeeded. Complete n1 returned in 2299.319 ms with 1699 ms left on its effective token; n2 returned in 3386.654 ms with 611 ms left. Complete n3 returned an error in 2920.744 ms with 1077 ms left. No n3 completion producer, CurrentJoint activation, ACK4 or Strong-R4 assertion was reached.

The original fixture parent, 4500 ms wire request and effective role caps remain unchanged. Remaining values below belong to the actual logged collector/role token; they are not renewed caller budgets.

| Collector | Selector ms | Closed Ready ms | Synchronous local Ready ms | Remaining at Ready return ms |
| --- | ---: | ---: | ---: | ---: |
| n1 | 100.362 | 804.828 | 420.076 | 3088 |
| n2 | 1132.473 | 847.786 | 432.788 | 2013 |
| n3 | 1132.672 | 1782.904 (error) | 405.797 | 1077 |

The final n3 collector had two already-Completed peers. First-attempt queue delays were only 33–41 microseconds, readonly-role acquisition about 18 ms, and service acquisition 2–3 microseconds. Their full validation took 869.718/873.498 ms against 810 ms allocated child budgets. The two retries received 539 ms each and spent 53.466/61.069 ms waiting for the previous same-node handlers, followed by 874.397/876.851 ms of full validation. All expired observations were rejected; no successful late observation counted.

The previous selector handlers really outlived the sender drain: selector returned at `t_us=7318802`, the Ready collector began at `7323900`, and the last selector receiver exited at `7565479`. However, final-Ready peers were queued later at `7752860`/`7752871`, after the 405.797 ms local observation. Thus selector overlap did **not** cause the first final-Ready service waits in this run. The retries did overlap their first-attempt receiver handlers. The final Ready collector returned at `9106804`; final receiver exits were `9527636`/`9527650`, with actual Operation strong count zero after original owner drop.

## All actual peer attempts

Values are milliseconds except queue microseconds. `-` means the named stage was not reached or not applicable; it is not proof of zero cost. Each attempt has a distinct actual challenge retained in the private analysis. S = completion-status selector; R = closed Ready.

| Collector | Peer / attempt | Child budget ms | Queue us | Role ms | Service wait ms | Full local validation ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| S n1 | n2 / 1 | 657 | 32 | 32.637 | 0.002 | 0.125 |
| S n1 | n3 / 1 | 657 | 15 | 29.147 | 0.001 | 0.164 |
| S n1 | n3 / 2 | 646 | 22 | 28.950 | 0.002 | 0.121 |
| S n1 | n2 / 2 | 645 | 20 | 27.713 | 0.003 | 0.139 |
| R n1 | n2 / 1 | 1153 | 25 | 27.572 | 0.002 | 411.026 |
| R n1 | n3 / 1 | 1153 | 21 | 29.856 | 0.001 | 334.765 |
| S n2 | n1 / 1 | 650 | 26 | 15.302 | 0.002 | 853.786 |
| S n2 | n3 / 1 | 650 | 17 | 26.931 | 0.003 | 0.188 |
| S n2 | n3 / 2 | 639 | 22 | 26.531 | 0.002 | 0.133 |
| S n2 | n1 / 2 | 433 | 54 | 15.666 | 198.090 | 402.569 |
| R n2 | n1 / 1 | 799 | 43 | 15.508 | 0.002 | 874.963 |
| R n2 | n3 / 1 | 799 | 30 | 26.309 | 0.002 | 356.483 |
| S n3 | n1 / 1 | 650 | 26 | 14.759 | 0.002 | 863.669 |
| S n3 | n2 / 1 | 650 | 19 | 26.503 | 0.006 | 850.086 |
| S n3 | n1 / 2 | 433 | 24 | 19.702 | 202.997 | 450.516 |
| S n3 | n2 / 2 | 433 | 23 | 31.944 | 188.313 | 400.401 |
| R n3 | n1 / 1 | 810 | 41 | 18.135 | 0.003 | 869.718 |
| R n3 | n2 / 1 | 810 | 33 | 18.430 | 0.002 | 873.498 |
| R n3 | n2 / 2 | 539 | 24 | 30.900 | 53.466 | 874.397 |
| R n3 | n1 / 2 | 539 | 27 | 19.533 | 61.069 | 876.851 |

For the final Ready first attempts, the CurrentJoint domain checks cost 402.532/403.162 ms, owned physical checks 52.696/53.324 ms, and full binding checks 414.184/416.691 ms. The binding check includes another physical check (53.070/53.061 ms), current lineage (172.551/173.411 ms) and serving receipt currentness (186.437/188.140 ms). These are **inclusive nested spans**, not additive independent scans. The profile does not separately attribute the internals of the CurrentJoint domain check.

The earlier n1 Ready round could use two Prepared peers, whose full local validation took 411.026/334.765 ms. The n2 Ready round could still use the Prepared n3 (356.483 ms) alongside local n2, even though Completed n1 took 874.963 ms and missed its 799 ms child. This distinguishes the final pair of Completed peers from a generic slow network or first-acquisition mutex problem. Instrumentation overhead and run-to-run variation remain limitations.

## Retained evidence

All paths below are private under `/tmp/bicdb-native-foundations-ci`; no engine was opened to inspect these copies.

| Evidence | SHA-256 |
| --- | --- |
| Positive raw job log (`ded3-ready-profile-positive-job131479178648.log.gz`, hash of decompressed bytes) | `544efa589dfd91b1078a69ef91a7e1b0eec090e12e04d797c4f8a6cde090c03c` |
| Controls raw job log (`ded3-ready-profile-controls-job131479193636.log.gz`, decompressed) | `de37dd3c4fce8af1a77488ba593e30db5b8b8043739a7eb4a3d724c1461aabfd` |
| Correlated positive analysis, 884 events parsed / all 20 attempts / 6 collectors | `9150e2b0fffc824a5dea01e35f92d8a7b729f64f006c74cc58ac14ae9d1155be` |
| Final summary (`ded3-ready-profile-final-summary.json`) | `9c68bec6f6bdcf61bfd08cdc8ec87806dc959f09088ba0da9684beec99556a8a` |
| Positive all-target/consumer job 131479125888, decompressed raw log | `fc39d871442612bcac918abf0a619429cb6d9af3b3197d53fa47a434920b974d` |
| Controls all-target/consumer job 131479147859, decompressed raw log | `15c7a2c2fd16ce7141f020a0475efcdf0b19a5315fa001c10e690c5d0139d683` |
| Positive artifact 13981654968 ZIP | `3ba4160c916f595c81f3c291b3d23e2286f372ab3749744023caa27e39c5bdbe` |
| Positive custody manifest | `244b21f42bca744cbb54ea8aa8fe6cc3e95b2372bde51914f11c5813e26fec33` |
| Controls artifact 13981959641 ZIP | `925aad2ed5ed099a52bc2923bcc2a93b66f7a0a40e995bc3d065e09d00fe1421` |
| Controls custody manifest | `07d2b878eb8b2e7dbaf92d2ae46308617ad2cc33a470bac270c1e6ced63ebe02` |

Every manifest entry was checked against its actual ZIP bytes: positive 82/82 files, 6,100,203 bytes; controls 82/82 files, 6,016,702 bytes. Positive observed catalogue phases are n1/n2 NativeCompleted and n3 NativePrepared. Controls observed n1 NativePrepared; n2/n3 did not reach new NativePrepared. These are observed-stable snapshots with live engine owners and locks held during copying, not independently replayed power-loss snapshots. Phase observations by custody capture do not by themselves establish ordering before a client timeout.

The controls first-Prepare trace directly shows catalogue persistence with 498 ms remaining, followed by a 535.192 ms final-current check and a client RPC timeout. That is a separate earlier failure from the positive closed-Ready profile. It does not validate the new Runtime idempotence path.

## Next bounded work

No production optimization is included. The next source audit may test whether one nonescaping, readonly actual observer scope can preserve every current-domain/native/history/UID/catalogue/original-writer check while avoiding direct repetitions under the same exclusive Store, DB borrow, retained Frozen and pin. That requires its own mutation/exclusion argument and real foreign-open/store, original-caller, writer-replacement and expiry controls. Standalone observers and every required boundary must remain full when no such actual scope exists. No checked boolean, cached grant, changed worker budget or TTL renewal is justified by this profile.

The separate expired-delivery control establishes rejection of actual late replies, not the cause of this cost. These results qualify neither full recovery nor physical/operator HA, loss bounds or Azure deployment.
