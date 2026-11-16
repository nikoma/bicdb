# Native d74: complete failed execution and next checks

Source **1.0.520-beta / `d74d363addb35b5b97308b1afcc1c2231e021d29`**,
tree `51aa988b08c42f8a935f43e54a37d8fd4eda51c2`. The tested pull-request
checkout was `0408b5ca6758927032b5536dbdb968683ca9813e`, with the same tree
and exact main519/candidate parents. Main519 is
`587dd6e4775e454cc9e8224d5fed76e8795c6ff6`; source, executable, release,
deployment and qualification remain separate states.

The [native job](https://github.com/nikoma/bicdb/actions/runs/43790716008/job/133048283864)
compiled and **failed at runtime: 397 passed, 93 failed, 1 ignored**,
989 filtered, in 3,656.27 seconds. The
[complete 491-case inventory](../evidence/native-d74-runtime-2026-11-16/complete-case-inventory.json)
and [all 93 failures with original errors, source pins and reached cuts](../evidence/native-d74-runtime-2026-11-16/failed-cuts.json)
retain the original execution. A case name does not prove its fault was reached.
Serial direct-report intervals can include the next case's startup; original
panic/error, matching actual call and source order constrain attribution.

## Results and limits

| Scope | Classification | Evidence and limit |
| --- | --- | --- |
| Workspace all-target and no-default compilation | Passed within scope | Compilation includes tests; it does not execute them |
| Nonserial Core | Passed within scope | Main suites1875/0/16, plus one actual isolated subprocess pass; whole log1876/0/16. Do not count the child twice |
| SQL / PGwire | Passed within scope | SQL2682/0/4; PG511/0/2. Generic SQL checks do not establish native distributed SQL |
| Rails | Passed within scope | 20 runs,100 assertions, zero failures/errors/skips,24 reports/messages; includes the existing single-database restart scenario |
| Native serial | **Failed** | 93 runtime failures across FIRST68, hot-policy24 and metadata-supervisor1; no whole-native acceptance |
| Remaining workspace | Passed within scope | 1062/0/6 in129 summaries; native CLI6/0; Python47+3+34 actual passes; Node2/0 |
| New bootstrap cancellation guarantee | Not tested by its failed case | Fixture failed before the held-service cut; the other three wait controls passed |
| Common cause for all native failures | Inconclusive | Several distinct stages/errors; no hardware, storage or acknowledged-loss attribution follows |
| Physical DATA expansion, convergence, capacity and RPO/RTO | Not tested by this run | Original Azure qualification remains failed |

## Failures and smallest deterministic follow-ups

| Original failed group | Count | Reached boundary and next check |
| --- | ---: | --- |
| Allocation activation-domain prerequisites | 4 | Shared mode0 allocation exercise failed its selected-after SDK APP before domain comparisons. First restore that unchanged SDK tail, then require actual comparison cuts |
| Allocation ordinary SDK tails | 3 | Selected-after APP reported installed/current descriptor mismatch. Exercise actual allocation through the same SDK and original2s full-value assertions |
| Allocation generic-marker negative | 1 | APP was refused for a different reason than the required native-readiness refusal. Reach that exact original oracle without treating a generic recovered marker as native Ready |
| FIRST post-placement registry/maintenance prerequisite | 54 | Original selected/untouched ACK+Strong and allocation commit occurred; next registry call failed before FIRST factory. Separate43 query timeouts,8 active-relocation refusals and3 unresolved policy-publication errors. Hold/release the actual relevant state deterministically and retain original outcomes |
| Cross-protocol prerequisites | 2 | Registry call failed before intended cross-protocol exercise. Require real enrollment/discovery cuts after recovery of the prerequisite |
| FIRST ordinary setup | 1 | Registered control Store was busy. Hold the real Store under the same original OWN/caller, then release or cancel/expire; retain exact current-member checks |
| Bootstrap cancellation setup | 1 | Missing metadata leader hint during DATA open. Establish a real election gap; wait only within the original request, then reach the original held-service cancellation cut |
| FIRST population Strong | 1 | A real Strong call expired during population. Retain full request/response/timing evidence and prove its declared consistency result under the same2s caller budget |
| FIRST OLD-constrain prerequisite | 1 | Configuration mismatch before the intended wait cut. Restore actual current descriptor admission, then require that exact wait/cancel/expiry cut |
| Hot-policy catalogue Open | 23 | Catalogue activation timed out. Trace actual member collection, external sweep and local scoped validation; preserve authority deadlines and every native check |
| Hot-policy large-head case | 1 | Original activation returned query timeout with fault counter0. Intended directory-sync fault was unreached; require actual fault entry before reporting that scenario |
| Metadata supervisor anchor progress | 1 | Automatic current-term anchor did not commit within the existing bound. Reproduce actual election/reader contention and identify the blocked stage; election alone is insufficient |

### Prepared source corrections, not passing results

The [transaction-descriptor correction](native-allocation-transaction-descriptor-d74.md)
uses the already verified SAME-State allocation descriptor instead of the
obsolete original epoch, preserving all native/readiness validators. The
[recovery-turn correction](native-allocation-role-turn-d74.md) obtains fresh
authority for each distinct operation inside the unchanged caller bound and
retains validated success before late outer errors. Their initial source leaf
is `9fa174737a68bfb9f98dd0bad1dab398822dfd2f`; source review does not establish
that the original failures now pass.

The [startup correction](native-initial-open-leader-wait-d74.md) waits for an
actually absent leader hint within the original finite caller and for registered
Store validation within both the original OWN and caller. It neither replaces
proofs nor grants readiness. None of these corrections is compiled or executed
by the historical d74 run, and none explains all93 failures.

For one retained hot-policy call, actual component traces show member collection
of roughly370/320/115ms, a final foreign sweep of287ms and local scoped validation
of167ms. Catalogue execution expired its inherited authority window. These spans
overlap; do not add them as independent totals or extrapolate to production.
They identify work to investigate, not a disk or hardware cause. Increasing a
timeout or discarding expired authority is not an accepted correction.

## Reproduction and evidence custody

At the exact historical source, the normal workflow's native command is:

```sh
python3 scripts/qualification/guard-build-memory.py --log "$RUNNER_TEMP/required-ci-pressure/native.jsonl" -- cargo test --config 'profile.dev.package."*".opt-level=2' --config profile.dev.opt-level=1 -p bicdb-core --lib --locked -- distribution_native_host:: range_image_native::rf3_fixture:: --test-threads=1
```

Its preceding `chaos_child` helper build and normal workflow dependencies are
required. No local Rust/compiler/SDK/database execution was performed during
this audit. The correction must get its own exact-source automatic execution;
earlier passes do not transfer.

The complete historical run is failed: **10 passing jobs,1 failed job,4 optional
skips** across Tests and Rails. Its immutable archive contains95 files,
92 premanifest files,11 complete logs and8 original ZIPs/28 members. Manifest
SHA256:
`91110f4182a22d401e9c4dc4976815608ceccce78c6fd184963abd4a9129a015`.
Complete independent archive verification:
`cfd7e447d059be3ef7bb1b0f9d1443dddcaa63d0e120411a52787372d8101145`.
Source/checkout/parent/closure and wrong-manifest control:
`1373fa9e38d513cdebbaaa84ccf4da919ca0868a43f09554af213bfc6c017203`.

Original Native raw log:174,365,442 bytes, SHA256
`0fb6aca4e8deb61f9d3fe1ff331d30041e19f4fefe7650bf9ff29f3d2e781f4f`.
Full491-case/hash reconciliation:
`58aac5bd3c0c5fbe36d34fe2cf8af32f10e643d492cc7b3123487b0d6123600c`.
All93 source/cut/raw-row verification:
`05b167ba809025b46557d2a3507709cda0acc8283335f0f50f16b64318f4a4ac`.

CLI journals are allowlisted client evidence, not a complete global attempt
ledger. Passing a fifth metadata join or opening a replica-only listener does
not demonstrate automatic physical DATA relocation, fault-safe convergence,
ordinary PostgreSQL transparency or sustained post-convergence capacity gain.
