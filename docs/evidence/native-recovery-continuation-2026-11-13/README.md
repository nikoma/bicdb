# Native recovery continuation — November 13, 2026

**Production qualification remains incomplete.** This packet concerns distributed
virtual ranges. It does not qualify single-primary HA, Cell HA, application
fleets, transparent PostgreSQL routing or multi-range SQL transactions.

Public main inspected was **1.0.495-beta**,
`462e0c1d64857710cd28c3f2f09d03aec2c00f6e`, including PR170's separate HTTP
connection-task shutdown repair. The measured candidates below are **unmerged
1.0.494-beta source**, not that main artifact. Source, CI execution, a release
binary, deployment and physical qualification remain separate states.

## Exact results

[manifest.json](manifest.json) pins full source identities, terminal GitHub job
metadata, complete compressed logs and raw/compressed SHA-256 values. Runtime
tests use actual native hosts and pinned TLS in one GitHub-hosted test process.
They are not independent-machine or power-loss experiments.

| Source | Run / Core job | Result | Classification |
| --- | --- | --- | --- |
| `ff8f10fdbf6a244ca6dda0c436e3ab8e46b0f4ed` | [43440146438](https://github.com/nikoma/bicdb/actions/runs/43440146438) / 131935397580 | 12 passed / 0 failed; 88.85 s | **Passed**, focused cohort once |
| Same `ff8f10fdbf6a` | [43440151120](https://github.com/nikoma/bicdb/actions/runs/43440151120) / 131935399494 | 11 passed / 1 failed; 66.38 s | **Failed**, independent repeat; lost-follower-fence intermediate-state assertion; cause **inconclusive** |
| `6c9a8b65131d122970de2e496b5539396b517e65` | [43442632698](https://github.com/nikoma/bicdb/actions/runs/43442632698) / 131943916112 | 12 passed / 0 failed; 67.34 s | **Passed**, origin fault cohort after sequential follower-subrequest change |
| Same `6c9a8b65131d` | [43442628690](https://github.com/nikoma/bicdb/actions/runs/43442628690) / 131943935960 | 11 passed / 0 failed; 7.53 s | **Passed**, different focused recovery cohort; not another twelve-test execution |
| `1c5addeeffb95cb9651fc4bd41188ab7e4ae1835` | [43442888614](https://github.com/nikoma/bicdb/actions/runs/43442888614) / 131944862173 | 0 passed / 1 failed; 37.38 s | **Failed**, required regression reaches actual second Unknown, then detects lost prior recovery tracking |
| `0175a151117d35e5220de519e7476d0c96da6d57` | [43442830345](https://github.com/nikoma/bicdb/actions/runs/43442830345) / 131944623442 | 7 passed / 6 failed; 97.03 s | **Failed**, complete thirteen-test cohort; new custody and original repeated-Unknown cases individually **passed** |
| `f641725b3574dfa19b8b5e5204b116cb7bb1f514` | [43447384029](https://github.com/nikoma/bicdb/actions/runs/43447384029) / 131960338148 | 5 passed / 9 failed; 115.85 s | **Failed**, fourteen-test cohort; new unfinished-replica case reached its actual second-fault recovery stage, then failed the ACK/Strong gate |
| `8ddfd561e1ff2e2b2ace760fc4d1caad29ab2915` | [43450918583](https://github.com/nikoma/bicdb/actions/runs/43450918583) / 131972578522 | 1 passed / 0 failed; 23.68 s | **Passed**, separate passive phase-progress diagnostic; it does not replace the failed workload gate |
| `4081523f16e0d5ee1c36d332145ad6d56e479080` | [43446784706](https://github.com/nikoma/bicdb/actions/runs/43446784706) / compile 131958210090, Core131958272584 | Build failed: missing original `NativeInitialDataAdmission.database` field | **Not tested** at runtime; two jobs in one run are not independent reproductions |
| `156a586b18d1324de04c468ca941f3bcc989fdee` | [43447592065](https://github.com/nikoma/bicdb/actions/runs/43447592065) and [43447596023](https://github.com/nikoma/bicdb/actions/runs/43447596023) | Both selectors fail compilation: two E0502 activation borrow errors | **Not tested** at runtime; compile and Core logs for both runs retained |
| `a9b6a8532751d19cea5b5edafef9fef29c8c9310` | [43450029882](https://github.com/nikoma/bicdb/actions/runs/43450029882) / 131969531371 | 10 passed / 0 failed; 33.60 s | **Passed**, remote OLD/source and actual Registered NoDomain prerequisites; no target rows or Serving |
| Same `a9b6a8532751` | [43450034110](https://github.com/nikoma/bicdb/actions/runs/43450034110) / 131969540059 | 2 passed / 0 failed; 6.48 s | **Passed**, lower-level writer DATA-domain point facts; not host-wide Serving/ACK acceptance |
| `6d795aac644a71b36ffef395a649ffc21b62206d` | [43453994180](https://github.com/nikoma/bicdb/actions/runs/43453994180) / 131983273642 | 1 passed / 3 failed; 14.36 s | **Failed** cohort; cancellation-before-rotation passed; other three transfer scenarios **not tested** because first-ACK setup failed |

These rows are different histories. They cannot be combined into a green release.
The later source correction for the `408` build error does not change its result.
Earlier dispatch 43442826469 selected the wrong test name and was canceled; it
supplies no runtime evidence. Full execution logs retain failures unchanged.

## Reproducible custody defect

Relevant source is under
`crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/hot_recovery/`,
the adjacent `hot_recovery.rs`, and the
`provisional_listener/tests/initial_data_tests/origin_pre_wal_fault_tests/one_peer_loss_tests/`
fixtures. On `1c5`, select
`native_hot_peer_repeat_unknown_canceled_successor_acquisition_preserves_prior_custody`.

The case obtains real ACKs and completed replica recovery, creates a second
uncertain write, then cancels the actual bounded maintenance turn before fresh
role acquisition. The original code removes prior tracking before acquisition
succeeds. The test detects lost tracking with no successor job. This proves a
custody/liveness defect; it does not demonstrate acknowledged-data loss or a
stale-cache shortcut to Ready.

`0175` retains old tracking through fallible acquisition and in the successor job
until genuine recovered admission publishes. The targeted journal records the
canceled turn, actual role error, retained prior session and no premature job.
Subsequent observations in that case include a real ACK6, full Strong payloads
and matching native histories on the original three nodes. This narrow case
passed while the complete cohort failed. The next smallest test creates another
uncertain write **before** the earlier lagging replica has converged, requiring
both obligations to finish under original owners and unchanged deadlines.

## What the failed thirteen-test cohort observed

The complete `0175` result remains **failed**. Its twelve retained journals cover
eleven one-peer-loss cases and the original origin case; no stopped database
custody is retained for independent cold reopen.

| Case | Actual observation | Classification |
| --- | --- | --- |
| Native gap before fence; lost follower completion reply | Next append returned no user ACK, then a later Strong read returned its full value | **Failed availability gate**; uncertain write later materialized, not rollback or an inferred ACK |
| Stopped peer; lost follower fence reply | A next write ACKed, but a required old-record read timed out in the final round | **Failed joint gate**; timeout is not successful absence |
| Blackholed peer | Next write never ACKed; final next record absent at native2, while old and injected records remained | **Failed recovery/availability**; absence of an unacknowledged write does not prove rollback |
| Lost follower Prepare reply | Old ACK/Strong setup failed before the intended fault prerequisite | **Inconclusive for that fault**; overall test failed |

The recorded successful Strong observations contain no detected false absence
after a prior actual ACK or wrong acknowledged payload in this finite fixture.
Unavailable reads remain unavailable. This is not zero-loss qualification:
availability failed, observations are incomplete, and there is no cold database
custody or independent-host execution.

The nominal health limit is twenty seconds. The fixture stops scheduling when
fewer than eight seconds remain for four two-second calls. Five final-gate
failures stopped at approximately 12.62–15.76 seconds. Those are fixture receipt
times, not proof of continuous twenty-second unavailability or physical RTO.
Original budgets and failed assertions are preserved.

## Subsequent unfinished-replica witness: progress and failed availability

`f641` composes the sequential follower change, custody retention and a distinct
known-Pending/historical-activation continuation. It does not weaken ordinary
serving getters or turn historical activation into a current read grant. Its
complete fourteen-test execution **failed: 5 passed / 9 failed**; all-target compilation passed.
The full ZIP and complete Core log are retained here.

The new partial-debt case reached its genuine second Unknown. At 7.956 seconds,
prior anchor 2 work had finished: the lagging original node reached native4/H4,
the other survivor held native5/H5, and the source retained known4/Pending5.
A new anchor 5 session subsequently published. These are bounded progress
observations, not evidence that the complete application gate passed.

In the second health round, old and committed-record reads timed out. The next6
append tried requests 253/257/261, which explicitly returned
`cluster_data_operation_failed` with “leader recovery is not complete; writes
remain unavailable.” Final write attempt 265 timed out at the caller with no
response frame retained. The following Strong read returned the exact next6
payload at 14.696 seconds. There was **no external next6 ACK**. This is an
uncertain write later observed present, not rollback, inferred ACK, route404 or
proof of native-data loss. The fixture did not schedule a third round because
less than its eight-second reserve remained.

The two other repeat-fault cases in this execution failed earlier first-recovery
prerequisites; their second-fault assertions were not exercised. Do not report
them as completed second-fault tests or retroactively erase their failures.
The next smallest diagnostic observes genuine phase progress passively under
the same absolute deadline, then issues normal ACK/Strong calls. It must remain
separate from the unchanged failed workload gate and cannot qualify availability.

The separate `8dd` passive diagnostic subsequently **passed**. Its actual prior
native head was 5, its second uncertain command was 6, and the next acknowledged
command was 7: fixture retries consume real slots, so illustrative names such as
“native5/ACK6” are not authoritative physical indices. All three original nodes
reached native6 with matching histories/current opens at 7.574 seconds of the
second fixture window. Ordinary client calls then obtained ACK7 and full Strong
values; all original replicas reached native7 with exact full prior records and
history 1..7 by **9.289 seconds**. This is a finite same-process diagnostic under
the original twenty-second window, not measured hardware RTO. The overlapping
workload's `f641` **5/9 failure remains unchanged**.

The earlier successful `6c9` repeat has a further coverage limit: its actual
head advances 5→uncertain6→final8, but final snapshots retain only a subset of
the earlier full record values. Missing snapshot fields do not establish missing
database data. Its exact native command-history assertions are CI evidence;
independent final reconciliation of every earlier payload is not established.
The `8dd` diagnostic retains the complete prior record set instead.

## Actual empty-target prerequisites and evidence custody

The ten `a9b` tests exercise actual remote OLD Close/Observe, cancellation after
successful native WAL followed by a distinct exact retry, source Manifest /
History / Rows / Finished replies, and original fifth-node NoDomain checks.
NoDomain means verified selected-range absence under the real original receiver,
MStore, database/Store slot, actual runtime freeze and schema/namespace pins.
Native indices remain `None`; no synthetic native R=0, row permission or Serving
state is created. Hook registration replacement, actual metadata-term advance
and schema-generation invalidation revoke the necessary point.

The public projection retains every recorded logical OLD/source attempt, error,
retry and full ACK record. The checker independently verifies request/session /
manifest cross-bindings, partition tokens, full payload hashes, recorded native
history links, duplicate cursor replies and seals. Five NoDomain before/after
snapshots and the constraint-only before/after case can be reconciled directly.
Positive source-stream reports lack a fresh explicit target-after snapshot;
their unchanged-target result is a source-pinned CI assertion, not independent
before/after reconciliation. Ten tests and ten case directories are retained,
but those old artifacts lack a direct per-directory Rust-test-name identity.
Later instrumentation must not be retrofitted into these historical artifacts.

The two lower-policy tests retain only source/toolchain/selection/execution logs,
without structured native-state ledgers. They prove the exact tests ran, not an
independent native-state reconciliation or successful Serving publication.

The original remote-image artifact also contains fixture database and signing
material. Complete originals remain in private workspace custody; only explicit
logical ledgers/reports/logs are published here. Projection manifests retain each
omitted member's path, size and SHA-256 plus the complete original archive hash.
The fixture database capture held live owners and guards; it was **not a stopped
database, atomic power-loss snapshot, cold replay or qualified restore**. Presence
of a custody subtree alone does not prove a verified database capture.

The later `6d7` transfer cohort compiled, including
[all workspace targets](https://github.com/nikoma/bicdb/actions/runs/43453994180/job/131983226127),
but failed three first-ACK prerequisites. Each retained one uncertain append for
`items/registered-fifth-ack-r1-8`, range 1 / epoch 1 / topology generation 4, expected
leader n1, with the original two-second client timeout and collapsed “no event
cluster member is reachable” error. Raw SDK attempts and first-call readiness /
metadata snapshots were not captured. Database capture was partial because an
actual service was busy. Durable effect, actual request path and root cause
remain **inconclusive**; first-call code is unchanged from the earlier `a9b` pass.
Opened DATA owners and retained guards alone do not prove client-visible read
readiness. This is not evidence of an ownership-transfer defect or success.

The sole passing case reached the actual cancellation-before-rotation boundary:
prepared attempt retained, original N freeze retained, no A lease and unchanged
full target state. Positive transfer, callback-contention retry and hook-replacement
boundaries were not exercised by the failed cases. The smallest next diagnostic
captures actual pre-APP ownership/native/metadata state and raw SDK attempts,
then separately checks ordinary Strong readiness within the existing setup
deadline. It must preserve these original first-call failures.

## Remaining gates and smallest deterministic tests

| Gate | Status here | Next test |
| --- | --- | --- |
| Second failure with unfinished replica work | **Failed** at `f641`; prior work progressed and new anchor published, but combined ACK/Strong gate failed | Separate actual phase progress from normal request availability under the same absolute deadline; preserve original workload failure |
| Remote OLD closure and lost reply | **CI passed** at `a9b` for successful-WAL/lost-response continuation; ambiguous WAL, cold recovery and unavailable minority remain open | Genuine pending retiree plus closed majority, preserving every actual constraint; explicit unavailable-peer refusal and later qualified repair |
| Registered target installation | **NoDomain prerequisites CI passed**; transfer cancellation passed but positive transfer **not tested** at `6d7`; Installing/rows/finalization **not tested** | Actual unique freeze transfer, retained Installing refusal before WAL, bounded authenticated history/rows, actual transaction checks and finalization |
| Populated target's second range | **Not tested** | First range reaches Serving/ACK/Strong, then another range installs while retaining the original writer and first range's correctness |
| Bounded source/OLD-owner retirement | **Not tested** | Exceed eight retained entries using actual decision/Open/cleanup-backed retirement; never evict unresolved owners by TTL |
| Automatic four-to-five convergence and faults | **Not tested** | Original isolated physical experiment with complete external ledger and explicit convergence/fault gates |
| Capacity; physical RPO/RTO/p50/p95/p99 | **Not tested** | Stable four-node versus fully converged five-node runs under the same latency/error budget and independent ACK reconciliation |

The original Azure run remains a **failed scale-out qualification**. These runs
neither provide stable five-node capacity measurements nor establish the cause
of its two temporarily unreadable acknowledged records. No Azure resources were
created by this evidence work; production admission remains unchanged.

## Verify and replay

Run `python3 docs/evidence/native-recovery-continuation-2026-11-13/verify.py`.
It checks retained integrity and exact CI outcomes; it does not execute BicDB.
The separate `verify-native-origin-evidence.py` classifier retains every logical call, physical attempt,
retry, uncertainty and decoded frame projection in a reproducible derived report.
`verify-native-remote-projection.py` checks the public `a9b` ledgers while keeping
their scope limits explicit. Public projection integrity cannot re-open omitted
databases or validate omitted original bytes without the private original.

For an engine replay use a separate checkout of the full source SHA in its row,
the toolchain retained in that job, and the workflow's original commands:

```sh
cargo build -p bicdb-core --example chaos_child --locked
cargo test -p bicdb-core --lib --locked origin_pre_wal_fault_tests -- --test-threads=4 --show-output
```

For `1c5`, replace the filter with its exact selector above. For `6c9`'s eleven
tests use `native_hot_recovery_`. A new replay has its own outcome and never
replaces a historical failed execution. Durations above are whole test-suite
times, not operation latency percentiles or database RTO.
