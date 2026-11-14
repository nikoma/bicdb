# Native HA phase evidence follow-up — November 13, 2026

**Production qualification remains incomplete.** The original Azure experiment
remains a **failed scale-out qualification**. This packet supplies no new physical
experiment, converged five-node comparison, hardware RPO/RTO/latency percentiles,
transparent PostgreSQL execution or distributed SQL transaction qualification.

The documentation base is **1.0.497-beta**, main
`3d9e4701d222e0b15ea547d6a0265490b16eb921`. Measured candidates below report
**1.0.494-beta**, with their own exact unmerged source revisions. Their CI passes
are not passes of main497 or of one assembled release. This documentation changes
no runtime behavior, production admission, binary, release or deployment.

The [earlier seventeen-job packet](../native-recovery-continuation-2026-11-13/README.md)
is preserved byte for byte. [Its identity snapshot](previous-packet-identity.json)
pins all seventy files. This separate follow-up is bounded to **seventeen jobs,
eleven runs and nine explicit public projections** in [corpus.json](corpus.json).
The excluded successor `22c` run `43474702124` is tracked separately. No row
result from that successor belongs to this fixed corpus or replaces either
recorded compilation failure.

## Actual execution outcomes

Each duration is a whole fixture/cohort duration, **not database RTO or a latency
percentile**. [manifest.json](manifest.json) pins every public file, full source
identity, raw/compressed job-log digest and projection/omission manifest.

| Candidate source | Run / focused Core job | Actual result | Classification and boundary |
| --- | --- | --- | --- |
| `031681576891112929f65f77b09787e56120b41c` | [43465672013](https://github.com/nikoma/bicdb/actions/runs/43465672013) / 132024082465; build132024038303 | Both compile jobs fail E0308 | **Failed compilation; runtime not tested.** Diagnostic returned `&Option<String>` where `Option` was required. No selected/executed test or fixture proof. |
| `d09d1ca494cfb5d629817cafb18b88388f2f1403` | [43466821238](https://github.com/nikoma/bicdb/actions/runs/43466821238) / 132028116691 | **Passed 1/0**, 9.75s; all-target compile passed | Separate serial traffic-first diagnostic: two real APP ACKs and one first-range ownership transfer, without Strong warmup. |
| `7f2c5f0abc4b2a65700971e41ec362c5da0fbb03` | [43469307523](https://github.com/nikoma/bicdb/actions/runs/43469307523) / 132036734162 | **Failed 0/4**, 12.25s; all-target compile passed | Four original-case counterparts with passive traces: seven ACKs, one uncertain second write; three later original-round authority failures. No transfer boundary reached. |
| `6d5fe913fb6bdb032c53801b52ad6161fe6d0d95` | [43472904695](https://github.com/nikoma/bicdb/actions/runs/43472904695) / 132049216248 | **Passed 1/0**, 7.05s; all-target compile passed | Separate genuine-expiry component: distinct fresh MQ counts the same three still-live native close observations. No engine repair or failed-cohort replacement. |
| `9434dcdb1f9bc53683dc3e2c433de337dcc3ff83` | [43461532306](https://github.com/nikoma/bicdb/actions/runs/43461532306) / 132009482769 | **Passed 1/0**, 9.22s | Actual A owner and protected native proposal CURRENT/archive; no catalogue Installing or rows. |
| Same `9434dcdb1f9b` | [43463319835](https://github.com/nikoma/bicdb/actions/runs/43463319835) / 132016004732 | **Passed 1/0**, 10.62s | Actual cancellation after native commit; distinct exact retry retains the same proposal/A. Cancellation is not rollback. |
| `7349adaaa987042e164ca06bb7e9eb8522fdb851` | [43468856482](https://github.com/nikoma/bicdb/actions/runs/43468856482) / 132035186132 | **Passed 1/0**, 10.36s | Actual reader-fence post-rename/pre-directory-sync EIO and exact retry. No old-binary or power-loss proof. |
| `fd4d0595d70e82dff20889c19918e34dbd42575d` | [43468368964](https://github.com/nikoma/bicdb/actions/runs/43468368964) / 132033549088 | **Passed 1/0**, 9.43s | Actual catalogue Installing refusal tied to the protected proposal and original A; no native row copying. |
| Same `fd4d0595d70e` | [43469675906](https://github.com/nikoma/bicdb/actions/runs/43469675906) / 132038089731 | **Passed 3/0**, 9.95s | Happy, actual directory-sync EIO/exact retry, and actual pre-verification cancellation/exact retry. No rows, Ready or Serving. |
| `ea7d8e3494d788fc9b1c88bb87101df2ba1d0d45` | [43472582176](https://github.com/nikoma/bicdb/actions/runs/43472582176) / build132048033766, Core132048081792 | Both fail E0277 | **Failed compilation; runtime not tested.** Missing `Debug` on native row grant; zero focused Rust tests, no actual row fixture. |
| `a5ffc9d15ce75b2b6b0d987c7ea61f184cfe8988` | [43473382435](https://github.com/nikoma/bicdb/actions/runs/43473382435) / build132050841240, Core132050903126 | Both fail E0382 | **Failed compilation; runtime not tested.** Test serializes/moves an Arc then reuses it; zero focused Rust tests. Chaos-helper compilation is not row-test execution. |

The `d09d` fix changes only the diagnostic's optional-error borrow. It does not
change the earlier `0316` failed result. Likewise, preparing a source correction
for EA or A5 does not produce runtime evidence for either failed artifact. Each
compile/Core job pair reports one source/run’s compile error, not two executed
row tests or independent runtime reproductions.

## Maturity boundaries

| Deployment/capability | Implemented / source inspected | CI evidence here | Physical / production-qualified / verified deployed |
| --- | --- | --- | --- |
| Legacy/single-primary synchronous or asynchronous HA | Separate deployment contract in [original audit](../../ha-horizontal-scaling-audit-2026-11-10.md) | **Not tested by this corpus** | No new evidence; cannot inherit native-range component results |
| Distributed virtual-range recovery and relocation | Real native owner, closure, proposal and Installing paths on named candidates | **Passed components; failed four-case cohort; incomplete integrated path** | **Not established** by these same-process tests |
| Ordinary PostgreSQL routing, cross-range SQL and atomic multi-range writes | [Existing SQL limitations](../../ha-horizontal-scaling-audit-2026-11-10.md#current-postgresql-integration-check--2026-11-13-utc) remain | **Not tested or qualified here** | **Not established**; no application-transparent scale-out claim |
| Cell-scoped HA/encryption/fencing/recovery | Separate Cell authority/storage contract | **Not tested by this corpus** | No new evidence; no Cell or clinical production admission |
| Application fleet placement/scaling | Separate workload-placement contract | **Not tested by this corpus** | No new fleet capacity or database write-scaling evidence |
| Native target history/row installation | EA/A5 source exists; its tests fail compilation | **Not tested at runtime in this corpus** | No Ready/Serving, cold-recovery, failover or capacity qualification |
| Automatic add-node data/load redistribution | Earlier implementation evidence remains distinct | **No automatic four-to-five experiment here** | Original Azure qualification remains **failed**; stable capacity gain **not tested** |

The earlier audit’s distinction between merged GAP-1 source repairs and
qualification of an exact integrated artifact remains unchanged.

A catalogue refusal named Installing is not installed user data. A source
proposal or existing API is not proof that an operator can add a node and safely
obtain more application capacity. Index/FTS/queue/audit migration and recovery
are not qualified by these event-record/native-catalogue controls.

## Four-case cohort: exact failures, overlap and uncertainty

The original four `6d795aac644a` test bodies and assertions remain unchanged.
`7f2c` adds four observer-only counterparts under `first_app_observed_cohort`, with
its existing `--test-threads=4`, original INITIAL-open limit and once-only
two-second APP calls. It adds no Strong warmup, synthetic start barrier, manual
maintenance, routing hint or extra APP deadline. Journaling adds IO and changes
timing; observer effects cannot be dismissed.

All four actual fixture identities share process **7759**. Recorded APP
intent/reply intervals overlap across four fixtures; the common fixture-start
to last-API-reply interval is **7.908497780s**. These intervals include observer
IO and do not establish precise engine execution or socket-write completion.
Four configured test threads alone would not prove overlap. These are four
independent fixture clusters on one runner, not a physical four-node experiment.

All four first range-1 writes ACKed, with recorded intent/reply intervals of
approximately **769–835ms**. Three range-2 writes also ACKed, with intervals of
approximately **917–957ms**. These include observer IO. Those three cases then fail at
**`remote_source_tests.rs:862`**, recording a returned actual close into the
original OLD round. The individual `close_initial_old_step` has already returned
at line861. No target transfer callback, hook or cancellation boundary was reached.

The first check in `NativeJointOldRound::record_retained_closed_actual` is the
original round's `metadata.authority.require_live()`. Its transport observation
checks the original business ID and deadline. The ID was validated at construction
and is immutable; the exact retained error therefore localizes to expiration of
that original MQ, rather than an error returned by the individual fresh close.
This does **not** provide its missing historical context ID or exact deadline
sample. Original round/Role/OWN timestamps were not captured by this APP-only
observer, and earlier baseline-fence contexts must not be substituted.

A bounded read of retained logical catalogue JSONs finds same-scope native
OLD-closed hashes and promises through1 on **n1, n2 and n3 in each of those three
cases**. Actual close work progressed and its original owners were retained;
recording into the old round failed. This inspection is not a DB reopen or a
power-loss/WAL replay. Its script, source-file identities and limits are retained
in [the analysis](reports/7f2c-first-app-cohort-analysis.json.gz).

The remaining positive-transfer case ACKed range1, then its **second** APP timed
out. This differs from the historical `6d7` three **first**-APP “no member reachable”
failures. The three-case `fd4d` cohort also reached its APP prerequisites and
passed. Neither serial-pass/group-fail comparison nor observed overlap establishes
concurrency as the cause of either campaign.

The exact uncertain operation is:

| Field | Retained value |
| --- | --- |
| Record ID | `items/registered-fifth-ack-r2-0` |
| Expected JSON SHA-256 | `392cbf7c149065bb0e1c9d2cc7684c0c45b9ed228b5649e51e265629fe05b6a7` |
| Range / epoch / topology generation | `2 / 1 / 4` |
| Expected/actual APP destination | n2, `127.0.0.1:45217` |
| Recorded intent/reply interval / exact API error | `2,009,533,696ns`; `canceling statement due to query timeout` |
| Actual physical attempts | Two APP attempts, ten metadata-discovery attempts; original helper invoked once |
| APP request bytes | Both APP attempts wrote373 bytes; no complete APP reply was retained |
| Replica n0 | DB read guard unavailable at post-APP snapshot; full row and native command body unavailable |
| Replicas n2/n3 | Native applied index1 observed; full row payload and native command body not retained |
| Custody/Strong coverage | No DB custody for this case: `actual service busy at custody cut`; no live Strong read recorded |
| Assessment | **Inconclusive durable effect/full-payload reconciliation.** No observed row absence, no externally acknowledged range-2 commit, no rollback or data-loss inference |

All intents, immutable replies, discovery/error frames and internal attempts are
retained in [the public projection](artifacts/7f2c-first-app-cohort-public-projection.zip).
Discovery fanout is not counted wholesale as retries. Seven ACKs, one uncertain
write and zero helper retries do not make the four failed tests pass. These
are typed APP/metadata timeout outcomes, not observed HTTP404s; they do not
resolve the original Azure404 classification.

## Genuine expiry control and actual authority lifetime

`6d5` obtains a genuine original MQ with a **requested one-second budget**, then
produces all three actual close observations using independently fresh Role/OWN
requests with the existing requested five-second budgets. It waits only for the
observed real original deadline if needed. The old round refuses. A distinct
fresh genuine MQ counts the same retained close Arcs only while their own role,
plan and native checks remain live; no expired observation is retagged.

The actual original context is `01a1d5f8-30be-7041-9cd0-f12a243c1e22`; its
business is `01a1d5f8-30b6-7741-a336-de4b5a927e9c`. The fresh context is
`01a1d5f8-34fd-7fa0-a6c2-34ad98f58457`, with distinct business
`01a1d5f8-34f6-7f70-b037-fab889851a8a`. Both witnessed term1/read-index5.
Original/after-state native close hashes, scope, B, open IDs, namespace IDs,
rows and complete raw catalogues match; each close hash also matches its actual
retained catalogue record. No replacement close, birth, owner or epoch is minted.

**Requested five seconds is not actual five-second authority.** In this fixture,
`io_timeout_ms=1500`; server ingress creates a request deadline from that limit
(`distribution_transport.rs:1150`). The returned authority's actual
`context_budget_ms` is serialized (`distribution_consensus.rs:1244`) and bounds
the client's observed deadline (`:663`). The control records original remaining
budget about976ms and fresh remaining budget about1476ms. Its close-role deadlines
are approximately1.499/1.739/1.966s from control start, reflecting their different
acquisition times. The exact `7f2c` original observed deadline was not captured;
its expiry is localized by the predicate, not assigned a fabricated five-second
lifetime or reconstructed timestamp.

The [control verifier](verify-native-old-round-expiry-evidence.py) checks actual
context/business distinction, all three still-live fact checks, exact before/after
state and matching native-catalogue hashes. It cannot recreate sealed capabilities
or independently reopen WAL. Same-Arc and native commit status remain source-pinned
Rust assertions. The control excludes partial-debt/unclosable peers, does not
implement an automatic continuation worker, and does not repair the failed cohort.
Caller cancellation was not used as an expiry substitute: the generic transport
metadata observation retains a deadline rather than the original caller token.

## Proposal and Installing controls

`9434` retains an actual destination A lease and protected native proposal,
CURRENT/archive and commit-return XID. Full target state remains unchanged. A
post-native-commit cancellation returns an error while the exact native proposal
survives; a distinct fresh retry verifies the same proposal. The JSON checker
reconciles intent bytes, hashes, pointers, XIDs and existing native inventory;
it does not replay WAL or independently prove fsync.

`7349` injects actual post-rename/pre-directory-sync errors in the reader-fence
format path. Two prepared outcomes have no native XID; three EIO responses remain
errors. Only a later verified retry yields the exact native proposal. Target
state stays unchanged. Actual reader-flag durability is a Rust test assertion;
real old-binary rejection and physical crash behavior are **not tested** here.

`fd4d` then publishes one catalogue Installing refusal bound to the same A and
protected proposal. The actual raw catalogue changes format15→17 with one intent;
its checksum, candidate, intent bytes and pointer reconcile, while other target
state stays unchanged. EIO and cancellation attempts retain the same unverified
candidate and return their real errors; a distinct retry verifies that candidate.
The three-case cohort passes. **The old artifact has no explicit `native_after`
inventory field.** Hidden native inventory equality is asserted by Rust, not
independently reconstructed. No future instrumentation is retrofitted into it.
No history/rows, Ready, Serving, cold recovery or automatic relocation is proved.

[verify-registered-components.py](verify-registered-components.py) performs these
bounded logical checks directly from the five retained projections. Eleven
structured phase records correspond to seven selected/executed tests; phase-record
count is not test count. The historical ad-hoc check reports remain separate.

## Open findings and smallest next tests

| Finding | Classification | Smallest independent next test; original evidence retained |
| --- | --- | --- |
| Historical `6d7` first-APP no-member failures | **Inconclusive cause**; setup failures remain **failed** | Capture original first-APP actual request/error and readiness without warmup at that boundary; later-stage `7f2c` failures are not the same reproduction. |
| Original OLD collector expires after real close work | **Failed** three `7f2c` cases; fresh same-live-fact counting component **passed** | Exercise an actual continuation under the original scope, accounting for every retained closed/constraint/pending obligation. When issuance expires, require a genuine native reread producer; never renew a DTO or drop debt. |
| Timed-out range-2 write | Full payload/durable effect **inconclusive** | Separate bounded forensic recording after the unchanged failed API assertion: ordinary Strong outcomes and exact row/native-command observations on all three original replicas, plus successful custody. Do not infer rollback from timeout or native index alone. |
| Availability under cohort load | **Failed**; resource/scheduler cause **inconclusive** | Preserve the original two-second calls and record actual phase timing/ingress work. Include the passing three-case `fd4d` control; instrument before changing scheduling or budgets. |
| Row/history target pipeline | EA/A5 compilation **failed**, runtime **not tested** | Finish compilation of an immutable successor, then execute the exact stored-history/rows selector with current actual native proofs. No generated source or API signature substitutes for execution. |
| Post-install hidden inventory | **CI assertion passed**, independent after-inventory reconciliation **not tested** | Future source-only instrumentation captures a fresh explicit after-inventory; retain original `fd4d` artifact unchanged. |
| Cold recovery, real old binaries, source retirement, populated second range | **Not tested by this corpus** | Actual closed-scope recovery/compatibility and first-range Serving/ACK/Strong before second-range install, with original owners and bounded cleanup. |
| Automatic physical expansion, zero lost ACKs and capacity gain | **Not tested here; original Azure qualification failed** | Isolated four-to-five run with external ledger, fault cuts and complete convergence; compare sustained successful throughput only after convergence at equal latency/error budget. |

Machine-readable local findings are retained in [issues.json](issues.json);
[source-index.json](source-index.json) links the exact source revisions and predicates.
These identifiers are local findings, not claims that GitHub issues were opened.

The production gate is unchanged: preserve every acknowledged commit, provide
correct read consistency/routing, finish bounded fault-safe rebalance, and show
sustained throughput gain after convergence. The ultimate unchanged-application
PostgreSQL requirement additionally needs transparent distributed query execution
and durable, recoverable multi-range transactions. A roadmap checkbox is no proof.

## Verify or reproduce

Run:

```sh
python3 docs/evidence/native-recovery-phase-followup-2026-11-13/verify.py
```

This verifies identities, complete retained logs, every safe ZIP member/digest,
omission manifests, prior-packet immutability and independent logical reports.
Exit zero means evidence consistency; it does **not** turn failed CI or uncertain
writes into passes. Private originals include fixture DB/signing material and are
not copied here. Omitted members retain only path, byte length and SHA-256.
For the bounded private catalogue inspection, `summarize-native-first-app-cohort.py`
accepts the pinned original ZIP, `--cohort-report` and `--output`; it decodes only
named bounded logical range-catalogue JSONs, never key files or native pages.

Custody was observed with live owners and guards; it is not a stopped database,
atomic snapshot, cold replay or verified deployment. Public verification cannot
reopen omitted databases or validate their omitted bytes without the original.

For a new engine execution, use the exact full source in its row, its retained
`rustc.txt`, and the job's original commands:

```sh
cargo build -p bicdb-core --example chaos_child --locked
cargo test -p bicdb-core --lib --locked first_app_observed_cohort -- --test-threads=4 --show-output
```

For `6d5`, use
`native_registered_fifth_old_round_expiry_then_fresh_mq_counts_same_live_closes`.
Other exact selectors are retained in each job name and selection log. A new run
has its own result and never overwrites this bounded historical corpus.
