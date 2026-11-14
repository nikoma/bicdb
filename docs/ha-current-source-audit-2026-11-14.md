# HA and horizontal scaling: evidence checkpoint, November 14, 2026

Status checkpoint: November14, 2026, 18:56UTC. Running jobs below are not
classified as passes; later completion does not rewrite the pinned failures.

**Production qualification remains incomplete.** The original Azure scale-out
experiment remains failed. A new single-primary streaming election defect is
recorded in [issue190](https://github.com/nikoma/bicdb/issues/190); snapshot ballot
rollback is recorded separately in [issue191](https://github.com/nikoma/bicdb/issues/191).
Post-vote old-primary acknowledgments are recorded in
[issue192](https://github.com/nikoma/bicdb/issues/192).
The separate snapshot target-data-head gate is tracked in
[issue 193](https://github.com/nikoma/bicdb/issues/193).

The latest remote-main checkpoint is **1.0.513-beta**,
`2bd41020466a127e33af311aefeb3027ab8c8df3`, tree
`421b94455ce3a21cb61a138a9f3d453f2ebe5e13`. This is the backend-only handoff
refactor merged through PR199. Its [required run43625321980](https://github.com/nikoma/bicdb/actions/runs/43625321980)
has passed Core, native, PGwire and build jobs; SQL and remaining-workspace are
still running at this checkpoint. It is not an overall completed pass, and it
does not deliver the separate streaming or native recovery fixes below. No
binary, published release or deployment is inferred from that merge.
The documentation delivery version changes no runtime behavior. The inspected
main513 source still uses HA handshake4 and the issue190 data-epoch behavior; the failed handshake5
streaming candidate is not delivered by that backend refactor.

The earlier delivered-main inspection checkpoint is **1.0.511-beta**,
`74f17b912cec7f2fe093d10dcf1618a43b35f454`, tree
`38fdbe27eed492a617ed6b176de301f5965967f5`. Its tree equals tested source
`dd61ccce453531fe4f1898810316e0dc36fbeea2`. Its
[required CI](https://github.com/nikoma/bicdb/actions/runs/43590107125) and
[stock Rails PostgreSQL-adapter run](https://github.com/nikoma/bicdb/actions/runs/43590107140)
passed. Its separately built optimized CLI identifies as `bicdb 1.0.511-beta`:
428,212,584 bytes, SHA-256
`a1bf4ebbc16df9e4408d668fa6ca41e571788811eec976a9015d2fcbb1dcf296`.
Source delivery, compilation, application acceptance, physical qualification and
deployment are separate evidence states.

## Preserved failed-candidate checkpoints

Earlier failures remain tied to their exact source and original captures. They
are not relabeled by a later pass. Counts below are passed/failed/ignored.

| Exact source | Required run | Actual result |
| --- | --- | --- |
| Native `b7791177fc94fa8d1ea2072f6b4f62bb659000b6` | [43601117766](https://github.com/nikoma/bicdb/actions/runs/43601117766) | Native324/13/1 failed; other required jobs and Rails20 runs/100 assertions passed. |
| Native `503964ae93aa22f4c76b698a549b6ef857c834be` | [43608687107](https://github.com/nikoma/bicdb/actions/runs/43608687107) | Native324/13/1 failed; Core1817/0/16, PG511/0/2, SQL2681/0/4, workspace, compilation and Rails20/100 passed. |
| Streaming `2c0193c37852546c51b99e2918c24ea17bb44e2b` | [43609026644](https://github.com/nikoma/bicdb/actions/runs/43609026644) | Core/PG/workspace failed at issues197/198; native216/0/1, SQL2681/0/4, compilation and Rails20/100 passed. Twelve new epoch controls passed. |

The b779/503 activation errors were twelve pre-pin `QueryTimedOut` failures;
later intended cuts were not tested. The thirteenth foreign-service control
returned an error but its expected-text assertion failed. Source503 retains
`native range image: native writer store/root/open changed`; it did not identify
which identity disjunct failed. Neither a CPU/I/O cause nor deadline widening is
established. Later PR201 results are classified separately below.

The complete 503 capture retains eleven full logs/eight original ZIPs. Its
canonical manifest SHA256 is
`1ef5836a8c5431c453911cc81923f12f1dad0a13756d763bef25017190120777`;
complete verification SHA256 is
`80658df9c5a1ce9ff67ad1f162b5faca5ad1713fc313b070b74e7c10e4fcfdb8`.
The complete 2c capture retains eleven logs/seven ZIPs, zero unavailable logs or
partials, canonical manifest SHA256
`45314c66cab5fd23a2b28f098f1bea51742f2b753035c3a74cc49ba120febf7f`.
Its CLI failure-evidence upload had no matching files; no missing ledger is
invented. The original issue197 CLI ACK-set assertion passed before its old
primary rejoin wait failed. These fixture results do not supply physical RTO or
an independently complete external ACK/uncertain ledger.

PR188 was closed as failed historical evidence, preserving b779/503 and their
branch, before transferring its old512 reservation to streaming candidate363.
PR194 is now also closed as failed historical evidence, preserving source363
and its branch. That stale512 reservation is explicitly retired. Current main
is513; neither failed512 branch is an active release candidate and no next
version is reserved here. Full source SHA, rather than a shared historical
version label, identifies each result. No binary, release or deployment follows
from these old source-version assignments.

## Native source-validation feedback: PR201

The separate validation-only head
`27be25e3ee34f7ae1ba8d873ecd6aca495688b88` carries native source from
`7e9189160d3561f3c4e5de4a4889cae6b768049b` with source-version metadata
restored to the earlier main511 baseline. It is not a reserved release version
or a current-main513 integration test. Actual checkout
`a6cc44e94e553024d3ddb6fe8cb6dbbf0c50499f` has the same tree.
[Required run43621069383](https://github.com/nikoma/bicdb/actions/runs/43621069383)
has a terminal native failure: **330 passed, 13 failed, one ignored**, in
3,134.09 s. The remaining-workspace job is still live at this checkpoint;
completion of that job cannot turn the failed native suite into a pass.
All-target compilation, Core, PGwire, SQL and Rails passed on this candidate.
The native failure still blocks the candidate overall.

All twelve earlier activation failures now reach the actual native-page-WAL
operation, whose stage records completion, before a later poststate check
refuses `range mutation gate: controlled WAL/native callback is still active`.
Source tracing identifies the operation's own Controlled owner: activation keeps
it until after the postcondition calls a quiescent-Frozen validator. These are
actual errors after WAL, not evidence of rollback. The failed path does not
retain an exact native xid or a returned bridge-success receipt. The WAL stage
completed, but final Serving admission did not; the API error does not imply
rollback and does not authorize a new operation or an invented retry outcome.
The [source reproduction and required repair boundary](https://github.com/nikoma/bicdb/issues/195#issuecomment-6882534454)
retain this result. The next repair releases only the operation's own Controlled
guard after its synchronous writes finish, before the unchanged strict
postcondition. It retains the same Frozen owner and journal. Any other active
Controlled owner must still refuse; deadlines and publication/recovery guards
must remain unchanged.

Five of the six new post-collection negative controls passed their original
assertions: foreign archive, local archive, producer Session cancellation,
current MStore change and controlled writer. Passing stdout was not retained
by the broad runner, so no missing numeric clocks or JSON values are supplied.
The Store-ABA case really replaced/restored the slot and correctly revoked its
old owner, but its diagnostic baseline then refused
`actual service range Store slot ended or changed` before invoking the actual
publisher. That case failed; its intended publisher refusal/WAL-absence
assertions are **not tested**. The original issue196 foreign-service control's
corrected typed matcher passed on this source.

The complete native raw log is 13,407,991 bytes, SHA256
`6e8366723c57bb36f5e2e6d036d2a22de820865e8e64d9611aae732cc7c38c26`.
The native classification receipt SHA256 is
`0037c0e8bbc72d9ea1c0a64f20664db4eb379f57ba47cae0e513a7de27518b4c`;
the original Store-ABA pre-assertion observation SHA256 is
`cf11ad70e55827b14e056d32d9c81efb973b90e2249cb738984951066646e680`.
Earlier b779/503 failures remain unchanged. These component results establish
neither full native handoff nor production HA qualification.

## Final-versioned streaming feedback: 363d

The separate **1.0.512-beta** streaming candidate is
`363d29ff83ff117695e010e55a713973e9ceb3a7`, tree
`df53322f18037877ff8ad1abd954394a78bd3713`. Runtime and test source match f41;
the additional changes are documentation and synchronized version metadata.
Its actual CI checkout `cd33aee2760515f4362bf2dd013b21bcc3ce26e5` has the same
tree. [Required run43619046752](https://github.com/nikoma/bicdb/actions/runs/43619046752)
**failed overall**, with all jobs terminal. All-target/non-default-consumer
compilation and repository checks passed.
[Rails43619046683](https://github.com/nikoma/bicdb/actions/runs/43619046683)
passed 20 runs/100 assertions. These are actual executed component results.

PGwire **failed: 509 passed, one failed, two ignored** across 25 completed
targets. The original issue197 embedded returning-primary acceptance now passed,
including its original fencing assertions. That corrects the earlier observed
failure in this scenario; it does not qualify every failover schedule.
The newly reached failure is
`scheduled_restore_results_are_available_through_the_protocol` at
`crates/bicdb-pgwire/tests/verification.rs:65`, tracked as
[issue200](https://github.com/nikoma/bicdb/issues/200). Its unchanged 40-second
wait found only failed restore/promotion results with
`unqualified legacy HA data epoch; validated snapshot bootstrap required`.
The verification target recorded two passed/one failed. Its complete PG log is
848,910 bytes, SHA256
`cd621609daadeb085d768e04ce511bb4c385e305fa744420cbc9f2215d30462f`.
The actual stale-Ping TCP/ballot control also passed in the PG library target.

The test creates a genuinely fresh standalone source, commits rows, compacts it
and drops it before starting archiving. Source inspection shows the compact's
WAL-cut path calls retention persistence, but its unchanged empty slots/holds
return without publishing the already-known in-memory genesis. Reopening the
populated source then correctly refuses unknown history. The next correction
must preserve that original qualified history at the actual producer boundary;
it must not bless arbitrary legacy data or bypass scratch promotion checks.
There is a second source boundary: `restore_from_log_archive` in
`crates/bicdb-core/src/db/log_archiving.rs` explicitly removes the HA state file
before standby replay and again before returning an ordinary database. Even a
qualified archived base therefore loses its lineage evidence. A correction
must preserve verified DATA history and validate the actual post-replay durable
point while separately removing copied source primary/voting authority.
Retention publication alone cannot fix this path. A source-only prototype
`f334aa9a8328e954a72e8f39dc9944f8725ad478`, early actual-directory-lock correction
`a03e4d3fdede1a1b0e356df43f3e4dfb02df8fd7` and full base-map correction
`a9aa6bc4878efb8b3126f53f6ea73f5878d663f5` are uncompiled and unexecuted.
Their nonempty-replay qualification is **unapproved**: retaining only the base's
qualified epoch loses later archive epoch boundaries, and retaining only one
base-map entry also loses earlier divergence refusals. Contiguous sequence and
frame checksum checks do not prove those histories. The a9 child preserves all
base refusal entries, but does not supply missing suffix provenance.

An actual archive-owned lineage producer is an unimplemented design: capture
qualified source history and exact frames under real commit exclusion, bind the
verified base/prefix and persist proof before releasing its WAL hold. The online
PGwire base-capture path needs the same actual producer. A batch below the
current qualified epoch's start cannot borrow that future epoch or choose a
past one from gossip. The producer also needs its own physical sync barriers:
`write_durable` uses the configured sync mode and is not by itself an
unconditional fsync when the original fixtures use buffered mode. No raw append
DTO, blanket Unknown result, scratch genesis or waived promotion assertion
completes this recovery path. Compatibility, reopen/trim/prune, actual writer
exclusion and source-branch continuity remain required. None of this design was
executed by run363.

Core subsequently **failed: 1,456 passed, two failed, 14 ignored** across 70
completed outer targets. `log_archive_pitr` recorded six passed/two failed:
`an_empty_database_also_has_a_verifiable_archive_identity` at line367 and
`restore_drill_compares_source_and_proves_scratch_promotion_and_restart` at
line338 returned the same explicit epoch refusal. The empty case selects
EmbeddedMemory; the looping case does not label its failing storage iteration,
so its per-mode reachability is not established. These tests use fsync=false;
they establish functional results. Core's complete raw log is 1,723,836 bytes,
SHA256 `ec11a49ef2e7b732b87311ae5226255201ca2fbf664efc38b50ae24929f275d6`.

All **15** genuine qualified-epoch Core controls passed in 2.96 s, including the
three f41 controls. The original issue198 attachment acceptance also explicitly
passed. Those case-level results do not make the failed Core target set pass.
The retained component receipt SHA256 is
`1d3747e5e074c2e7bad736174fec67bba732d8ac4dbcd8cdcfbc44d3fb20c49e`.
Native passed **216/0/1** and SQL passed **2,681/0/4**; Rails passed
20 runs/100 assertions. The remaining-workspace job **failed**, with 121
completed outer targets recording 1,028 passed/three failed/six ignored, plus
one unfinished nested target; nested totals are not additive. All three
failures occur at the shared `Database::open_replicated` helper in
`crates/bicdb-app-host/tests/replicated_state.rs:86`, returning the same explicit
unqualified-epoch refusal. The failing cases are
`server_paged_native_open_keeps_the_persisted_engine`,
`native_failover_preserves_migrations_outbox_job_leases_and_effect_receipts`, and
`server_paged_native_failover_preserves_migrations_jobs_and_effect_receipts`.
The failing node/iteration and any acknowledged-effect loss are not established
by that helper failure. These replicated-open failures are distinct from the
PITR failures despite sharing the refusal text; the
[issue200 follow-up](https://github.com/nikoma/bicdb/issues/200#issuecomment-6882553415)
retains their separate bootstrap investigation and archive-producer blocker.
No intended later failover/effect guarantee is inferred from a test name. The REST raw log is 107,774,173 bytes, SHA256
`adc53dc310ebb106ab69191c4a606ff95478b5ae589e3251e46329cddc6abb27`.

The original issue197 CLI case now passed, and its entire unchanged
`ha_failover_cli` target passed four cases in 34.31 s. The embedded issue197 and
attachment issue198 cases also explicitly passed. These correct three earlier
case-level failures without retroactively changing run2c. Fixture duration is
not a production RTO, and its test-process ACK set is not an independent
complete external ledger. Run363 still has six new failures: Core2, PG1 and
REST3. It remains failed and unmerged, and PR194 is closed as historical evidence.

The frozen terminal capture retains eleven lossless job logs and eight original
artifact ZIPs, verified against API hashes, gzip/ZIP CRC and member hashes:
68 retained files/34,029,568 bytes, zero unavailable files or partials. Canonical
manifest SHA256 is
`8326957eed9fd4e7536eaec3d9ac9b7dde244db30ee35e3514e5b4d5bd821d25`;
complete verification SHA256 is
`71a56088de6293b286ed89197e51c5107127dab598648e520a4f7fd3c2092b97`.
The exact old-case correspondence receipt SHA256 is
`f9bee37801594f39752c56953bc6fdaf13b2dfeac81d898e833f69a9fe792c27`.
The raw files remain separately retained; these hashes are provenance, not new
execution or production qualification.

## Findings classified at this checkpoint

| Finding | Classification | Scope |
| --- | --- | --- |
| Original Azure four-to-five convergence within 300 s | Failed | Historical experiment remains failed; no stable converged five-node comparison. |
| Original two temporarily unreadable ACK records' causes | Inconclusive | Later matches do not establish routing/read-watermark/catch-up cause or the causal visibility-restoration event. |
| b779 native handoff acceptance | Failed | 324 passed/13 failed/one ignored; raw log and complete capture retained. |
| [Native activation issue195](https://github.com/nikoma/bicdb/issues/195) | Failed | Historical503 pre-pin timeouts remain failed. PR201's twelve cases reach native WAL, then refuse their own active Controlled poststate; no final native receipt/outcome is invented. |
| [Foreign-service control196](https://github.com/nikoma/bicdb/issues/196) | Passed on PR201 | Correct typed matcher executed; historical503 assertion failure remains unchanged. |
| 2c's new epoch/ballot component controls | Passed | Twelve file-backed Core cases at exact2c source; no physical/production qualification. |
| 2c returning-primary and attachment-recovery acceptance | Failed | Core, PGwire and CLI failures at issues197/198 boundaries; CLI ACK-set survival passed before rejoin failed. Later assertions were not reached. |
| 363 file-backed qualified-epoch Core controls | Passed | All fifteen cases executed; hosted component evidence, not physical qualification. |
| 363 returning-primary embedded acceptance | Passed | Exact-source original issue197 case passed; this is one hosted scenario, not physical qualification. |
| 363 original attachment recovery acceptance | Passed | Exact-source original issue198 case passed; the later archive-restore failures remain separate. |
| 363 scheduled restore/promotion acceptance | Failed | Issue200; the PG case fails with explicit epoch refusal. Separate Core archive and REST bootstrap results are listed below. All363 jobs are terminal. |
| 363 application replicated-open acceptance | Failed | Three REST cases refuse unqualified history before their intended failover/effect checks. Source bootstrap and PITR causes remain distinct. |
| 363 original CLI rejoin acceptance | Passed | All four original CLI cases passed; functional evidence, not RTO or an external ledger. |
| PR201 new collection controls | Failed | Five passed; Store-ABA diagnostic failed before the actual publisher, leaving its intended publisher assertions not tested. REST remains live. |
| Archive-lineage producer for issue200 | Not tested | No producer implementation exists. a9 retains full base refusal DATA only; nonempty suffix qualification remains unapproved pending an actual source-owned proof. |
| 363 Core archive restore/promotion acceptance | Failed | Two actual log_archive_pitr failures; no arbitrary Unknown-history qualification is an acceptable correction. |
| Snapshot target-data-head/ACK-copy protection193 | Not tested | Open source/API gate; the separate genuine regression source 6bb1f64e is unexecuted. |
| Original transaction-effect replication148 | Failed | Source protocol coverage: intent/command omit the original audit, sync and broker envelope. Dynamic acknowledged-effect loss has not been demonstrated. |
| Current-artifact physical RPO/RTO and latency percentiles | Not tested | No independent-machine current-artifact qualification dataset. |
| Sustained throughput gain after complete convergence | Not tested | Neither relocation-phase rates nor shared-host debug measurements demonstrate this gate. |

## Evidence levels by deployment mode

The following matrix retains the delivered511 subsystem inspection and
identifies separate candidate results explicitly. The backend-only513 refactor
does not establish new operational HA qualification. Streaming2c/f41 results above do not
retroactively change delivered511. Implemented means the specified code path exists. CI-tested means an identified
scenario actually executed; a compiled API or a test definition is insufficient.
Physical evidence states its host/guest and revision limits. Qualification means
the complete release-specific production gate passed. Deployed means an
operational instance and its configuration were independently verified.

| Mode/profile | Current implementation | Test evidence and limit | Physical evidence | Production-qualified | Verified deployment |
| --- | --- | --- | --- | --- | --- |
| 1: legacy single-primary file shipping | Ship/status/promote, with explicit fencing and redirection | Retained durability/crash regressions; no new physical run here | No current-main RPO/RTO campaign | Not established | Not established |
| 1: native single-primary synchronous/asynchronous streaming | Ordered streaming, voting-only ACKs, elections/lease fencing; zero-loss claim blocked by issue190 | Configuration/election/bootstrap/kill regressions exist and required main511 CI passed; partial higher-epoch catch-up followed by another election is not covered | Historical limited failover results do not qualify511 | No; source safety blocker plus incomplete qualification | Not established |
| 2: generic/legacy distributed range controller | Metadata consensus, per-range quorum, automatic placement and relocation | Retained functional and shared-host tests; ordinary SQL integration remains incomplete | Original461 Azure run showed some automatic movement; scale-out qualification failed | No | Not established |
| 2: native distributed data/recovery path | Genuine physical owners, OLD/image/NEW/decision components and local continuation; automatic operator closure incomplete | Native503 failed324/13/1; later validation201 failed330/13/1. Neither is current-main513 integrated acceptance | No new pinned four-to-five native campaign | No | Not established |
| 3: Cell-scoped HA | Signed/encrypted Cell authorities, lease/fencing and authorized recovery; Phase5 asynchronous positive-RPO contract | Authority tests and synthetic signed drill fixtures; an admission verifier is not an operational drill | No current release-level recovery drill established | Not established | Not established |
| 4: application fleet | Worker placement, leases/reassignment and single-writer state groups | Retained multi-process loopback and lifecycle scenarios; application workers do not create distributed SQL capacity | Independent-machine throughput gain not established | Not established | Not established |

The complete [maturity audit](ha-horizontal-scaling-audit-2026-11-10.md) retains
the configuration and subsystem boundaries. The
[original Azure forensics](azure-original-run-forensics-2026-11-11.md) preserves
exact failed evidence: **404 was a count of failed logical operations, not an
HTTP status code**. Its two unverified ACK reads were later matched, but the
original responses/routes/read-time watermarks and causal visibility-restoration
event were not retained. Their exact causes remain inconclusive. The 300-second
convergence check failed; bounded useful progress versus churn was inconclusive.
The Azure observations concern distinct VM guests. Independent physical-host or
device-power-loss resilience was not established.

## Streaming election: unmerged correction and current open defect

Main511 rejects an automatic five-voter configuration with nonzero synchronous
K=1. `HaServerConfig::validate` checks the current and every future-primary role;
Nonzero K must satisfy `K >= floor(group_size / 2)`, giving a majority of
durable voting copies including the primary. Only voting peers count toward ACKs
and the liveness lease. K=0 and timeout degradation deliberately permit
asynchronous acknowledged-loss risk. The original arbitrary-K configuration
finding is therefore closed at source, not physically qualified.

The separate issue190 counterexample still applies to a valid five-voter/K2,
timeout-fail configuration. A heartbeat calls `record_primary` before applying
its bounded batch; that function immediately raises persisted `data_epoch`.
`consider_vote` compares `(data_epoch, applied_seq)` lexicographically. A new
primary at sequence4096 can consequently label a standby with its newer epoch
after only512 commits, and that standby can later outrank an old-epoch voter
holding4096 acknowledged commits. Read catch-up readiness is not consulted by
elections. Two sequential primary failures expose the unsafe schedule.

This is a **confirmed source counterexample; runtime reproduction and physical
data-loss testing are not performed**. The issue contains the exact schedule,
pinned source links and next deterministic tests. A safe correction must bind
voting history to actual accepted contiguous durable history, handle old
mislabeled persisted state conservatively, preserve legitimate divergent-history
ordering, and reject unsafe old/new protocol mixing. Merely moving the epoch
assignment after a partial batch, comparing sequence alone, or taking the maximum
of all announced epoch starts does not establish that invariant.

Relevant source: `crates/bicdb-pgwire/src/high_availability.rs`,
`crates/bicdb-core/src/db/ha_node.rs`, `ha_bootstrap.rs`, and `switchover.rs`.
Existing regressions are in `crates/bicdb-core/tests/live_replication.rs`,
`crates/bicdb-pgwire/tests/embedded_ha.rs`, and
`crates/bicdb-cli/tests/ha_failover_cli.rs`. Passing those existing tests does
not resolve an uncovered failure schedule. This issue is independent of the
virtual-range and Cell protocols.

Issue191 identifies a separate source-level snapshot race. Staging captures the
target's ballot, but a peer can persist a later vote before shutdown. Installation
does not compare the latest target HA state before replacing it, allowing that
later ballot to disappear. The deterministic stage→vote→reopen schedule and
required recovery controls are retained in the issue. No runtime double-vote or
data-loss experiment was performed. Protecting this path requires install-time
authority/data checks; blindly merging ballots can still overwrite data after a
promotion. A refusal of changed state preserves safety but does not establish
automatic bounded recovery. Old/new installer compatibility must account for
bootstrap installation occurring before the ordinary format-reader check.

Issue192 records the related vote/ACK induction gap. After a standby votes in a
higher term, an old-primary stream can resume: its epoch is compared to the last
known primary epoch, rather than the durable ballot term. A delayed candidate
can promote using earlier votes after the old primary has obtained newer ACKs.
The initial silence check does not exclude reconnection after voting. Both early
and final apply/ACK term checks are necessary; a vote can occur between them.
This is another confirmed source schedule, not an executed loss experiment.

Issue193 records that HA-file equality does not bind the target's current applied
data: same-lineage frame progress can update WAL and replication state without
changing that file. A stage→later durable apply→install control is still required.
Whether the ordinary CLI permits that interleaving needs a separate exclusion
proof; the public Core/embedding API has no demonstrated data-head binding.
This is an open API/qualification gap, not a reproduced CLI data-loss incident.

## Native range components and the missing automatic operator

Candidate b779 adds genuine protocol implementations beyond delivered511.
The older [operator-closure review](design/native-operator-scale-out-closure-2026-11-13.json)
must be read with these source-specific corrections:

| Boundary | Actual candidate b779 state |
| --- | --- |
| Native placement/capacity loop | Missing. CLI/operator maintenance still drives CONFIG enrollment and initial-registry refresh. |
| Source Start/Describe | Authenticated implementation exists. Initiating callers are fixtures. |
| Target image work | Genuine bounded operations and retained owners exist; process maintenance does not schedule their execution. |
| Local joint/Serving continuation | Substantial actual-service implementation exists; its host adapter/driver remains fixture-only. |
| Remote phases | Materialize, Observe, PREPARE and native DECISION exist. Remote catalogue decision, Serving preparation, activation, Open and ALL-release are not connected. |
| Retirement and cleanup | Export-reference retirement exists. Complete OLD retirement, physical deletion and terminal convergence remain missing. |
| Serving and restart | Local Registered-to-Joined publication exists. Automatic operator publication/status and crash-resumable orchestration remain missing. |

Concrete source/caller evidence:

- `crates/bicdb-cli/src/native_metadata_command.rs:553` runs the 500 ms maintenance
  loop, explicitly limited to CONFIG enrollment.
  `crates/bicdb-core/src/distribution_native_host/operator.rs:280` calls
  `maintain_joins`;
  `crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/maintenance.rs:20`
  ends at metadata promotion.
  These files are byte-identical between delivered511 and candidate b779.
- `crates/bicdb-core/src/distribution_native_host/joined_data_handoff.rs:31`
  explicitly excludes a production caller and cold restart. Its only
  `NativeJoinedDataHandoffHosts` implementation is the `Five` fixture in
  `fifth_data_path_tests.rs:241`. Local service/State/OLD owners cannot become
  remote capabilities through a TCP proxy.
- `crates/bicdb-core/src/distribution_transport/native_source_discovery.rs:43`
  implements Start/Describe; initiating calls are in the discovery/retirement
  fixtures. `crates/bicdb-core/src/distribution_range_handoff/joint/remote_new.rs:62`
  ends at `PublishDecision`, explicitly
  excluding catalogue vote or Serving.
- `crates/bicdb-core/src/distribution_range_image/remote_source.rs:1019`
  retires typed OLD custody, with its initiating caller
  in `hot_policy_tests.rs`. Registry admission still has an eight-session cap
  and no session-removal producer. `AwaitingCleanup`/`Cleaned` definitions have
  no producer that advances a relocation through those stages.
- Generic balanced supervision cannot bypass the native contract:
  `crates/bicdb-core/src/distribution/native_joint_handoff.rs:468`
  refuses protected membership/cleanup mutation;
  `crates/bicdb-core/src/distribution_transport/initial_data_listener.rs:94` refuses generic
  snapshot/import/cleanup operations.

Thus a newly joined native metadata node is not automatically a materialized,
Serving data node. The next remote protocol dependency is recipient-owned
catalogue-decision publication/reconciliation over the same retained native
DECISION, followed by genuine remote preparation/activation/Open/release. A
worker stopping safely CLOSED after image installation is useful partial work;
it does not demonstrate automatic fault-safe scale-out.

## Application and qualification gates

The range-quorum contract must also remain narrower than full transaction-effect
HA. [Issue148](https://github.com/nikoma/bicdb/issues/148) remains open at delivered511
and streaming candidate363: `CommitAdmissionIntent` in
`crates/bicdb-core/src/db.rs` and `RangeWriteCommand` in
`crates/bicdb-core/src/distribution_range_consensus.rs` carry record mutations,
not the original signed audit/sync/broker envelope. The native bounded-command
constructor retains that shape. Later local WAL effects in
`crates/bicdb-core/src/db/spatial_paged.rs` do not establish original-effect
parity on surviving range replicas. This is a source-confirmed coverage gap,
not a demonstrated cause of the Azure failures or an executed audit-loss test.
Single-primary streaming transports `CommitFrame` records; its optional
`record_journal` event payload is disabled by default. It is not whole-WAL
shipping and does not establish every original transaction effect. Cell HA and
fleet placement remain separate.

The next deterministic test must retain original effect IDs and payload hashes
for one genuine same-range row/audit/sync/broker transaction, obtain actual
durable quorum evidence, remove only its test-owned source and reconcile all
effects using actual survivors. Recreated audit IDs cannot substitute for
original parity. A row-only external ledger cannot qualify this broader gate.

Ordinary PostgreSQL applications still lack a demonstrated general transparent
cross-range SQL executor, joins, global snapshot and recoverable atomic
multi-range transaction coordinator. The stable namespace/record-key hash is not
a tenant-locality declaration. Locality must remain an optional optimization;
it cannot substitute for correct cross-range transactions. Stock Rails/local
SQL passes do not qualify distributed SQL.
The [locking consistency alternative](https://github.com/nikoma/bicdb/blob/363d29ff83ff117695e010e55a713973e9ceb3a7/docs/design/distributed-sql-locking-consistency-alternative.md)
is an unimplemented design amendment. It separates rigorous locking from fixed
PostgreSQL MVCC snapshots and preserves the requirement for ordinary clients,
atomic cross-range writes and joins, crash recovery and optional locality. It
changes no capability or qualification status.


The original Azure run, local component controls, simulated/synthetic fixtures,
CI compilation and existing APIs are not production qualification. Current
physical RPO/RTO and sustained p50/p95/p99 under fault-safe rebalance remain
unestablished. No sustained capacity gain is demonstrated without a stable,
fully converged five-node comparison at the same workload and latency/error
budget. The production gate remains zero lost acknowledged commits, correct
read consistency/routing, bounded fault-safe rebalance and measured sustained
throughput gain after convergence.

## Later terminal follow-up: PR201

This follow-up preserves the earlier18:56UTC checkpoint above. The remaining
workspace job132526673940 completed successfully at19:12:58UTC on November14.
PR201's required run43621069383 is now terminal **failed** because its native
job remains330 passed/13 failed/one ignored; all other non-skipped jobs passed,
as did the separate Rails run. None of these results tests the changed515
integration candidate.

The complete locally restricted archive retains eleven full gzip job logs and
eight unique original ZIPs. All stored/decompressed hashes and ZIP CRCs were
verified;344 unique native test names reconcile to330 passes/13 failures/one
ignored. Four skipped manual jobs have no log; no non-skipped log or partial
download is missing. Canonical manifest SHA256:
`735a9da19fe68b84b95b2e821a588cc7bf2fd70f7af0e13a68c6ee5fab9350bd`;
complete verification SHA256:
`c58958d3a133249d389da504e292560f71cd6be69009a8c91b4e93a5083927bf`.
Local restricted modes do not imply owner-only GitHub artifact access. The
original failed source and all prior classifications remain unchanged.

## Later terminal follow-up: PR203

Delivered source is now **1.0.514-beta**, `7fc833e3a9130ea710a0fbbfbaba6b065eedf8d3`;
PR202 delivered this documentation checkpoint and synchronized version metadata.
It changed no runtime behavior. Its required PR checks passed. The separate
push checks are a distinct run and are not implied by the PR result.

The changed native candidate in [PR203](https://github.com/nikoma/bicdb/pull/203)
is **unmerged 1.0.515-beta**, source
`7b965c6854843b7a127a5575214deddee2bc2c36`, actual CI checkout
`316c34d465b186a7c3a72b069a3696d70919f19d`, identical tree
`dbb9160973c722fc63b3b13546ce0194c41281c9`. It includes the main513 backend
refactor, retained native handoff batch, own-Controlled-guard lifetime repair,
StoreABA diagnostic correction and post-WAL failure observations. None of the
following evidence establishes a delivered runtime fix or physical qualification.

[Required run43632072415](https://github.com/nikoma/bicdb/actions/runs/43632072415)
is terminal **failed**. Its serial native job132560706302 reports **329 passed,
14 failed, one ignored**, in3856.86s. This duration is not RTO. Core, SQL,
PostgreSQL-wire, compilation and the remaining workspace passed; the latter
completed at20:52:24UTC. The separate Rails run43632072572 reports20 runs,
100 assertions, zero failures/errors/skips. These passes do not override the
failed native acceptance tests.

| Finding | Classification | Actual evidence and limit |
| --- | --- | --- |
| Six collection negative controls | Passed | All six exercised Registered n4 refusal, including StoreABA. They do not exercise successful survivor n2/n1 publication or full serving availability. |
| Separately retained Controlled callback | Passed | Core's two-Controlled regression preserves refusal while another actual callback remains; the same Frozen owner is retained. |
| Twelve original hot serving scenarios | Failed | All panic at `hot_policy_tests.rs:863`, during `publication.publish_actual_local_member`, with `QueryTimedOut`. |
| Three native-commit/postcondition cuts | Failed | Native commit returned before `complete_postcondition` timed out. Historical readback DATA is present; successful publication/recovery is unproven. |
| Eight post-preflight cuts | Failed | Final foreign validation and journal preflight complete; timeout precedes the WAL-entry marker. No marker does not prove absence of prior native effects. |
| One early-ALL cut | Failed | Foreign prepared n4 validation expires before journal entry. Later early-ALL admission assertion is not tested. |
| Foreign-MStore writer-domain scenario | Failed | An incidental fixture `try_lock().unwrap()` fails before the intended foreign-MStore cut. The intended safety rejection is not tested. |
| Closed-ingress metadata recovery | Failed | A newly elected leader lacks the committed current-term anchor before the wait expires. Later ingress rebind/worker resume is not tested. |
| CPU versus disk cause; two additional failures' causal schedules | Inconclusive | Traces locate stages, but no controlled causal reproduction or profile establishes the cause. |
| Later Open, ALL-release, FIRST/new-DATA APP and full Strong assertions | Not tested | The twelve whole-test failures stop before these assertions. |
| Physical fault-safe scale-out, stable five-node throughput, current physical RPO/RTO | Not tested | No replacement Azure cluster or new qualification run is implied by CI. |

The three post-WAL cases are
`native_registered_fifth_remote_hot_closed_actual_mstore_contention_same_own_progress`,
`native_registered_fifth_remote_hot_data_lost_committed_decision_reply_keeps_same_attempt`
and `native_registered_fifth_prepared_fresh_actual_expired_round_then_new_collection`.
Each reports n2/range1/native xid36, matching expected CURRENT/archive checksums
and equal raw record hashes, durableLSN160, nextLSN161 and nextXid37. The gate
reports Controlled0, no capture pin, no poisoning and the original Frozen owner
retained. These are helper-reported readback facts, **not independently
cold-opened database reconciliation**. The failed API call does not establish
rollback, durable-data loss or permission to execute a new logical transaction.
WAL begins with9/11/19ms left and takes5088/5117/4911us. The physical
postcondition starts with3/5/13ms left and takes approximately76–77ms. Observed
cancellation-request bits are false; the original deadline expires. These
measurements identify a finite-window failure, not its CPU/I/O cause.

Earlier assertions did execute before these later failures: CLOSED audit
reply-loss and both CLOSED MStore controls; both Registered DECISION same-attempt
lost-reply retries; and genuine new collection after an expired round. These
are bounded partial passes, not twelve wholly untested fault scenarios or
successful end-to-end serving tests.

The additional RF3 failure is
`native_writer_data_domain_actual_foreign_mstore_refuses_without_native_change`,
at `writer_data_domain_fact_tests.rs:102`, before owned-member observation and
before the deliberate foreign-MStore fault; the member ordinal and lock holder
were not retained. The additional supervisor failure is
`native_owned_metadata_supervisor_closed_ingress_cannot_hold_leadership_same_worker_resumes`:
the wait at `metadata_supervisor_tests.rs:205` follows actual n2 election and
stopping n2 ingress. Final n0 is Leader/term3/commit0/log2/last-term3; n1 is
Follower/term3/commit1/log1/last-term1. Neither an unanchored Leader status nor
the observed store-busy messages prove successful recovery or its failure cause.

Reproduction remains the exact source's unchanged serial native CI target;
all344 actual case names and complete failure sections are retained. See the
[executed issue195 update](https://github.com/nikoma/bicdb/issues/195#issuecomment-6884036164)
for source identity, cuts and the next isolated checks. A standalone filtered
pass would not replace the original serial-suite result.

The smallest next tests must preserve original deadlines and application
assertions. Validate one publication with the exact held journal/DB/Frozen
owner, independently reread post-write physical state and retain complete
foreign validation plus fresh request/MStore/Role checks. Corrupt CURRENT,
archive or a retained middle history entry; change the actual owner/slot; and
hold another Controlled callback: each must refuse without false Serving or
Strong readiness. Exercise the original positive serving path after those
controls. For the RF3 setup, distinguish original-OWN acquisition from the
deliberate foreign-MStore rejection. For the supervisor, control the suspected
MStore contention schedule and independently verify current-term anchoring;
do not widen deadlines, remove assertions or classify failures as flakiness.

Native raw log13,364,752 bytes has SHA256
`e4d350e73723c7ab8da14bdca191b8aaa2ac3df72cf6e629a9a6ae1d10d5d903`;
gzip SHA256
`c53e857455bd226d131468b0e40dde9ee3ef8e7db46b423deef313844cf38977`.
The failure classification SHA256 is
`411f65f5bacbf9b89f0da545435158a1e563170708a6198185064c005d34e855`;
the independently source-pinned cut matrix SHA256 is
`734ffe4fa2821063b85ce63d7f3f68b1b810afc77cbdd1f6e042768cc0a46250`.
The complete restricted archive retains eleven full gzip logs and eight unique
original artifact ZIPs. Offline rehashing, ZIP API-digest/CRC checks and all
fifteen job states (four skipped) reconcile without a missing non-skipped log.
Canonical manifest SHA256:
`1a8fc56fbd309a7e8d4960399920c24030be7d3d766a457b9dcdfde8d745fcf0`;
complete archive verification SHA256:
`cdc84cacc4de953d2d76f2686927628c6f58ac6b5ce63f713bf4b777b2243c93`.
All prior failed candidates and the failed Azure experiment remain unchanged.

## Source-only operational follow-up: candidate1fc

This follow-up inspects candidate **1.0.515-beta**,
`1fcfdc3adf7e38422173e072e6fec4ad56e48188`, tree
`9d24cdf8cac1d9db4da35227182c71ff72a1bb7e`. Delivered main at this checkpoint
remains **1.0.514-beta**, `7fc833e3a9130ea710a0fbbfbaba6b065eedf8d3`.
Several candidate paths below are absent or changed on that delivered main;
their source implementation must not be described as an already delivered
runtime. The candidate's all-targets workspace/consumer compile check and
Rails20-run/100-assertion check passed; native/runtime acceptance is still
running at this checkpoint. This review executes no SDK or physical test.

The [scoped publication correction](design/native-publication-scoped-validation.md)
and [same-round supervisor admission](design/native203-supervisor-bounded-store-admission.md)
remain subject to that actual CI result. Passing compilation does not close
either the original failures or the following operational gaps.

### Readiness is three different contracts

| Indicator | Actual candidate source contract | Practical limit |
| --- | --- | --- |
| Replica observation | `joined_data/registered.rs::registered_range_status` reads actual native applied position, invalidation and Pending state against the retained bridge floor. | `replica_ready_observed` does not compare with a fresh globally committed head; it is not proof of current catch-up. INITIAL status lacks equivalent numeric replica diagnostics. |
| Recovered leader observation | `distribution_range_consensus.rs::recover_local_leadership` fills recovered-epoch state only after actual prefix/tail recovery. Current writer/fences remain independently required. | A recovered map/status observation is not a fresh lease or future read permission. A follower can bind DATA without inventing local leader authority. |
| Strong request admission | `distribution_native_read/event_dispatch.rs`, `distribution_range_recovery/runtime/strong_point_read.rs`, and `distribution_native_read/{strong,quorum}.rs` acquire fresh routing MQ, distinct read MQ, actual matching range quorum/history, a materialized snapshot at the required floor and a distinct final MQ/currentness proof. | Every Strong request builds its own proof. Published sockets, image bytes, metadata leadership and earlier readiness observations cannot substitute. Status explicitly requires a new request-bound barrier. |

The typed SDK/transport/dispatcher path is a genuine production entry for
explicitly opened native DATA. Only an actual observed Strong result becomes
the tagged present/absent value; redirect and unavailable remain distinct.
`event_wire.rs` rejects a legacy value for an explicit Strong request and
distinguishes present JSON null from absence. These are source-confirmed
guards, **not** proof that physical failover never produces a false absence.
They also do not establish ordinary PostgreSQL/HTTP transparency.

### Operational blockers and their next tests

| Boundary | Classification at candidate1fc | Required next evidence |
| --- | --- | --- |
| Automatic full remote movement into Joined Serving | Source-confirmed integration gap | A normal process must drive genuine target ownership, remote phases, Serving/Open/release and terminal cleanup. `joined_data_handoff.rs` explicitly excludes a production caller; the CLI currently exposes INITIAL DATA open and Registered replica-only open. Fixture adapters are not an automatic operator. |
| Permanent loss of an original replica during remote replacement | Unsupported remote lane | `joint/remote_old_collect.rs::NativeRemoteOldRound::finish` requires responses from **all** original OLD members and explicitly refuses unavailable/minority-tail histories. A surviving majority alone cannot complete this path. A separately sealed unavailable-member protocol must preserve actual committed/uncertain/sparse obligations and old-writer fencing; lowering the existing count is not that protocol. |
| Cold DATA continuation after original owner/process loss | Source-confirmed integration gap | Recover the exact persisted intent/native outcomes under new actual authority and ownership. Metadata-only reopen cannot recreate Serving. Pending Registered-row retention deliberately refuses before generic replay; this is quarantine, not evidence of data loss or bounded recovery. |
| Automatic hot-origin repair | Implemented bounded subset | `initial_data/hot_recovery.rs` retains original INITIAL ownership and the same epoch for a supported next unmaterialized Pending command. Materialized Pending, other lineage, unresolved earlier replica debt and new-session cases refuse; this is not general new-leader/cold repair. |
| Ready/lag observability | Incomplete observation contract | Export actual applied and freshly observed committed positions separately, with observation identity/age. Do not turn a historical floor or cached status into Strong permission. |

The donor's actual `collect_remote_old_for_source` loops over every original
OLD member and aborts on a failed request. Dead-replica planning exists, but no
sealed dead-member/minority-tail substitute feeds this certificate. The local
old-owner round's different majority API cannot be reconstructed from remote
reply DATA. Generic decommission also refuses remaining replicas/active moves;
it does not prove this native replacement lane works after permanent loss.

New rebalance plans already receive a stable `handoff_session` from
`distribution.rs::apply_rebalance_plan`. The definition-only legacy session
initializer is not evidence that every new plan lacks a session. The missing
normal FIRST chain concerns actual coordinator/recipient-owned producers and
continuations. Its current plan factory requires the genuine range-leader
requester; the donor can be a different member. An authenticated donor or
manifest cannot impersonate that coordinator. Existing receive preparation
also requires mutable actual host ownership, not a host reconstructed by a
wire listener from request DATA.

The smallest read-consistency controls are an actual authority change after
snapshot/before final MQ, and an actual newly selected leader held below the
required materialization position. Require the latest acknowledged value or
bounded typed unavailable/redirect, never successful false absence. Full
movement acceptance additionally needs the genuine remote Joined producer,
stale-route refresh, retained ACK/attempt reconciliation, crash/cold recovery
and observed convergence. None was executed by this source review.

The independently pinned [27-file readiness review](design/native-readiness-1fc-source-audit.json)
has SHA256
`479f9424f2f8724090e770704f8ca8342fe3bb90d342e99c236a4586992dad58`.
It establishes source/callgraph findings only. Physical RPO/RTO, sustained
capacity gain and production admission remain unqualified.
