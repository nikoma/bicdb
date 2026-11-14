# HA and horizontal scaling: evidence checkpoint, November 14, 2026

**Production qualification remains incomplete.** The original Azure scale-out
experiment remains failed. A new single-primary streaming election defect is
recorded in [issue190](https://github.com/nikoma/bicdb/issues/190); snapshot ballot
rollback is recorded separately in [issue191](https://github.com/nikoma/bicdb/issues/191).
Post-vote old-primary acknowledgments are recorded in
[issue192](https://github.com/nikoma/bicdb/issues/192).
The separate snapshot target-data-head gate is tracked in
[issue 193](https://github.com/nikoma/bicdb/issues/193).

The delivered-main inspection checkpoint is **1.0.511-beta**,
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

The historical failed range-recovery branch used source version **1.0.512-beta**;
its original candidate is
`b7791177fc94fa8d1ea2072f6b4f62bb659000b6`, tree
`e05d4b742ec49af5c06b75a65eaf4df1685df4df`. Its
[required run](https://github.com/nikoma/bicdb/actions/runs/43601117766) completed
with **failure** on November14 at15:01:34UTC. Native multi-node tests recorded
**324 passed,13 failed and one ignored**. All-target/consumer compilation,
Core(1817 passed), SQL(2681 passed), PGwire(511 passed), the remaining-workspace
job and [Rails20 runs/100 assertions](https://github.com/nikoma/bicdb/actions/runs/43601117775)
succeeded. These are component/job results; the candidate remains unmerged and
failed overall. The CI checkout
`50a1359db62f58d7d88f2889fbb06d6fb53cc5dd` has the exact candidate tree.
This is an immutable source/result checkpoint, not a live CI dashboard.

Twelve native failures occur later at local activation with `QueryTimedOut`,
after fresh preparation, resumed publication, member acquisition and phase
completion. The thirteenth is a foreign-service negative control whose expected
error-text assertion failed; the actual error was not retained. That control
returned an error and passed its preceding original-ownership/liveness checks.
All three previously failing remote-decision cases passed, but this does not
establish the cause of their earlier failures. Failure-stage clocks and the
negative control's exact error are the next diagnostic evidence required;
neither a deadline increase nor a relaxed assertion is justified. The raw
native log is12,991,102 bytes, SHA-256
`70a2f2a1d255bf00bd815bc3761843f0321639ebea12b419f525931c52821860`.
No physical fault-safe scale-out result is inferred from these tests.

The diagnostic-only successor components `eacd70fc0` and `fc680d226` retain
fixed-capacity failure-stage clocks around the same activation operations and
log the candidate-count outcome before fallible fixture evidence export. They
preserve all original assertions and deadlines. Diagnostics report omitted or
truncated fields explicitly, perform no fresh authority checks after failure,
and retain no native owner or request token. Standalone formatting and diff
checks passed; Rust compilation and execution for this successor have not run
at this source checkpoint. This instrumentation is not a recovery fix.


The subsequent diagnostic range head is
`503964ae93aa22f4c76b698a549b6ef857c834be`, tree
`92ad46fee99fd50dc77022f6195b3b13086448aa`; its actual required-CI checkout
`ddcd6a1fcca63a4594cea7abb51cccc642c09959` has the same tree. Its
[required run](https://github.com/nikoma/bicdb/actions/runs/43608687107)
passed compilation, Core 1,817/0/16, PGwire 511/0/2 and SQL 2,681/0/4, where counts
mean passed/failed/ignored. Its
[Rails run](https://github.com/nikoma/bicdb/actions/runs/43608687112) passed
20 runs/100 assertions. Serial native **failed: 324 passed, 13 failed, one
ignored** in 3,755.32 s, with the same thirteen failing cases as b779. The
remaining-workspace job passed; the complete required run is **failed** solely
on the native job. These results neither repair nor reclassify the failed b779
native run. Its source/head remains preserved with its evidence.

The retained diagnostics narrow issue195 without establishing an underlying
CPU or I/O cause. All twelve activation errors end at
`pin_release_before_full_sweep`, before pin release or post-quorum metadata RPC.
At activation entry their phase budgets had 249,464–614,948 microseconds left;
the calls returned 319,357–704,409 microseconds later, after expiry. Original
caller/thread deadlines were absent and the recorded cancellation-request bits
were false. The exact member or inner validator consuming that time was not
instrumented. Later intended crash/recovery cuts were **not tested** because
activation failed first. The raw native log is 12,967,020 bytes, SHA256
`2a2c06352869a17cecb227cad273270a173c4fa0c65721c8e33889dfa303b82c`.

The foreign-service control now retains the exact refusal:
`cluster error: native range image: native writer store/root/open changed`.
It refused counting and retained original ownership/liveness; its expected-text
assertion failed. The marker does not identify which store/root/open disjunct
failed or independently prove a final serving/WAL state. Narrow correction
`5b3fdab4a039abdd1cb28611607843dc676f68cc` matches the exact typed Cluster
error, preserves all other assertions and is **uncompiled and unexecuted**.
The native diagnostic supplement SHA256 is
`84e77f9aa17826efbaabfe606d24875d2e749b98f826cd1e7a31f4a67ed721c9`.
The complete native candidate capture retains eleven full logs and eight
original artifact ZIPs, with zero unavailable files, partials or hash mismatches.
Its canonical manifest SHA256 is
`1ef5836a8c5431c453911cc81923f12f1dad0a13756d763bef25017190120777`;
complete-capture verification SHA256 is
`80658df9c5a1ce9ff67ad1f162b5faca5ad1713fc313b070b74e7c10e4fcfdb8`.

## Separate streaming correction and actual regression feedback

Single-primary source `2c0193c37852546c51b99e2918c24ea17bb44e2b`, tree
`b74d35a243132989645d9c716add13a5df9f2566`, is an unmerged feature branch
inheriting source version511, with no assigned release version. Its actual
checkout `dba3a015df88ecc81ca5515258a2950680662aa2` has the same tree.
[Required run43609026644](https://github.com/nikoma/bicdb/actions/runs/43609026644)
**failed Core, PGwire and remaining-workspace jobs**. All-target/non-default-consumer compilation,
repository checks, native 216/0/1 and SQL 2,681/0/4 passed.
[Rails](https://github.com/nikoma/bicdb/actions/runs/43609026629) passed 20
runs/100 assertions. All jobs are terminal and their complete logs are retained. The
native component result is25 minutes of CI execution, not a measured RTO.

The twelve new file-backed epoch controls passed 12/0 in 1.59 s on that source.
They exercise actual commits, imports, qualification, ballots, fencing and
snapshot custody. The complete candidate remains failed:

- [Issue197](https://github.com/nikoma/bicdb/issues/197): existing
  `embedded_quorum_survives_primary_loss_and_fences_a_returning_writer` failed
  its 25-second wait at `embedded_ha.rs:393`. The successor election and UPDATE 8
  succeeded. The returning original primary's stale Ping was rejected before
  informational Pong, blocking the existing discovery/rejoin path. The later
  UPDATE 9 and stale-writer assertions were not reached. The target recorded
  11 passed/one failed; raw 789,941 bytes, SHA256
  `e9718a8d4d69a8e1f1e45e693d4fa32ee5ecb238f5357559bb960d59af302645`.
- The existing CLI `killing_the_primary_loses_no_acknowledged_write` also
  failed its **old-primary rejoin** wait at `ha_failover_cli.rs:389`.
  Successor takeover and the preceding test-process in-memory ACK-set survival assertion
  passed; the recorded takeover observation was 6.2 s. Final old-writer fencing
  and complete standby checks were not reached. Logs retain stale-term refusals,
  consistent with the issue197 discovery boundary; no additional independent
  cause or current-release RTO guarantee is established. The CLI target recorded
  three passed/one failed. The complete remaining-workspace log is 70,457,194
  bytes, SHA256
  `30c32b33238c9c885b6fa9fc5b5b2fc93324cd2bbc93988e7214730fe7f9739c`.
- [Issue198](https://github.com/nikoma/bicdb/issues/198): existing
  `external_attachments_require_a_snapshot_and_cannot_be_acknowledged_as_missing_blobs`
  failed at `live_replication.rs:1311`. A genuinely fresh standalone source
  was populated and archived, then reopened with HA enabled. The new guard
  refused its unqualified snapshot. Earlier attachment-write/no-sequence-advance
  and missing-blob refusal assertions passed; snapshot restore, full attachment
  content and manifest comparison were not reached. The target recorded
  31 passed/one failed; raw 1,720,542 bytes, SHA256
  `a1b937ed99eca9f209636341ff52ee898093e3588a1578cb5f344c3b887351e0`.

Reviewed corrective source `f41a3986e17cfff9096cda8682fc460b884ae7c1`, tree
`e11517ce5209ed5e05d8d76e9bc4e0e666b15ae2`, is a separate **unpushed,
uncompiled and unexecuted** successor at this checkpoint. It:

- Retains informational Pong for an allowed stale peer while denying stale
  contact renewal. Same-runtime development-TCP controls use
  n1 after actual Core ballots and promotion for the positive request. A later
  actual n2 ballot tests refusal to renew contact when the sender matches the
  known primary epoch but is below the new durable ballot. Informational epoch
  knowledge grants no writer lease.
- Retains genesis in memory only from actual empty-root birth under the original
  DirectoryLock, independently of initial HA enablement. Opening with HA enabled
  persists it immediately; another first HA-state producer can persist the same
  origin through its reader/durability barrier. Populated unknown roots remain
  refused. Closing without publication loses that in-memory origin.
- Before copying a qualified snapshot, persists that same qualified HA state
  and sticky reader barrier after checking the actual durable applied prefix.
  A publication failure returns before creating an apparently usable archive.

Original attachment and embedded acceptance files and the original twelve Core
controls remain byte-identical. Three additional public-producer Core controls
are source definitions, not executed results. Independent review found no
confirmed source blocker; formatting, whitespace and catalog source-path checks
passed. The catalog retains conditional ACK preservation and the actual
protocol 5 condition; static quorum settings alone do not establish preservation.
See [the source contract](design/ha-qualified-data-epoch.md),
[returning-primary correction](design/ha-returning-primary-stale-ping-197.md) and
[fresh standalone origin](design/ha-fresh-standalone-origin-198.md).

No mixed-version upgrade, all-legacy bootstrap-source procedure, data-head/ACK-copy
protection for issue 193 or complete physical HA qualification is supplied by
these corrections. Versions, binaries, release publication and deployment remain
separate states. A next unused version and final versioned-head validation are
required before merging. After both failed captures were frozen, PR188 was
closed as historical evidence, preserving its branch and exact source heads.
Its active version reservation was retired. The next unused source version,
**1.0.512-beta**, is assigned to the separate corrected streaming batch and
this documentation; final versioned-head required CI remains pending at this
checkpoint. Historical failed branches sharing a version label must always be
identified by full source SHA. No binary, release or deployment is produced by
that source-version assignment.

The terminal streaming capture retains eleven complete executed-job logs and
seven original artifact ZIPs, with raw hashes, gzip/ZIP CRC verification and
25 artifact-member hashes. Zero unavailable logs or partial captures remain.
Its canonical manifest SHA256 is
`45314c66cab5fd23a2b28f098f1bea51742f2b753035c3a74cc49ba120febf7f`.
The CLI failure-evidence upload reported no matching files; no absent ledger or
HA-state artifact is invented. This archive documents a failed candidate.

## Findings classified at this checkpoint

| Finding | Classification | Scope |
| --- | --- | --- |
| Original Azure four-to-five convergence within 300 s | Failed | Historical experiment remains failed; no stable converged five-node comparison. |
| Original two temporarily unreadable ACK records' causes | Inconclusive | Later matches do not establish routing/read-watermark/catch-up cause or the causal visibility-restoration event. |
| b779 native handoff acceptance | Failed | 324 passed/13 failed/one ignored; raw log and complete capture retained. |
| [Native activation issue195](https://github.com/nikoma/bicdb/issues/195) | Failed | All twelve 503 timeouts occur in the pre-pin full validation sweep; exact inner cause remains inconclusive. Later intended cuts were not tested. |
| [Foreign-service control196](https://github.com/nikoma/bicdb/issues/196) | Failed | 503 retains exact store/root/open refusal; counting refused and expected-error assertion failed. Narrow matcher correction is not tested. |
| 2c's new epoch/ballot component controls | Passed | Twelve file-backed Core cases at exact2c source; no physical/production qualification. |
| 2c returning-primary and attachment-recovery acceptance | Failed | Core, PGwire and CLI failures at issues197/198 boundaries; CLI ACK-set survival passed before rejoin failed. Later assertions were not reached. |
| f41 corrective runtime behavior and three new Core controls | Not tested | Source review and static checks are distinct from execution. |
| Snapshot target-data-head/ACK-copy protection193 | Not tested | Open source/API gate; the separate genuine regression source 6bb1f64e is unexecuted. |
| Current-artifact physical RPO/RTO and latency percentiles | Not tested | No independent-machine current-artifact qualification dataset. |
| Sustained throughput gain after complete convergence | Not tested | Neither relocation-phase rates nor shared-host debug measurements demonstrate this gate. |

## Evidence levels by deployment mode

The following matrix describes the delivered511 checkpoint and identifies the
separate range candidate explicitly. Streaming2c/f41 results above do not
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
| 2: native distributed data/recovery path | Genuine physical owners, OLD/image/NEW/decision components and local continuation; automatic operator closure incomplete | Diagnostic503 compiles but fails native324/13/one ignored, repeating b779; previous4ef failed307/23/one ignored | No new pinned four-to-five native campaign | No | Not established |
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

## Streaming election: configuration fix and a separate open defect

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

Ordinary PostgreSQL applications still lack a demonstrated general transparent
cross-range SQL executor, joins, global snapshot and recoverable atomic
multi-range transaction coordinator. The stable namespace/record-key hash is not
a tenant-locality declaration. Locality must remain an optional optimization;
it cannot substitute for correct cross-range transactions. Stock Rails/local
SQL passes do not qualify distributed SQL.
The [locking consistency alternative](design/distributed-sql-locking-consistency-alternative.md)
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
